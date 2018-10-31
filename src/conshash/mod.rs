use std;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::{RwLock, RwLockWriteGuard};
use serde;

use bifrost_hasher::{hash_str, hash_bytes};
use membership::client::{ObserverClient as MembershipClient, Member};
use conshash::weights::DEFAULT_SERVICE_ID;
use conshash::weights::client::{SMClient as WeightSMClient};
use raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use raft::state_machine::master::ExecError;
use utils::bincode::{serialize};
use rand;

use futures::prelude::*;
use std::collections::BTreeSet;
use std::collections::BTreeMap;

pub mod weights;

#[derive(Debug)]
pub enum Action {
    Joined,
    Left,
}

#[derive(Debug)]
pub enum InitTableError {
    GroupNotExisted,
    NoWeightService(ExecError),
    NoWeightGroup,
    NoWeightInfo,
    Unknown,
}

#[derive(Debug)]
pub enum CHError {
    WatchError(Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>),
    InitTableError(InitTableError),
}

struct LookupTables {
    nodes: Vec<u64>,
    addrs: HashMap<u64, String>
}

pub struct ConsistentHashing {
    tables: RwLock<LookupTables>,
    membership: Arc<MembershipClient>,
    weight_sm_client: WeightSMClient,
    group_name: String,
    watchers: RwLock<Vec<Box<Fn(&Member, &Action, &LookupTables, &Vec<u64>) + Send + Sync>>>,
    version: AtomicU64
}

impl ConsistentHashing {
    pub fn new_with_id(id: u64, group: &str, raft_client: &Arc<RaftClient>) -> Result<Arc<ConsistentHashing>, CHError> {
        let membership = Arc::new(MembershipClient::new(raft_client));
        let ch = Arc::new(ConsistentHashing {
            tables: RwLock::new(LookupTables {
                nodes: Vec::new(),
                addrs: HashMap::new()
            }),
            membership: membership.clone(),
            weight_sm_client: WeightSMClient::new(id, &raft_client),
            group_name: group.to_string(),
            watchers: RwLock::new(Vec::new()),
            version: AtomicU64::new(0)
        });
        {
            let ch = ch.clone();
            let res = membership.on_group_member_joined(move |r| {
                if let Ok((member, version)) = r { server_joined(&ch, member, version); }
            }, group).wait();
            if let Ok(Ok(_)) = res {} else {return Err(CHError::WatchError(res));}
        }
        {
            let ch = ch.clone();
            let res = membership.on_group_member_online(move |r| {
                if let Ok((member, version)) = r { server_joined(&ch, member, version); }
            }, group).wait();
            if let Ok(Ok(_)) = res {} else {return Err(CHError::WatchError(res));}
        }
        {
            let ch = ch.clone();
            let res = membership.on_group_member_left(move |r| {
                if let Ok((member, version)) = r { server_left(&ch, member, version); }
            }, group).wait();
            if let Ok(Ok(_)) = res {} else {return Err(CHError::WatchError(res));}
        }
        {
            let ch = ch.clone();
            let res = membership.on_group_member_offline(move |r| {
                if let Ok((member, version)) = r { server_left(&ch, member, version); }
            }, group).wait();
            if let Ok(Ok(_)) = res {} else {return Err(CHError::WatchError(res));}
        }
        Ok(ch)
    }
    pub fn new(group: &str, raft_client: &Arc<RaftClient>) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_with_id(DEFAULT_SERVICE_ID, group, raft_client)
    }
    pub fn new_client(group: &str, raft_client: &Arc<RaftClient>) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_client_with_id(DEFAULT_SERVICE_ID, group, raft_client)
    }
    pub fn new_client_with_id(id: u64, group: &str, raft_client: &Arc<RaftClient>) -> Result<Arc<ConsistentHashing>, CHError> {
        match ConsistentHashing::new_with_id(id, group, raft_client) {
            Err(e) => Err(e),
            Ok(ch) => {
                match ch.init_table() {
                    Err(e) => Err(CHError::InitTableError(e)),
                    Ok(_) =>  Ok(ch.clone())
                }
            },
        }
    }
    pub fn init_table(&self) -> Result<(), InitTableError> {
        let mut table = &mut self.tables.write();
        self.init_table_(&mut table)
    }
    pub fn to_server_name(&self, server_id: u64) -> String {
        let lookup_table = self.tables.read();
        lookup_table.addrs.get(&server_id).unwrap().clone()
    }
    pub fn to_server_name_option(&self, server_id: Option<u64>) -> Option<String> {
        server_id.map(|id| {
            let lookup_table = self.tables.read();
            lookup_table.addrs.get(&id).unwrap().clone()
        })
    }
    pub fn get_server_id(&self, hash: u64) -> Option<u64> {
        let lookup_table = self.tables.read();
        let nodes = &lookup_table.nodes;
        let slot_count = nodes.len();
        if slot_count == 0 {return None;}
        let result = nodes.get(self.jump_hash(slot_count, hash));
        debug!("Hash {} have been point to {:?}", hash, result);
        result.cloned()
    }
    pub fn jump_hash(&self, slot_count: usize, hash: u64) -> usize {
        let mut b: i64 = -1;
        let mut j: i64 = 0;
        let mut h = hash;
        while j < (slot_count as i64) {
            b = j;
            h = h.wrapping_mul(2862933555777941757).wrapping_add(1);
            j = (((b.wrapping_add(1)) as f64) * ((1i64 << 31) as f64) /
                (((h >> 33).wrapping_add(1)) as f64)) as i64;
        }
        debug!("Jump hash point to index {} for {}, with slots {}", b, hash, slot_count);
        b as usize
    }
    pub fn get_server(&self, hash: u64) -> Option<String> {
        self.to_server_name_option(self.get_server_id(hash))
    }
    pub fn get_server_by_string(&self, string: &String) -> Option<String> {
        self.get_server(hash_str(string))
    }
    pub fn get_server_by<T>(&self, obj: &T) -> Option<String> where T: serde::Serialize {
        self.get_server(hash_bytes(serialize(obj).as_slice()))
    }
    pub fn get_server_id_by_string(&self, string: &String) -> Option<u64> {
        self.get_server_id(hash_str(string))
    }
    pub fn get_server_id_by<T>(&self, obj: &T) -> Option<u64> where T: serde::Serialize {
        self.get_server_id(hash_bytes(serialize(obj).as_slice()))
    }
    pub fn rand_server(&self) -> Option<String> {
        let rand = rand::random::<u64>();
        self.get_server(rand)
    }
    pub fn nodes_count(&self) -> usize {
        let lookup_table = self.tables.read();
        return lookup_table.nodes.len();
    }
    pub fn set_weight(&self, server_name: &String, weight: u64) -> impl Future<Item = bool, Error = ExecError> {
        let group_id = hash_str(&self.group_name);
        let server_id = hash_str(server_name);
        self.weight_sm_client.set_weight(&group_id, &server_id, &weight)
            .map(|res| if let Ok(_) = res { true } else { false })
    }
    pub fn watch_all_actions<F>(&self, f: F)
        where F: Fn(&Member, &Action, &LookupTables, &Vec<u64>) + 'static + Send + Sync {
        let mut watchers = self.watchers.write();
        watchers.push(Box::new(f));
    }
    pub fn watch_server_nodes_range_changed<F>(&self, server: &String, f: F)
        // return ranges [...,...)
        where F: Fn((usize, u32)) + 'static + Send + Sync {
        let server_id = hash_str(server);
        let wrapper = move |
            _: &Member,_: &Action,
            lookup_table: &LookupTables, _: &Vec<u64>| {
            let nodes = &lookup_table.nodes;
            let node_len = nodes.len();
            let mut weight = 0;
            let mut start = None;
            for ni in 0..node_len {
                let node = nodes[ni];
                if node == server_id {
                    weight += 1;
                    if start.is_none() { start = Some(ni) }
                }
            }
            if start.is_some() {
                f((start.unwrap(), weight));
            } else {
                warn!("No node exists for watch");
            }
        };
        self.watch_all_actions(wrapper);
    }

    fn init_table_(&self, lookup_table: &mut RwLockWriteGuard<LookupTables>) -> Result<(), InitTableError> {
        if let Ok(Ok((mut members, version))) = self.membership.group_members(&self.group_name, true).wait() {
            let group_id = hash_str(&self.group_name);
            match self.weight_sm_client.get_weights(&group_id).wait() {
                Ok(Ok(Some(weights))) =>  {
                    if let Some(min_weight) = weights.values().min() {
                        let mut factors: BTreeMap<u64, u32> = BTreeMap::new();
                        let min_weight = *min_weight as f64;
                        for member in members.iter() {
                            let k = member.id;
                            let w = match weights.get(&k) {
                                Some(w) => *w as f64,
                                None => min_weight,
                            };
                            factors.insert(k, (w / min_weight) as u32);
                        }
                        let factor_sum: u32 = factors.values().sum();
                        lookup_table.nodes = Vec::with_capacity(factor_sum as usize);
                        for member in members.iter() {
                            lookup_table.addrs.insert(member.id, member.address.clone());
                        }
                        for (server_id, weight) in factors.into_iter() {
                            for i in 0..weight {
                                lookup_table.nodes.push(server_id);
                            }
                        };
                        self.version.store(version, Ordering::Relaxed);
                        Ok(())
                    } else {
                        Err(InitTableError::NoWeightInfo)
                    }
                },
                Err(e) => Err(InitTableError::NoWeightService(e)),
                Ok(Ok(None)) => Err(InitTableError::NoWeightGroup),
                _ => Err(InitTableError::Unknown)

            }
        } else {
            Err(InitTableError::GroupNotExisted)
        }
    }
}

fn server_joined(ch: &Arc<ConsistentHashing>, member: Member, version: u64) {
    server_changed(ch, member, Action::Joined, version);
}
fn server_left(ch: &Arc<ConsistentHashing>, member: Member, version: u64) {
    server_changed(ch, member, Action::Left, version);
}
fn server_changed(ch: &Arc<ConsistentHashing>, member: Member, action: Action, version: u64) {
    let ch_version = ch.version.load(Ordering::Relaxed);
    if ch_version < version {
        let ch = ch.clone();
        thread::spawn(move || {
            let mut lookup_table = ch.tables.write();
            let watchers = ch.watchers.read();
            let ch_version = ch.version.load(Ordering::Relaxed);
            if ch_version >= version {return;}
            let old_nodes = lookup_table.nodes.clone();
            ch.init_table_(&mut lookup_table);
            for watch in watchers.iter() {
                watch(&member, &action, &*lookup_table, &old_nodes);
            }
        });
    }
}