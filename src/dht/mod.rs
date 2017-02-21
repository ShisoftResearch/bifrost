use std;
use std::collections::HashMap;
use std::sync::Arc;
use std::cmp::max;
use std::thread;
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::{RwLock, RwLockWriteGuard};
use bincode::{SizeLimit, serde as bincode};
use serde;

use bifrost_hasher::{hash_str, hash_bytes};
use membership::client::{Client as MembershipClient, Member};
use dht::weights::DEFAULT_SERVICE_ID;
use dht::weights::client::{SMClient as WeightSMClient};
use raft::client::{RaftClient, SubscriptionError};

pub mod weights;

pub static DEFAULT_NODE_LIST_SIZE: f64 = 2048f64;

#[derive(Debug)]
pub enum Action {
    Joined,
    Left,
}

#[derive(Debug)]
pub enum InitTableError {
    GroupNotExisted,
    NoWeightService,
    NoWeightGroup,
    NoWeightInfo,
    Unknown,
}

#[derive(Debug)]
pub enum DHTError {
    WatchError,
    InitTableError(InitTableError),
}

#[derive(Clone)]
struct Node {
    start: u64,
    server: u64,
}

struct LookupTables {
    nodes: Vec<Node>,
    addrs: HashMap<u64, String>
}

pub struct DHT {
    tables: RwLock<LookupTables>,
    membership: Arc<MembershipClient>,
    weight_sm_client: WeightSMClient,
    group_name: String,
    watchers: RwLock<Vec<Box<Fn(&Member, &Action, &LookupTables, &Vec<Node>) + Send + Sync>>>,
    version: AtomicU64
}

impl DHT {
    pub fn new(group: &String, raft_client: &Arc<RaftClient>) -> Result<Arc<DHT>, DHTError>  {
        let membership = Arc::new(MembershipClient::new(raft_client));
        let dht = Arc::new(DHT {
            tables: RwLock::new(LookupTables {
                nodes: Vec::new(),
                addrs: HashMap::new()
            }),
            membership: membership.clone(),
            weight_sm_client: WeightSMClient::new(DEFAULT_SERVICE_ID, &raft_client),
            group_name: group.clone(),
            watchers: RwLock::new(Vec::new()),
            version: AtomicU64::new(0)
        });
        {
            let dht = dht.clone();
            let res = membership.on_group_member_joined(move |r| {
                if let Ok((member, version)) = r { server_joined(&dht, member, version); }
            }, group);
            if let Ok(Ok(_)) = res {} else {return Err(DHTError::WatchError);}
        }
        {
            let dht = dht.clone();
            let res = membership.on_group_member_online(move |r| {
                if let Ok((member, version)) = r { server_joined(&dht, member, version); }
            }, group);
            if let Ok(Ok(_)) = res {} else {return Err(DHTError::WatchError);}
        }
        {
            let dht = dht.clone();
            let res = membership.on_group_member_left(move |r| {
                if let Ok((member, version)) = r { server_left(&dht, member, version); }
            }, group);
            if let Ok(Ok(_)) = res {} else {return Err(DHTError::WatchError);}
        }
        {
            let dht = dht.clone();
            let res = membership.on_group_member_offline(move |r| {
                if let Ok((member, version)) = r { server_left(&dht, member, version); }
            }, group);
            if let Ok(Ok(_)) = res {} else {return Err(DHTError::WatchError);}
        }
        Ok(dht)
    }
    pub fn new_client(group: &String, raft_client: &Arc<RaftClient>) -> Result<Arc<DHT>, DHTError> {
        let dht = DHT::new(group, raft_client);
        match dht {
            Err(e) => Err(e),
            Ok(dht) => {
                match dht.init_table() {
                    Err(e) => Err(DHTError::InitTableError(e)),
                    Ok(_) =>  Ok(dht.clone())
                }
            },
        }
    }
    pub fn init_table(&self) -> Result<(), InitTableError> {
        let mut table = &mut self.tables.write();
        self.init_table_(&mut table)
    }
    pub fn get_server(&self, hash: u64) -> Option<String> {
        let lookup_table = self.tables.read();
        let nodes = &lookup_table.nodes;
        let len = nodes.len();
        if len == 0 {return None;}
        let first_node = nodes.first().unwrap();
        let last_node = nodes.last().unwrap();
        if hash < first_node.start {return Some(lookup_table.addrs.get(&first_node.server).unwrap().clone())}
        if hash >= last_node.start {return Some(lookup_table.addrs.get(&last_node.server).unwrap().clone())}
        let mut low = 0;
        let mut high = len - 1;
        while low <= high {
            let mid = low + (high - low) / 2;
            let curr_node = &nodes[mid];
            let next_node = &nodes[mid + 1];
            if hash >= curr_node.start && hash < next_node.start {
                return Some(lookup_table.addrs.get(&curr_node.server).unwrap().clone());
            } else if hash < curr_node.start {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        None
    }
    pub fn get_server_by_string(&self, string: &String) -> Option<String> {
        self.get_server(hash_str(string))
    }
    pub fn get_server_by<T>(&self, obj: &T) -> Option<String> where T: serde::Serialize {
        self.get_server(hash_bytes(serialize!(obj).as_slice()))
    }
    pub fn nodes_count(&self) -> usize {
        let lookup_table = self.tables.read();
        return lookup_table.nodes.len();
    }
    pub fn set_weight(&self, server_name: &String, weight: u64) -> bool {
        let group_id = hash_str(&self.group_name);
        let server_id = hash_str(server_name);
        if let Ok(Ok(_)) = self.weight_sm_client.set_weight(group_id, server_id, weight) { true } else { false }
    }
    pub fn watch_all_actions<F>(&self, f: F)
        where F: Fn(&Member, &Action, &LookupTables, &Vec<Node>) + 'static + Send + Sync {
        let mut watchers = self.watchers.write();
        watchers.push(Box::new(f));
    }
    pub fn watch_server_nodes_range_changed<F>(&self, server: &String, f: F)
        // return ranges [...,...)
        where F: Fn(Vec<(u64, u64)>) + 'static + Send + Sync {
        let server_id = hash_str(server);
        let wrapper = move |
            _: &Member,_: &Action,
            lookup_table: &LookupTables, _: &Vec<Node>| {
            let nodes = &lookup_table.nodes;
            let node_len = nodes.len();
            let mut ranges: Vec<(u64, u64)> = Vec::new();
            for ni in 0..node_len {
                let node = &nodes[ni];
                if node.server == server_id {
                    ranges.push((node.start, if ni == node_len - 1 {
                        std::u64::MAX
                    } else {
                        nodes[ni + 1].start
                    }));
                }
            }
            if !ranges.is_empty() {f(ranges);}
        };
        self.watch_all_actions(wrapper);
    }

    fn init_table_(&self, lookup_table: &mut RwLockWriteGuard<LookupTables>) -> Result<(), InitTableError> {
        if let Ok(Ok((members, version))) = self.membership.group_members(&self.group_name, true) {
            let group_id = hash_str(&self.group_name);
            match self.weight_sm_client.get_weights(group_id) {
                Ok(Ok(Some(weights))) =>  {
                    if let Some(min_weight) = weights.values().min() {
                        lookup_table.nodes.clear(); // refresh nodes
                        let mut factors: HashMap<u64, f64> = HashMap::new();
                        let min_weight = *min_weight as f64;
                        for member in members.iter() {
                            let k = member.id;
                            let w = match weights.get(&k) {
                                Some(w) => *w as f64,
                                None => min_weight,
                            };
                            factors.insert(k, w / min_weight);
                        }
                        let factor_sum: f64 = factors.values().sum();
                        let nodes_size = if factor_sum > DEFAULT_NODE_LIST_SIZE {factor_sum} else {DEFAULT_NODE_LIST_SIZE};
                        let factor_scale = nodes_size / factor_sum;
                        for member in members.iter() {lookup_table.addrs.insert(member.id, member.address.clone());}
                        for (k, f) in factors.iter() {
                            let weight = (*f * factor_scale) as u64;
                            for i in 0..weight {
                                let node_key = format!("{}_{}", k, i);
                                let hash = hash_str(&node_key);
                                lookup_table.nodes.push(Node{
                                    start: hash, server: *k
                                });
                            }
                        };
                        lookup_table.nodes.sort_by(|n1, n2| n1.start.cmp(&n2.start));
                        self.version.store(version, Ordering::Relaxed);
                        Ok(())
                    } else {
                        Err(InitTableError::NoWeightInfo)
                    }
                },
                Err(_) => Err(InitTableError::NoWeightService),
                Ok(Ok(None)) => Err(InitTableError::NoWeightGroup),
                _ => Err(InitTableError::Unknown)

            }
        } else {
            Err(InitTableError::GroupNotExisted)
        }
    }
}

fn server_joined(dht: &Arc<DHT>, member: Member, version: u64) {
    server_changed(dht, member, Action::Joined, version);
}
fn server_left(dht: &Arc<DHT>, member: Member, version: u64) {
    server_changed(dht, member, Action::Left, version);
}
fn server_changed(dht: &Arc<DHT>, member: Member, action: Action, version: u64) {
    let dht_version = dht.version.load(Ordering::Relaxed);
    if dht_version < version {
        let dht = dht.clone();
        thread::spawn(move || {
            let mut lookup_table = dht.tables.write();
            let watchers = dht.watchers.read();
            let dht_version = dht.version.load(Ordering::Relaxed);
            if dht_version >= version {return;}
            let old_nodes = lookup_table.nodes.clone();
            dht.init_table_(&mut lookup_table);
            for watch in watchers.iter() {
                watch(&member, &action, &*lookup_table, &old_nodes);
            }
        });
    }
}