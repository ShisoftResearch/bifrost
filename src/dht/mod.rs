use std;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use bincode::{SizeLimit, serde as bincode};
use serde;

use bifrost_hasher::{hash_str, hash_bytes};
use membership::client::{Client as MembershipClient, Member};
use dht::weights::DEFAULT_SERVICE_ID;
use dht::weights::client::{SMClient as WeightSMClient};
use raft::client::{RaftClient, SubscriptionError};
use std::cmp::max;

pub mod weights;

pub static DEFAULT_NODE_LIST_SIZE: f64 = 2048f64;

pub enum Action {
    Joined,
    Left,
}

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
    watchers: RwLock<Vec<Box<Fn(&Member, &Action, &LookupTables, &Vec<Node>) + Send + Sync>>>
}

impl DHT {
    pub fn new(group: &String, raft_client: &Arc<RaftClient>) -> Option<Arc<DHT>>  {
        let membership = Arc::new(MembershipClient::new(raft_client));
        let dht = Arc::new(DHT {
            tables: RwLock::new(LookupTables {
                nodes: Vec::new(),
                addrs: HashMap::new()
            }),
            membership: membership.clone(),
            weight_sm_client: WeightSMClient::new(DEFAULT_SERVICE_ID, &raft_client),
            group_name: group.clone(),
            watchers: RwLock::new(Vec::new())
        });
        {
            let dht = dht.clone();
            let res = membership.on_group_member_joined(move |r| {
                if let Ok(member) = r { dht.server_joined(member); }
            }, group);
            if let Ok(Ok(_)) = res {} else {return None;}
        }
        {
            let dht = dht.clone();
            let res = membership.on_group_member_online(move |r| {
                if let Ok(member) = r { dht.server_joined(member) }
            }, group);
            if let Ok(Ok(_)) = res {} else {return None;}
        }
        {
            let dht = dht.clone();
            let res = membership.on_group_member_left(move |r| {
                if let Ok(member) = r { dht.server_left(member) }
            }, group);
            if let Ok(Ok(_)) = res {} else {return None;}
        }
        {
            let dht = dht.clone();
            let res = membership.on_group_member_offline(move |r| {
                if let Ok(member) = r { dht.server_left(member) }
            }, group);
            if let Ok(Ok(_)) = res {} else {return None;}
        }
        if !dht.init_table() {return None;}
        return Some(dht);
    }
    pub fn get_server(&self, hash: u64) -> Option<String> {
        let lookup_table = self.tables.read();
        let nodes = &lookup_table.nodes;
        let len = nodes.len();
        if len == 0 {return None;}
        let first_node = nodes.first().unwrap();
        let last_ndoe = nodes.last().unwrap();
        if hash < first_node.start {return Some(lookup_table.addrs.get(&first_node.server).unwrap().clone())}
        if hash >= last_ndoe.start {return Some(lookup_table.addrs.get(&last_ndoe.server).unwrap().clone())}
        let mut low = 0;
        let mut high = len - 1;
        while low < high {
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
            f(ranges)
        };
    }

    fn init_table(&self) -> bool {
        if let Ok(Ok(members)) = self.membership.group_members(&self.group_name, true) {
            let group_id = hash_str(&self.group_name);
            if let Ok(Ok(Some(weights))) = self.weight_sm_client.get_weights(group_id) {
                if let Some(min_weight) = weights.values().min() {
                    let mut factors: HashMap<u64, f64> = HashMap::new();
                    let min_weight = *min_weight as f64;
                    for (k, w) in weights.iter() {factors.insert(*k, *w as f64 / min_weight);}
                    let factor_sum: f64 = factors.values().sum();
                    let nodes_size = if factor_sum > DEFAULT_NODE_LIST_SIZE {factor_sum} else {DEFAULT_NODE_LIST_SIZE};
                    let factor_scale = nodes_size / factor_sum;
                    let mut lookup_table = self.tables.write();
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
                    return true;
                }
            }
        }
        return false;
    }
    fn server_joined(&self, member: Member) {
        self.init_table();
    }
    fn server_left(&self, member: Member) {
        self.init_table();
    }
}