use raft::{SyncClient, ClientClusterInfo, RaftMsg, RaftStateMachine, LogEntry, ClientQryResponse};
use raft::state_machine::OpType;
use std::collections::{HashMap, BTreeMap, HashSet};
use std::iter::FromIterator;
use std::sync::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;
use bifrost_plugins::hash_str;
use rand;

const ORDERING: Ordering = Ordering::Relaxed;

struct QryMeta {
    pos: AtomicU64
}

struct Members {
    clients: BTreeMap<u64, Mutex<SyncClient>>,
    id_map: HashMap<u64, String>,
}

pub struct RaftClient {
    qry_meta: QryMeta,
    members: RwLock<Members>,
    leader_id: AtomicU64,
    last_log_id: AtomicU64,
    last_log_term: AtomicU64,
}

impl RaftClient {
    pub fn new(servers: Vec<String>) -> Option<RaftClient> {
        let mut client = RaftClient {
            qry_meta: QryMeta {
                pos: AtomicU64::new(rand::random::<u64>())
            },
            members: RwLock::new(Members {
                clients: BTreeMap::new(),
                id_map: HashMap::new()
            }),
            leader_id: AtomicU64::new(0),
            last_log_id: AtomicU64::new(0),
            last_log_term: AtomicU64::new(0),
        };
        match client.update_info(&HashSet::from_iter(servers)) {
            Ok(_) => Some(client),
            Err(_) => None
        }
    }

    fn update_info(&self, addrs: &HashSet<String>) -> Result<(), ()> {
        let info: ClientClusterInfo;
        let mut servers = None;
        let mut members = self.members.write().unwrap();
        for server_addr in addrs {
            let id = hash_str(server_addr.clone());
            let mut client = members.clients.entry(id).or_insert_with(|| {
                Mutex::new(SyncClient::new(server_addr))
            });
            if let Some(Ok(info)) = client.lock().unwrap().c_server_cluster_info() {
                servers = Some(info);
                break;
            }
        }
        match servers {
            Some(servers) => {
                let remote_members = servers.members;
                let mut remote_ids = HashSet::with_capacity(remote_members.len());
                members.id_map.clear();
                for (id, addr) in remote_members {
                    members.id_map.insert(id, addr);
                    remote_ids.insert(id);
                }
                let mut connected_ids = HashSet::with_capacity(members.clients.len());
                for id in members.clients.keys() {connected_ids.insert(*id);}
                let ids_to_remove = connected_ids.difference(&remote_ids);
                for id in ids_to_remove {members.clients.remove(id);}
                for id in remote_ids.difference(&connected_ids) {
                    let addr = members.id_map.get(id).unwrap().clone();
                    members.clients.entry(id.clone()).or_insert_with(|| {
                        Mutex::new(SyncClient::new(&addr))
                    });
                }
                Ok(())
            },
            None => Err(()),
        }
    }

    pub fn execute<R>(&self, sm: RaftStateMachine, msg: &RaftMsg<R>) -> Option<R> {
        let (fn_id, op, req_data) = msg.encode();
        let sm_id = sm.id;
        let response = match op {
            OpType::QUERY => {

            },
            OpType::COMMAND => {

            },
        };
        None
    }

    fn query(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {
        let pos = self.qry_meta.pos.fetch_add(1, ORDERING);
        let res = {
            let members = self.members.read().unwrap();
            let mut client = {
                let clients_count = members.clients.len();
                members.clients.values()
                    .nth(pos as usize % clients_count)
                    .unwrap().lock().unwrap()
            };
            client.c_query(LogEntry {
                id: self.last_log_id.load(ORDERING),
                term: self.last_log_term.load(ORDERING),
                sm_id: sm_id,
                fn_id: fn_id,
                data: data.clone()
            })
        };
        match res {
            Some(Ok(res)) => {
                match res {
                    ClientQryResponse::LeftBehind => {
                        self.query(sm_id, fn_id, data)
                    },
                    ClientQryResponse::Success{
                        data: data,
                        last_log_term: last_log_term,
                        last_log_id: last_log_id
                    } => {
                        swap_when_larger(&self.last_log_id, last_log_id);
                        swap_when_larger(&self.last_log_term, last_log_term);
                        Some(data)
                    },
                }
            },
            _ => None
        }
    }

//    fn command(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>) -> Option<u8> {
//
//    }
}

fn swap_when_larger(atomic: &AtomicU64, value: u64) {
    let mut orig_num = atomic.load(ORDERING);
    loop {
        if orig_num >= value {
            return;
        }
        let actual = atomic.compare_and_swap(orig_num, value, ORDERING);
        if actual == orig_num {
            return;
        } else {
            orig_num = actual;
        }
    }
}