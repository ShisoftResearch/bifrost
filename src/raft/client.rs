use raft::{
    SyncClient, ClientClusterInfo, RaftMsg,
    RaftStateMachine, LogEntry,
    ClientQryResponse, ClientCmdResponse};
use raft::state_machine::OpType;
use std::collections::{HashMap, BTreeMap, HashSet};
use std::iter::FromIterator;
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
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
                self.query(sm_id, fn_id, &req_data, 0)
            },
            OpType::COMMAND => {
                self.command(sm_id, fn_id, &req_data)
            },
        };
        match response {
            Some(data) => {
                Some(msg.decode_return(&data))
            },
            None => None
        }
    }

    fn query(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>, deepth: usize) -> Option<Vec<u8>> {
        let pos = self.qry_meta.pos.fetch_add(1, ORDERING);
        let mut num_members = 0;
        let res = {
            let members = self.members.read().unwrap();
            let mut client = {
                let members_count = members.clients.len();
                members.clients.values()
                    .nth(pos as usize % members_count)
                    .unwrap().lock().unwrap()
            };
            num_members = members.clients.len();
            client.c_query(self.gen_log_entry(sm_id, fn_id, data))
        };
        match res {
            Some(Ok(res)) => {
                match res {
                    ClientQryResponse::LeftBehind => {
                        if deepth >= num_members {
                            None
                        } else {
                            self.query(sm_id, fn_id, data, deepth + 1)
                        }
                    },
                    ClientQryResponse::Success{
                        data: data,
                        last_log_term: last_log_term,
                        last_log_id: last_log_id
                    } => {
                        swap_when_greater(&self.last_log_id, last_log_id);
                        swap_when_greater(&self.last_log_term, last_log_term);
                        Some(data)
                    },
                }
            },
            _ => None
        }
    }

    fn command(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {
        let mut leader = None;
        let mut searched = 0;
        let members = self.members.read().unwrap();
        loop {
            let leader_id = self.leader_id.load(ORDERING);
            let num_members = members.clients.len();
            if searched >= num_members {break;}
            if members.clients.contains_key(&leader_id) {
                leader = Some(leader_id);
                break;
            } else {
                let pos = self.qry_meta.pos.load(ORDERING);
                let index = members.clients.keys()
                    .nth(pos as usize % num_members)
                    .unwrap();
                self.leader_id.compare_and_swap(leader_id, *index, ORDERING);
            }
            searched += 1;
        }
        match leader {
            Some(leader_id) => {
                let mut client = members.clients.get(&leader_id).unwrap().lock().unwrap();
                match client.c_command(self.gen_log_entry(sm_id, fn_id, data)) {
                    Some(Ok(ClientCmdResponse::Success{
                                 data: data, last_log_term: last_log_term,
                                 last_log_id: last_log_id
                             })) => {
                        swap_when_greater(&self.last_log_id, last_log_id);
                        swap_when_greater(&self.last_log_term, last_log_term);
                        Some(data)
                    },
                    Some(Ok(ClientCmdResponse::NotLeader(leader_id))) => {
                        self.leader_id.store(leader_id, ORDERING);
                        self.command(sm_id, fn_id, data)
                    }
                    _ => None
                }
            },
            _ => None
        }
    }

    fn gen_log_entry(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>) -> LogEntry {
        LogEntry {
            id: self.last_log_id.load(ORDERING),
            term: self.last_log_term.load(ORDERING),
            sm_id: sm_id,
            fn_id: fn_id,
            data: data.clone()
        }
    }
}

fn swap_when_greater(atomic: &AtomicU64, value: u64) {
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