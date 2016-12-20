use raft::{
    SyncClient, ClientClusterInfo, RaftMsg,
    RaftStateMachine, LogEntry,
    ClientQryResponse, ClientCmdResponse};
use raft::state_machine::OpType;
use raft::state_machine::master::{ExecResult, ExecError};
use std::collections::{HashMap, BTreeMap, HashSet};
use std::iter::FromIterator;
use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
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
        let init = {
            let mut members = client.members.write().unwrap();
            RaftClient::update_info(
                &mut members,
                &HashSet::from_iter(servers)
            )
        };
        match init {
            Ok(_) => Some(client),
            Err(_) => None
        }
    }

    fn update_info(members: &mut RwLockWriteGuard<Members>, addrs: &HashSet<String>) -> Result<(), ()> {
        let info: ClientClusterInfo;
        let mut cluster_info = None;
        for server_addr in addrs {
            let id = hash_str(server_addr.clone());
            let mut client = members.clients.entry(id).or_insert_with(|| {
                Mutex::new(SyncClient::new(server_addr))
            });
            if let Some(Ok(info)) = client.lock().unwrap().c_server_cluster_info() {
                cluster_info = Some(info);
                break;
            }
        }
        match cluster_info {
            Some(info) => {
                let remote_members = info.members;
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

    pub fn execute<R>(&self, sm: RaftStateMachine, msg: &RaftMsg<R>) -> Result<R, ExecError> {
        let (fn_id, op, req_data) = msg.encode();
        let sm_id = sm.id;
        let response = match op {
            OpType::QUERY => {
                self.query(sm_id, fn_id, &req_data, 0)
            },
            OpType::COMMAND => {
                self.command(sm_id, fn_id, &req_data, 0)
            },
        };
        match response {
            Some(data) => {
                match data {
                    Ok(data) => Ok(msg.decode_return(&data)),
                    Err(e) => Err(e)
                }
            },
            None => Err(ExecError::ServerUnreachable)
        }
    }

    fn query(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>, depth: usize) -> Option<ExecResult> {
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
                        if depth >= num_members {
                            None
                        } else {
                            self.query(sm_id, fn_id, data, depth + 1)
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

    fn command(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>, depth: usize) -> Option<ExecResult> {
        enum FailureAction {
            SwitchLeader,
            UpdateInfo,
            NotLeader,
            NotUpdated,
        }
        let failure = {
            let members = self.members.read().unwrap();
            let num_members = members.clients.len();
            if depth >= num_members {return None};
            let mut leader = {
                let leader_id = self.leader_id.load(ORDERING);
                if members.clients.contains_key(&leader_id) {
                    Some(leader_id)
                } else {
                    None
                }
            };
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
                            return Some(data);
                        },
                        Some(Ok(ClientCmdResponse::NotLeader(leader_id))) => {
                            self.leader_id.store(leader_id, ORDERING);
                            FailureAction::NotLeader
                        },
                        Some(Ok(ClientCmdResponse::NotUpdated)) => {
                            FailureAction::NotUpdated
                        }
                        _ => FailureAction::SwitchLeader // need switch server for leader
                    }
                },
                None => FailureAction::UpdateInfo // need update members
            }
        }; //
        match failure {
            FailureAction::UpdateInfo => {
                let mut members = self.members.write().unwrap();
                let mut members_addrs = HashSet::new();
                for address in members.id_map.values() {
                    members_addrs.insert(address.clone());

                }
                RaftClient::update_info(&mut members, &members_addrs);
            },
            FailureAction::SwitchLeader => {
                let members = self.members.read().unwrap();
                let num_members = members.clients.len();
                let pos = self.qry_meta.pos.load(ORDERING);
                let leader_id = self.leader_id.load(ORDERING);
                let index = members.clients.keys()
                    .nth(pos as usize % num_members)
                    .unwrap();
                self.leader_id.compare_and_swap(leader_id, *index, ORDERING);
            },
            FailureAction::NotUpdated => {
                return None
            },
            _ => {}
        }
        self.command(sm_id, fn_id, data, depth + 1)
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