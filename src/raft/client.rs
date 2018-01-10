use raft::{
    AsyncServiceClient, RaftMsg, LogEntry, ClientQryResponse,
    ClientCmdResponse};
use raft::state_machine::OpType;
use raft::state_machine::master::{ExecResult, ExecError};
use raft::state_machine::callback::client::SubscriptionService;
use raft::state_machine::configs::CONFIG_SM_ID;
use raft::state_machine::configs::commands::{subscribe as conf_subscribe};
use std::collections::{HashMap, BTreeMap, HashSet};
use std::iter::FromIterator;
use parking_lot::{RwLockWriteGuard};
use utils::future_parking_lot::{RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::cmp::max;
use bifrost_hasher::{hash_str, hash_bytes};
use rand;
use rpc;
use futures::prelude::*;

const ORDERING: Ordering = Ordering::Relaxed;
pub type Client = Arc<AsyncServiceClient>;

lazy_static! {
    pub static ref CALLBACK: RwLock<Option<Arc<SubscriptionService>>> = RwLock::new(None);
}

#[derive(Debug)]
pub enum ClientError {
    LeaderIdValid,
    ServerUnreachable,
}

#[derive(Debug)]
pub enum SubscriptionError {
    RemoteError,
    SubServiceNotSet,
}

struct QryMeta {
    pos: AtomicU64
}

struct Members {
    clients: BTreeMap<u64, Client>,
    id_map: HashMap<u64, String>,
}

pub struct RaftClient {
    qry_meta: QryMeta,
    members: RwLock<Members>,
    leader_id: AtomicU64,
    last_log_id: AtomicU64,
    last_log_term: AtomicU64,
    service_id: u64
}

impl RaftClient {
    pub fn new(servers: &Vec<String>, service_id: u64) -> Result<Arc<RaftClient>, ClientError> {
        let client = RaftClient {
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
            service_id,
        };
        let init = {
            let mut members = client.members.write();
            client.update_info(
                &mut members,
                &HashSet::from_iter(servers.iter().cloned())
            )
        };
        match init {
            Ok(_) => Ok(Arc::new(client)),
            Err(e) => Err(e)
        }
    }
    #[async]
    pub fn prepare_subscription(server: Arc<rpc::Server>) -> Result<(), ()> {
        let mut callback = await!(CALLBACK.write_async())?;
        if callback.is_none() {
            let sub_service = await!(SubscriptionService::initialize(server))?;
            *callback = Some(sub_service.clone());
            return Ok(())
        } else {
            return Err(())
        }
    }

    fn update_info(this: Arc<Self>, members: &mut RwLockWriteGuard<Members>, servers: &HashSet<String>)
        -> Result<(), ClientError>
    {
        let mut cluster_info = None;
        for server_addr in servers {
            let id = hash_str(&server_addr);
            if !members.clients.contains_key(&id) {
                match await!(rpc::DEFAULT_CLIENT_POOL.get(server_addr.clone())) {
                    Ok(client) => {
                        members.clients.insert(id, AsyncServiceClient::new(this.service_id, &client));
                    },
                    Err(_) => {continue;}
                }
            }
            let client = members.clients.get(&id).unwrap();
            if let Ok(Ok(info)) = await!(client.c_server_cluster_info()) {
                if info.leader_id != 0 {
                    cluster_info = Some(info);
                    break;
                }
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
                    if !members.clients.contains_key(id) {
                        if let Ok(client) = await!(rpc::DEFAULT_CLIENT_POOL.get(addr)) {
                            members.clients.insert(*id, AsyncServiceClient::new(this.service_id, &client));
                        }
                    }
                }
                this.leader_id.store(info.leader_id, ORDERING);
                Ok(())
            },
            None => Err(ClientError::ServerUnreachable),
        }
    }

    pub fn execute<R>(&self, sm_id: u64, msg: &RaftMsg<R>) -> Result<R, ExecError> {
        let (fn_id, op, req_data) = msg.encode();
        let response = match op {
            OpType::QUERY => {
                self.query(sm_id, fn_id, &req_data, 0)
            },
            OpType::COMMAND | OpType::SUBSCRIBE => {
                self.command(sm_id, fn_id, &req_data, 0)
            },
        };
        match response {
            Ok(data) => {
                match data {
                    Ok(data) => Ok(msg.decode_return(&data)),
                    Err(e) => Err(e)
                }
            },
            Err(e) => Err(e)
        }
    }

    pub fn can_callback() -> bool {
        let callback = CALLBACK.read();
        callback.is_some()
    }

    pub fn subscribe
    <M, R, F>
    (&self, sm_id: u64, msg: M, f: F) -> Result<Result<u64, SubscriptionError>, ExecError>
        where M: RaftMsg<R> + 'static,
              F: Fn(R) + 'static + Send + Sync
    {
        let callback = CALLBACK.read();
        if callback.is_none() {
            debug!("Subscription service not set");
            return Ok(Err(SubscriptionError::SubServiceNotSet))
        }
        let callback = callback.clone().unwrap();
        let raft_sid = self.service_id;
        let (fn_id, pattern_id) = {
            let (fn_id, _, pattern_data) = msg.encode();
            (fn_id, hash_bytes(pattern_data.as_slice()))
        };
        let wrapper_fn = move |data: Vec<u8>| {
            f(msg.decode_return(&data))
        };
        let key = (raft_sid, sm_id, fn_id, pattern_id);
        let mut subs_map = callback.subs.write();
        let mut subs_lst = subs_map.entry(key).or_insert_with(|| Vec::new());
        subs_lst.push(Box::new(wrapper_fn));
        let cluster_subs = self.execute(
            CONFIG_SM_ID,
            &conf_subscribe::new(&key, &callback.server_address, &callback.session_id)
        );
        match cluster_subs {
            Ok(sub_result) => match sub_result {
                Ok(sub_id) => Ok(Ok(sub_id)),
                Err(_) => Ok(Err(SubscriptionError::RemoteError))
            },
            Err(e) => Err(e)
        }
    }

    #[async]
    fn query(this: Arc<Self>, sm_id: u64, fn_id: u64, data: Vec<u8>, depth: usize) -> Result<ExecResult, ExecError> {
        let pos = this.qry_meta.pos.fetch_add(1, ORDERING);
        let mut num_members = 0;
        let res = {
            let members = await!(this.members.read_async()).unwrap();
            let client = {
                let members_count = members.clients.len();
                if members_count < 1 {
                    return Err(ExecError::ServersUnreachable)
                } else {
                    members.clients.values().nth(pos as usize % members_count).unwrap()
                }
            };
            num_members = members.clients.len();
            await!(client.c_query(&this.gen_log_entry(sm_id, fn_id, &data)))
        };
        match res {
            Ok(Ok(res)) => {
                match res {
                    ClientQryResponse::LeftBehind => {
                        if depth >= num_members {
                            Err(ExecError::TooManyRetry)
                        } else {
                            await!(Self::query(this.clone(), sm_id, fn_id, data, depth + 1))
                        }
                    },
                    ClientQryResponse::Success{
                        data, last_log_term, last_log_id
                    } => {
                        swap_when_greater(&this.last_log_id, last_log_id);
                        swap_when_greater(&this.last_log_term, last_log_term);
                        Ok(data)
                    },
                }
            },
            _ => Err(ExecError::Unknown)
        }
    }

    fn command(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>, depth: usize) -> Result<ExecResult, ExecError> {
        enum FailureAction {
            SwitchLeader,
            NotCommitted,
            UpdateInfo,
            NotLeader,
            Retry,
        }
        let failure = {
            if depth > 0 {
                let members = self.members.read();
                let num_members = members.clients.len();
                if depth >= max(num_members, 5) {
                    return Err(ExecError::TooManyRetry)
                };
            }
            match self.current_leader_client() {
                Some((leader_id, client)) => {
                    match client.c_command(&self.gen_log_entry(sm_id, fn_id, data)).wait() {
                        Ok(Ok(ClientCmdResponse::Success {
                                  data, last_log_term, last_log_id
                              })) => {
                            swap_when_greater(&self.last_log_id, last_log_id);
                            swap_when_greater(&self.last_log_term, last_log_term);
                            return Ok(data);
                        },
                        Ok(Ok(ClientCmdResponse::NotLeader(leader_id))) => {
                            self.leader_id.store(leader_id, ORDERING);
                            FailureAction::NotLeader
                        },
                        Ok(Ok(ClientCmdResponse::NotCommitted)) => {
                            FailureAction::NotCommitted
                        },
                        Err(e) => {
                            debug!("CLIENT: E1 - {} - {:?}", leader_id, e);
                            FailureAction::SwitchLeader // need switch server for leader
                        }
                        Ok(Err(e)) => {
                            debug!("CLIENT: E2 - {} - {:?}", leader_id, e);
                            FailureAction::SwitchLeader // need switch server for leader
                        }
                    }
                },
                None => FailureAction::UpdateInfo // need update members
            }
        }; //
        match failure {
            FailureAction::SwitchLeader => {
                let members = self.members.read();
                let num_members = members.clients.len();
                let pos = self.qry_meta.pos.load(ORDERING);
                let leader_id = self.leader_id.load(ORDERING);
                let index = members.clients.keys()
                    .nth(pos as usize % num_members)
                    .unwrap();
                self.leader_id.compare_and_swap(leader_id, *index, ORDERING);
                debug!("CLIENT: Switch leader");
            },
            _ => {}
        }
        self.command(sm_id, fn_id, data, depth + 1)
    }

    fn gen_log_entry(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>) -> LogEntry {
        LogEntry {
            id: self.last_log_id.load(ORDERING),
            term: self.last_log_term.load(ORDERING),
            sm_id,
            fn_id,
            data: data.clone()
        }
    }
    pub fn leader_id(&self) -> u64 {self.leader_id.load(ORDERING)}
    pub fn leader_client(&self) -> Option<(u64, Client)> {
        let members = self.members.read();
        let leader_id = self.leader_id();
        if let Some(client) = members.clients.get(&leader_id) {
            Some((leader_id,  client.clone()))
        } else {
            None
        }
    }
    pub fn current_leader_client(&self) -> Option<(u64, Client)> {
        {
            let leader_client = self.leader_client();
            if leader_client.is_some() {
                return leader_client
            }
        }
        {
            let mut members = self.members.write();
            let mut members_addrs = HashSet::new();
            for address in members.id_map.values() {
                members_addrs.insert(address.clone());

            }
            self.update_info(&mut members, &members_addrs);
            let leader_id = self.leader_id.load(ORDERING);
            if let Some(client) = members.clients.get(&leader_id) {
                Some((leader_id,  client.clone()))
            } else {
                None
            }
        }
    }
    pub fn current_leader_rpc_client(&self) -> Option<Arc<rpc::RPCClient>> {
        match self.current_leader_client() {
            Some((_, client)) => Some(client.client.clone()),
            None => None
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