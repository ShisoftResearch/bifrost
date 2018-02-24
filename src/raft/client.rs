use raft::{
    AsyncServiceClient, RaftMsg, LogEntry, ClientQryResponse,
    ClientCmdResponse};
use raft::state_machine::OpType;
use raft::state_machine::master::{ExecResult, ExecError};
use raft::state_machine::callback::client::SubscriptionService;
use raft::state_machine::callback::SubKey;
use raft::state_machine::configs::CONFIG_SM_ID;
use raft::state_machine::configs::commands::{subscribe as conf_subscribe, unsubscribe as conf_unsubscribe};
use std::collections::{HashMap, BTreeMap, HashSet};
use std::iter::FromIterator;
use utils::async_locks::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::cmp::max;
use std::clone::Clone;
use bifrost_hasher::{hash_str, hash_bytes};
use rand;
use rpc;
use backtrace::Backtrace;
use futures::prelude::*;
use super::*;

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
    CannotFindSubId
}

struct QryMeta {
    pos: AtomicU64
}

struct Members {
    clients: BTreeMap<u64, Client>,
    id_map: HashMap<u64, String>,
}

struct RaftClientInner {
    qry_meta: QryMeta,
    members: RwLock<Members>,
    leader_id: AtomicU64,
    last_log_id: AtomicU64,
    last_log_term: AtomicU64,
    service_id: u64
}

pub struct RaftClient {
    inner: Arc<RaftClientInner>
}

impl RaftClient {
    pub fn new(servers: &Vec<String>, service_id: u64) -> Result<Arc<RaftClient>, ClientError> {
        Ok(Arc::new(RaftClient {
            inner: RaftClientInner::new(servers, service_id)?
        }))
    }

    pub fn prepare_subscription(server: &Arc<rpc::Server>) -> Option<()> {
        RaftClientInner::prepare_subscription(server)
    }

    pub fn execute<R, M>(&self, sm_id: u64, msg: M)
        -> Box<Future<Item = R, Error = ExecError>>
        where R: 'static, M: RaftMsg<R> + 'static
    {
        RaftClientInner::execute(self.inner.clone(), sm_id, msg)
    }

    pub fn can_callback() -> bool {
        RaftClientInner::can_callback()
    }

    pub fn subscribe
    <M, R, F>
    (&self, sm_id: u64, msg: M, f: F)
        -> Box<Future<Item = Result<u64, SubscriptionError>, Error = ExecError>>
        where M: RaftMsg<R> + 'static,
              R: 'static,
              F: Fn(R) + 'static + Send + Sync
    {
        RaftClientInner::subscribe(self.inner.clone(), sm_id, msg, f)
    }

    pub fn unsubscribe<M, R>(&self, sm_id: u64, msg: M, sub_id: u64)
        -> Box<Future<Item = Result<(), SubscriptionError>, Error = ExecError>>
        where M: RaftMsg<R> + 'static, R: 'static
    {
        RaftClientInner::unsubscribe(self.inner.clone(), sm_id, msg, sub_id)
    }

    pub fn leader_id(&self) -> u64 {
        self.inner.leader_id.load(ORDERING)
    }

    pub fn leader_client(&self) -> Option<(u64, Client)> {
        self.inner.leader_client()
    }

    pub fn current_leader_rpc_client(&self)
        -> impl Future<Item = Arc<rpc::RPCClient>, Error = ()>
    {
        RaftClientInner::current_leader_rpc_client(self.inner.clone())
    }
}

impl RaftClientInner {
    pub fn new(servers: &Vec<String>, service_id: u64) -> Result<Arc<RaftClientInner>, ClientError> {
        let client = Arc::new(RaftClientInner {
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
        });
        Self::update_info(
            client.clone(),
            HashSet::from_iter(servers.iter().cloned()))
            .wait()
            .map(move |_| client)
    }
    pub fn prepare_subscription(server: &Arc<rpc::Server>) -> Option<()> {
        let mut callback = CALLBACK.write();
        if callback.is_none() {
            let sub_service = SubscriptionService::initialize(&server);
            *callback = Some(sub_service.clone());
            return Some(())
        } else {
            return None
        }
    }

    #[async(boxed)]
    fn cluster_info(this: Arc<Self>, servers: HashSet<String>)
        -> Result<(Option<ClientClusterInfo>, RwLockWriteGuard<Members>), ()>
    {
        let members = await!(this.members.write_async()).unwrap();
        for server_addr in servers {
            let id = hash_str(&server_addr);
            if !members.clients.contains_key(&id) {
                match rpc::DEFAULT_CLIENT_POOL.get(&server_addr) {
                    Ok(client) => {
                        let members = members.mutate();
                        members.clients.insert(id, AsyncServiceClient::new(this.service_id, &client));
                    },
                    Err(_) => {continue;}
                }
            }
            if let Ok(Ok(info)) = await!(members.clients.get(&id).unwrap().c_server_cluster_info()) {
                if info.leader_id != 0 {
                    return Ok((Some(info), members));
                }
            }
        }
        return Ok((None, members));
    }

    #[async(boxed)]
    fn update_info(this: Arc<Self>, servers: HashSet<String>)
        -> Result<(), ClientError>
    {
        let (cluster_info, members) = await!(Self::cluster_info(this.clone(), servers)).unwrap();
        match cluster_info {
            Some(info) => {
                let remote_members = info.members;
                let mut remote_ids = HashSet::with_capacity(remote_members.len());
                let mut members = members.mutate();
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
                        if let Ok(client) = rpc::DEFAULT_CLIENT_POOL.get(&addr) {
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

    #[async(boxed)]
    pub fn execute<R, M>(this: Arc<Self>, sm_id: u64, msg: M)
        -> Result<R, ExecError>
        where R: 'static, M: RaftMsg<R> + 'static
    {
        let (fn_id, op, req_data) = msg.encode();
        let response = match op {
            OpType::QUERY => {
                await!(Self::query(this, sm_id, fn_id, req_data, 0))
            },
            OpType::COMMAND | OpType::SUBSCRIBE => {
                await!(Self::command(this, sm_id, fn_id, req_data, 0))
            },
        };
        match response {
            Ok(data) => {
                match data {
                    Ok(data) => Ok(M::decode_return(&data)),
                    Err(e) => Err(e)
                }
            },
            Err(e) => Err(e)
        }
    }

    pub fn can_callback() -> bool {
        CALLBACK.read().is_some()
    }
    fn get_sub_key<M, R>(&self, sm_id: u64, msg: M)
        -> SubKey where M: RaftMsg<R> + 'static, R: 'static
    {
        let raft_sid = self.service_id;
        let (fn_id, pattern_id) = {
            let (fn_id, _, pattern_data) = msg.encode();
            (fn_id, hash_bytes(pattern_data.as_slice()))
        };
        return (raft_sid, sm_id, fn_id, pattern_id);
    }
    #[async]
    pub fn get_callback(this: Arc<Self>) -> Result<Arc<SubscriptionService>, SubscriptionError> {
        match (*await!(CALLBACK.read_async()).unwrap()).clone() {
            None => {
                debug!("Subscription service not set: {:?}", Backtrace::new());
                Err(SubscriptionError::SubServiceNotSet)
            },
            Some(c) => Ok(c)
        }
    }
    #[async(boxed)]
    pub fn subscribe
    <M, R, F>
    (this: Arc<Self>, sm_id: u64, msg: M, f: F) -> Result<Result<u64, SubscriptionError>, ExecError>
        where M: RaftMsg<R> + 'static,
              R: 'static,
              F: Fn(R) + 'static + Send + Sync
    {
        let callback = match await!(Self::get_callback(this.clone())) {
            Ok(c) => c, Err(e) => return Ok(Err(e))
        };
        let key = this.get_sub_key(sm_id, msg);
        let wrapper_fn = move |data: Vec<u8>| {
            f(M::decode_return(&data))
        };
        let cluster_subs = await!(Self::execute(
            this.clone(),
            CONFIG_SM_ID,
            conf_subscribe::new(&key, &callback.server_address, &callback.session_id)
        ));
        match cluster_subs {
            Ok(Ok(sub_id)) => {
                let mut subs_map = callback.subs.write();
                let mut subs_lst = subs_map.entry(key).or_insert_with(|| Vec::new());
                subs_lst.push((Box::new(wrapper_fn), sub_id));
                Ok(Ok(sub_id))
            },
            Ok(Err(_)) => Ok(Err(SubscriptionError::RemoteError)),
            Err(e) => Err(e)
        }
    }

    #[async(boxed)]
    pub fn unsubscribe<M, R>(this: Arc<Self>, sm_id: u64, msg: M, sub_id: u64)
        -> Result<Result<(), SubscriptionError>, ExecError>
        where M: RaftMsg<R> + 'static, R: 'static
    {
        match await!(Self::get_callback(this.clone())) {
            Ok(callback) => {
                let key = this.get_sub_key(sm_id, msg);
                let unsub = await!(Self::execute(
                        this.clone(),
                        CONFIG_SM_ID,
                        conf_unsubscribe::new(&sub_id)
                    ));
                match unsub{
                    Ok(Ok(_)) => {
                        let mut subs_map = callback.subs.write();
                        let mut subs_lst = subs_map.entry(key).or_insert_with(|| Vec::new());
                        let mut sub_index = 0;
                        for i in 0..subs_lst.len() {
                            if subs_lst[i].1 == sub_id {
                                sub_index = i;
                                break;
                            }
                        }
                        if subs_lst.len() > 0 && subs_lst[sub_index].1 == sub_id {
                            subs_lst.remove(sub_index);
                            Ok(Ok(()))
                        } else {
                            Ok(Err(SubscriptionError::CannotFindSubId))
                        }
                    },
                    Ok(Err(_)) => Ok(Err(SubscriptionError::RemoteError)),
                    Err(e) => Err(e)
                }
            },
            Err(e) => {
                debug!("Subscription service not set: {:?}", Backtrace::new());
                return Ok(Err(e))
            }
        }

    }

    #[async(boxed)]
    fn query(this: Arc<Self>, sm_id: u64, fn_id: u64, data: Vec<u8>, depth: usize)
        -> Result<ExecResult, ExecError>
    {
        let pos = this.qry_meta.pos.fetch_add(1, ORDERING);
        let members = await!(this.members.read_async()).unwrap();
        let num_members = members.clients.len();
        if num_members >= 1 {
            let res = {;
                await!(
                    members.clients.values().nth(pos as usize % num_members).unwrap()
                    .c_query(&this.gen_log_entry(sm_id, fn_id, &data)))
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
        } else {
            Err(ExecError::ServersUnreachable)
        }
    }

    #[async(boxed)]
    fn command(this: Arc<Self>, sm_id: u64, fn_id: u64, data: Vec<u8>, depth: usize)
        -> Result<ExecResult, ExecError>
    {
        enum FailureAction {
            SwitchLeader,
            NotCommitted,
            UpdateInfo,
            NotLeader,
            Retry,
        }
        let failure = {
            if depth > 0 {
                let members = this.members.read();
                let num_members = members.clients.len();
                if depth >= max(num_members, 5) {
                    return Err(ExecError::TooManyRetry)
                };
            }
            match await!(Self::current_leader_client(this.clone())) {
                Ok((leader_id, client)) => {
                    match await!(client.c_command(&this.gen_log_entry(sm_id, fn_id, &data))) {
                        Ok(Ok(ClientCmdResponse::Success {
                                  data, last_log_term, last_log_id
                              })) => {
                            swap_when_greater(&this.last_log_id, last_log_id);
                            swap_when_greater(&this.last_log_term, last_log_term);
                            return Ok(data);
                        },
                        Ok(Ok(ClientCmdResponse::NotLeader(leader_id))) => {
                            this.leader_id.store(leader_id, ORDERING);
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
                Err(()) => FailureAction::UpdateInfo // need update members
            }
        }; //
        match failure {
            FailureAction::SwitchLeader => {
                let members = this.members.read();
                let num_members = members.clients.len();
                let pos = this.qry_meta.pos.load(ORDERING);
                let leader_id = this.leader_id.load(ORDERING);
                let index = members.clients.keys()
                    .nth(pos as usize % num_members)
                    .unwrap();
                this.leader_id.compare_and_swap(leader_id, *index, ORDERING);
                debug!("CLIENT: Switch leader");
            },
            _ => {}
        }
        await!(Self::command(this, sm_id, fn_id, data, depth + 1))
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
    #[async(boxed)]
    fn current_leader_client(this: Arc<Self>) -> Result<(u64, Client), ()> {
        {
            let leader_client = this.leader_client();
            if leader_client.is_some() {
                return leader_client.ok_or(())
            }
        }
        {
            let servers = {
                let members = this.members.read();
                HashSet::from_iter(members.id_map.values().cloned())
            };
            await!(Self::update_info(this.clone(), servers));
            let leader_id = this.leader_id.load(ORDERING);
            let members = this.members.read();
            if let Some(client) = members.clients.get(&leader_id) {
                Ok((leader_id,  client.clone()))
            } else {
                Err(())
            }
        }
    }
    pub fn current_leader_rpc_client(this: Arc<Self>)
        -> impl Future<Item = Arc<rpc::RPCClient>, Error = ()>
    {
        Self::current_leader_client(this)
            .map(|(_, client)| client.client.clone())
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