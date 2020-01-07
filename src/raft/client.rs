use super::*;
use bifrost_hasher::{hash_bytes, hash_str};
use std::clone::Clone;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use crate::raft::state_machine::master::ExecError;
use crate::rpc;
use crate::raft::state_machine::callback::SubKey;
use crate::raft::state_machine::callback::client::SubscriptionService;

const ORDERING: Ordering = Ordering::Relaxed;
pub type Client = Arc<AsyncServiceClient>;
pub type SubscriptionReceipt = (SubKey, u64);

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
    CannotFindSubId,
}

struct QryMeta {
    pos: AtomicU64,
}

struct Members {
    clients: BTreeMap<u64, Client>,
    id_map: HashMap<u64, String>,
}

struct RaftClient {
    qry_meta: QryMeta,
    members: RwLock<Members>,
    leader_id: AtomicU64,
    last_log_id: AtomicU64,
    last_log_term: AtomicU64,
    service_id: u64,
}

impl RaftClient {
    pub fn new(
        servers: &Vec<String>,
        service_id: u64,
    ) -> Result<Self, ClientError> {
        let client = RaftClient {
            qry_meta: QryMeta {
                pos: AtomicU64::new(rand::random::<u64>()),
            },
            members: RwLock::new(Members {
                clients: BTreeMap::new(),
                id_map: HashMap::new(),
            }),
            leader_id: AtomicU64::new(0),
            last_log_id: AtomicU64::new(0),
            last_log_term: AtomicU64::new(0),
            service_id,
        };
        Self::update_info(client.clone(), HashSet::from_iter(servers.iter().cloned()))
            .wait()
            .map(move |_| client)
    }
    pub fn prepare_subscription(server: &Arc<rpc::Server>) -> Option<()> {
        let mut callback = CALLBACK.write();
        if callback.is_none() {
            let sub_service = SubscriptionService::initialize(&server);
            *callback = Some(sub_service.clone());
            return Some(());
        } else {
            return None;
        }
    }

    async fn cluster_info(
        &self,
        servers: HashSet<String>,
    ) -> (Option<ClientClusterInfo>, RwLockWriteGuard<Members>) {
        let members = self.members.write().await;
        for server_addr in servers {
            let id = hash_str(&server_addr);
            if !members.clients.contains_key(&id) {
                match rpc::DEFAULT_CLIENT_POOL.get(&server_addr) {
                    Ok(client) => {
                        let members = members.mutate();
                        members
                            .clients
                            .insert(id, AsyncServiceClient::new(self.service_id, &client));
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
            if let Ok(Ok(info)) = members.clients.get(&id).unwrap().c_server_cluster_info().await
            {
                if info.leader_id != 0 {
                    return (Some(info), members);
                }
            }
        }
        return (None, members);
    }

    async fn update_info(&self, servers: HashSet<String>) -> Result<(), ClientError> {
        let (cluster_info, members) = self.cluster_info(servers).await;
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
                for id in members.clients.keys() {
                    connected_ids.insert(*id);
                }
                let ids_to_remove = connected_ids.difference(&remote_ids);
                for id in ids_to_remove {
                    members.clients.remove(id);
                }
                for id in remote_ids.difference(&connected_ids) {
                    let addr = members.id_map.get(id).unwrap().clone();
                    if !members.clients.contains_key(id) {
                        if let Ok(client) = rpc::DEFAULT_CLIENT_POOL.get(&addr) {
                            members
                                .clients
                                .insert(*id, AsyncServiceClient::new(self.service_id, &client));
                        }
                    }
                }
                self.leader_id.store(info.leader_id, ORDERING);
                Ok(())
            }
            None => Err(ClientError::ServerUnreachable),
        }
    }

    pub async fn execute<R, M>(self, sm_id: u64, msg: M) -> Result<R, ExecError>
    where
        R: 'static,
        M: RaftMsg<R> + 'static,
    {
        let (fn_id, op, req_data) = msg.encode();
        let response = match op {
            OpType::QUERY => self.query(sm_id, fn_id, req_data, 0).await,
            OpType::COMMAND | OpType::SUBSCRIBE => {
                self.command(sm_id, fn_id, req_data, 0).await
            }
        };
        match response {
            Ok(data) => match data {
                Ok(data) => Ok(M::decode_return(&data)),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    pub fn can_callback() -> bool {
        CALLBACK.read().is_some()
    }
    fn get_sub_key<M, R>(&self, sm_id: u64, msg: M) -> SubKey
    where
        M: RaftMsg<R> + 'static,
        R: 'static,
    {
        let raft_sid = self.service_id;
        let (fn_id, pattern_id) = {
            let (fn_id, _, pattern_data) = msg.encode();
            (fn_id, hash_bytes(pattern_data.as_slice()))
        };
        return (raft_sid, sm_id, fn_id, pattern_id);
    }

    pub async fn get_callback(self) -> Result<Arc<SubscriptionService>, SubscriptionError> {
        match CALLBACK.read().await.clone() {
            None => {
                debug!("Subscription service not set");
                Err(SubscriptionError::SubServiceNotSet)
            }
            Some(c) => Ok(c),
        }
    }

    pub async fn subscribe<M, R, F>(
        &self,
        sm_id: u64,
        msg: M,
        f: F,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        M: RaftMsg<R> + 'static,
        R: 'static,
        F: Fn(R) + 'static + Send + Sync,
    {
        let callback = match self.get_callback().await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        let key = self.get_sub_key(sm_id, msg);
        let wrapper_fn = move |data: Vec<u8>| f(M::decode_return(&data));
        let cluster_subs = self.execute(CONFIG_SM_ID, conf_subscribe::new(&key, &callback.server_address, &callback.session_id)).await;
        match cluster_subs {
            Ok(Ok(sub_id)) => {
                let mut subs_map = callback.subs.write();
                let mut subs_lst = subs_map.entry(key).or_insert_with(|| Vec::new());
                subs_lst.push((Box::new(wrapper_fn), sub_id));
                Ok(Ok((key, sub_id)))
            }
            Ok(Err(_)) => Ok(Err(SubscriptionError::RemoteError)),
            Err(e) => Err(e),
        }
    }

    pub async fn unsubscribe(
        &self,
        receipt: SubscriptionReceipt,
    ) -> Result<Result<(), SubscriptionError>, ExecError> {
        match self.get_callback().await {
            Ok(callback) => {
                let (key, sub_id) = receipt;
                let unsub = self.execute(CONFIG_SM_ID, conf_unsubscribe::new(&sub_id)).await;
                match unsub {
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
                    }
                    Ok(Err(_)) => Ok(Err(SubscriptionError::RemoteError)),
                    Err(e) => Err(e),
                }
            }
            Err(e) => {
                debug!("Subscription service not set");
                return Ok(Err(e));
            }
        }
    }

    async fn query(
        &self,
        sm_id: u64,
        fn_id: u64,
        data: Vec<u8>,
        depth: usize,
    ) -> Result<ExecResult, ExecError> {
        let pos = self.qry_meta.pos.fetch_add(1, ORDERING);
        let members = self.members.read().await;
        let num_members = members.clients.len();
        if num_members >= 1 {
            let res = members
                .clients
                .values()
                .nth(pos as usize % num_members)
                .unwrap()
                .c_query(self.gen_log_entry(sm_id, fn_id, &data))
                .await;
            match res {
                Ok(Ok(res)) => match res {
                    ClientQryResponse::LeftBehind => {
                        if depth >= num_members {
                            Err(ExecError::TooManyRetry)
                        } else {
                            self.query(sm_id, fn_id, data, depth + 1).await
                        }
                    }
                    ClientQryResponse::Success {
                        data,
                        last_log_term,
                        last_log_id,
                    } => {
                        swap_when_greater(&self.last_log_id, last_log_id);
                        swap_when_greater(&self.last_log_term, last_log_term);
                        Ok(data)
                    }
                },
                _ => Err(ExecError::Unknown),
            }
        } else {
            Err(ExecError::ServersUnreachable)
        }
    }

    async fn command(
        &self,
        sm_id: u64,
        fn_id: u64,
        data: Vec<u8>,
        depth: usize,
    ) -> Result<ExecResult, ExecError> {
        enum FailureAction {
            SwitchLeader,
            NotCommitted,
            UpdateInfo,
            NotLeader,
            Retry,
        }
        let failure = {
            if depth > 0 {
                let members = self.members.read().await;
                let num_members = members.clients.len();
                if depth >= max(num_members, 5) {
                    return Err(ExecError::TooManyRetry);
                };
            }
            match self.current_leader_client().await {
                Some((leader_id, client)) => {
                    match self.c_command(sm_id, fn_id, &data).await {
                        Ok(Ok(ClientCmdResponse::Success {
                            data,
                            last_log_term,
                            last_log_id,
                        })) => {
                            swap_when_greater(&self.last_log_id, last_log_id);
                            swap_when_greater(&self.last_log_term, last_log_term);
                            return Ok(data);
                        }
                        Ok(Ok(ClientCmdResponse::NotLeader(leader_id))) => {
                            self.leader_id.store(leader_id, ORDERING);
                            FailureAction::NotLeader
                        }
                        Ok(Ok(ClientCmdResponse::NotCommitted)) => FailureAction::NotCommitted,
                        Err(e) => {
                            debug!("CLIENT: E1 - {} - {:?}", leader_id, e);
                            FailureAction::SwitchLeader // need switch server for leader
                        }
                        Ok(Err(e)) => {
                            debug!("CLIENT: E2 - {} - {:?}", leader_id, e);
                            FailureAction::SwitchLeader // need switch server for leader
                        }
                    }
                }
                None => FailureAction::UpdateInfo, // need update members
            }
        }; //
        match failure {
            FailureAction::SwitchLeader => {
                let members = self.members.read().await;
                let num_members = members.clients.len();
                let pos = self.qry_meta.pos.load(ORDERING);
                let leader_id = self.leader_id.load(ORDERING);
                let index = members
                    .clients
                    .keys()
                    .nth(pos as usize % num_members)
                    .unwrap();
                self.leader_id.compare_and_swap(leader_id, *index, ORDERING);
                debug!("CLIENT: Switch leader");
            }
            _ => {}
        }
        self.command(sm_id, fn_id, data, depth + 1).await
    }

    fn gen_log_entry(&self, sm_id: u64, fn_id: u64, data: &Vec<u8>) -> LogEntry {
        LogEntry {
            id: self.last_log_id.load(ORDERING),
            term: self.last_log_term.load(ORDERING),
            sm_id,
            fn_id,
            data: data.clone(),
        }
    }
    pub fn leader_id(&self) -> u64 {
        self.leader_id.load(ORDERING)
    }
    pub fn leader_client(&self) -> Option<(u64, Client)> {
        let members = self.members.read();
        let leader_id = self.leader_id();
        if let Some(client) = members.clients.get(&leader_id) {
            Some((leader_id, client.clone()))
        } else {
            None
        }
    }

    async fn current_leader_client(&self) -> Option<(u64, Client)> {
        {
            let leader_client = self.leader_client();
            if leader_client.is_some() {
                return leader_client;
            }
        }
        {
            let servers = {
                let members = self.members.read().await;
                HashSet::from_iter(members.id_map.values().cloned())
            };
            self.update_info(servers).await;
            let leader_id = self.leader_id.load(ORDERING);
            let members = self.members.read().await;
            if let Some(client) = members.clients.get(&leader_id) {
                Some((leader_id, client.clone()))
            } else {
                None
            }
        }
    }
    pub fn current_leader_rpc_client(
        &self,
    ) -> impl Future<Item = Arc<rpc::RPCClient>, Error = ()> {
        self.current_leader_client().map(|(_, client)| client.client.clone())
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
