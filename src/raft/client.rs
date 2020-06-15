use super::*;
use crate::raft::state_machine::callback::client::SubscriptionService;
use crate::raft::state_machine::callback::SubKey;
use crate::raft::state_machine::configs::commands::{
    subscribe as conf_subscribe, unsubscribe as conf_unsubscribe,
};
use crate::raft::state_machine::master::ExecError;
use crate::raft::state_machine::StateMachineClient;
use crate::rpc;
use bifrost_hasher::{hash_bytes, hash_str};
use futures::future::BoxFuture;
use std::clone::Clone;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::delay_for;

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

pub struct RaftClient {
    qry_meta: QryMeta,
    members: RwLock<Members>,
    leader_id: AtomicU64,
    last_log_id: AtomicU64,
    last_log_term: AtomicU64,
    service_id: u64,
}

impl RaftClient {
    pub async fn new(servers: &Vec<String>, service_id: u64) -> Result<Arc<Self>, ClientError> {
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
        client.update_info(servers).await?;
        Ok(Arc::new(client))
    }
    pub async fn prepare_subscription(server: &Arc<rpc::Server>) -> Option<()> {
        let mut callback = CALLBACK.write().await;
        return if callback.is_none() {
            let sub_service = SubscriptionService::initialize(&server).await;
            *callback = Some(sub_service.clone());
            Some(())
        } else {
            None
        };
    }

    async fn cluster_info<'a>(&'a self, servers: &Vec<String>) -> Option<ClientClusterInfo> {
        debug!("Getting server info for {:?}", servers);
        let mut attempt_remains: i32 = 10;
        loop {
            debug!(
                "Trying to get cluster info, attempt from {:?}...{}",
                servers, attempt_remains
            );
            let mut futs: FuturesUnordered<_> = servers
                .iter()
                .map(|server_addr| {
                    let id = hash_str(server_addr);
                    let server_addr = server_addr.clone();
                    async move {
                        let mut members = self.members.write().await;
                        debug!("Checking server info on {}", server_addr);
                        if !members.clients.contains_key(&id) {
                            debug!("Connecting to node {}", server_addr);
                            match rpc::DEFAULT_CLIENT_POOL.get(&server_addr).await {
                                Ok(client) => {
                                    debug!("Added server info on {} to members", server_addr);
                                    members.clients.insert(
                                        id,
                                        AsyncServiceClient::new(self.service_id, &client),
                                    );
                                    debug!("Member {} added", server_addr);
                                }
                                Err(e) => {
                                    warn!("Cannot find server info from {}, {}", server_addr, e);
                                    return None;
                                }
                            }
                        }
                        debug!("Getting server info from {}", server_addr);
                        let member_client = members.clients.get(&id);
                        debug!("Checking server client {}", server_addr);
                        if member_client.is_none() {
                            debug!("Server not found, skip {}", server_addr);
                            return None;
                        }
                        debug!("Invoking server_cluster_info on {}", server_addr);
                        let info_res = member_client.unwrap().c_server_cluster_info().await;
                        debug!("Checking response from {}", server_addr);
                        return match info_res {
                            Ok(info) => {
                                if info.leader_id != 0 {
                                    debug!("Found server info with leader id {}", info.leader_id);
                                    Some(info)
                                } else {
                                    debug!("Discovered zero leader id from {}", server_addr);
                                    None
                                }
                            }
                            Err(e) => {
                                debug!(
                                    "Error on getting cluster info from {}, {:?}",
                                    server_addr, e
                                );
                                None
                            }
                        };
                    }
                })
                .collect();
            while let Some(res) = futs.next().await {
                if let Some(info) = res {
                    return Some(info);
                }
            }
            if attempt_remains > 0 {
                // We found an uninitialized node, should try again
                // Random sleep
                debug!(
                    "This fail attempt have zero leader id, retry...{}",
                    attempt_remains
                );
                let delay_sec = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(1, 10)
                };
                delay_for(Duration::from_secs(delay_sec)).await;
                attempt_remains -= 1;
                continue;
            } else {
                debug!("Continuously getting zero leader id, give up");
                break;
            }
        }
        warn!("Cannot find anything useful from list: {:?}", servers);
        return None;
    }

    async fn update_info(&self, servers: &Vec<String>) -> Result<(), ClientError> {
        let cluster_info = self.cluster_info(servers).await;
        match cluster_info {
            Some(info) => {
                let mut members = self.members.write().await;
                let remote_members = info.members;
                let mut remote_ids = HashSet::with_capacity(remote_members.len());
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
                        if let Ok(client) = rpc::DEFAULT_CLIENT_POOL.get(&addr).await {
                            members
                                .clients
                                .insert(*id, AsyncServiceClient::new(self.service_id, &client));
                        }
                    }
                }
                self.leader_id.store(info.leader_id, ORDERING);
                Ok(())
            }
            None => {
                debug!("Cannot update info, cannot get cluster info");
                Err(ClientError::ServerUnreachable)
            }
        }
    }

    pub async fn probe_servers(
        servers: &Vec<String>,
        server_address: &String,
        service_id: u64,
    ) -> bool {
        servers
            .iter()
            .map(|peer_addr| {
                timeout(Duration::from_secs(2), async move {
                    if peer_addr == server_address {
                        // Should not include the server we are running
                        return false;
                    }
                    match rpc::DEFAULT_CLIENT_POOL.get(peer_addr).await {
                        Ok(client) => {
                            ImmeServiceClient::c_ping(service_id, &client).await.is_ok()
                        }
                        Err(_) => false,
                    }
                })
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .any(|r| match r {
                Ok(true) => true,
                _ => false,
            })
    }

    pub async fn execute<R, M>(&self, sm_id: u64, msg: M) -> Result<R, ExecError>
    where
        R: 'static,
        M: RaftMsg<R> + 'static,
    {
        let (fn_id, op, req_data) = msg.encode();
        let response = match op {
            OpType::QUERY => self.query(sm_id, fn_id, req_data).await,
            OpType::COMMAND | OpType::SUBSCRIBE => self.command(sm_id, fn_id, req_data).await,
        };
        match response {
            Ok(data) => match data {
                Ok(data) => Ok(M::decode_return(&data)),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }
    }

    pub async fn can_callback() -> bool {
        CALLBACK.read().await.is_some()
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

    pub async fn get_callback(&self) -> Result<Arc<SubscriptionService>, SubscriptionError> {
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
        R: 'static + Send,
        F: Fn(R) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        let callback = match self.get_callback().await {
            Ok(c) => c,
            Err(e) => return Ok(Err(e)),
        };
        let key = self.get_sub_key(sm_id, msg);
        let wrapper_fn =
            move |data: Vec<u8>| -> BoxFuture<'static, ()> { f(M::decode_return(&data)).boxed() };
        let cluster_subs = self
            .execute(
                CONFIG_SM_ID,
                conf_subscribe::new(&key, &callback.server_address, &callback.session_id),
            )
            .await;
        match cluster_subs {
            Ok(Ok(sub_id)) => {
                let mut subs_map = callback.subs.write().await;
                let subs_lst = subs_map.entry(key).or_insert_with(|| Vec::new());
                let boxed_fn = Box::new(wrapper_fn);
                subs_lst.push((boxed_fn, sub_id));
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
                let unsub = self
                    .execute(CONFIG_SM_ID, conf_unsubscribe::new(&sub_id))
                    .await;
                match unsub {
                    Ok(_) => {
                        let mut subs_map = callback.subs.write().await;
                        let subs_lst = subs_map.entry(key).or_insert_with(|| Vec::new());
                        let mut sub_index = 0;
                        for i in 0..subs_lst.len() {
                            if subs_lst[i].1 == sub_id {
                                sub_index = i;
                                break;
                            }
                        }
                        if subs_lst.len() > 0 && subs_lst[sub_index].1 == sub_id {
                            let _ = subs_lst.remove(sub_index);
                            Ok(Ok(()))
                        } else {
                            Ok(Err(SubscriptionError::CannotFindSubId))
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            Err(e) => {
                debug!("Subscription service not set");
                return Ok(Err(e));
            }
        }
    }

    async fn query(&self, sm_id: u64, fn_id: u64, data: Vec<u8>) -> Result<ExecResult, ExecError> {
        let mut depth = 0;
        loop {
            let pos = self.qry_meta.pos.fetch_add(1, ORDERING);
            let members = self.members.read().await;
            let num_members = members.clients.len();
            if num_members >= 1 {
                let node_index = pos as usize % num_members;
                let rpc_client = members.clients.values().nth(node_index).unwrap();
                let res = rpc_client
                    .c_query(self.gen_log_entry(sm_id, fn_id, &data))
                    .await;
                match res {
                    Ok(res) => match res {
                        ClientQryResponse::LeftBehind => {
                            debug!("Found left behind record...{}", depth);
                            if depth >= num_members {
                                return Err(ExecError::TooManyRetry);
                            } else {
                                depth += 1;
                                continue;
                            }
                        }
                        ClientQryResponse::Success {
                            data,
                            last_log_term,
                            last_log_id,
                        } => {
                            swap_when_greater(&self.last_log_id, last_log_id);
                            swap_when_greater(&self.last_log_term, last_log_term);
                            if depth > 0 {
                                warn!("Retry successful...{}", depth);
                            }
                            return Ok(data);
                        }
                    },
                    Err(e) => {
                        error!(
                            "Got unknown error on query: {:?}, server {}",
                            e, rpc_client.client.address
                        );
                        if depth >= num_members {
                            return Err(ExecError::Unknown);
                        } else {
                            debug!("Retry query...{}", depth);
                            depth += 1;
                            continue;
                        }
                    }
                }
            } else {
                return Err(ExecError::ServersUnreachable);
            }
        }
    }

    async fn command(
        &self,
        sm_id: u64,
        fn_id: u64,
        data: Vec<u8>,
    ) -> Result<ExecResult, ExecError> {
        enum FailureAction {
            SwitchLeader,
            NotCommitted,
            UpdateInfo,
            NotLeader,
        }
        let mut depth = 0;
        loop {
            let failure = {
                if depth > 0 {
                    let members = self.members.read().await;
                    let num_members = members.clients.len();
                    if depth >= max(num_members + 1, 5) {
                        return Err(ExecError::TooManyRetry);
                    };
                }
                match self.current_leader_client().await {
                    Some((leader_id, client)) => {
                        let cmd_res = client
                            .c_command(self.gen_log_entry(sm_id, fn_id, &data))
                            .await;
                        match cmd_res {
                            Ok(ClientCmdResponse::Success {
                                data,
                                last_log_term,
                                last_log_id,
                            }) => {
                                swap_when_greater(&self.last_log_id, last_log_id);
                                swap_when_greater(&self.last_log_term, last_log_term);
                                return Ok(data);
                            }
                            Ok(ClientCmdResponse::NotLeader(new_leader_id)) => {
                                if new_leader_id == 0 || leader_id == leader_id {
                                    debug!(
                                        "CLIENT: NOT LEADER, SUGGESTION NOT USEFUL, PROBE. GOT: {}",
                                        new_leader_id
                                    );
                                    FailureAction::SwitchLeader
                                } else {
                                    debug!(
                                        "CLIENT: NOT LEADER, REMOTE SUGGEST SWITCH TO {}",
                                        new_leader_id
                                    );
                                    self.leader_id.store(new_leader_id, ORDERING);
                                    FailureAction::NotLeader
                                }
                            }
                            Ok(ClientCmdResponse::NotCommitted) => FailureAction::NotCommitted,
                            Err(e) => {
                                debug!("CLIENT: ERROR - {} - {:?}", leader_id, e);
                                FailureAction::SwitchLeader // need switch server for leader
                            }
                        }
                    }
                    None => FailureAction::UpdateInfo, // need update members
                }
            }; //
            match failure {
                FailureAction::SwitchLeader => {
                    debug!("Switch leader by probing");
                    let members = self.members.read().await;
                    let num_members = members.clients.len();
                    let leader_id = self.leader_id.load(ORDERING);
                    let new_leader_id = members
                        .clients
                        .keys()
                        .nth(depth as usize % num_members)
                        .unwrap();
                    self.leader_id
                        .compare_and_swap(leader_id, *new_leader_id, ORDERING);
                    debug!("CLIENT: Switch leader {}", new_leader_id);
                }
                _ => {}
            }
            depth += 1;
        }
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
    pub async fn leader_client(&self) -> Option<(u64, Client)> {
        let members = self.members.read().await;
        let leader_id = self.leader_id();
        if let Some(client) = members.clients.get(&leader_id) {
            Some((leader_id, client.clone()))
        } else {
            None
        }
    }

    async fn current_leader_client(&self) -> Option<(u64, Client)> {
        {
            let leader_client = self.leader_client().await;
            if leader_client.is_some() {
                return leader_client;
            }
        }
        {
            let servers = {
                let members = self.members.read().await;
                Vec::from_iter(members.id_map.values().cloned())
            };
            self.update_info(&servers).await.unwrap();
            let leader_id = self.leader_id.load(ORDERING);
            let members = self.members.read().await;
            if let Some(client) = members.clients.get(&leader_id) {
                Some((leader_id, client.clone()))
            } else {
                None
            }
        }
    }
    pub async fn current_leader_rpc_client(&self) -> Result<Arc<rpc::RPCClient>, ()> {
        let (_, client) = self.current_leader_client().await.ok_or_else(|| ())?;
        Ok(client.client.clone())
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

pub struct CachedStateMachine<T: StateMachineClient> {
    server_list: Vec<String>,
    raft_service_id: u64,
    state_machine_id: u64,
    cache: RwLock<Option<Arc<T>>>,
}

impl<T: StateMachineClient> CachedStateMachine<T> {
    pub fn new(server_list: &Vec<String>, raft_service_id: u64, state_machine_id: u64) -> Self {
        debug!(
            "Construct cached state machine for list {:?}, service id {}, state machine {}",
            server_list, raft_service_id, state_machine_id
        );
        Self {
            server_list: server_list.clone(),
            raft_service_id,
            state_machine_id,
            cache: RwLock::new(None),
        }
    }
    pub async fn get(&self) -> Arc<T> {
        loop {
            {
                let client = self.cache.read().await;
                if let Some(cache) = &*client {
                    return (*cache).clone();
                }
            }
            {
                let mut place_holder = self.cache.write().await;
                if place_holder.is_none() {
                    debug!(
                        "Creating state machine client instance, service {}, state machine id {}",
                        self.raft_service_id, self.state_machine_id
                    );
                    let raft_client = RaftClient::new(&self.server_list, self.raft_service_id)
                        .await
                        .unwrap();
                    // Create a client for the state machine on the raft service
                    *place_holder = Some(Arc::new(T::new_instance(
                        self.state_machine_id,
                        &raft_client,
                    )))
                }
            }
        }
    }
}
