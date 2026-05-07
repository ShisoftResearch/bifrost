use super::*;
use crate::raft::state_machine::callback::client::SubscriptionService;
use crate::raft::state_machine::callback::SubKey;
use crate::raft::state_machine::configs::commands::{
    del_member_ as conf_del_member, member_address as conf_member_address,
    new_member_ as conf_new_member, subscribe as conf_subscribe, unsubscribe as conf_unsubscribe,
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
use std::time::Duration;
use tokio::time::sleep;

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

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::LeaderIdValid => write!(f, "leader id is invalid"),
            ClientError::ServerUnreachable => write!(f, "seed nodes are unreachable"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug)]
pub enum SubscriptionError {
    RemoteError,
    SubServiceNotSet,
    CannotFindSubId,
}

struct PlaneClientState {
    pos: AtomicU64,
    leader_id: AtomicU64,
    last_log_id: AtomicU64,
    last_log_term: AtomicU64,
}

struct Members {
    clients: BTreeMap<u64, Client>,
    id_map: HashMap<u64, String>,
}

pub trait AsRaftPlaneClient: Send + Sync {
    fn as_raft_plane_client(self: &Arc<Self>) -> Arc<RaftPlaneClient>;
}

#[derive(Clone)]
pub struct RaftPlaneClient {
    client: Arc<RaftClient>,
    plane_id: PlaneId,
}

impl RaftPlaneClient {
    pub fn plane_id(&self) -> PlaneId {
        self.plane_id
    }

    pub async fn execute<R, M>(&self, sm_id: u64, msg: M) -> Result<R, ExecError>
    where
        R: 'static,
        M: RaftMsg<R> + 'static,
    {
        self.client
            .execute_on_plane(self.plane_id, sm_id, msg)
            .await
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
        self.client
            .subscribe_on_plane(self.plane_id, sm_id, msg, f)
            .await
    }

    pub async fn unsubscribe(
        &self,
        receipt: SubscriptionReceipt,
    ) -> Result<Result<(), SubscriptionError>, ExecError> {
        self.client.unsubscribe(receipt).await
    }

    pub async fn cluster_info(&self) -> Result<ClientClusterInfo, ExecError> {
        self.client.cluster_info_on_plane(self.plane_id).await
    }

    pub async fn have_state_machine(&self, sm_id: u64) -> Result<bool, ExecError> {
        self.client
            .have_state_machine_on_plane(self.plane_id, sm_id)
            .await
    }
}

pub struct RaftClient {
    members: RwLock<Members>,
    type1_state: Arc<PlaneClientState>,
    plane_states: RwLock<HashMap<PlaneId, Arc<PlaneClientState>>>,
    service_id: u64,
}

impl AsRaftPlaneClient for RaftClient {
    fn as_raft_plane_client(self: &Arc<Self>) -> Arc<RaftPlaneClient> {
        self.plane(PlaneId::type1())
    }
}

impl AsRaftPlaneClient for RaftPlaneClient {
    fn as_raft_plane_client(self: &Arc<Self>) -> Arc<RaftPlaneClient> {
        self.clone()
    }
}

impl RaftClient {
    fn new_plane_state() -> Arc<PlaneClientState> {
        Arc::new(PlaneClientState {
            pos: AtomicU64::new(rand::random::<u64>()),
            leader_id: AtomicU64::new(0),
            last_log_id: AtomicU64::new(0),
            last_log_term: AtomicU64::new(0),
        })
    }

    pub async fn new(servers: &Vec<String>, service_id: u64) -> Result<Arc<Self>, ClientError> {
        let client = RaftClient {
            members: RwLock::new(Members {
                clients: BTreeMap::new(),
                id_map: HashMap::new(),
            }),
            type1_state: Self::new_plane_state(),
            plane_states: RwLock::new(HashMap::new()),
            service_id,
        };
        client.update_info(servers).await?;
        Ok(Arc::new(client))
    }

    async fn plane_state(&self, plane_id: PlaneId) -> Arc<PlaneClientState> {
        if plane_id.is_type1() {
            return self.type1_state.clone();
        }

        {
            let states = self.plane_states.read().await;
            if let Some(state) = states.get(&plane_id) {
                return state.clone();
            }
        }

        let mut states = self.plane_states.write().await;
        states
            .entry(plane_id)
            .or_insert_with(Self::new_plane_state)
            .clone()
    }

    pub fn plane(self: &Arc<Self>, plane_id: PlaneId) -> Arc<RaftPlaneClient> {
        Arc::new(RaftPlaneClient {
            client: self.clone(),
            plane_id,
        })
    }

    pub fn type1(self: &Arc<Self>) -> Arc<RaftPlaneClient> {
        self.plane(PlaneId::type1())
    }

    pub async fn add_root_member(&self, address: &String) -> Result<bool, ExecError> {
        self.execute(CONFIG_SM_ID, conf_new_member::new(address))
            .await
    }

    pub async fn remove_root_member(&self, address: &String) -> Result<(), ExecError> {
        self.execute(CONFIG_SM_ID, conf_del_member::new(address))
            .await
    }

    pub async fn root_member_addresses(&self) -> Result<Vec<String>, ExecError> {
        self.execute(CONFIG_SM_ID, conf_member_address::new()).await
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

    async fn cluster_info<'a>(
        &'a self,
        plane_id: PlaneId,
        servers: &Vec<String>,
    ) -> Option<ClientClusterInfo> {
        debug!(
            "Getting server info for plane {} from {:?}",
            plane_id.raw(),
            servers
        );
        let mut attempt_remains: i32 = 10;
        loop {
            debug!(
                "Trying to get cluster info for plane {}, attempt from {:?}...{}",
                plane_id.raw(),
                servers,
                attempt_remains
            );
            let mut futs: FuturesUnordered<_> = servers
                .iter()
                .map(|server_addr| {
                    let id = hash_str(server_addr);
                    let server_addr = server_addr.clone();
                    async move {
                        let mut members = self.members.write().await;
                        debug!(
                            "Checking server info for plane {} on {}",
                            plane_id.raw(),
                            server_addr
                        );
                        if !members.clients.contains_key(&id) {
                            debug!(
                                "Connecting to node {} for plane {}",
                                server_addr,
                                plane_id.raw()
                            );
                            match rpc::DEFAULT_CLIENT_POOL.get(&server_addr).await {
                                Ok(client) => {
                                    debug!(
                                        "Added server info on {} to members for plane {}",
                                        server_addr,
                                        plane_id.raw()
                                    );
                                    members.clients.insert(
                                        id,
                                        AsyncServiceClient::new_with_service_id(
                                            self.service_id,
                                            &client,
                                        ),
                                    );
                                    debug!(
                                        "Member {} added for plane {}",
                                        server_addr,
                                        plane_id.raw()
                                    );
                                }
                                Err(e) => {
                                    warn!(
                                        "Cannot find server info for plane {} from {}, {}",
                                        plane_id.raw(),
                                        server_addr,
                                        e
                                    );
                                    return None;
                                }
                            }
                        }
                        debug!(
                            "Getting server info for plane {} from {}, id {}",
                            plane_id.raw(),
                            server_addr,
                            id
                        );
                        let member_client = match members.clients.get(&id) {
                            Some(client) => client,
                            None => {
                                debug!(
                                    "Server not found for plane {}, skip {}, id {}",
                                    plane_id.raw(),
                                    server_addr,
                                    id
                                );
                                return None;
                            }
                        };
                        debug!(
                            "Invoking server_cluster_info for plane {} on {}, id {}",
                            plane_id.raw(),
                            server_addr,
                            id
                        );
                        let info_res = member_client.c_server_cluster_info(plane_id).await;
                        debug!(
                            "Checking cluster info response for plane {} from {}",
                            plane_id.raw(),
                            server_addr
                        );
                        return match info_res {
                            Ok(info) => {
                                if info.leader_id != 0 {
                                    debug!(
                                        "Found server info for plane {} with leader id {}",
                                        plane_id.raw(),
                                        info.leader_id
                                    );
                                    Some(info)
                                } else {
                                    debug!(
                                        "Discovered zero leader id for plane {} from {}",
                                        plane_id.raw(),
                                        server_addr
                                    );
                                    None
                                }
                            }
                            Err(e) => {
                                debug!(
                                    "Error on getting cluster info for plane {} from {}, {:?}",
                                    plane_id.raw(),
                                    server_addr,
                                    e
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
                    "Plane {} fail attempt had zero leader id, retry...{}",
                    plane_id.raw(),
                    attempt_remains
                );
                let delay_sec = 1 + (rand::random::<u64>() % 9);
                sleep(Duration::from_secs(delay_sec)).await;
                attempt_remains -= 1;
                continue;
            } else {
                debug!(
                    "Continuously getting zero leader id for plane {}, give up",
                    plane_id.raw()
                );
                break;
            }
        }
        warn!(
            "Cannot find anything useful for plane {} from list: {:?}",
            plane_id.raw(),
            servers
        );
        return None;
    }

    async fn update_info(&self, servers: &Vec<String>) -> Result<(), ClientError> {
        debug!(
            "Updating cluster info for plane {} from servers: {:?}",
            PlaneId::type1().raw(),
            servers
        );
        let cluster_info = self.cluster_info(PlaneId::type1(), servers).await;
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
                    warn!(
                        "Removed server with id {} while refreshing plane {}",
                        id,
                        PlaneId::type1().raw()
                    );
                    members.clients.remove(id);
                }
                for id in remote_ids.difference(&connected_ids) {
                    let addr = match members.id_map.get(id) {
                        Some(addr) => addr.clone(),
                        None => {
                            error!(
                                "Cannot find address for server id {} while refreshing plane {}",
                                id,
                                PlaneId::type1().raw()
                            );
                            continue;
                        }
                    };
                    if !members.clients.contains_key(id) {
                        if let Ok(client) = rpc::DEFAULT_CLIENT_POOL.get(&addr).await {
                            info!(
                                "Having new server addr {} id {} for plane {}",
                                addr,
                                id,
                                PlaneId::type1().raw()
                            );
                            members.clients.insert(
                                *id,
                                AsyncServiceClient::new_with_service_id(self.service_id, &client),
                            );
                        } else {
                            error!(
                                "Cannot connect to new server addr {}, id {} for plane {}",
                                addr,
                                id,
                                PlaneId::type1().raw()
                            );
                        }
                    }
                }
                info!(
                    "UPDATE_INFO Setting plane {} leader to {}, was {}",
                    PlaneId::type1().raw(),
                    info.leader_id,
                    self.type1_state.leader_id.load(Relaxed)
                );
                self.type1_state.leader_id.store(info.leader_id, ORDERING);
                swap_when_greater(&self.type1_state.last_log_id, info.last_log_id);
                swap_when_greater(&self.type1_state.last_log_term, info.last_log_term);
                Ok(())
            }
            None => {
                error!(
                    "Cannot update info for plane {}, cannot get cluster info",
                    PlaneId::type1().raw()
                );
                Err(ClientError::ServerUnreachable)
            }
        }
    }

    async fn update_plane_info(
        &self,
        plane_id: PlaneId,
        servers: &Vec<String>,
    ) -> Result<(), ClientError> {
        if plane_id.is_type1() {
            return self.update_info(servers).await;
        }

        let cluster_info = self.cluster_info(plane_id, servers).await;
        match cluster_info {
            Some(info) => {
                let state = self.plane_state(plane_id).await;
                info!(
                    "UPDATE_INFO Setting plane {} leader to {}, was {}",
                    plane_id.raw(),
                    info.leader_id,
                    state.leader_id.load(Relaxed)
                );
                state.leader_id.store(info.leader_id, ORDERING);
                swap_when_greater(&state.last_log_id, info.last_log_id);
                swap_when_greater(&state.last_log_term, info.last_log_term);
                Ok(())
            }
            None => {
                error!(
                    "Cannot update info for plane {}, cannot get cluster info",
                    plane_id.raw()
                );
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
                        Ok(client) => ImmeServiceClient::c_ping(service_id, &client).await.is_ok(),
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
        self.execute_on_plane(PlaneId::type1(), sm_id, msg).await
    }

    pub async fn execute_on_plane<R, M>(
        &self,
        plane_id: PlaneId,
        sm_id: u64,
        msg: M,
    ) -> Result<R, ExecError>
    where
        R: 'static,
        M: RaftMsg<R> + 'static,
    {
        let (fn_id, op, req_data) = msg.encode();
        let response = match op {
            OpType::QUERY => self.query_on_plane(plane_id, sm_id, fn_id, req_data).await,
            OpType::COMMAND | OpType::SUBSCRIBE => {
                self.command_on_plane(plane_id, sm_id, fn_id, req_data)
                    .await
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

    pub async fn can_callback() -> bool {
        CALLBACK.read().await.is_some()
    }
    fn get_sub_key<M, R>(&self, plane_id: PlaneId, sm_id: u64, msg: M) -> SubKey
    where
        M: RaftMsg<R> + 'static,
        R: 'static,
    {
        let raft_sid = self.service_id;
        let (fn_id, pattern_id) = {
            let (fn_id, _, pattern_data) = msg.encode();
            (fn_id, hash_bytes(pattern_data.as_slice()))
        };
        SubKey::new(raft_sid, plane_id, sm_id, fn_id, pattern_id)
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
        self.subscribe_on_plane(PlaneId::type1(), sm_id, msg, f)
            .await
    }

    pub async fn subscribe_on_plane<M, R, F>(
        &self,
        plane_id: PlaneId,
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
        let key = self.get_sub_key(plane_id, sm_id, msg);
        let wrapper_fn =
            move |data: Vec<u8>| -> BoxFuture<'static, ()> { f(M::decode_return(&data)).boxed() };
        let cluster_subs = self
            .execute_on_plane(
                plane_id,
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
                    .execute_on_plane(key.plane_id, CONFIG_SM_ID, conf_unsubscribe::new(&sub_id))
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

    async fn query_on_plane(
        &self,
        plane_id: PlaneId,
        sm_id: u64,
        fn_id: u64,
        data: Vec<u8>,
    ) -> Result<ExecResult, ExecError> {
        let state = self.plane_state(plane_id).await;
        let mut depth = 0;
        loop {
            if depth == 0 {
                trace!(
                    "Raft client query plane_id={} sm_id {}, fn_id {}",
                    plane_id.raw(),
                    sm_id,
                    fn_id
                );
            } else {
                warn!(
                    "Retry client query plane_id={} sm_id {}, fn_id {}",
                    plane_id.raw(),
                    sm_id,
                    fn_id
                );
            }
            let pos = state.pos.fetch_add(1, ORDERING);
            let members = self.members.read().await;
            let num_members = members.clients.len();
            if num_members >= 1 {
                let node_index = pos as usize % num_members;
                let rpc_client = match members.clients.values().nth(node_index) {
                    Some(client) => client,
                    None => {
                        error!(
                            "Cannot find client for plane {} at index {} (total: {})",
                            plane_id.raw(),
                            node_index,
                            num_members
                        );
                        return Err(ExecError::ServersUnreachable);
                    }
                };
                trace!(
                    "Query for plane {} from node {} for sm_id {}, fn_id {}",
                    plane_id.raw(),
                    node_index,
                    sm_id,
                    fn_id
                );
                let res = rpc_client
                    .c_query(plane_id, &self.gen_log_entry(&state, sm_id, fn_id, &data))
                    .await;
                trace!(
                    "Query for plane {} from node {} for sm_id {}, fn_id {} completed",
                    plane_id.raw(),
                    node_index,
                    sm_id,
                    fn_id
                );
                match res {
                    Ok(res) => match res {
                        ClientQryResponse::LeftBehind {
                            last_log_term,
                            last_log_id,
                        } => {
                            debug!("Found left behind record on plane {}...{}, updating client state: server has log_id={}, term={}", plane_id.raw(), depth, last_log_id, last_log_term);
                            // Update client state from server to avoid retry loop
                            swap_when_greater(&state.last_log_id, last_log_id);
                            swap_when_greater(&state.last_log_term, last_log_term);
                            // Add a small delay to allow server to catch up if under stress
                            if depth > 0 {
                                sleep(Duration::from_millis(50)).await;
                            }
                            if depth >= num_members {
                                error!("Too many retry on query for plane {}, num_members {}, due to left behind record {}", plane_id.raw(), num_members, depth);
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
                            swap_when_greater(&state.last_log_id, last_log_id);
                            swap_when_greater(&state.last_log_term, last_log_term);
                            if depth > 0 {
                                warn!("Retry successful on plane {}...{}", plane_id.raw(), depth);
                            }
                            trace!("Query for plane {} from node {} for sm_id {}, fn_id {}, successful at log id {}, term {}", plane_id.raw(), node_index, sm_id, fn_id, last_log_id, last_log_term);
                            return Ok(data);
                        }
                    },
                    Err(e) => {
                        error!(
                            "Got unknown error on query for plane {}: {:?}, server {}",
                            plane_id.raw(),
                            e,
                            rpc_client.client.address
                        );
                        if depth >= num_members {
                            return Err(ExecError::Unknown);
                        } else {
                            debug!("Retry query on plane {}...{}", plane_id.raw(), depth);
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

    async fn command_on_plane(
        &self,
        plane_id: PlaneId,
        sm_id: u64,
        fn_id: u64,
        data: Vec<u8>,
    ) -> Result<ExecResult, ExecError> {
        const NOT_COMMITTED_RETRY_DELAY_MS: u64 = 10;
        const UPDATE_INFO_RETRY_DELAY_MS: u64 = 10;
        let not_committed_retry_limit = std::cmp::max(
            64,
            ((HEARTBEAT_MS * 2) / NOT_COMMITTED_RETRY_DELAY_MS as i64) as i32,
        );
        let update_info_retry_limit = std::cmp::max(
            64,
            ((HEARTBEAT_MS * 2) / UPDATE_INFO_RETRY_DELAY_MS as i64) as i32,
        );

        enum FailureAction {
            SwitchLeader,
            NotCommitted,
            UpdateInfo,
            NotLeader,
            ShuttingDown,
        }
        let state = self.plane_state(plane_id).await;
        let mut leader_retry_depth = 0;
        let mut not_committed_depth = 0;
        let mut update_info_depth = 0;
        loop {
            let failure = {
                if leader_retry_depth > 0 {
                    let members = self.members.read().await;
                    let num_members = members.clients.len();
                    if leader_retry_depth >= max(num_members + 1, 5) {
                        error!(
                            "Too many retry on command for plane {}, num_members {}, due to leader retry attempts {}",
                            plane_id.raw(), num_members,
                            leader_retry_depth
                        );
                        return Err(ExecError::TooManyRetry);
                    };
                }
                match self.preferred_client_on_plane(&state).await {
                    Some((leader_id, client)) => {
                        let cmd_res = client
                            .c_command(plane_id, self.gen_log_entry(&state, sm_id, fn_id, &data))
                            .await;
                        match cmd_res {
                            Ok(ClientCmdResponse::Success {
                                data,
                                last_log_term,
                                last_log_id,
                            }) => {
                                swap_when_greater(&state.last_log_id, last_log_id);
                                swap_when_greater(&state.last_log_term, last_log_term);
                                return Ok(data);
                            }
                            Ok(ClientCmdResponse::NotLeader(new_leader_id)) => {
                                if new_leader_id == 0 || new_leader_id == leader_id {
                                    warn!(
                                        "RAFTDBG_V2 client plane_id={} notleader-zero_or_same leader_id={} suggested={} depth={} update_info_depth={} not_committed_depth={}",
                                        plane_id.raw(), leader_id,
                                        new_leader_id,
                                        leader_retry_depth,
                                        update_info_depth,
                                        not_committed_depth
                                    );
                                    debug!(
                                        "CLIENT plane_id={}: NOT LEADER, SUGGESTION NOT USEFUL, REFRESH INFO. GOT: {}",
                                        plane_id.raw(), new_leader_id
                                    );
                                    FailureAction::UpdateInfo
                                } else {
                                    warn!(
                                        "RAFTDBG_V3 client plane_id={} notleader-redirect current_leader={} suggested_leader={} depth={}",
                                        plane_id.raw(), leader_id,
                                        new_leader_id,
                                        leader_retry_depth
                                    );
                                    debug!(
                                        "CLIENT plane_id={}: NOT LEADER, REMOTE SUGGEST SWITCH TO {}",
                                        plane_id.raw(), new_leader_id
                                    );
                                    info!(
                                        "CMD Setting plane {} leader to {}, was {}",
                                        plane_id.raw(),
                                        new_leader_id,
                                        state.leader_id.load(Relaxed)
                                    );
                                    state.leader_id.store(new_leader_id, ORDERING);
                                    FailureAction::NotLeader
                                }
                            }
                            Ok(ClientCmdResponse::NotCommitted {
                                last_log_term,
                                last_log_id,
                            }) => {
                                debug!(
                                    "CLIENT plane_id={}: NOT COMMITTED at leader {}, refreshing client log cursor to term {}, id {}",
                                    plane_id.raw(), leader_id,
                                    last_log_term,
                                    last_log_id
                                );
                                swap_when_greater(&state.last_log_id, last_log_id);
                                swap_when_greater(&state.last_log_term, last_log_term);
                                FailureAction::NotCommitted
                            }
                            Ok(ClientCmdResponse::ShuttingDown) => FailureAction::ShuttingDown,
                            Err(e) => {
                                warn!(
                                    "RAFTDBG_V3 client plane_id={} transport_or_rpc_error leader_id={} depth={} error={:?}",
                                    plane_id.raw(), leader_id,
                                    leader_retry_depth,
                                    e
                                );
                                debug!(
                                    "CLIENT plane_id={}: ERROR - {} - {:?}",
                                    plane_id.raw(),
                                    leader_id,
                                    e
                                );
                                FailureAction::SwitchLeader // need switch server for leader
                            }
                        }
                    }
                    None => {
                        warn!("Need update members for plane {}", plane_id.raw());
                        FailureAction::UpdateInfo
                    }
                }
            }; //
            match failure {
                FailureAction::NotCommitted => {
                    not_committed_depth += 1;
                    update_info_depth = 0;
                    if not_committed_depth >= not_committed_retry_limit {
                        error!(
                            "Too many retry on command for plane {} due to NotCommitted responses {}",
                            plane_id.raw(), not_committed_depth
                        );
                        return Err(ExecError::TooManyRetry);
                    }
                    debug!(
                        "Retrying command for plane {} after NotCommitted response {}/{}",
                        plane_id.raw(),
                        not_committed_depth,
                        not_committed_retry_limit
                    );
                    sleep(Duration::from_millis(NOT_COMMITTED_RETRY_DELAY_MS)).await;
                    continue;
                }
                FailureAction::UpdateInfo => {
                    update_info_depth += 1;
                    not_committed_depth = 0;
                    warn!(
                        "RAFTDBG_V2 client plane_id={} update_info_retry count={} depth={}",
                        plane_id.raw(),
                        update_info_depth,
                        leader_retry_depth
                    );
                    if update_info_depth >= update_info_retry_limit {
                        error!(
                            "Too many retry on command for plane {} due to cluster-info refresh attempts {}",
                            plane_id.raw(), update_info_depth
                        );
                        return Err(ExecError::TooManyRetry);
                    }
                    let servers = {
                        let members = self.members.read().await;
                        Vec::from_iter(members.id_map.values().cloned())
                    };
                    if servers.is_empty() {
                        warn!(
                            "Cannot refresh cluster info for plane {}: no known servers",
                            plane_id.raw()
                        );
                        return Err(ExecError::ServersUnreachable);
                    }
                    debug!(
                        "Refreshing cluster info for plane {} after transient NotLeader/leader-miss {}/{} from {:?}",
                        plane_id.raw(), update_info_depth,
                        update_info_retry_limit,
                        servers
                    );
                    if let Err(e) = self.update_plane_info(plane_id, &servers).await {
                        warn!(
                            "Failed to refresh cluster info for plane {} during command retry: {:?}",
                            plane_id.raw(), e
                        );
                    }
                    sleep(Duration::from_millis(UPDATE_INFO_RETRY_DELAY_MS)).await;
                    continue;
                }
                FailureAction::SwitchLeader => {
                    not_committed_depth = 0;
                    update_info_depth = 0;
                    leader_retry_depth += 1;
                    warn!(
                        "RAFTDBG_V3 client plane_id={} switch_leader depth={}",
                        plane_id.raw(),
                        leader_retry_depth
                    );
                    debug!("Switch leader for plane {} by probing", plane_id.raw());
                    let members = self.members.read().await;
                    let num_members = members.clients.len();
                    let leader_id = state.leader_id.load(ORDERING);
                    let new_leader_id = match members
                        .clients
                        .keys()
                        .nth(leader_retry_depth as usize % num_members)
                    {
                        Some(id) => *id,
                        None => {
                            error!(
                                "Cannot find new leader for plane {} at index {} (total: {})",
                                plane_id.raw(),
                                leader_retry_depth as usize % num_members,
                                num_members
                            );
                            return Err(ExecError::ServersUnreachable);
                        }
                    };
                    let leadder_switch = state.leader_id.compare_exchange(
                        leader_id,
                        new_leader_id,
                        ORDERING,
                        Relaxed,
                    );
                    info!(
                        "SWITCH plane {} exchange leader to {}, was {:?}",
                        plane_id.raw(),
                        new_leader_id,
                        leadder_switch
                    );
                    debug!(
                        "CLIENT plane_id={}: Switch leader {}",
                        plane_id.raw(),
                        new_leader_id
                    );
                }
                FailureAction::NotLeader => {
                    leader_retry_depth += 1;
                    not_committed_depth = 0;
                    update_info_depth = 0;
                    continue;
                }
                FailureAction::ShuttingDown => {
                    return Err(ExecError::ShuttingDown);
                }
            }
        }
    }

    fn gen_log_entry(
        &self,
        state: &PlaneClientState,
        sm_id: u64,
        fn_id: u64,
        data: &Vec<u8>,
    ) -> LogEntry {
        LogEntry {
            id: state.last_log_id.load(ORDERING),
            term: state.last_log_term.load(ORDERING),
            sm_id,
            fn_id,
            data: data.clone(),
        }
    }

    pub fn leader_id(&self) -> u64 {
        self.type1_state.leader_id.load(ORDERING)
    }

    pub async fn leader_client(&self) -> Option<(u64, Client)> {
        self.current_leader_client_on_plane(PlaneId::type1(), &self.type1_state)
            .await
    }

    async fn any_known_client(&self) -> Option<Client> {
        let members = self.members.read().await;
        members.clients.values().next().cloned()
    }

    async fn preferred_client_on_plane(&self, state: &PlaneClientState) -> Option<(u64, Client)> {
        if let Some((leader_id, client)) = self.leader_client_on_plane(state).await {
            return Some((leader_id, client));
        }

        self.any_known_client().await.map(|client| (0, client))
    }

    async fn leader_client_on_plane(&self, state: &PlaneClientState) -> Option<(u64, Client)> {
        let members = self.members.read().await;
        let leader_id = state.leader_id.load(ORDERING);
        if let Some(client) = members.clients.get(&leader_id) {
            Some((leader_id, client.clone()))
        } else {
            None
        }
    }

    async fn current_leader_client_on_plane(
        &self,
        plane_id: PlaneId,
        state: &PlaneClientState,
    ) -> Option<(u64, Client)> {
        {
            let leader_client = self.leader_client_on_plane(state).await;
            if leader_client.is_some() {
                return leader_client;
            }
        }
        debug!(
            "Obtaining leader client for plane {} by updating cluster info",
            plane_id.raw()
        );
        {
            let servers = {
                let members = self.members.read().await;
                Vec::from_iter(members.id_map.values().cloned())
            };
            if let Err(e) = self.update_plane_info(plane_id, &servers).await {
                error!(
                    "Failed to update cluster info for plane {}: {:?}",
                    plane_id.raw(),
                    e
                );
                return None;
            }
            let leader_id = state.leader_id.load(ORDERING);
            let members = self.members.read().await;
            if let Some(client) = members.clients.get(&leader_id) {
                debug!(
                    "Obtained leader client for plane {} with id: {}",
                    plane_id.raw(),
                    leader_id
                );
                Some((leader_id, client.clone()))
            } else {
                warn!(
                    "Cannot obtain leader client for plane {} with id {}. Having {:?}",
                    plane_id.raw(),
                    leader_id,
                    members.clients.keys().collect::<Vec<_>>()
                );
                None
            }
        }
    }
    pub async fn current_leader_rpc_client(&self) -> Result<Arc<rpc::RPCClient>, ()> {
        let (_, client) = self
            .current_leader_client_on_plane(PlaneId::type1(), &self.type1_state)
            .await
            .ok_or_else(|| ())?;
        Ok(client.client.clone())
    }

    pub async fn cluster_info_on_plane(
        &self,
        plane_id: PlaneId,
    ) -> Result<ClientClusterInfo, ExecError> {
        let state = self.plane_state(plane_id).await;
        let client = match self.leader_client_on_plane(&state).await {
            Some((_, client)) => client,
            None => self
                .any_known_client()
                .await
                .ok_or(ExecError::ServersUnreachable)?,
        };
        client
            .c_server_cluster_info(plane_id)
            .await
            .map_err(|_| ExecError::ServersUnreachable)
    }

    pub async fn have_state_machine_on_plane(
        &self,
        plane_id: PlaneId,
        sm_id: u64,
    ) -> Result<bool, ExecError> {
        let state = self.plane_state(plane_id).await;
        let client = match self.leader_client_on_plane(&state).await {
            Some((_, client)) => client,
            None => self
                .any_known_client()
                .await
                .ok_or(ExecError::ServersUnreachable)?,
        };
        client
            .c_have_state_machine(plane_id, sm_id)
            .await
            .map_err(|_| ExecError::ServersUnreachable)
    }
}

fn swap_when_greater(atomic: &AtomicU64, value: u64) {
    let mut orig_num = atomic.load(ORDERING);
    loop {
        if orig_num >= value {
            return;
        }
        match atomic.compare_exchange(orig_num, value, ORDERING, Relaxed) {
            Ok(_) => {
                return;
            }
            Err(actual) => {
                orig_num = actual;
            }
        }
    }
}

pub struct CachedStateMachine<T: StateMachineClient> {
    server_list: Vec<String>,
    raft_service_id: u64,
    plane_id: PlaneId,
    state_machine_id: u64,
    cache: RwLock<Option<Arc<T>>>,
}

impl<T: StateMachineClient> CachedStateMachine<T> {
    pub fn new(
        server_list: &Vec<String>,
        raft_service_id: u64,
        plane_id: PlaneId,
        state_machine_id: u64,
    ) -> Self {
        debug!(
            "Construct cached state machine for list {:?}, service id {}, plane {}, state machine {}",
            server_list,
            raft_service_id,
            plane_id.raw(),
            state_machine_id
        );
        Self {
            server_list: server_list.clone(),
            raft_service_id,
            plane_id,
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
                    let raft_client =
                        match RaftClient::new(&self.server_list, self.raft_service_id).await {
                            Ok(client) => client,
                            Err(e) => {
                                error!(
                                    "Failed to create RaftClient for service {} and sm {}: {:?}",
                                    self.raft_service_id, self.state_machine_id, e
                                );
                                // Drop the lock and retry after a delay
                                drop(place_holder);
                                sleep(Duration::from_millis(100)).await;
                                continue;
                            }
                        };
                    let plane_client = raft_client.plane(self.plane_id);
                    // Create a client for the state machine on the raft service
                    *place_holder = Some(Arc::new(T::new_instance(
                        self.state_machine_id,
                        &plane_client,
                    )))
                }
            }
        }
    }
}
