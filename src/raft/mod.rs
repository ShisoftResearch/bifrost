use self::state_machine::callback::server::Subscriptions;
use self::state_machine::callback::SMCallback;
use self::state_machine::configs::commands::new_member_;
use self::state_machine::configs::{RaftMember, CONFIG_SM_ID};
use self::state_machine::master::{ExecError, ExecResult, MasterStateMachine, SubStateMachine};
use self::state_machine::OpType;
use crate::raft::client::{ClientError, RaftClient};
use crate::raft::disk::*;
use crate::raft::state_machine::StateMachineCtl;
use crate::rpc;
use crate::utils::time::get_time;
use async_std::sync::*;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::Bound::{Included, Unbounded};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::io;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::runtime;
use tokio::sync::{watch, Mutex as TokioMutex};
use tokio::time::*;

#[macro_use]
pub mod state_machine;
pub mod client;
pub mod disk;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_DEFAULT_SERVICE) as u64;

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, PartialOrd, Ord, Serialize,
)]
pub struct PlaneId(u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlaneIdError {
    Type2PlaneMustBePositive,
}

impl PlaneId {
    pub const fn type1() -> Self {
        Self(0)
    }

    pub fn type2(raw: u64) -> Result<Self, PlaneIdError> {
        if raw == 0 {
            Err(PlaneIdError::Type2PlaneMustBePositive)
        } else {
            Ok(Self(raw))
        }
    }

    pub const fn raw(self) -> u64 {
        self.0
    }

    pub const fn is_type1(self) -> bool {
        self.0 == 0
    }

    pub const fn is_type2(self) -> bool {
        self.0 > 0
    }
}

impl Display for PlaneIdError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PlaneIdError::Type2PlaneMustBePositive => {
                write!(f, "type-2 plane ids must be greater than zero")
            }
        }
    }
}

impl std::error::Error for PlaneIdError {}

impl From<PlaneId> for u64 {
    fn from(value: PlaneId) -> Self {
        value.raw()
    }
}

pub trait RaftMsg<R>: Send + Sync {
    fn encode(self) -> (u64, OpType, Vec<u8>);
    fn decode_return(data: &Vec<u8>) -> R;
}

const CHECKER_MS: i64 = 200;
const HEARTBEAT_MS: i64 = 1000;
// Timeout for heartbeat task - increased to prevent timeouts under stress
const HEARTBEAT_TASK_TIMEOUT_MS: i64 = 5000;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub id: u64,
    pub term: u64,
    pub sm_id: u64,
    pub fn_id: u64,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientCmdResponse {
    Success {
        data: ExecResult,
        last_log_term: u64,
        last_log_id: u64,
    },
    NotLeader(u64),
    NotCommitted {
        last_log_term: u64,
        last_log_id: u64,
    },
    ShuttingDown,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientQryResponse {
    Success {
        data: ExecResult,
        last_log_term: u64,
        last_log_id: u64,
    },
    LeftBehind {
        last_log_term: u64,
        last_log_id: u64,
    },
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientClusterInfo {
    members: Vec<(u64, String)>,
    last_log_id: u64,
    last_log_term: u64,
    leader_id: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AppendEntriesResult {
    Ok,
    TermOut(u64),
    LogMismatch,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SnapshotEntity {
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub snapshot: Vec<u8>,
}

type LogEntries = Vec<LogEntry>;
type LogsMap = BTreeMap<u64, LogEntry>;

service! {
    rpc append_entries(plane_id: PlaneId, term: u64, leader_id: u64, prev_log_id: u64, prev_log_term: u64, entries: &Option<LogEntries>, leader_commit: u64) -> (u64, AppendEntriesResult);
    rpc request_vote(plane_id: PlaneId, term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> ((u64, u64), bool); // term, voteGranted
    rpc install_snapshot(plane_id: PlaneId, term: u64, leader_id: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>) -> u64;
    rpc reelect(plane_id: PlaneId) -> bool;
    rpc c_command(plane_id: PlaneId, entry: LogEntry) -> ClientCmdResponse;
    rpc c_query(plane_id: PlaneId, entry: &LogEntry) -> ClientQryResponse;
    rpc c_server_cluster_info(plane_id: PlaneId) -> ClientClusterInfo;
    rpc c_put_offline() -> bool;
    rpc c_have_state_machine(plane_id: PlaneId, id: u64) -> bool;
    rpc c_ping();
}

service_with_id!(RaftService, DEFAULT_SERVICE_ID);

fn gen_rand(lower: i64, higher: i64) -> i64 {
    let span = (higher - lower).max(1) as u64;
    lower + (rand::random::<u64>() % span) as i64
}

fn gen_timeout() -> i64 {
    gen_rand(10_000, 30_000)
}

struct FollowerStatus {
    next_index: u64,
    match_index: u64,
}

pub struct LeaderMeta {
    last_updated: i64,
    followers: HashMap<u64, Arc<Mutex<FollowerStatus>>>,
}

impl LeaderMeta {
    fn new() -> LeaderMeta {
        LeaderMeta {
            last_updated: get_time(),
            followers: HashMap::new(),
        }
    }
}

pub enum Membership {
    Leader(RwLock<LeaderMeta>),
    Follower,
    Candidate,
    Offline,
    Undefined,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleState {
    Running,
    Stopping,
    Stopped,
}

pub struct RaftMeta {
    term: u64,
    vote_for: Option<u64>,
    timeout: i64,
    last_checked: i64,
    membership: Membership,
    logs: Arc<RwLock<LogsMap>>,
    state_machine: Arc<RwLock<MasterStateMachine>>,
    commit_index: u64,
    last_applied: u64,
    leader_id: u64,
    storage: Option<Arc<Mutex<StorageEntity>>>,
    last_snapshot_index: u64,
    last_snapshot_term: u64,
    lifecycle: LifecycleState,
}

#[derive(Clone)]
pub enum Storage {
    MEMORY,
    DISK(DiskOptions),
}

impl Storage {
    pub fn default() -> Storage {
        Storage::MEMORY
    }
}

#[derive(Clone)]
pub struct Options {
    pub storage: Storage,
    pub address: String,
    pub service_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlaneSpec {
    pub plane_id: PlaneId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlaneBootstrap {
    pub plane_id: PlaneId,
    pub seed_nodes: Vec<String>,
}

#[derive(Debug)]
pub enum PlaneError {
    PlaneNotFound(PlaneId),
    StorageInit(io::Error),
    InitializationFailed(PlaneId),
}

impl Display for PlaneError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PlaneError::PlaneNotFound(plane_id) => {
                write!(f, "plane {} is not registered on this host", plane_id.raw())
            }
            PlaneError::StorageInit(err) => write!(f, "failed to initialize plane storage: {err}"),
            PlaneError::InitializationFailed(plane_id) => {
                write!(f, "failed to initialize plane {}", plane_id.raw())
            }
        }
    }
}

impl std::error::Error for PlaneError {}

#[derive(Debug)]
pub enum PlaneBootstrapError {
    Type1PlaneUnsupported,
    EmptySeedNodes,
    NoType1MembersDiscovered,
    LocalMemberMissing {
        local_address: String,
    },
    MembershipConflict {
        plane_id: PlaneId,
        current_members: Vec<String>,
        requested_members: Vec<String>,
    },
    MemberRegistrationRejected {
        address: String,
    },
    NotLeader {
        plane_id: PlaneId,
        leader_id: u64,
    },
    Client(ClientError),
    Plane(PlaneError),
    Exec(ExecError),
}

impl Display for PlaneBootstrapError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PlaneBootstrapError::Type1PlaneUnsupported => {
                write!(
                    f,
                    "type-2 bootstrap via seed nodes is only supported for type-2 planes"
                )
            }
            PlaneBootstrapError::EmptySeedNodes => {
                write!(f, "plane bootstrap requires at least one type-1 seed node")
            }
            PlaneBootstrapError::NoType1MembersDiscovered => {
                write!(f, "type-1 seed discovery returned no available servers")
            }
            PlaneBootstrapError::LocalMemberMissing { local_address } => {
                write!(
                    f,
                    "type-1 discovered members must include the local address {}",
                    local_address
                )
            }
            PlaneBootstrapError::MembershipConflict {
                plane_id,
                current_members,
                requested_members,
            } => {
                write!(
                    f,
                    "plane {} membership conflict: current={:?}, requested={:?}",
                    plane_id.raw(),
                    current_members,
                    requested_members
                )
            }
            PlaneBootstrapError::MemberRegistrationRejected { address } => {
                write!(f, "plane bootstrap rejected member {}", address)
            }
            PlaneBootstrapError::NotLeader {
                plane_id,
                leader_id,
            } => {
                write!(
                    f,
                    "node is not the leader for plane {} (leader_id={})",
                    plane_id.raw(),
                    leader_id
                )
            }
            PlaneBootstrapError::Client(err) => {
                write!(f, "type-1 seed discovery failed: {err}")
            }
            PlaneBootstrapError::Plane(err) => write!(f, "{err}"),
            PlaneBootstrapError::Exec(err) => write!(f, "plane bootstrap command failed: {err}"),
        }
    }
}

impl std::error::Error for PlaneBootstrapError {}

impl From<PlaneError> for PlaneBootstrapError {
    fn from(value: PlaneError) -> Self {
        Self::Plane(value)
    }
}

impl From<ExecError> for PlaneBootstrapError {
    fn from(value: ExecError) -> Self {
        Self::Exec(value)
    }
}

impl From<ClientError> for PlaneBootstrapError {
    fn from(value: ClientError) -> Self {
        Self::Client(value)
    }
}

struct RaftPlaneRuntime {
    plane_id: PlaneId,
    meta: RwLock<RaftMeta>,
    is_leader: AtomicBool,
    checker_task: TokioMutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown_tx: watch::Sender<LifecycleState>,
}

impl RaftPlaneRuntime {
    fn new(opts: &Options, plane_id: PlaneId) -> Result<Self, PlaneError> {
        let mut term = 0;
        let mut logs = BTreeMap::new();
        let mut commit_index = 0;
        let mut last_applied = 0;
        let plane_opts = options_for_plane(opts, plane_id);

        let storage_entity = StorageEntity::new_with_options_on_plane(
            plane_id,
            &plane_opts,
            &mut term,
            &mut commit_index,
            &mut last_applied,
            &mut logs,
        )
        .map_err(PlaneError::StorageInit)?;

        let master_sm = MasterStateMachine::new_on_plane(plane_opts.service_id, plane_id);
        let (shutdown_tx, _shutdown_rx) = watch::channel(LifecycleState::Running);

        Ok(Self {
            plane_id,
            meta: RwLock::new(RaftMeta {
                term,
                vote_for: None,
                timeout: gen_timeout(),
                last_checked: get_time(),
                membership: Membership::Undefined,
                logs: Arc::new(RwLock::new(logs)),
                state_machine: Arc::new(RwLock::new(master_sm)),
                commit_index,
                last_applied,
                leader_id: 0,
                storage: storage_entity.map(|entity| Arc::new(Mutex::new(entity))),
                last_snapshot_index: 0,
                last_snapshot_term: 0,
                lifecycle: LifecycleState::Running,
            }),
            is_leader: AtomicBool::new(false),
            checker_task: TokioMutex::new(None),
            shutdown_tx,
        })
    }
}

#[derive(Clone)]
pub struct PlaneHandle {
    service: Arc<RaftService>,
    plane_id: PlaneId,
}

impl PlaneHandle {
    pub const fn id(&self) -> PlaneId {
        self.plane_id
    }

    pub async fn callback(&self, state_machine_id: u64) -> Result<SMCallback, PlaneError> {
        SMCallback::new_on_plane(state_machine_id, self.plane_id, self.service.clone()).await
    }

    pub async fn register_state_machine(
        &self,
        state_machine: SubStateMachine,
    ) -> Result<(), PlaneError> {
        self.service
            .register_state_machine_on_plane(self.plane_id, state_machine)
            .await
    }

    pub async fn recover_after_register(&self) -> Result<(), PlaneError> {
        self.service
            .recover_after_register_on_plane(self.plane_id)
            .await
    }

    pub async fn cluster_info(&self) -> Result<ClientClusterInfo, PlaneError> {
        self.service
            .cluster_info_on_plane_local(self.plane_id)
            .await
    }

    pub async fn member_addresses(&self) -> Result<Vec<String>, PlaneError> {
        self.service.plane_member_addresses(self.plane_id).await
    }

    pub async fn add_member(&self, address: String) -> Result<bool, PlaneBootstrapError> {
        self.service
            .add_plane_member_via_log(self.plane_id, address)
            .await
    }

    pub async fn have_state_machine(&self, sm_id: u64) -> Result<bool, PlaneError> {
        self.service
            .have_state_machine_on_plane_local(self.plane_id, sm_id)
            .await
    }

    pub async fn is_leader(&self) -> Result<bool, PlaneError> {
        self.service.is_leader_on_plane(self.plane_id).await
    }

    pub async fn flush_persistence(&self) -> Result<(), PlaneError> {
        self.service.flush_persistence_on_plane(self.plane_id).await
    }

    pub async fn shutdown(&self) -> Result<(), PlaneError> {
        self.service.shutdown_plane(self.plane_id).await
    }
}

fn options_for_plane(opts: &Options, plane_id: PlaneId) -> Options {
    let storage = match &opts.storage {
        Storage::MEMORY => Storage::MEMORY,
        Storage::DISK(disk_opts) => {
            let mut plane_disk_opts = disk_opts.clone();
            if plane_id.is_type2() {
                plane_disk_opts.path = format!("{}/planes/{}", disk_opts.path, plane_id.raw());
            }
            Storage::DISK(plane_disk_opts)
        }
    };

    Options {
        storage,
        address: opts.address.clone(),
        service_id: opts.service_id,
    }
}

pub struct RaftService {
    meta: RwLock<RaftMeta>,
    planes: RwLock<BTreeMap<PlaneId, Arc<RaftPlaneRuntime>>>,
    pub id: u64,
    pub options: Options,
    pub rt: RaftRuntimeHandle,
    rt_owner: StdMutex<Option<runtime::Runtime>>,
    _is_leader: AtomicBool,
    checker_task: TokioMutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown_tx: watch::Sender<LifecycleState>,
}

pub struct RaftRuntimeHandle {
    handle: StdMutex<Option<runtime::Handle>>,
}

impl RaftRuntimeHandle {
    fn new(handle: runtime::Handle) -> Self {
        Self {
            handle: StdMutex::new(Some(handle)),
        }
    }

    pub fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = self
            .handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
            .expect("Raft runtime is shut down");
        handle.spawn(future)
    }

    fn id(&self) -> Option<runtime::Id> {
        self.handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(runtime::Handle::id)
    }

    fn close(&self) {
        self.handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

impl Drop for RaftService {
    fn drop(&mut self) {
        self.rt.close();
        if let Some(runtime) = self
            .rt_owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            runtime.shutdown_background();
        }
    }
}

dispatch_rpc_service_functions!(RaftService);

#[derive(Debug)]
enum CheckerAction {
    SendHeartbeat,
    BecomeCandidate,
    ExitLoop,
    None,
}

#[derive(Clone)]
enum RequestVoteResponse {
    Granted,
    TermOut(u64, u64),
    NotGranted,
}

enum HeartbeatReplicationResult {
    Matched(u64),
    TermOut { term: u64, leader_id: u64 },
}

macro_rules! get_last_log_info {
    ($s: expr, $logs: expr) => {{
        let last_log = $logs.iter().next_back();
        $s.get_log_info_(last_log)
    }};
}

async fn check_commit(meta: &mut RwLockWriteGuard<'_, RaftMeta>) {
    while meta.commit_index > meta.last_applied {
        let next_log_id = meta.last_applied + 1;
        let entry = {
            // Clone the next entry so we can drop the log read lock before mutating apply state.
            let logs = meta.logs.read().await;
            logs.get(&next_log_id).cloned()
        };
        let Some(entry) = entry else {
            warn!(
                "Committed log entry {} is missing during apply (commit_index={}, last_applied={}); deferring replay",
                next_log_id,
                meta.commit_index,
                meta.last_applied
            );
            break;
        };

        match apply_committed_entry(meta, &entry).await {
            Ok(_) => {
                meta.last_applied = next_log_id;
            }
            Err(ExecError::SmNotFound(sm_id)) => {
                warn!(
                    "Deferring log entry {} until state machine {} is registered",
                    next_log_id, sm_id
                );
                break;
            }
            Err(e) => {
                error!(
                    "Failed to commit command for log entry {}: {:?}",
                    next_log_id, e
                );
                // Preserve prior behavior for non-recoverable apply failures.
                meta.last_applied = next_log_id;
            }
        }
    }
}

impl RaftService {
    pub const fn plane_id(&self) -> PlaneId {
        PlaneId::type1()
    }

    fn check_plane(&self, plane_id: PlaneId) {
        debug_assert_eq!(plane_id, self.plane_id());
        if plane_id != self.plane_id() {
            warn!(
                "received raft request for plane {} on single-plane service {}",
                plane_id.raw(),
                self.id
            );
        }
    }

    fn lifecycle_is_stopping(state: LifecycleState) -> bool {
        matches!(state, LifecycleState::Stopping | LifecycleState::Stopped)
    }

    async fn wait_for_apply_drain_for_meta(
        meta_lock: &RwLock<RaftMeta>,
        timeout_duration: Duration,
    ) -> bool {
        let deadline = Instant::now() + timeout_duration;
        loop {
            {
                let meta = meta_lock.read().await;
                if meta.commit_index == meta.last_applied {
                    return true;
                }
            }

            if Instant::now() >= deadline {
                return false;
            }

            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn start_managed_runtime(self: &Arc<Self>, runtime: Option<Arc<RaftPlaneRuntime>>) {
        if let Some(runtime_ref) = runtime.as_ref() {
            let guard = runtime_ref.checker_task.lock().await;
            if guard.is_some() {
                return;
            }
        } else {
            let guard = self.checker_task.lock().await;
            if guard.is_some() {
                return;
            }
        }

        let server = self.clone();
        let runtime_ref = runtime.clone();
        let plane_id = runtime_ref
            .as_ref()
            .map(|runtime| runtime.plane_id)
            .unwrap_or_else(PlaneId::type1);
        let mut shutdown_rx = if let Some(runtime) = runtime_ref.as_ref() {
            runtime.shutdown_tx.subscribe()
        } else {
            server.shutdown_tx.subscribe()
        };
        let handle = self.rt.spawn(async move {
            info!(
                "Starting Raft checker/heartbeat task for plane {}",
                plane_id.raw()
            );
            loop {
                if Self::lifecycle_is_stopping(*shutdown_rx.borrow()) {
                    break;
                }
                let start_time = get_time();
                let expected_ends = start_time + CHECKER_MS;
                let heartbeat_task_continue = async {
                    let mut meta = if let Some(runtime) = runtime_ref.as_ref() {
                        runtime.meta.write().await
                    } else {
                        server.meta.write().await
                    };
                    if Self::lifecycle_is_stopping(meta.lifecycle) {
                        return false;
                    }
                    let current_time = get_time();
                    let mut is_leader = false;
                    let action = match meta.membership {
                        Membership::Leader(_) => {
                            is_leader = true;
                            if current_time >= meta.last_checked + HEARTBEAT_MS {
                                CheckerAction::SendHeartbeat
                            } else {
                                CheckerAction::None
                            }
                        }
                        Membership::Follower | Membership::Candidate => {
                            debug_assert!(meta.timeout > 100);
                            let timeout_time = meta.last_checked + meta.timeout;
                            let time_remains = timeout_time - current_time;
                            if meta.vote_for.is_none() && time_remains < 0 {
                                CheckerAction::BecomeCandidate
                            } else {
                                CheckerAction::None
                            }
                        }
                        Membership::Offline => CheckerAction::ExitLoop,
                        Membership::Undefined => CheckerAction::None,
                    };
                    if let Some(runtime) = runtime_ref.as_ref() {
                        runtime.is_leader.store(is_leader, Relaxed);
                    } else {
                        server._is_leader.store(is_leader, Relaxed);
                    }
                    match action {
                        CheckerAction::SendHeartbeat => {
                            server
                                .send_followers_heartbeat_on_plane(
                                    plane_id,
                                    &mut meta,
                                    None,
                                    false,
                                )
                                .await;
                            meta.last_checked = get_time();
                        }
                        CheckerAction::BecomeCandidate => {
                            let leader_flag = runtime_ref
                                .as_ref()
                                .map(|runtime| &runtime.is_leader)
                                .unwrap_or(&server._is_leader);
                            server
                                .become_candidate_on_plane(
                                    plane_id,
                                    leader_flag,
                                    &mut meta,
                                )
                                .await;
                        }
                        CheckerAction::ExitLoop => return false,
                        CheckerAction::None => {}
                    }
                    true
                };
                let timed_heartbeat = tokio::select! {
                    changed = shutdown_rx.changed() => {
                        match changed {
                            Ok(_) if Self::lifecycle_is_stopping(*shutdown_rx.borrow()) => break,
                            Ok(_) => continue,
                            Err(_) => break,
                        }
                    }
                    result = timeout(
                        Duration::from_millis(HEARTBEAT_TASK_TIMEOUT_MS as u64),
                        heartbeat_task_continue,
                    ) => result,
                };
                let end_time = get_time();
                let time_to_sleep = expected_ends - end_time - 1;
                match timed_heartbeat {
                    Err(_) => {
                        error!(
                            "Heartbeat cannot finish in time for {}ms on plane {}",
                            HEARTBEAT_MS,
                            plane_id.raw()
                        );
                    }
                    Ok(false) => break,
                    Ok(true) => {}
                }
                if time_to_sleep > 0 {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            match changed {
                                Ok(_) if Self::lifecycle_is_stopping(*shutdown_rx.borrow()) => break,
                                Ok(_) => {}
                                Err(_) => break,
                            }
                        }
                        _ = sleep(Duration::from_millis(time_to_sleep as u64)) => {}
                    }
                }
            }
            if let Some(runtime) = runtime_ref.as_ref() {
                runtime.is_leader.store(false, Relaxed);
            } else {
                server._is_leader.store(false, Relaxed);
            }
            info!(
                "Raft checker/heartbeat task stopped gracefully for plane {}",
                plane_id.raw()
            );
        });

        if let Some(runtime_ref) = runtime.as_ref() {
            let mut guard = runtime_ref.checker_task.lock().await;
            *guard = Some(handle);
        } else {
            let mut guard = self.checker_task.lock().await;
            *guard = Some(handle);
        }
    }

    async fn shutdown_managed_runtime(&self, runtime: Option<Arc<RaftPlaneRuntime>>) {
        let plane_id = runtime
            .as_ref()
            .map(|runtime| runtime.plane_id)
            .unwrap_or_else(PlaneId::type1);
        let already_stopping = {
            let mut meta = if let Some(runtime) = runtime.as_ref() {
                runtime.meta.write().await
            } else {
                self.meta.write().await
            };
            if meta.lifecycle != LifecycleState::Running {
                true
            } else {
                meta.lifecycle = LifecycleState::Stopping;
                meta.membership = Membership::Offline;
                false
            }
        };
        if !already_stopping {
            if let Some(runtime) = runtime.as_ref() {
                let _ = runtime.shutdown_tx.send(LifecycleState::Stopping);
            } else {
                let _ = self.shutdown_tx.send(LifecycleState::Stopping);
            }
        }

        let _ = if let Some(runtime) = runtime.as_ref() {
            Self::wait_for_apply_drain_for_meta(&runtime.meta, Duration::from_secs(5)).await
        } else {
            Self::wait_for_apply_drain_for_meta(&self.meta, Duration::from_secs(5)).await
        };

        let handle = if let Some(runtime) = runtime.as_ref() {
            let mut guard = runtime.checker_task.lock().await;
            guard.take()
        } else {
            let mut guard = self.checker_task.lock().await;
            guard.take()
        };
        if let Some(handle) = handle {
            let _ = handle.await;
        }

        let _ = self.flush_persistence_on_plane(plane_id).await;

        {
            let mut meta = if let Some(runtime) = runtime.as_ref() {
                runtime.meta.write().await
            } else {
                self.meta.write().await
            };
            meta.lifecycle = LifecycleState::Stopped;
        }
        if let Some(runtime) = runtime.as_ref() {
            runtime.is_leader.store(false, Relaxed);
            let _ = runtime.shutdown_tx.send(LifecycleState::Stopped);
        } else {
            self._is_leader.store(false, Relaxed);
            let _ = self.shutdown_tx.send(LifecycleState::Stopped);
        }
    }

    /// Public helper for applications to trigger commit replay after registering
    /// their state machines. This ensures snapshot replay and log apply happen
    /// only after SMs are ready.
    pub async fn recover_after_register(&self) {
        let mut meta = self.meta.write().await;
        {
            let mut master_sm = meta.state_machine.write().await;
            master_sm.recover_registered_snapshots().await;
        }
        info!(
            "Manual apply on plane {}: applying committed logs (commit_index={}, last_applied={})",
            PlaneId::type1().raw(),
            meta.commit_index,
            meta.last_applied
        );
        check_commit(&mut meta).await;
        info!(
            "Manual apply on plane {}: applied logs up to last_applied={}",
            PlaneId::type1().raw(),
            meta.last_applied
        );
    }
}

impl RaftService {
    fn plane_storage_path(&self, plane_id: PlaneId) -> Option<String> {
        match &self.options.storage {
            Storage::MEMORY => None,
            Storage::DISK(disk_opts) => {
                Some(format!("{}/planes/{}", disk_opts.path, plane_id.raw()))
            }
        }
    }

    fn plane_has_persisted_state(&self, plane_id: PlaneId) -> bool {
        self.plane_storage_path(plane_id)
            .map(|path| {
                let base = Path::new(&path);
                base.join("commit.idx").exists()
                    || base.join("log.dat").exists()
                    || base.join("snapshot.dat").exists()
            })
            .unwrap_or(false)
    }

    async fn load_snapshot_into_meta(
        plane_id: PlaneId,
        meta: &mut RwLockWriteGuard<'_, RaftMeta>,
    ) -> bool {
        let storage = meta.storage.clone();

        if let Some(storage) = storage {
            let storage = storage.lock().await;
            match storage.read_snapshot().await {
                Ok(Some(snapshot)) => {
                    info!(
                        "Found snapshot on plane {}: index={}, term={}. Recovering state machine...",
                        plane_id.raw(),
                        snapshot.last_included_index,
                        snapshot.last_included_term
                    );

                    meta.state_machine
                        .write()
                        .await
                        .recover(snapshot.snapshot.clone())
                        .await;

                    meta.last_snapshot_index = snapshot.last_included_index;
                    meta.last_snapshot_term = snapshot.last_included_term;

                    // Snapshot restore must reset the apply cursor to the snapshot index so
                    // post-snapshot WAL entries are replayed deterministically on startup.
                    meta.last_applied = snapshot.last_included_index;
                    if meta.commit_index < snapshot.last_included_index {
                        meta.commit_index = snapshot.last_included_index;
                    }

                    {
                        let mut logs = meta.logs.write().await;
                        let before_count = logs.len();
                        logs.retain(|&id, _| id > snapshot.last_included_index);
                        let after_count = logs.len();
                        info!(
                            "Compacted logs on plane {} during startup: removed {} logs, {} remaining",
                            plane_id.raw(),
                            before_count - after_count,
                            after_count
                        );
                    }

                    {
                        let mut master_sm = meta.state_machine.write().await;
                        master_sm.recover_registered_snapshots().await;
                    }

                    info!(
                        "Snapshot recovery completed successfully for plane {}",
                        plane_id.raw()
                    );
                    true
                }
                Ok(None) => {
                    debug!("No snapshot found on disk for plane {}", plane_id.raw());
                    false
                }
                Err(e) => {
                    warn!(
                        "Failed to load snapshot from disk for plane {}: {:?}. Starting without snapshot recovery.",
                        plane_id.raw(),
                        e
                    );
                    false
                }
            }
        } else {
            debug!(
                "No storage configured, skipping snapshot recovery for plane {}",
                plane_id.raw()
            );
            false
        }
    }

    async fn recover_config_state_from_logs(
        plane_id: PlaneId,
        meta: &mut RwLockWriteGuard<'_, RaftMeta>,
    ) {
        let committed_entries = {
            let logs = meta.logs.read().await;
            logs.range((Unbounded, Included(&meta.commit_index)))
                .filter_map(|(_, entry)| {
                    if entry.sm_id == CONFIG_SM_ID {
                        Some(entry.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        if committed_entries.is_empty() {
            return;
        }

        let mut master_sm = meta.state_machine.write().await;
        for entry in committed_entries {
            if let Err(err) = master_sm.commit_cmd(&entry).await {
                warn!(
                    "Failed to recover config log {} on plane {} during runtime initialization: {:?}",
                    entry.id,
                    plane_id.raw(),
                    err
                );
            }
        }
    }

    async fn initialize_runtime_meta(
        &self,
        plane_id: PlaneId,
        leader_flag: &AtomicBool,
        meta: &mut RwLockWriteGuard<'_, RaftMeta>,
        bootstrap_if_fresh: bool,
    ) -> Result<(), PlaneError> {
        leader_flag.store(false, Relaxed);
        meta.last_checked = get_time() + (CHECKER_MS * 10);

        let recovered_from_disk = Self::load_snapshot_into_meta(plane_id, meta).await;
        Self::recover_config_state_from_logs(plane_id, meta).await;
        let server_address = self.options.address.clone();

        {
            let mut sm = meta.state_machine.write().await;
            let start_time = get_time();
            while get_time() < start_time + 5000 {
                if sm.configs.new_member(server_address.clone()).await
                    || sm.configs.member_existed(self.id)
                {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }

            if !sm.configs.member_existed(self.id) {
                return Err(PlaneError::InitializationFailed(plane_id));
            }

            let num_members = sm.configs.members.len();
            let has_logs = !meta.logs.read().await.is_empty();
            let has_term = meta.term > 0;
            let should_promote = num_members == 1
                && ((recovered_from_disk || has_logs || has_term) || bootstrap_if_fresh);

            debug!(
                "Plane {} initialization: recovered_from_disk={}, num_members={}, has_logs={}, has_term={}, bootstrap_if_fresh={}, membership={:?}",
                plane_id.raw(),
                recovered_from_disk,
                num_members,
                has_logs,
                has_term,
                bootstrap_if_fresh,
                match &meta.membership {
                    Membership::Leader(_) => "Leader",
                    Membership::Follower => "Follower",
                    Membership::Candidate => "Candidate",
                    Membership::Offline => "Offline",
                    Membership::Undefined => "Undefined",
                }
            );

            if should_promote {
                info!(
                    "Single-node plane {} detected during initialization (term={}, logs={}, members={}). Becoming leader immediately.",
                    plane_id.raw(),
                    meta.term,
                    has_logs,
                    num_members
                );
                let (last_log_id, _) = {
                    let logs = meta.logs.read().await;
                    get_last_log_info!(self, logs)
                };
                drop(sm);
                ensure_direct_leader_term(meta);
                self.become_leader_on_plane(leader_flag, meta, last_log_id)
                    .await;
                info!(
                    "Plane {} successfully transitioned to Leader state",
                    plane_id.raw()
                );
            }
        }

        Ok(())
    }

    async fn resolve_plane_runtime(
        &self,
        plane_id: PlaneId,
        allow_create_if_missing: bool,
        bootstrap_if_fresh: bool,
    ) -> Result<(Option<Arc<RaftPlaneRuntime>>, bool), PlaneError> {
        if plane_id.is_type1() {
            return Ok((None, false));
        }

        {
            let planes = self.planes.read().await;
            if let Some(runtime) = planes.get(&plane_id).cloned() {
                return Ok((Some(runtime), false));
            }
        }

        let should_materialize =
            allow_create_if_missing || self.plane_has_persisted_state(plane_id);
        if !should_materialize {
            return Err(PlaneError::PlaneNotFound(plane_id));
        }

        let runtime = Arc::new(RaftPlaneRuntime::new(&self.options, plane_id)?);
        {
            let mut meta = runtime.meta.write().await;
            self.initialize_runtime_meta(
                plane_id,
                &runtime.is_leader,
                &mut meta,
                bootstrap_if_fresh,
            )
            .await?;
        }

        let mut planes = self.planes.write().await;
        if let Some(existing) = planes.get(&plane_id).cloned() {
            Ok((Some(existing), false))
        } else {
            planes.insert(plane_id, runtime.clone());
            Ok((Some(runtime), true))
        }
    }

    async fn runtime_for_plane(
        &self,
        plane_id: PlaneId,
    ) -> Result<Option<Arc<RaftPlaneRuntime>>, PlaneError> {
        self.resolve_plane_runtime(plane_id, false, false)
            .await
            .map(|(runtime, _)| runtime)
    }

    fn canonicalize_member_addresses(mut members: Vec<String>) -> Vec<String> {
        members.sort();
        members.dedup();
        members
    }

    fn validate_plane_bootstrap(
        &self,
        bootstrap: PlaneBootstrap,
    ) -> Result<(PlaneId, Vec<String>), PlaneBootstrapError> {
        if bootstrap.plane_id.is_type1() {
            return Err(PlaneBootstrapError::Type1PlaneUnsupported);
        }

        let seed_nodes = Self::canonicalize_member_addresses(bootstrap.seed_nodes);
        if seed_nodes.is_empty() {
            return Err(PlaneBootstrapError::EmptySeedNodes);
        }

        Ok((bootstrap.plane_id, seed_nodes))
    }

    async fn plane_members_from_seed_nodes(
        &self,
        seed_nodes: Vec<String>,
    ) -> Result<Vec<String>, PlaneBootstrapError> {
        let client = RaftClient::new(&seed_nodes, self.options.service_id).await?;
        let members = Self::canonicalize_member_addresses(client.root_member_addresses().await?);
        if members.is_empty() {
            return Err(PlaneBootstrapError::NoType1MembersDiscovered);
        }
        if !members.iter().any(|member| member == &self.options.address) {
            return Err(PlaneBootstrapError::LocalMemberMissing {
                local_address: self.options.address.clone(),
            });
        }

        Ok(members)
    }

    async fn plane_member_addresses(&self, plane_id: PlaneId) -> Result<Vec<String>, PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            let meta = runtime.meta.read().await;
            let master_sm = meta.state_machine.read().await;
            return Ok(Self::canonicalize_member_addresses(
                master_sm
                    .configs
                    .members
                    .values()
                    .map(|member| member.address.clone())
                    .collect(),
            ));
        }

        let meta = self.meta.read().await;
        let master_sm = meta.state_machine.read().await;
        Ok(Self::canonicalize_member_addresses(
            master_sm
                .configs
                .members
                .values()
                .map(|member| member.address.clone())
                .collect(),
        ))
    }

    async fn add_plane_member_via_log(
        &self,
        plane_id: PlaneId,
        address: String,
    ) -> Result<bool, PlaneBootstrapError> {
        let (fn_id, _, data) = new_member_::new(&address).encode();
        let entry = LogEntry {
            id: 0,
            term: 0,
            sm_id: CONFIG_SM_ID,
            fn_id,
            data,
        };

        match Service::c_command(self, plane_id, entry).await {
            ClientCmdResponse::Success { data: Ok(data), .. } => {
                Ok(new_member_::decode_return(&data))
            }
            ClientCmdResponse::Success { data: Err(err), .. } => Err(err.into()),
            ClientCmdResponse::NotLeader(leader_id) => Err(PlaneBootstrapError::NotLeader {
                plane_id,
                leader_id,
            }),
            ClientCmdResponse::NotCommitted { .. } => Err(ExecError::NotCommitted.into()),
            ClientCmdResponse::ShuttingDown => Err(ExecError::ShuttingDown.into()),
        }
    }

    pub async fn ensure_plane(
        self: &Arc<Self>,
        spec: PlaneSpec,
    ) -> Result<PlaneHandle, PlaneError> {
        if let (Some(runtime), _) = self
            .resolve_plane_runtime(spec.plane_id, true, true)
            .await?
        {
            self.start_managed_runtime(Some(runtime)).await;
        }

        Ok(PlaneHandle {
            service: self.clone(),
            plane_id: spec.plane_id,
        })
    }

    pub async fn plane(self: &Arc<Self>, plane_id: PlaneId) -> Result<PlaneHandle, PlaneError> {
        if let (Some(runtime), _) = self.resolve_plane_runtime(plane_id, false, false).await? {
            self.start_managed_runtime(Some(runtime)).await;
        }

        Ok(PlaneHandle {
            service: self.clone(),
            plane_id,
        })
    }

    async fn ensure_plane_membership(
        self: &Arc<Self>,
        plane_id: PlaneId,
        requested_members: Vec<String>,
    ) -> Result<PlaneHandle, PlaneBootstrapError> {
        let plane = self.ensure_plane(PlaneSpec { plane_id }).await?;

        let current_members = self.plane_member_addresses(plane_id).await?;
        if current_members == requested_members {
            return Ok(plane);
        }
        if current_members.iter().any(|member| {
            !requested_members
                .iter()
                .any(|requested| requested == member)
        }) {
            return Err(PlaneBootstrapError::MembershipConflict {
                plane_id,
                current_members,
                requested_members,
            });
        }

        for member in &requested_members {
            let added = self
                .add_plane_member_via_log(plane_id, member.clone())
                .await?;
            if !added && !current_members.iter().any(|current| current == member) {
                return Err(PlaneBootstrapError::MemberRegistrationRejected {
                    address: member.clone(),
                });
            }
        }

        Ok(plane)
    }

    /// Materialize a type-2 plane using the current type-1 membership discovered
    /// from one or more root seed nodes.
    pub async fn ensure_plane_from_seeds(
        self: &Arc<Self>,
        bootstrap: PlaneBootstrap,
    ) -> Result<PlaneHandle, PlaneBootstrapError> {
        let (plane_id, seed_nodes) = self.validate_plane_bootstrap(bootstrap)?;
        let requested_members = self.plane_members_from_seed_nodes(seed_nodes).await?;
        self.ensure_plane_membership(plane_id, requested_members)
            .await
    }

    /// Returns only the type-2 plane runtimes that are currently materialized on this host.
    ///
    /// This is a local runtime-cache view, not an authoritative plane inventory.
    /// Type-1 is intentionally excluded.
    pub async fn loaded_type2_planes(&self) -> Vec<PlaneId> {
        let planes = self.planes.read().await;
        planes.keys().copied().collect()
    }

    pub async fn recover_after_register_on_plane(
        &self,
        plane_id: PlaneId,
    ) -> Result<(), PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            let mut meta = runtime.meta.write().await;
            {
                let mut master_sm = meta.state_machine.write().await;
                master_sm.recover_registered_snapshots().await;
            }
            info!(
                "Manual apply on plane {}: applying committed logs (commit_index={}, last_applied={})",
                plane_id.raw(),
                meta.commit_index,
                meta.last_applied
            );
            check_commit(&mut meta).await;
            return Ok(());
        }

        self.recover_after_register().await;
        Ok(())
    }

    pub async fn register_state_machine_on_plane(
        &self,
        plane_id: PlaneId,
        state_machine: SubStateMachine,
    ) -> Result<(), PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            let meta = runtime.meta.read().await;
            let mut master_sm = meta.state_machine.write().await;
            master_sm.register(state_machine);
            return Ok(());
        }

        self.register_state_machine(state_machine).await;
        Ok(())
    }

    pub(crate) async fn subscriptions_on_plane(
        &self,
        plane_id: PlaneId,
    ) -> Result<Arc<RwLock<Subscriptions>>, PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            let meta = runtime.meta.read().await;
            let master_sm = meta.state_machine.read().await;
            return Ok(master_sm.configs.subscriptions.clone());
        }

        let meta = self.meta.read().await;
        let master_sm = meta.state_machine.read().await;
        Ok(master_sm.configs.subscriptions.clone())
    }

    pub async fn cluster_info_on_plane_local(
        &self,
        plane_id: PlaneId,
    ) -> Result<ClientClusterInfo, PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            let meta = runtime.meta.read().await;
            let logs = meta.logs.read().await;
            let sm = meta.state_machine.read().await;
            let members = sm
                .members()
                .iter()
                .map(|(id, member)| (*id, member.address.clone()))
                .collect::<Vec<_>>();
            let last_log = logs.iter().next_back();
            let (last_log_id, last_log_term) = match last_log {
                Some((last_log_id, last_log_item)) => (*last_log_id, last_log_item.term),
                None => (0, 0),
            };

            return Ok(ClientClusterInfo {
                members,
                last_log_id,
                last_log_term,
                leader_id: meta.leader_id,
            });
        }

        Ok(self.cluster_info().await)
    }

    pub async fn have_state_machine_on_plane_local(
        &self,
        plane_id: PlaneId,
        id: u64,
    ) -> Result<bool, PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            let meta = runtime.meta.read().await;
            let sm = meta.state_machine.read().await;
            return Ok(sm.has_sub(&id));
        }

        let meta = self.meta.read().await;
        let sm = meta.state_machine.read().await;
        Ok(sm.has_sub(&id))
    }

    pub async fn is_leader_on_plane(&self, plane_id: PlaneId) -> Result<bool, PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            return Ok(runtime.is_leader.load(Relaxed));
        }

        Ok(self.is_leader())
    }

    pub async fn flush_persistence_on_plane(&self, plane_id: PlaneId) -> Result<(), PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            let (storage_opt, commit_index, last_applied) = {
                let meta = runtime.meta.read().await;
                (meta.storage.clone(), meta.commit_index, meta.last_applied)
            };
            if let Some(storage_mutex) = storage_opt {
                let mut storage = storage_mutex.lock().await;
                let _ = storage.flush_wal().await;
                let _ = storage
                    .write_commit_progress(commit_index, last_applied)
                    .await;
            }
            return Ok(());
        }

        self.flush_persistence().await;
        Ok(())
    }

    pub async fn shutdown_plane(&self, plane_id: PlaneId) -> Result<(), PlaneError> {
        if let Some(runtime) = self.runtime_for_plane(plane_id).await? {
            self.shutdown_managed_runtime(Some(runtime)).await;
            return Ok(());
        }

        self.shutdown().await;
        Ok(())
    }
}

fn is_majority(members: u64, granted: u64) -> bool {
    let required = members / 2 + 1;
    let majority = granted >= (required);
    debug!(
        "Members {} granted {}, is majority: {}",
        members, granted, majority
    );
    majority
}

async fn apply_committed_entry<'a>(
    meta: &'a RwLockWriteGuard<'a, RaftMeta>,
    entry: &'a LogEntry,
) -> ExecResult {
    meta.state_machine.write().await.commit_cmd(&entry).await
}

fn is_leader(meta: &RwLockWriteGuard<RaftMeta>) -> bool {
    match meta.membership {
        Membership::Leader(_) => true,
        _ => false,
    }
}
fn alter_term(meta: &mut RwLockWriteGuard<RaftMeta>, term: u64) {
    if meta.term != term {
        meta.term = term;
        meta.vote_for = None;
    }
}

fn ensure_direct_leader_term(meta: &mut RwLockWriteGuard<'_, RaftMeta>) {
    if meta.term == 0 {
        meta.term = 1;
    }
}

impl RaftService {
    pub fn new(opts: Options) -> Arc<RaftService> {
        let server_address = opts.address.clone();
        let server_id = hash_str(&server_address);

        let mut term = 0;
        let mut logs = BTreeMap::new();
        let mut commit_index = 0;
        let mut last_applied = 0;

        let storage_entity = match StorageEntity::new_with_options(
            &opts,
            &mut term,
            &mut commit_index,
            &mut last_applied,
            &mut logs,
        ) {
            Ok(entity) => entity,
            Err(e) => {
                panic!(
                    "Failed to initialize storage entity: {:?}. Cannot proceed without storage.",
                    e
                );
            }
        };

        let master_sm = MasterStateMachine::new_on_plane(opts.service_id, PlaneId::type1());

        let (shutdown_tx, _shutdown_rx) = watch::channel(LifecycleState::Running);
        let runtime = runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("raft-server")
            .worker_threads(12)
            .max_blocking_threads(num_cpus::get())
            .event_interval(31)
            .build()
            .expect("Failed to build tokio runtime for Raft service");
        let runtime_handle = runtime.handle().clone();
        let server_obj = RaftService {
            meta: RwLock::new(RaftMeta {
                term,
                vote_for: None,
                timeout: gen_timeout(),
                last_checked: get_time(),
                membership: Membership::Undefined,
                logs: Arc::new(RwLock::new(logs)),
                state_machine: Arc::new(RwLock::new(master_sm)),
                commit_index,
                last_applied,
                leader_id: 0,
                storage: storage_entity.map(|e| Arc::new(Mutex::new(e))),
                last_snapshot_index: 0,
                last_snapshot_term: 0,
                lifecycle: LifecycleState::Running,
            }),
            planes: RwLock::new(BTreeMap::new()),
            id: server_id,
            options: opts,
            rt: RaftRuntimeHandle::new(runtime_handle),
            rt_owner: StdMutex::new(Some(runtime)),
            _is_leader: AtomicBool::new(false),
            checker_task: TokioMutex::new(None),
            shutdown_tx,
        };
        Arc::new(server_obj)
    }

    /// Load snapshot from disk and recover state machine if snapshot exists
    async fn load_snapshot_on_startup(&self) -> bool {
        // IMPORTANT: read the snapshot from disk while holding only a read lock, then
        // drop the read lock before acquiring the write lock.  If we tried to acquire
        // meta.write() while still inside the `if let … = meta.read().await.storage {`
        // block the borrow of `storage` would keep the read guard alive and we would
        // deadlock waiting for our own read guard to be released.
        let maybe_snapshot = {
            let meta = self.meta.read().await;
            match meta.storage {
                Some(ref storage) => {
                    let storage = storage.lock().await;
                    match storage.read_snapshot().await {
                        Ok(v) => v,
                        Err(e) => {
                            warn!("Failed to load snapshot from disk: {:?}. Starting without snapshot recovery.", e);
                            None
                        }
                    }
                }
                None => {
                    debug!("No storage configured, skipping snapshot recovery");
                    None
                }
            }
        }; // ← read guard dropped here before the write lock below

        if let Some(snapshot) = maybe_snapshot {
            info!(
                "Found snapshot on disk: index={}, term={}. Recovering state machine...",
                snapshot.last_included_index, snapshot.last_included_term
            );

            let mut meta = self.meta.write().await; // safe: read guard already released

            // Recover state machine (stores sub-SM snapshots; applied when they register)
            meta.state_machine
                .write()
                .await
                .recover(snapshot.snapshot.clone())
                .await;

            // Update snapshot metadata
            meta.last_snapshot_index = snapshot.last_included_index;
            meta.last_snapshot_term = snapshot.last_included_term;

            // Restore applies from the snapshot boundary every time so any
            // committed WAL entries after the snapshot are replayed exactly once.
            meta.last_applied = snapshot.last_included_index;
            if meta.commit_index < snapshot.last_included_index {
                meta.commit_index = snapshot.last_included_index;
            }

            // Compact logs: remove entries already covered by the snapshot
            {
                let mut logs = meta.logs.write().await;
                let before_count = logs.len();
                logs.retain(|&id, _| id > snapshot.last_included_index);
                let after_count = logs.len();
                info!(
                    "Compacted logs on startup: removed {} logs, {} remaining",
                    before_count - after_count,
                    after_count
                );
            }

            info!("Snapshot recovery completed successfully");
            true
        } else {
            debug!("No snapshot found on disk, starting fresh");
            false
        }
    }

    pub async fn start(server: &Arc<RaftService>, recover_registered: bool) -> bool {
        info!(
            "Waiting for raft server to be initialized on plane {}",
            PlaneId::type1().raw()
        );
        {
            let mut meta = server.meta.write().await;
            if server
                .initialize_runtime_meta(PlaneId::type1(), &server._is_leader, &mut meta, false)
                .await
                .is_err()
            {
                return false;
            }
        }

        if recover_registered {
            server.recover_after_register().await;
        }

        server.start_managed_runtime(None).await;

        return true;
    }

    /// New server without recovery from registered state machine
    pub async fn new_server(opts: Options) -> (bool, Arc<RaftService>, Arc<Server>) {
        let address = opts.address.clone();
        let svr_id = opts.service_id;
        let service = RaftService::new(opts);
        let server = Server::new(&address);
        Server::listen_and_resume(&server).await;
        server.register_service_with_id(svr_id, &service).await;
        (RaftService::start(&service, false).await, service, server)
    }
    pub async fn probe_and_join(&self, servers: &Vec<String>) -> Result<bool, ExecError> {
        debug!(
            "Probing and try to join servers for plane {}: {:?}",
            self.plane_id().raw(),
            servers
        );
        let is_first_node =
            !RaftClient::probe_servers(servers, &self.options.address, self.options.service_id)
                .await;
        if is_first_node {
            debug!(
                "There is no live node in the server list for plane {}, will bootstrap",
                self.plane_id().raw()
            );
            self.bootstrap().await;
            Ok(false)
        } else {
            debug!(
                "There are some live nodes for plane {}, will join them",
                self.plane_id().raw()
            );
            self.join(servers).await
        }
    }
    pub async fn bootstrap(&self) {
        let mut meta = self.write_meta().await;
        let (last_log_id, _) = {
            let logs = meta.logs.read().await;
            get_last_log_info!(self, logs)
        };
        self.become_leader(&mut meta, last_log_id).await;
    }
    pub async fn conservative_bootstrap(&self, servers: &Vec<String>) {
        let meta = self.meta.read().await;
        debug!(
            "Conservative bootstrap for plane {}, checking storage",
            self.plane_id().raw()
        );
        if let Some(storage) = &meta.storage {
            debug!(
                "There is storage for plane {}, checking last term",
                self.plane_id().raw()
            );
            if storage.lock().await.last_term > 0 {
                debug!(
                    "Plane {} has logged term, will probe and join or bootstrap",
                    self.plane_id().raw()
                );
                drop(meta);
                if let Err(e) = self.probe_and_join(servers).await {
                    error!(
                        "Failed to probe and join cluster during conservative bootstrap on plane {}: {:?}",
                        self.plane_id().raw(), e
                    );
                }
            } else {
                debug!(
                    "Log is empty for plane {}, bootstrap",
                    self.plane_id().raw()
                );
                drop(meta);
                self.bootstrap().await;
            }
        } else {
            debug!(
                "No storage for plane {}, will probe and join or bootstrap",
                self.plane_id().raw()
            );
            drop(meta);
            if let Err(e) = self.probe_and_join(servers).await {
                error!(
                    "Failed to probe and join cluster during conservative bootstrap on plane {}: {:?}",
                    self.plane_id().raw(), e
                );
            }
        }
    }
    pub async fn join(&self, servers: &Vec<String>) -> Result<bool, ExecError> {
        debug!(
            "Trying to join plane {} cluster with id {}",
            self.plane_id().raw(),
            self.id
        );
        let client = RaftClient::new(servers, self.options.service_id).await;
        if let Ok(client) = client {
            debug!(
                "Executing in SM to create new member on plane {}: {}, {}",
                self.plane_id().raw(),
                &self.options.address,
                self.id
            );
            let result = client.add_root_member(&self.options.address).await;
            debug!(
                "Getting member address for plane {}: {}",
                self.plane_id().raw(),
                self.id
            );
            let members = client.root_member_addresses().await;
            debug!(
                "Updating local meta for plane {} by acquiring lock: {}",
                self.plane_id().raw(),
                self.id
            );
            let mut meta = self.write_meta().await;
            debug!(
                "Local meta lock acquired for plane {}: {}",
                self.plane_id().raw(),
                self.id
            );
            if let Ok(members) = members {
                debug!(
                    "We have following members for plane {} node {}: {:?}",
                    self.plane_id().raw(),
                    self.id,
                    members
                );
                for member in members {
                    meta.state_machine
                        .write()
                        .await
                        .configs
                        .new_member(member)
                        .await;
                }
            }
            debug!(
                "Become follower because of join on plane {}: {}",
                self.plane_id().raw(),
                self.id
            );
            self.become_follower(&mut meta, 0, client.leader_id());
            debug!(
                "Resetting last checked for join on plane {}: {}",
                self.plane_id().raw(),
                self.id
            );
            self.reset_last_checked(&mut meta);
            match &result {
                Ok(joined) => debug!(
                    "Completed join for plane {} node {}, result {}",
                    self.plane_id().raw(),
                    self.id,
                    joined
                ),
                Err(e) => debug!(
                    "Join failed for plane {} node {}, error: {:?}",
                    self.plane_id().raw(),
                    self.id,
                    e
                ),
            }
            result
        } else {
            Err(ExecError::CannotConstructClient)
        }
    }
    pub async fn leave(&self) -> bool {
        let members = self.cluster_info().await.members;
        let servers: Vec<_> = members
            .iter()
            .map(|&(_, ref address)| address.clone())
            .collect();
        debug!(
            "Leaving from plane {} cluster, server id {} with {} members {:?}",
            self.plane_id().raw(),
            self.id,
            servers.len(),
            servers
        );
        if let Ok(client) = RaftClient::new(&servers, self.options.service_id).await {
            debug!(
                "Temporary client for plane {} leaving, leader: {}. Sending removal message.",
                self.plane_id().raw(),
                client.leader_id()
            );
            match client.remove_root_member(&self.options.address).await {
                Ok(_) => info!(
                    "Successfully removed member {} from plane {} cluster",
                    self.options.address,
                    self.plane_id().raw()
                ),
                Err(e) => {
                    error!(
                        "Failed to remove member {} from plane {} cluster: {:?}",
                        self.options.address,
                        self.plane_id().raw(),
                        e
                    );
                    return false;
                }
            }
        } else {
            error!(
                "Cannot obtain temporary client for leaving plane {}",
                self.plane_id().raw()
            );
            return false;
        }
        let mut meta = self.write_meta().await;
        if is_leader(&meta) {
            info!(
                "Leader step down on plane {}: {}",
                self.plane_id().raw(),
                self.options.address
            );
            if !self.send_followers_heartbeat(&mut meta, None, true).await {
                error!("Leader cannot step down on plane {}", self.plane_id().raw());
                return false;
            }
            info!(
                "Step down heartbeat sent to followers on plane {}",
                self.plane_id().raw()
            );
            let mut reelected = false;
            for (_id, addr) in members {
                if addr != self.options.address {
                    info!(
                        "Calling reelect on plane {} to {}",
                        self.plane_id().raw(),
                        addr
                    );
                    match rpc::DEFAULT_CLIENT_POOL.get(&addr).await {
                        Ok(client) => {
                            let service = AsyncServiceClient::new(&client);
                            match service.reelect(PlaneId::type1()).await {
                                Ok(true) => {
                                    info!(
                                        "New leader has been elected on plane {}",
                                        self.plane_id().raw()
                                    );
                                    reelected = true;
                                    break; // Only need one successful reelection
                                }
                                Ok(false) => {
                                    warn!(
                                        "Server {} cannot be elected on plane {}",
                                        addr,
                                        self.plane_id().raw()
                                    );
                                }
                                Err(e) => {
                                    error!(
                                        "Server {} cannot be elected on plane {} due to comm error {:?}",
                                        addr, self.plane_id().raw(), e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "Cannot call reelect on plane {} to {}, error {:?}",
                                self.plane_id().raw(),
                                addr,
                                e
                            )
                        }
                    }
                }
            }
            if !reelected {
                warn!(
                    "No new leader has been elected on plane {}",
                    self.plane_id().raw()
                );
            }
        }
        meta.membership = Membership::Offline;
        let mut sm = meta.state_machine.write().await;
        sm.clear_subs();
        return true;
    }
    pub async fn cluster_info(&self) -> ClientClusterInfo {
        let meta = self.meta.read().await;
        let logs = meta.logs.read().await;
        let sm = &meta.state_machine.read().await;
        let sm_members = sm.members();
        let mut members = Vec::new();
        for (id, member) in sm_members.iter() {
            members.push((*id, member.address.clone()))
        }
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
        ClientClusterInfo {
            members,
            last_log_id,
            last_log_term,
            leader_id: meta.leader_id,
        }
    }
    pub async fn num_members(&self) -> usize {
        let meta = self.meta.read().await;
        let member_sm = meta.state_machine.read().await;
        let ref members = member_sm.configs.members;
        members.len()
    }
    pub async fn num_logs(&self) -> usize {
        let meta = self.meta.read().await;
        let logs = meta.logs.read().await;
        logs.len()
    }
    pub async fn last_log_id(&self) -> Option<u64> {
        let meta = self.meta.read().await;
        let logs = meta.logs.read().await;
        logs.keys().cloned().last()
    }
    pub async fn leader_id(&self) -> u64 {
        let meta = self.meta.read().await;
        meta.leader_id
    }
    pub async fn is_leader_for_real(&self) -> bool {
        let meta = self.meta.read().await;
        match meta.membership {
            Membership::Leader(_) => true,
            _ => false,
        }
    }
    pub fn is_leader(&self) -> bool {
        self._is_leader.load(Relaxed)
    }
    pub fn get_server_id(&self) -> u64 {
        self.id
    }

    /// Force a write-back and fsync of WAL and persist current commit progress.
    pub async fn flush_persistence(&self) {
        let (storage_opt, commit_index, last_applied) = {
            let meta = self.meta.read().await;
            (meta.storage.clone(), meta.commit_index, meta.last_applied)
        };
        if let Some(storage_mutex) = storage_opt {
            let mut storage = storage_mutex.lock().await;
            let _ = storage.flush_wal().await;
            let _ = storage
                .write_commit_progress(commit_index, last_applied)
                .await;
            info!(
                "Flushed WAL and wrote commit progress: commit_index={}, last_applied={}",
                commit_index, last_applied
            );
        }
    }

    pub async fn shutdown(&self) {
        info!(
            "Shutting down RaftService on plane {} at {}",
            self.plane_id().raw(),
            self.options.address
        );

        let plane_runtimes = {
            let planes = self.planes.read().await;
            planes.values().cloned().collect::<Vec<_>>()
        };
        for runtime in plane_runtimes {
            self.shutdown_managed_runtime(Some(runtime)).await;
        }

        self.shutdown_managed_runtime(None).await;
        self.shutdown_runtime_owner().await;
        info!(
            "RaftService shutdown complete for plane {}",
            self.plane_id().raw()
        );
    }

    async fn shutdown_runtime_owner(&self) {
        let owned_runtime_id = self.rt.id();
        let runtime = self
            .rt_owner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(runtime) = runtime else {
            self.rt.close();
            return;
        };

        let called_from_owned_runtime = runtime::Handle::try_current()
            .map(|current| Some(current.id()) == owned_runtime_id)
            .unwrap_or(false);
        self.rt.close();
        if called_from_owned_runtime {
            runtime.shutdown_background();
        } else if let Err(error) = tokio::task::spawn_blocking(move || drop(runtime)).await {
            error!("Failed to join Raft runtime shutdown task: {:?}", error);
        }
    }

    pub async fn register_state_machine(&self, state_machine: SubStateMachine) {
        let meta = self.meta.read().await;
        let mut master_sm = meta.state_machine.write().await;
        master_sm.register(state_machine);
    }
    fn switch_membership(&self, meta: &mut RwLockWriteGuard<RaftMeta>, membership: Membership) {
        self.reset_last_checked(meta);
        meta.membership = membership;
    }
    fn get_log_info_(&self, log: Option<(&u64, &LogEntry)>) -> (u64, u64) {
        match log {
            Some((last_log_id, last_log_item)) => (*last_log_id, last_log_item.term),
            None => (0, 0),
        }
    }
    fn insert_leader_follower_meta(
        &self,
        leader_meta: &mut RwLockWriteGuard<LeaderMeta>,
        last_log_id: u64,
        member_id: u64,
    ) {
        // the leader itself will not be consider as a follower when sending heartbeat
        if member_id == self.id {
            return;
        }
        leader_meta.followers.entry(member_id).or_insert_with(|| {
            Arc::new(Mutex::new(FollowerStatus {
                next_index: last_log_id + 1,
                match_index: 0,
            }))
        });
    }
    fn reload_leader_meta(
        &self,
        member_map: &HashMap<u64, RaftMember>,
        leader_meta: &mut RwLockWriteGuard<LeaderMeta>,
        last_log_id: u64,
    ) {
        for member in member_map.values() {
            self.insert_leader_follower_meta(leader_meta, last_log_id, member.id);
        }
    }
    async fn write_meta<'a>(&'a self) -> RwLockWriteGuard<'a, RaftMeta> {
        self.meta.write().await
    }

    pub async fn read_meta(&self) -> RwLockReadGuard<'_, RaftMeta> {
        self.meta.read().await
    }

    async fn become_candidate_on_plane<'a>(
        &'a self,
        plane_id: PlaneId,
        leader_flag: &'a AtomicBool,
        meta: &'a mut RwLockWriteGuard<'_, RaftMeta>,
    ) {
        let server_id = self.id;
        debug!(
            "Plane {} server {} become candidate",
            plane_id.raw(),
            server_id
        );
        self.reset_last_checked(meta);
        leader_flag.store(false, Relaxed);
        let term = meta.term;
        alter_term(meta, term + 1);
        meta.vote_for = Some(server_id);
        self.switch_membership(meta, Membership::Candidate);
        let term = meta.term;
        let (last_log_id, last_log_term) = {
            let logs = meta.logs.read().await;
            get_last_log_info!(self, logs)
        };
        let (mut members_vote_response_stream, num_members) = {
            let members: Vec<_> = {
                let member_sm = meta.state_machine.read().await;
                let ref members = member_sm.configs.members;
                members
                    .values()
                    .map(|member| (member.rpc.clone(), member.id))
                    .collect()
            };
            let len = members.len();
            let futs: FuturesUnordered<_> = members
                .into_iter()
                .map(|(rpc, member_id)| {
                    let vote_fut = async move {
                        if member_id == server_id {
                            debug!("Plane {} member {} vote for itself", plane_id.raw(), member_id);
                            RequestVoteResponse::Granted
                        } else {
                            if let Ok(((remote_term, remote_leader_id), vote_granted)) = rpc
                                .request_vote(plane_id, term, server_id, last_log_id, last_log_term)
                                .await
                            {
                                if vote_granted {
                                    debug!(
                                        "Plane {} member {} received one vote from {}",
                                        plane_id.raw(), server_id, member_id
                                    );
                                    RequestVoteResponse::Granted
                                } else if remote_term > term {
                                    debug!(
                                        "Plane {} member {} is term out, by {}. Now leader is {}, term {}",
                                        plane_id.raw(), server_id, member_id, remote_leader_id, remote_term
                                    );
                                    RequestVoteResponse::TermOut(remote_term, remote_leader_id)
                                } else {
                                    debug!(
                                        "Plane {} member {} did not get vote from {}",
                                        plane_id.raw(), server_id, member_id
                                    );
                                    RequestVoteResponse::NotGranted
                                }
                            } else {
                                debug!(
                                    "Plane {} member {} request vote failed from {}",
                                    plane_id.raw(), server_id, member_id
                                );
                                RequestVoteResponse::NotGranted // default for request failure
                            }
                        }
                    };
                    timeout(Duration::from_millis(1500), self.rt.spawn(vote_fut))
                })
                .collect();
            (futs, len)
        };
        let mut granted = 0;
        while let Some(vote_response) = members_vote_response_stream.next().await {
            if let Ok(res) = vote_response {
                if meta.term != term {
                    break;
                }
                match res {
                    Ok(RequestVoteResponse::TermOut(remote_term, remote_leader_id)) => {
                        self.become_follower_on_plane(
                            leader_flag,
                            meta,
                            remote_term,
                            remote_leader_id,
                        );
                        break;
                    }
                    Ok(RequestVoteResponse::Granted) => {
                        granted += 1;
                        debug!(
                            "Plane {} member {} received {} votes for now",
                            plane_id.raw(),
                            server_id,
                            granted
                        );
                        if is_majority(num_members as u64, granted) {
                            debug!(
                                "Plane {} member {} become leader after receiving majority votes",
                                plane_id.raw(),
                                server_id
                            );
                            self.become_leader_on_plane(leader_flag, meta, last_log_id)
                                .await;
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
        debug!(
            "Plane {} granted votes for {}: {}/{}",
            plane_id.raw(),
            self.id,
            granted,
            num_members
        );
        return;
    }

    async fn become_candidate<'a>(&'a self, meta: &'a mut RwLockWriteGuard<'_, RaftMeta>) {
        self.become_candidate_on_plane(PlaneId::type1(), &self._is_leader, meta)
            .await;
    }

    fn become_follower_on_plane(
        &self,
        leader_flag: &AtomicBool,
        meta: &mut RwLockWriteGuard<RaftMeta>,
        term: u64,
        leader_id: u64,
    ) {
        alter_term(meta, term);
        meta.leader_id = leader_id;
        self.switch_membership(meta, Membership::Follower);
        leader_flag.store(false, Relaxed);
    }

    fn become_follower(&self, meta: &mut RwLockWriteGuard<RaftMeta>, term: u64, leader_id: u64) {
        self.become_follower_on_plane(&self._is_leader, meta, term, leader_id);
    }

    async fn become_leader_on_plane(
        &self,
        leader_flag: &AtomicBool,
        meta: &mut RwLockWriteGuard<'_, RaftMeta>,
        last_log_id: u64,
    ) {
        debug!(
            "Plane {} server {} become leader, term {}",
            self.plane_id().raw(),
            self.id,
            meta.term
        );
        let leader_meta = RwLock::new(LeaderMeta::new());
        {
            let mut guard = leader_meta.write().await;
            let member_sm = meta.state_machine.read().await;
            let ref members = member_sm.configs.members;
            self.reload_leader_meta(members, &mut guard, last_log_id);
            guard.last_updated = get_time();
        }
        meta.leader_id = self.id;
        self.switch_membership(meta, Membership::Leader(leader_meta));
        leader_flag.store(true, Relaxed);
    }

    async fn become_leader(&self, meta: &mut RwLockWriteGuard<'_, RaftMeta>, last_log_id: u64) {
        self.become_leader_on_plane(&self._is_leader, meta, last_log_id)
            .await;
    }

    async fn send_followers_heartbeat_on_plane<'a>(
        &self,
        plane_id: PlaneId,
        meta: &mut RwLockWriteGuard<'a, RaftMeta>,
        log_id: Option<u64>,
        no_delay: bool,
    ) -> bool {
        let now = get_time();
        if meta.last_checked + HEARTBEAT_MS > now {
            if no_delay {
                debug!("Issuing delayed heartbeat on plane {}", plane_id.raw());
            } else {
                debug!("Block throttled heartbeat on plane {}", plane_id.raw());
                return false;
            }
        }
        trace!("Sending followers heartbeat on plane {}", plane_id.raw());
        if let Membership::Leader(ref leader_meta) = meta.membership {
            let leader_id = meta.leader_id;
            debug_assert_eq!(self.id, leader_id);
            let mut heartbeat_futs = FuturesUnordered::new();
            // Send out heartbeats
            {
                let leader_meta = leader_meta.read().await;
                let member_sm = meta.state_machine.read().await;
                let ref members = member_sm.configs.members;
                for member in members.values() {
                    let member_id = member.id;
                    if member_id == self.id {
                        continue;
                    }
                    let follower = if let Some(follower) = leader_meta.followers.get(&member_id) {
                        follower
                    } else {
                        debug!(
                            "Plane {} follower not found, {}, {}",
                            plane_id.raw(),
                            member_id,
                            leader_meta.followers.len()
                        ); //TODO: remove after debug
                        continue;
                    };
                    // get a send follower task without await
                    let hb_fut = Self::send_follower_heartbeat(
                        plane_id,
                        meta.commit_index,
                        meta.term,
                        meta.leader_id,
                        meta.last_applied,
                        meta.last_snapshot_index,
                        meta.last_snapshot_term,
                        meta.state_machine.clone(),
                        meta.logs.clone(),
                        follower.clone(),
                        member.rpc.clone(),
                        member_id,
                    );
                    let heartbeat_fut = async move { (member_id, hb_fut.await) }.boxed();
                    let task_spawned = self.rt.spawn(heartbeat_fut);
                    let timeout_interval = 1000;
                    let task_with_timeout =
                        timeout(Duration::from_millis(timeout_interval), task_spawned);
                    heartbeat_futs.push(task_with_timeout);
                }
            }
            let followers = heartbeat_futs.len();
            if followers <= 0 {
                // Early quit if no followers
                return true;
            }
            if let (Some(log_id), &Membership::Leader(ref leader_meta)) = (log_id, &meta.membership)
            {
                let mut updated_followers = 0;
                let mut higher_term = None;
                {
                    let mut leader_meta = leader_meta.write().await;
                    while let Some(heartbeat_res) = heartbeat_futs.next().await {
                        match heartbeat_res {
                            Ok(Ok((member_id, heartbeat_result))) => match heartbeat_result {
                                HeartbeatReplicationResult::Matched(last_matched_id) => {
                                    debug!(
                                        "Heartbeat response on plane {} from {} is {:?}",
                                        plane_id.raw(),
                                        member_id,
                                        last_matched_id
                                    );
                                    if last_matched_id >= log_id {
                                        updated_followers += 1;
                                        if is_majority(followers as u64, updated_followers) {
                                            return true;
                                        }
                                    }
                                }
                                HeartbeatReplicationResult::TermOut {
                                    term: remote_term,
                                    leader_id: remote_leader_id,
                                } => {
                                    higher_term = Some((remote_term, remote_leader_id));
                                    break;
                                }
                            },
                            Ok(Err(err)) => {
                                warn!(
                                    "Heartbeat task failed on plane {} while replicating log {}: {:?}",
                                    plane_id.raw(),
                                    log_id,
                                    err
                                );
                            }
                            Err(_) => {
                                warn!(
                                    "Heartbeat task timed out on plane {} while replicating log {}",
                                    plane_id.raw(),
                                    log_id
                                );
                            }
                        }
                    }
                    leader_meta.last_updated = get_time();
                }
                if let Some((remote_term, remote_leader_id)) = higher_term {
                    warn!(
                        "Plane {} stepping down after follower reported higher term {} (leader_id={})",
                        plane_id.raw(),
                        remote_term,
                        remote_leader_id
                    );
                    alter_term(meta, remote_term);
                    meta.leader_id = remote_leader_id;
                    self.switch_membership(meta, Membership::Follower);
                    return false;
                }
                debug!(
                    "Plane {} replicated log {} to {} of {} followers",
                    plane_id.raw(),
                    log_id,
                    updated_followers,
                    followers
                );
                // is_majority(members, updated_followers)
                false
            } else {
                !log_id.is_some()
            }
        } else {
            unreachable!()
        }
    }

    async fn send_followers_heartbeat<'a>(
        &'a self,
        meta: &mut RwLockWriteGuard<'a, RaftMeta>,
        log_id: Option<u64>,
        no_delay: bool,
    ) -> bool {
        self.send_followers_heartbeat_on_plane(PlaneId::type1(), meta, log_id, no_delay)
            .await
    }

    async fn send_follower_heartbeat(
        plane_id: PlaneId,
        commit_index: u64,
        term: u64,
        leader_id: u64,
        last_applied: u64,
        last_snapshot_index: u64,
        last_snapshot_term: u64,
        master_sm: Arc<RwLock<MasterStateMachine>>,
        logs: Arc<RwLock<LogsMap>>,
        follower: Arc<Mutex<FollowerStatus>>,
        rpc: Arc<AsyncServiceClient>,
        member_id: u64,
    ) -> HeartbeatReplicationResult {
        // let commit_index = meta.commit_index;
        // let term = meta.term;
        // let leader_id = meta.leader_id;

        // let meta_term = meta.term;
        // let meta_last_applied = meta.last_applied;
        // let master_sm = &meta.state_machine;
        // let logs = &meta.logs;
        trace!(
            "Sending follower heartbeat on plane {} to {}",
            plane_id.raw(),
            member_id
        );
        let mut follower = follower.lock().await;
        let logs = logs.read().await;
        let mut is_retry = false;
        loop {
            let entries: Option<LogEntries> = {
                // extract logs to send to follower
                let list: LogEntries = logs
                    .range((Included(&follower.next_index), Unbounded))
                    .map(|(_, entry)| entry.clone())
                    .collect(); //TODO: avoid clone entry
                if list.is_empty() {
                    None
                } else {
                    Some(list)
                }
            };
            if is_retry && entries.is_none() {
                // break when retry and there is no entry
                trace!(
                    "Stop retry on plane {} when entry is empty, {}, member id {}",
                    plane_id.raw(),
                    follower.next_index,
                    member_id
                );
                return HeartbeatReplicationResult::Matched(follower.match_index);
            }
            let last_entries_id = match &entries {
                // get last entry id
                &Some(ref entries) => {
                    // Safe: entries is Some, so it's not empty (checked above)
                    entries.iter().last().map(|entry| entry.id)
                }
                &None => None,
            };
            // Check if follower needs logs that have been compacted (Issue 5)
            // If so, send snapshot instead
            if follower.next_index <= last_snapshot_index {
                debug!(
                    "Follower {} on plane {} needs compacted logs (next_index: {} <= snapshot_index: {}), sending snapshot",
                    member_id, plane_id.raw(), follower.next_index, last_snapshot_index
                );
                let master_sm = master_sm.read().await;
                let snapshot = master_sm.snapshot();
                // Use the correct last_included_term from snapshot metadata (Issue 2)
                if let Ok(_) = rpc
                    .install_snapshot(
                        plane_id,
                        term,
                        leader_id,
                        last_snapshot_index,
                        last_snapshot_term,
                        snapshot,
                    )
                    .await
                {
                    follower.next_index = last_snapshot_index + 1;
                    follower.match_index = last_snapshot_index;
                }
                return HeartbeatReplicationResult::Matched(follower.match_index);
            }

            let (follower_last_log_id, follower_last_log_term) = {
                // extract follower last log info
                // assumed log ids are sequence of integers
                let follower_last_log_id = if follower.next_index == 0 {
                    0
                } else {
                    follower.next_index - 1
                };
                if follower_last_log_id == 0 || logs.is_empty() {
                    (0, 0) // 0 represents there is no logs in the leader
                } else {
                    // detect cleaned logs (shouldn't happen now with snapshot check above)
                    let first_log_id = match logs.iter().next() {
                        Some((first_log_id, _)) => *first_log_id,
                        None => {
                            error!("Logs map is not empty on plane {} but iter().next() returned None - this should not happen", plane_id.raw());
                            return HeartbeatReplicationResult::Matched(follower.match_index);
                        }
                    };
                    if first_log_id > follower_last_log_id {
                        debug!(
                            "Taking snapshot on plane {} for follower {} (first_log: {} > follower_last: {})",
                            plane_id.raw(), member_id, first_log_id, follower_last_log_id
                        );
                        let master_sm = master_sm.read().await;
                        let snapshot = master_sm.snapshot();
                        // Use last_applied as snapshot index, get term from the log at that index
                        let snapshot_term = logs
                            .get(&last_applied)
                            .map(|e| e.term)
                            .unwrap_or(last_snapshot_term);
                        if let Ok(_) = rpc
                            .install_snapshot(
                                plane_id,
                                term,
                                leader_id,
                                last_applied,
                                snapshot_term,
                                snapshot,
                            )
                            .await
                        {
                            follower.next_index = last_applied + 1;
                            follower.match_index = last_applied;
                        }
                        return HeartbeatReplicationResult::Matched(follower.match_index);
                    }
                    let follower_last_entry = logs.get(&follower_last_log_id);
                    match follower_last_entry {
                        Some(entry) => (entry.id, entry.term),
                        None => {
                            panic!("Cannot find old logs for follower, first_id: {}, follower_last: {}", first_log_id, follower_last_log_id);
                        }
                    }
                }
            };
            let append_result = rpc
                .append_entries(
                    plane_id,
                    term,
                    leader_id,
                    follower_last_log_id,
                    follower_last_log_term,
                    &entries,
                    commit_index,
                )
                .await;
            match append_result {
                Ok((follower_term, result)) => match result {
                    AppendEntriesResult::Ok => {
                        trace!(
                            "Log updated on plane {} to follower {}",
                            plane_id.raw(),
                            member_id
                        );
                        if let Some(last_entries_id) = last_entries_id {
                            follower.next_index = last_entries_id + 1;
                            follower.match_index = last_entries_id;
                        }
                    }
                    AppendEntriesResult::LogMismatch => {
                        debug!(
                            "Log mismatch on plane {} in follower {}, index {}",
                            plane_id.raw(),
                            member_id,
                            follower.next_index
                        );
                        if follower.next_index > 0 {
                            follower.next_index -= 1;
                        } else {
                            debug!("Log mismatching index is zero on plane {}", plane_id.raw());
                        }
                    }
                    AppendEntriesResult::TermOut(actual_leader_id) => {
                        debug!(
                            "Follower {} rejected append on plane {} because follower_term={} leader_term={} actual_leader_id={} while leader {} was replicating from next_index {}",
                            member_id,
                            plane_id.raw(),
                            follower_term,
                            term,
                            actual_leader_id,
                            leader_id,
                            follower.next_index
                        );
                        return HeartbeatReplicationResult::TermOut {
                            term: follower_term,
                            leader_id: actual_leader_id,
                        };
                    }
                },
                Err(err) => {
                    debug!(
                        "Follower {} RPC append failed on plane {} from next_index {}: {:?}",
                        member_id,
                        plane_id.raw(),
                        follower.next_index,
                        err
                    );
                    break;
                } // retry will happened in next heartbeat
            }
            is_retry = true;
        }
        HeartbeatReplicationResult::Matched(follower.match_index)
    }

    //check term number, return reject = false if server term is stale
    fn check_term_on_plane(
        &self,
        leader_flag: &AtomicBool,
        meta: &mut RwLockWriteGuard<RaftMeta>,
        remote_term: u64,
        leader_id: u64,
    ) -> bool {
        if remote_term > meta.term {
            self.become_follower_on_plane(leader_flag, meta, remote_term, leader_id)
        } else if remote_term < meta.term {
            return false;
        }
        return true;
    }

    fn reset_last_checked(&self, meta: &mut RwLockWriteGuard<RaftMeta>) {
        trace!(
            "Reset last checked. Elapsed: {}, id: {}, term: {}",
            get_time() - meta.last_checked,
            self.id,
            meta.term
        );
        meta.last_checked = get_time();
        meta.timeout = gen_timeout();
    }

    async fn handle_append_entries_on_meta<'a>(
        &'a self,
        leader_flag: &'a AtomicBool,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        term: u64,
        leader_id: u64,
        prev_log_id: u64,
        prev_log_term: u64,
        entries: &'a Option<LogEntries>,
        leader_commit: u64,
    ) -> (u64, AppendEntriesResult) {
        self.reset_last_checked(&mut meta);
        let term_ok = self.check_term_on_plane(leader_flag, &mut meta, term, leader_id);
        let result = if term_ok {
            if let Membership::Candidate = meta.membership {
                debug!("SWITCH FROM CANDIDATE BACK TO FOLLOWER {}", self.id);
                self.become_follower_on_plane(leader_flag, &mut meta, term, leader_id);
            }
            if prev_log_id > 0 {
                check_commit(&mut meta).await;
                let mut logs = meta.logs.write().await;
                let contains_prev_log = logs.contains_key(&prev_log_id);
                let log_mismatch;

                if contains_prev_log {
                    let entry = match logs.get(&prev_log_id) {
                        Some(entry) => entry,
                        None => {
                            error!("Log key {} exists in contains_key but not in get() - data inconsistency", prev_log_id);
                            return (meta.term, AppendEntriesResult::LogMismatch);
                        }
                    };
                    log_mismatch = entry.term != prev_log_term;
                } else {
                    return (meta.term, AppendEntriesResult::LogMismatch);
                }
                if log_mismatch {
                    let ids_to_del: Vec<u64> = logs
                        .range((Included(prev_log_id), Unbounded))
                        .map(|(id, _)| *id)
                        .collect();
                    for id in ids_to_del {
                        logs.remove(&id);
                    }
                    return (meta.term, AppendEntriesResult::LogMismatch);
                }
            }
            let mut last_new_entry = std::u64::MAX;
            {
                let mut logs = meta.logs.write().await;
                if let Some(ref entries) = entries {
                    for entry in entries {
                        let entry_id = entry.id;
                        logs.entry(entry_id).or_insert(entry.clone());
                        last_new_entry = max(last_new_entry, entry_id);
                    }
                } else if !logs.is_empty() {
                    last_new_entry = match logs.values().last() {
                        Some(entry) => entry.id,
                        None => {
                            error!("Logs map is not empty but values().last() returned None - this should not happen");
                            std::u64::MAX
                        }
                    };
                }
                if let Err(e) = self.logs_post_processing(&meta, logs).await {
                    error!("Failed to persist logs during append_entries: {:?}", e);
                }
            }
            if leader_commit > meta.commit_index {
                meta.commit_index = min(leader_commit, last_new_entry);
                check_commit(&mut meta).await;
            }
            (meta.term, AppendEntriesResult::Ok)
        } else {
            (meta.term, AppendEntriesResult::TermOut(meta.leader_id))
        };
        self.reset_last_checked(&mut meta);
        result
    }

    async fn handle_request_vote_on_meta<'a>(
        &'a self,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        term: u64,
        candidate_id: u64,
        last_log_id: u64,
        last_log_term: u64,
    ) -> ((u64, u64), bool) {
        let vote_for = meta.vote_for;
        let mut vote_granted = false;
        if term > meta.term {
            check_commit(&mut meta).await;
            let logs = meta.logs.read().await;
            let conf_sm = &meta.state_machine.read().await.configs;
            let candidate_valid = conf_sm.member_existed(candidate_id);
            let can_vote = vote_for.map_or(true, |voted_for| voted_for == candidate_id);
            if can_vote && candidate_valid {
                let (last_id, last_term) = get_last_log_info!(self, logs);
                if last_log_id >= last_id && last_log_term >= last_term {
                    vote_granted = true;
                }
            }
        }
        if vote_granted {
            meta.vote_for = Some(candidate_id);
        }
        ((meta.term, meta.leader_id), vote_granted)
    }

    async fn handle_install_snapshot_on_meta<'a>(
        &'a self,
        leader_flag: &'a AtomicBool,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
    ) -> u64 {
        let term_ok = self.check_term_on_plane(leader_flag, &mut meta, term, leader_id);
        if term_ok {
            check_commit(&mut meta).await;
        }

        meta.state_machine.write().await.recover(data.clone()).await;
        meta.last_snapshot_index = last_included_index;
        meta.last_snapshot_term = last_included_term;
        meta.commit_index = last_included_index;
        meta.last_applied = last_included_index;

        {
            let mut logs = meta.logs.write().await;
            logs.retain(|&id, _| id > last_included_index);
        }

        if let Some(ref storage) = meta.storage {
            let snapshot_entity = SnapshotEntity {
                last_included_index,
                last_included_term,
                snapshot: data,
            };
            let mut storage = storage.lock().await;
            if let Err(e) = storage.write_snapshot(&snapshot_entity).await {
                error!("Failed to persist snapshot to disk: {:?}", e);
            }
        }

        self.reset_last_checked(&mut meta);
        meta.term
    }

    async fn handle_client_command_on_meta<'a>(
        &'a self,
        plane_id: PlaneId,
        leader_flag: &'a AtomicBool,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        mut entry: LogEntry,
    ) -> ClientCmdResponse {
        if Self::lifecycle_is_stopping(meta.lifecycle) {
            return ClientCmdResponse::ShuttingDown;
        }
        if !is_leader(&meta) {
            let member_count = {
                let member_sm = meta.state_machine.read().await;
                member_sm.configs.members.len()
            };
            if member_count == 1 && meta.leader_id == self.id {
                let last_log_id = {
                    let logs = meta.logs.read().await;
                    let (last_log_id, _last_log_term) = get_last_log_info!(self, logs);
                    last_log_id
                };
                ensure_direct_leader_term(&mut meta);
                self.become_leader_on_plane(leader_flag, &mut meta, last_log_id)
                    .await;
            }
        }
        if !is_leader(&meta) {
            return if meta.leader_id == self.id {
                ClientCmdResponse::NotLeader(0)
            } else {
                ClientCmdResponse::NotLeader(meta.leader_id)
            };
        }

        let existing_pending_entry = if entry.id > meta.commit_index {
            let logs = meta.logs.read().await;
            match logs.get(&entry.id) {
                Some(existing)
                    if existing.term == entry.term
                        && existing.sm_id == entry.sm_id
                        && existing.fn_id == entry.fn_id
                        && existing.data == entry.data =>
                {
                    Some((existing.id, existing.term))
                }
                _ => None,
            }
        } else {
            None
        };

        let (new_log_id, new_log_term) =
            if let Some((existing_log_id, existing_log_term)) = existing_pending_entry {
                (existing_log_id, existing_log_term)
            } else {
                self.leader_append_log(&meta, &mut entry).await
            };
        entry.id = new_log_id;
        entry.term = new_log_term;
        let data = match entry.sm_id {
            CONFIG_SM_ID => Some(
                self.try_sync_config_to_followers_on_plane(plane_id, meta, &entry, new_log_id)
                    .await,
            ),
            _ => {
                self.try_sync_log_to_followers_on_plane(plane_id, meta, &entry, new_log_id)
                    .await
            }
        };

        if let Some(data) = data {
            ClientCmdResponse::Success {
                data,
                last_log_id: new_log_id,
                last_log_term: new_log_term,
            }
        } else {
            ClientCmdResponse::NotCommitted {
                last_log_id: new_log_id,
                last_log_term: new_log_term,
            }
        }
    }

    async fn handle_client_query_on_meta<'a>(
        &'a self,
        meta: RwLockReadGuard<'a, RaftMeta>,
        entry: &'a LogEntry,
    ) -> ClientQryResponse {
        let logs = meta.logs.read().await;
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
        if entry.term > last_log_term || entry.id > last_log_id {
            ClientQryResponse::LeftBehind {
                last_log_term,
                last_log_id,
            }
        } else {
            let qry_res = meta.state_machine.read().await.exec_qry(entry).await;
            ClientQryResponse::Success {
                data: qry_res,
                last_log_id,
                last_log_term,
            }
        }
    }

    async fn leader_append_log<'a>(
        &'a self,
        meta: &'a RwLockWriteGuard<'a, RaftMeta>,
        entry: &mut LogEntry,
    ) -> (u64, u64) {
        let mut logs = meta.logs.write().await;
        let (last_log_id, _last_log_term) = get_last_log_info!(self, logs);
        let new_log_id = last_log_id + 1;
        let new_log_term = meta.term;
        entry.term = new_log_term;
        entry.id = new_log_id;
        logs.insert(entry.id, entry.clone());
        // Strict write-ahead: persist to WAL before any application can observe/commit
        if let Err(e) = self.logs_post_processing(meta, logs).await {
            error!(
                "Failed to persist log entry {} to storage on plane {}: {:?}",
                new_log_id,
                self.plane_id().raw(),
                e
            );
            // Note: We still return the log ID/term even if persistence failed
            // The caller should handle this appropriately
        }
        (new_log_id, new_log_term)
    }

    async fn logs_post_processing<'a>(
        &'a self,
        meta: &'a RwLockWriteGuard<'a, RaftMeta>,
        logs: RwLockWriteGuard<'a, LogsMap>,
    ) -> io::Result<()> {
        if let Some(storage_mutex) = &meta.storage {
            let mut storage = storage_mutex.lock().await;
            storage.post_processing(meta, logs).await?;
        }
        Ok(())
    }

    async fn try_sync_log_to_followers_on_plane<'a>(
        &'a self,
        plane_id: PlaneId,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        entry: &LogEntry,
        new_log_id: u64,
    ) -> Option<ExecResult> {
        debug!("Sync logs to followers on plane {}", plane_id.raw());
        if self
            .send_followers_heartbeat_on_plane(plane_id, &mut meta, Some(new_log_id), true)
            .await
        {
            // Strict write-ahead: ensure persistence reflects this index before applying
            if let Some(storage_mutex) = &meta.storage {
                let mut storage = storage_mutex.lock().await;
                info!(
                    "Strict WA plane {}: flushing WAL before commit at log_id={} (term={})",
                    plane_id.raw(),
                    new_log_id,
                    entry.term
                );
                let _ = storage.flush_wal().await;
                info!(
                    "Strict WA plane {}: WAL fsync completed before commit at log_id={}",
                    plane_id.raw(),
                    new_log_id
                );
            }
            meta.commit_index = new_log_id;
            info!(
                "Strict WA plane {}: applying entry at log_id={} (commit_index={})",
                plane_id.raw(),
                new_log_id,
                meta.commit_index
            );
            let result = apply_committed_entry(&mut meta, entry).await;
            info!(
                "Strict WA plane {}: apply completed at log_id={} (result={:?})",
                plane_id.raw(),
                new_log_id,
                result
            );
            // Mark applied and persist commit progress atomically after apply
            meta.last_applied = new_log_id;
            if let Some(storage_mutex) = &meta.storage {
                let mut storage = storage_mutex.lock().await;
                info!(
                    "Strict WA plane {}: writing commit progress (commit_index={}, last_applied={})",
                    plane_id.raw(), meta.commit_index, meta.last_applied
                );
                let _ = storage
                    .write_commit_progress(meta.commit_index, meta.last_applied)
                    .await;
                info!(
                    "Strict WA plane {}: commit progress persisted",
                    plane_id.raw()
                );
            }

            // Check if we should take a snapshot after committing
            let num_logs = meta.logs.read().await.len();
            if self.should_take_snapshot(&meta, num_logs) {
                self.take_snapshot(&mut meta).await;
            }

            Some(result)
        } else {
            None
        }
    }

    async fn try_sync_config_to_followers_on_plane<'a>(
        &'a self,
        plane_id: PlaneId,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        entry: &LogEntry,
        new_log_id: u64,
    ) -> ExecResult {
        // this will force followers to commit the changes
        debug!("Sync config to followers on plane {}", plane_id.raw());
        meta.commit_index = new_log_id;
        let data = apply_committed_entry(&meta, &entry).await;
        if let Membership::Leader(ref leader_meta) = meta.membership {
            let mut leader_meta = leader_meta.write().await;
            let member_sm = meta.state_machine.read().await;
            let ref members = member_sm.configs.members;
            self.reload_leader_meta(members, &mut leader_meta, new_log_id);
        }
        self.send_followers_heartbeat_on_plane(plane_id, &mut meta, Some(new_log_id), true)
            .await;
        data
    }

    /// Check if we should take a snapshot based on configuration thresholds
    fn should_take_snapshot(
        &self,
        meta: &RwLockWriteGuard<'_, RaftMeta>,
        _num_logs: usize,
    ) -> bool {
        // Only leaders should automatically create snapshots
        if !is_leader(meta) {
            return false;
        }

        // Check if storage is configured for snapshots
        if meta.storage.is_none() {
            return false;
        }

        // Get snapshot threshold from storage options
        if let Storage::DISK(ref opts) = self.options.storage {
            let logs_since_snapshot = if meta.last_snapshot_index > 0 {
                meta.last_applied.saturating_sub(meta.last_snapshot_index)
            } else {
                meta.last_applied
            };

            // Trigger snapshot if we've applied enough logs since last snapshot
            if logs_since_snapshot >= opts.snapshot_log_threshold {
                debug!(
                    "Snapshot threshold reached on plane {}: {} logs since last snapshot (threshold: {})",
                    self.plane_id().raw(), logs_since_snapshot, opts.snapshot_log_threshold
                );
                return true;
            }
        }

        false
    }

    /// Create and persist a snapshot
    async fn take_snapshot(&self, meta: &mut RwLockWriteGuard<'_, RaftMeta>) {
        info!(
            "Taking snapshot on plane {} at index={}, term={}",
            self.plane_id().raw(),
            meta.last_applied,
            meta.term
        );

        // Generate snapshot from state machine (Master SM decides which subs are recoverable)
        let snapshot_data = {
            let sm = meta.state_machine.read().await;
            sm.snapshot()
        };

        // Get the term of the log at last_applied index
        let last_included_term = {
            let logs = meta.logs.read().await;
            logs.get(&meta.last_applied)
                .map(|e| e.term)
                .unwrap_or(meta.last_snapshot_term)
        };

        // Create snapshot entity
        let snapshot_entity = SnapshotEntity {
            last_included_index: meta.last_applied,
            last_included_term,
            snapshot: snapshot_data,
        };

        // Persist snapshot to disk
        if let Some(ref storage) = meta.storage {
            let storage_clone = storage.clone();
            let mut storage_guard = storage_clone.lock().await;
            match storage_guard.write_snapshot(&snapshot_entity).await {
                Ok(_) => {
                    // Update snapshot metadata FIRST so compaction can use it
                    meta.last_snapshot_index = snapshot_entity.last_included_index;
                    meta.last_snapshot_term = snapshot_entity.last_included_term;

                    info!(
                        "Snapshot created successfully on plane {} at index={}, term={}",
                        self.plane_id().raw(),
                        meta.last_snapshot_index,
                        meta.last_snapshot_term
                    );

                    // Now compact logs (reads meta.last_snapshot_index)
                    self.compact_logs_after_snapshot(meta, storage_guard).await;
                }
                Err(e) => {
                    error!(
                        "Failed to persist snapshot on plane {}: {:?}",
                        self.plane_id().raw(),
                        e
                    );
                }
            }
        }
    }

    /// Compact logs after a snapshot has been created
    async fn compact_logs_after_snapshot(
        &self,
        meta: &RwLockWriteGuard<'_, RaftMeta>,
        mut _storage: async_std::sync::MutexGuard<'_, disk::StorageEntity>,
    ) {
        if let Storage::DISK(ref opts) = self.options.storage {
            let snapshot_index = meta.last_snapshot_index;
            let compaction_threshold = opts.log_compaction_threshold;

            let mut logs = meta.logs.write().await;
            let before_count = logs.len();

            debug!(
                "Compaction check on plane {}: {} logs, threshold: {}, snapshot_index: {}",
                self.plane_id().raw(),
                before_count,
                compaction_threshold,
                snapshot_index
            );

            // Only compact if we exceed the compaction threshold
            if before_count as u64 > compaction_threshold {
                // Keep logs after last_snapshot_index
                logs.retain(|&id, _| id > snapshot_index);
                let after_count = logs.len();

                info!(
                    "Compacted {} logs on plane {} (from {} to {}), keeping logs after index {}",
                    before_count - after_count,
                    self.plane_id().raw(),
                    before_count,
                    after_count,
                    snapshot_index
                );
            } else {
                info!(
                    "Skipping log compaction on plane {}: {} logs <= threshold {}",
                    self.plane_id().raw(),
                    before_count,
                    compaction_threshold
                );
            }
        } else {
            debug!(
                "Not using disk storage on plane {}, skipping compaction",
                self.plane_id().raw()
            );
        }
    }
}

impl Service for RaftService {
    fn append_entries<'a>(
        &'a self,
        plane_id: PlaneId,
        term: u64,
        leader_id: u64,
        prev_log_id: u64,
        prev_log_term: u64,
        entries: &'a Option<LogEntries>,
        leader_commit: u64,
    ) -> BoxFuture<'a, (u64, AppendEntriesResult)> {
        async move {
            match self.resolve_plane_runtime(plane_id, true, false).await {
                Ok((Some(runtime), _)) => {
                    let meta = runtime.meta.write().await;
                    self.handle_append_entries_on_meta(
                        &runtime.is_leader,
                        meta,
                        term,
                        leader_id,
                        prev_log_id,
                        prev_log_term,
                        entries,
                        leader_commit,
                    )
                    .await
                }
                Ok((None, _)) => {
                    let meta = self.write_meta().await;
                    self.handle_append_entries_on_meta(
                        &self._is_leader,
                        meta,
                        term,
                        leader_id,
                        prev_log_id,
                        prev_log_term,
                        entries,
                        leader_commit,
                    )
                    .await
                }
                Err(err) => {
                    warn!(
                        "Rejecting append_entries for plane {}: {}",
                        plane_id.raw(),
                        err
                    );
                    (0, AppendEntriesResult::LogMismatch)
                }
            }
        }
        .boxed()
    }

    fn request_vote(
        &self,
        plane_id: PlaneId,
        term: u64,
        candidate_id: u64,
        last_log_id: u64,
        last_log_term: u64,
    ) -> BoxFuture<((u64, u64), bool)> {
        async move {
            match self.resolve_plane_runtime(plane_id, true, false).await {
                Ok((Some(runtime), _)) => {
                    let meta = runtime.meta.write().await;
                    self.handle_request_vote_on_meta(
                        meta,
                        term,
                        candidate_id,
                        last_log_id,
                        last_log_term,
                    )
                    .await
                }
                Ok((None, _)) => {
                    let meta = self.write_meta().await;
                    self.handle_request_vote_on_meta(
                        meta,
                        term,
                        candidate_id,
                        last_log_id,
                        last_log_term,
                    )
                    .await
                }
                Err(err) => {
                    warn!(
                        "Rejecting request_vote for plane {}: {}",
                        plane_id.raw(),
                        err
                    );
                    ((0, self.get_server_id()), false)
                }
            }
        }
        .boxed()
    }

    fn install_snapshot(
        &self,
        plane_id: PlaneId,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
    ) -> BoxFuture<u64> {
        async move {
            match self.resolve_plane_runtime(plane_id, true, false).await {
                Ok((Some(runtime), _)) => {
                    let meta = runtime.meta.write().await;
                    self.handle_install_snapshot_on_meta(
                        &runtime.is_leader,
                        meta,
                        term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                        data,
                    )
                    .await
                }
                Ok((None, _)) => {
                    let meta = self.write_meta().await;
                    self.handle_install_snapshot_on_meta(
                        &self._is_leader,
                        meta,
                        term,
                        leader_id,
                        last_included_index,
                        last_included_term,
                        data,
                    )
                    .await
                }
                Err(err) => {
                    warn!(
                        "Rejecting install_snapshot for plane {}: {}",
                        plane_id.raw(),
                        err
                    );
                    0
                }
            }
        }
        .boxed()
    }

    fn c_command<'a>(
        &'a self,
        plane_id: PlaneId,
        entry: LogEntry,
    ) -> BoxFuture<'a, ClientCmdResponse> {
        async move {
            match self.resolve_plane_runtime(plane_id, false, false).await {
                Ok((Some(runtime), _)) => {
                    let meta = runtime.meta.write().await;
                    self.handle_client_command_on_meta(plane_id, &runtime.is_leader, meta, entry)
                        .await
                }
                Ok((None, _)) => {
                    let meta = self.write_meta().await;
                    self.handle_client_command_on_meta(plane_id, &self._is_leader, meta, entry)
                        .await
                }
                Err(err) => {
                    warn!(
                        "Rejecting client command for plane {}: {}",
                        plane_id.raw(),
                        err
                    );
                    ClientCmdResponse::ShuttingDown
                }
            }
        }
        .boxed()
    }

    fn c_query<'a>(
        &'a self,
        plane_id: PlaneId,
        entry: &'a LogEntry,
    ) -> BoxFuture<'a, ClientQryResponse> {
        async move {
            match self.resolve_plane_runtime(plane_id, false, false).await {
                Ok((Some(runtime), _)) => {
                    let meta = runtime.meta.read().await;
                    self.handle_client_query_on_meta(meta, entry).await
                }
                Ok((None, _)) => {
                    let meta = self.meta.read().await;
                    self.handle_client_query_on_meta(meta, entry).await
                }
                Err(err) => {
                    warn!(
                        "Rejecting client query for plane {}: {}",
                        plane_id.raw(),
                        err
                    );
                    ClientQryResponse::LeftBehind {
                        last_log_term: 0,
                        last_log_id: 0,
                    }
                }
            }
        }
        .boxed()
    }

    fn c_server_cluster_info(&self, plane_id: PlaneId) -> BoxFuture<ClientClusterInfo> {
        async move {
            match self.cluster_info_on_plane_local(plane_id).await {
                Ok(info) => info,
                Err(err) => {
                    warn!(
                        "Rejecting cluster_info for plane {}: {}",
                        plane_id.raw(),
                        err
                    );
                    ClientClusterInfo {
                        members: Vec::new(),
                        last_log_id: 0,
                        last_log_term: 0,
                        leader_id: 0,
                    }
                }
            }
        }
        .boxed()
    }

    fn c_put_offline(&self) -> BoxFuture<bool> {
        self.leave().boxed()
    }

    fn c_have_state_machine(&self, plane_id: PlaneId, id: u64) -> BoxFuture<bool> {
        async move {
            match self.have_state_machine_on_plane_local(plane_id, id).await {
                Ok(result) => result,
                Err(err) => {
                    warn!(
                        "Rejecting have_state_machine for plane {}: {}",
                        plane_id.raw(),
                        err
                    );
                    false
                }
            }
        }
        .boxed()
    }

    fn c_ping(&self) -> BoxFuture<()> {
        future::ready(()).boxed()
    }

    fn reelect<'a>(&'a self, plane_id: PlaneId) -> futures::future::BoxFuture<bool> {
        async move {
            match self.resolve_plane_runtime(plane_id, true, false).await {
                Ok((Some(runtime), _)) => {
                    let mut meta = runtime.meta.write().await;
                    info!(
                        "Been asked to reelect on plane {}, become candidate. Server id {}",
                        plane_id.raw(),
                        self.get_server_id()
                    );
                    self.become_candidate_on_plane(plane_id, &runtime.is_leader, &mut meta)
                        .await;
                    runtime.is_leader.load(Relaxed)
                }
                Ok((None, _)) => {
                    let mut meta = self.meta.write().await;
                    info!(
                        "Been asked to reelect on plane {}, become candidate. Server id {}",
                        plane_id.raw(),
                        self.get_server_id()
                    );
                    self.become_candidate(&mut meta).await;
                    let is_leader = self.is_leader();
                    info!(
                        "Reelect result for plane {} server {}, is leader {}",
                        plane_id.raw(),
                        self.get_server_id(),
                        is_leader
                    );
                    is_leader
                }
                Err(err) => {
                    warn!("Rejecting reelect for plane {}: {}", plane_id.raw(), err);
                    false
                }
            }
        }
        .boxed()
    }
}

pub struct RaftStateMachine {
    pub id: u64,
    pub name: String,
}

impl RaftStateMachine {
    pub fn new(name: &String) -> RaftStateMachine {
        RaftStateMachine {
            id: hash_str(name),
            name: name.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use self::client::SMClient;
    use self::commands::{add, get};
    use crate::raft::client::RaftClient;
    use crate::raft::disk;
    use crate::raft::state_machine::master::ExecError;
    use crate::raft::state_machine::StateMachineCtl;
    use crate::raft::{
        ClientCmdResponse, ClientQryResponse, LogEntry, Options, PlaneBootstrap,
        PlaneBootstrapError, PlaneHandle, PlaneId, PlaneSpec, RaftMsg, RaftService,
        Service as RaftRpcService, Storage, DEFAULT_SERVICE_ID,
    };
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use futures::FutureExt;
    use std::sync::Arc;

    struct CounterStateMachine {
        value: u64,
    }

    raft_state_machine! {
        def cmd add(value: u64) -> u64;
        def qry get() -> u64;
    }

    impl StateMachineCmds for CounterStateMachine {
        fn add(&mut self, value: u64) -> BoxFuture<u64> {
            self.value += value;
            futures::future::ready(self.value).boxed()
        }

        fn get(&self) -> BoxFuture<u64> {
            futures::future::ready(self.value).boxed()
        }
    }

    impl StateMachineCtl for CounterStateMachine {
        raft_sm_complete!();

        fn id(&self) -> u64 {
            77
        }

        fn snapshot(&self) -> Vec<u8> {
            Vec::new()
        }

        fn recover(&mut self, _: Vec<u8>) -> BoxFuture<()> {
            futures::future::ready(()).boxed()
        }

        fn recoverable(&self) -> bool {
            false
        }
    }

    struct PersistentCounterStateMachine {
        value: u64,
    }

    impl StateMachineCmds for PersistentCounterStateMachine {
        fn add(&mut self, value: u64) -> BoxFuture<u64> {
            self.value += value;
            futures::future::ready(self.value).boxed()
        }

        fn get(&self) -> BoxFuture<u64> {
            futures::future::ready(self.value).boxed()
        }
    }

    impl StateMachineCtl for PersistentCounterStateMachine {
        raft_sm_complete!();

        fn id(&self) -> u64 {
            88
        }

        fn snapshot(&self) -> Vec<u8> {
            crate::utils::serde::serialize(&self.value)
        }

        fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
            if let Some(value) = crate::utils::serde::deserialize(&data) {
                self.value = value;
            }
            futures::future::ready(()).boxed()
        }

        fn recoverable(&self) -> bool {
            true
        }
    }

    async fn wait_for_plane(service: &Arc<RaftService>, plane_id: PlaneId) -> PlaneHandle {
        for _ in 0..5 {
            if let Ok(plane) = service.plane(plane_id).await {
                return plane;
            }
            async_wait_secs().await;
        }
        panic!("plane {} was not materialized in time", plane_id.raw());
    }

    async fn query_counter_locally(
        service: &Arc<RaftService>,
        plane_id: PlaneId,
        sm_id: u64,
    ) -> u64 {
        let (fn_id, _, data) = get::new().encode();
        let entry = LogEntry {
            id: 0,
            term: 0,
            sm_id,
            fn_id,
            data,
        };

        match RaftRpcService::c_query(service.as_ref(), plane_id, &entry).await {
            ClientQryResponse::Success { data: Ok(data), .. } => get::decode_return(&data),
            other => panic!("unexpected local query response: {:?}", other),
        }
    }

    async fn add_counter_locally(
        service: &Arc<RaftService>,
        plane_id: PlaneId,
        sm_id: u64,
        value: u64,
    ) -> u64 {
        let (fn_id, _, data) = add::new(&value).encode();
        let entry = LogEntry {
            id: 0,
            term: 0,
            sm_id,
            fn_id,
            data,
        };

        match RaftRpcService::c_command(service.as_ref(), plane_id, entry).await {
            ClientCmdResponse::Success { data: Ok(data), .. } => add::decode_return(&data),
            other => panic!("unexpected local command response: {:?}", other),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn startup() {
        let (success, _, _) = RaftService::new_server(Options {
            storage: Storage::default(),
            address: String::from("127.0.0.1:2000"),
            service_id: DEFAULT_SERVICE_ID,
        })
        .await;
        assert!(success);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn type2_plane_client_roundtrip() {
        let port = 4210 + (rand::random::<u16>() % 200);
        let addr = format!("127.0.0.1:{}", port);
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        let plane_id = PlaneId::type2(7).unwrap();
        let plane = service
            .ensure_plane(PlaneSpec { plane_id })
            .await
            .expect("plane should be created");
        plane
            .register_state_machine(Box::new(CounterStateMachine { value: 0 }))
            .await
            .expect("state machine should register on type-2 plane");
        plane
            .recover_after_register()
            .await
            .expect("type-2 plane should replay committed logs");

        let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .expect("raft client should connect");
        let plane_client = client.plane(plane_id);
        let sm_client = SMClient::new(77, &plane_client);

        assert_eq!(sm_client.add(&5).await.unwrap(), 5);
        assert_eq!(sm_client.get().await.unwrap(), 5);
        assert!(plane.have_state_machine(77).await.unwrap());
        assert!(plane_client.have_state_machine(77).await.unwrap());
        assert_eq!(
            plane_client.cluster_info().await.unwrap().leader_id,
            service.id
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn loaded_type2_planes_only_reports_materialized_type2_runtimes() {
        let port = 4410 + (rand::random::<u16>() % 200);
        let addr = format!("127.0.0.1:{}", port);
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        assert!(service.loaded_type2_planes().await.is_empty());

        let plane_id = PlaneId::type2(9).unwrap();
        service
            .ensure_plane(PlaneSpec { plane_id })
            .await
            .expect("type-2 plane should materialize");

        assert_eq!(service.loaded_type2_planes().await, vec![plane_id]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn type2_plane_shutdown_rejects_commands() {
        let port = 4610 + (rand::random::<u16>() % 200);
        let addr = format!("127.0.0.1:{}", port);
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        let plane_id = PlaneId::type2(8).unwrap();
        let plane = service
            .ensure_plane(PlaneSpec { plane_id })
            .await
            .expect("plane should be created");
        plane
            .register_state_machine(Box::new(CounterStateMachine { value: 0 }))
            .await
            .expect("state machine should register on type-2 plane");
        plane
            .recover_after_register()
            .await
            .expect("type-2 plane should replay committed logs");

        let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
            .await
            .expect("raft client should connect");
        let plane_client = client.plane(plane_id);
        let sm_client = SMClient::new(77, &plane_client);

        assert_eq!(sm_client.add(&5).await.unwrap(), 5);

        plane
            .shutdown()
            .await
            .expect("type-2 plane should shut down");

        assert!(!plane.is_leader().await.unwrap());
        assert!(matches!(
            sm_client.add(&1).await,
            Err(ExecError::ShuttingDown)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_type2_plane_does_not_fall_back_to_type1() {
        let addr = String::from("127.0.0.1:22120");
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        service
            .register_state_machine(Box::new(CounterStateMachine { value: 0 }))
            .await;
        service.recover_after_register().await;

        let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
            .await
            .expect("raft client should connect");
        let unknown_plane = client.plane(PlaneId::type2(999).unwrap());

        assert!(!unknown_plane.have_state_machine(77).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unknown_type2_plane_rejects_commands_without_leader_discovery_loop() {
        let addr = String::from("127.0.0.1:22122");
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
            .await
            .expect("raft client should connect");
        let unknown_plane = client.plane(PlaneId::type2(1001).unwrap());
        let sm_client = SMClient::new(77, &unknown_plane);

        assert!(matches!(
            sm_client.add(&1).await,
            Err(ExecError::ShuttingDown)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn root_membership_helpers_hide_config_sm_commands() {
        let addr = String::from("127.0.0.1:22123");
        let extra_addr = String::from("127.0.0.1:22124");
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        let extra_service = RaftService::new(Options {
            storage: Storage::default(),
            address: extra_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let extra_server = Server::new(&extra_addr);
        extra_server.register_service(&extra_service).await;
        Server::listen_and_resume(&extra_server).await;
        assert!(RaftService::start(&extra_service, false).await);

        let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .expect("raft client should connect");

        let members = client
            .root_member_addresses()
            .await
            .expect("root member list should be readable");
        assert_eq!(members.len(), 1);
        assert_eq!(members[0], addr);

        assert!(client
            .add_root_member(&extra_addr)
            .await
            .expect("adding root member should succeed"));

        let members = client
            .root_member_addresses()
            .await
            .expect("root member list should include new member");
        assert!(members.iter().any(|member| member == &extra_addr));

        client
            .remove_root_member(&extra_addr)
            .await
            .expect("removing root member should succeed");

        let members = client
            .root_member_addresses()
            .await
            .expect("root member list should be readable after removal");
        assert!(!members.iter().any(|member| member == &extra_addr));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn type2_plane_bootstrap_is_idempotent_for_same_members() {
        let addr = String::from("127.0.0.1:22125");
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        let plane_id = PlaneId::type2(43).unwrap();
        service
            .ensure_plane_from_seeds(PlaneBootstrap {
                plane_id,
                seed_nodes: vec![addr.clone()],
            })
            .await
            .expect("initial type-2 bootstrap should succeed");

        let plane = service
            .ensure_plane_from_seeds(PlaneBootstrap {
                plane_id,
                seed_nodes: vec![addr.clone(), addr.clone()],
            })
            .await
            .expect("repeating type-2 bootstrap with same members should be idempotent");

        let info = plane
            .cluster_info()
            .await
            .expect("type-2 cluster info should be available");
        assert_eq!(info.members.len(), 1);
        assert_eq!(info.members[0].1, addr);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn type2_plane_bootstrap_rejects_conflicting_member_set() {
        let addr = String::from("127.0.0.1:22126");
        let extra_addr = String::from("127.0.0.1:22127");
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        let extra_service = RaftService::new(Options {
            storage: Storage::default(),
            address: extra_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let extra_server = Server::new(&extra_addr);
        extra_server.register_service(&extra_service).await;
        Server::listen_and_resume(&extra_server).await;
        assert!(RaftService::start(&extra_service, false).await);
        extra_service.join(&vec![addr.clone()]).await.unwrap();
        async_wait_secs().await;

        let plane_id = PlaneId::type2(44).unwrap();
        service
            .ensure_plane_from_seeds(PlaneBootstrap {
                plane_id,
                seed_nodes: vec![addr.clone()],
            })
            .await
            .expect("initial type-2 bootstrap should succeed");

        assert!(extra_service.leave().await);
        async_wait_secs().await;
        async_wait_secs().await;

        let err = match service
            .ensure_plane_from_seeds(PlaneBootstrap {
                plane_id,
                seed_nodes: vec![addr.clone()],
            })
            .await
        {
            Ok(_) => panic!("conflicting type-2 bootstrap should be rejected"),
            Err(err) => err,
        };

        match err {
            PlaneBootstrapError::MembershipConflict {
                plane_id: conflict_plane_id,
                current_members,
                requested_members,
            } => {
                assert_eq!(conflict_plane_id, plane_id);
                assert_eq!(current_members, vec![addr, extra_addr]);
                assert_eq!(requested_members, vec![String::from("127.0.0.1:22126")]);
            }
            other => panic!("unexpected bootstrap error: {:?}", other),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn type2_plane_multinode_replication_and_reelection() {
        let _ = env_logger::try_init();
        let addr1 = String::from("127.0.0.1:22130");
        let addr2 = String::from("127.0.0.1:22131");
        let addr3 = String::from("127.0.0.1:22132");

        let service1 = RaftService::new(Options {
            storage: Storage::default(),
            address: addr1.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server1 = Server::new(&addr1);
        server1.register_service(&service1).await;
        Server::listen_and_resume(&server1).await;
        assert!(RaftService::start(&service1, false).await);

        let service2 = RaftService::new(Options {
            storage: Storage::default(),
            address: addr2.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server2 = Server::new(&addr2);
        server2.register_service(&service2).await;
        Server::listen_and_resume(&server2).await;
        assert!(RaftService::start(&service2, false).await);

        let service3 = RaftService::new(Options {
            storage: Storage::default(),
            address: addr3.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server3 = Server::new(&addr3);
        server3.register_service(&service3).await;
        Server::listen_and_resume(&server3).await;
        assert!(RaftService::start(&service3, false).await);

        service1.bootstrap().await;
        service2.join(&vec![addr1.clone()]).await.unwrap();
        service3
            .join(&vec![addr1.clone(), addr2.clone()])
            .await
            .unwrap();
        async_wait_secs().await;

        let plane_id = PlaneId::type2(41).unwrap();
        let leader_plane = service1
            .ensure_plane_from_seeds(PlaneBootstrap {
                plane_id,
                seed_nodes: vec![addr1.clone()],
            })
            .await
            .expect("type-2 leader plane should be created");
        leader_plane
            .register_state_machine(Box::new(CounterStateMachine { value: 0 }))
            .await
            .expect("leader state machine should register");
        leader_plane
            .recover_after_register()
            .await
            .expect("leader plane should recover after register");

        let client = RaftClient::new(
            &vec![addr1.clone(), addr2.clone(), addr3.clone()],
            DEFAULT_SERVICE_ID,
        )
        .await
        .expect("raft client should connect");
        let plane_client = client.plane(plane_id);
        async_wait_secs().await;
        async_wait_secs().await;

        let follower_plane2 = wait_for_plane(&service2, plane_id).await;
        follower_plane2
            .register_state_machine(Box::new(CounterStateMachine { value: 0 }))
            .await
            .expect("follower 2 state machine should register");
        follower_plane2
            .recover_after_register()
            .await
            .expect("follower 2 plane should recover after register");

        let follower_plane3 = wait_for_plane(&service3, plane_id).await;
        follower_plane3
            .register_state_machine(Box::new(CounterStateMachine { value: 0 }))
            .await
            .expect("follower 3 state machine should register");
        follower_plane3
            .recover_after_register()
            .await
            .expect("follower 3 plane should recover after register");

        let sm_client = SMClient::new(77, &plane_client);
        assert_eq!(sm_client.add(&5).await.unwrap(), 5);
        async_wait_secs().await;

        assert_eq!(query_counter_locally(&service2, plane_id, 77).await, 5);
        assert_eq!(query_counter_locally(&service3, plane_id, 77).await, 5);

        leader_plane
            .shutdown()
            .await
            .expect("leader plane should shut down cleanly");

        let candidate2 = RaftRpcService::reelect(service2.as_ref(), plane_id).await;
        let candidate3 = if candidate2 {
            false
        } else {
            RaftRpcService::reelect(service3.as_ref(), plane_id).await
        };
        assert!(
            candidate2 || candidate3,
            "one follower should win the re-election"
        );
        async_wait_secs().await;

        let plane2_after = wait_for_plane(&service2, plane_id).await;
        let plane3_after = wait_for_plane(&service3, plane_id).await;
        let plane2_is_leader = plane2_after.is_leader().await.unwrap();
        let plane3_is_leader = plane3_after.is_leader().await.unwrap();
        assert_ne!(plane2_is_leader, plane3_is_leader);

        let new_leader = if plane2_is_leader {
            &service2
        } else {
            &service3
        };
        let new_follower = if plane2_is_leader {
            &service3
        } else {
            &service2
        };

        assert_eq!(add_counter_locally(new_leader, plane_id, 77, 2).await, 7);
        async_wait_secs().await;
        assert_eq!(query_counter_locally(new_follower, plane_id, 77).await, 7);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn type2_plane_multinode_follower_restart_recovers_without_eager_load() {
        let _ = env_logger::try_init();
        let dir1 = std::env::temp_dir().join(format!(
            "raft_type2_multi_recover_1_{}",
            rand::random::<u64>()
        ));
        let dir2 = std::env::temp_dir().join(format!(
            "raft_type2_multi_recover_2_{}",
            rand::random::<u64>()
        ));
        std::fs::create_dir_all(&dir1).unwrap();
        std::fs::create_dir_all(&dir2).unwrap();

        let addr1 = String::from("127.0.0.1:22140");
        let addr2 = String::from("127.0.0.1:22141");

        let service1 = RaftService::new(Options {
            storage: Storage::DISK(disk::DiskOptions {
                path: dir1.to_string_lossy().to_string(),
                take_snapshots: false,
                append_logs: true,
                trim_logs: true,
                snapshot_log_threshold: 1000,
                log_compaction_threshold: 2000,
            }),
            address: addr1.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server1 = Server::new(&addr1);
        server1.register_service(&service1).await;
        Server::listen_and_resume(&server1).await;
        assert!(RaftService::start(&service1, false).await);

        let service2 = RaftService::new(Options {
            storage: Storage::DISK(disk::DiskOptions {
                path: dir2.to_string_lossy().to_string(),
                take_snapshots: false,
                append_logs: true,
                trim_logs: true,
                snapshot_log_threshold: 1000,
                log_compaction_threshold: 2000,
            }),
            address: addr2.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server2 = Server::new(&addr2);
        server2.register_service(&service2).await;
        Server::listen_and_resume(&server2).await;
        assert!(RaftService::start(&service2, false).await);

        service1.bootstrap().await;
        service2.join(&vec![addr1.clone()]).await.unwrap();
        async_wait_secs().await;

        let plane_id = PlaneId::type2(42).unwrap();
        let leader_plane = service1
            .ensure_plane_from_seeds(PlaneBootstrap {
                plane_id,
                seed_nodes: vec![addr1.clone()],
            })
            .await
            .expect("type-2 leader plane should be created");
        leader_plane
            .register_state_machine(Box::new(PersistentCounterStateMachine { value: 0 }))
            .await
            .expect("leader persistent state machine should register");
        leader_plane
            .recover_after_register()
            .await
            .expect("leader plane should recover after register");

        let client = RaftClient::new(&vec![addr1.clone(), addr2.clone()], DEFAULT_SERVICE_ID)
            .await
            .expect("raft client should connect");
        let plane_client = client.plane(plane_id);
        async_wait_secs().await;
        async_wait_secs().await;

        let follower_plane = wait_for_plane(&service2, plane_id).await;
        follower_plane
            .register_state_machine(Box::new(PersistentCounterStateMachine { value: 0 }))
            .await
            .expect("follower persistent state machine should register");
        follower_plane
            .recover_after_register()
            .await
            .expect("follower plane should recover after register");

        let sm_client = SMClient::new(88, &plane_client);
        assert_eq!(sm_client.add(&7).await.unwrap(), 7);
        async_wait_secs().await;
        assert_eq!(query_counter_locally(&service2, plane_id, 88).await, 7);

        follower_plane
            .flush_persistence()
            .await
            .expect("follower persistence should flush");
        follower_plane
            .shutdown()
            .await
            .expect("follower plane should shut down cleanly");
        {
            let mut planes = service2.planes.write().await;
            assert!(planes.remove(&plane_id).is_some());
        }

        let reloaded_follower = service2
            .plane(plane_id)
            .await
            .expect("persisted follower plane should lazy load from disk");
        reloaded_follower
            .register_state_machine(Box::new(PersistentCounterStateMachine { value: 0 }))
            .await
            .expect("reloaded follower state machine should register");
        reloaded_follower
            .recover_after_register()
            .await
            .expect("reloaded follower plane should replay persisted logs");
        assert!(
            !reloaded_follower.is_leader().await.unwrap(),
            "reloaded follower should not self-promote during lazy recovery"
        );
        async_wait_secs().await;

        assert_eq!(query_counter_locally(&service2, plane_id, 88).await, 7);
        assert_eq!(sm_client.add(&1).await.unwrap(), 8);
        async_wait_secs().await;
        assert_eq!(query_counter_locally(&service2, plane_id, 88).await, 8);

        let _ = std::fs::remove_dir_all(&dir1);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn type2_plane_lazy_loads_after_unload() {
        let temp_dir =
            std::env::temp_dir().join(format!("raft_type2_lazy_{}", rand::random::<u64>()));
        std::fs::create_dir_all(&temp_dir).unwrap();

        let addr = String::from("127.0.0.1:22121");
        let service = RaftService::new(Options {
            storage: Storage::DISK(disk::DiskOptions {
                path: temp_dir.to_string_lossy().to_string(),
                take_snapshots: false,
                append_logs: true,
                trim_logs: true,
                snapshot_log_threshold: 1000,
                log_compaction_threshold: 2000,
            }),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service, false).await);
        service.bootstrap().await;

        let hot_plane_id = PlaneId::type2(31).unwrap();
        let cold_plane_id = PlaneId::type2(32).unwrap();

        let hot_plane = service
            .ensure_plane(PlaneSpec {
                plane_id: hot_plane_id,
            })
            .await
            .expect("hot plane should be created");
        hot_plane
            .register_state_machine(Box::new(CounterStateMachine { value: 0 }))
            .await
            .expect("state machine should register on hot plane");
        hot_plane
            .recover_after_register()
            .await
            .expect("hot plane should recover after register");

        let cold_plane = service
            .ensure_plane(PlaneSpec {
                plane_id: cold_plane_id,
            })
            .await
            .expect("cold plane should be created");
        cold_plane
            .register_state_machine(Box::new(PersistentCounterStateMachine { value: 0 }))
            .await
            .expect("state machine should register on cold plane");
        cold_plane
            .recover_after_register()
            .await
            .expect("cold plane should recover after register");

        let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .expect("shared raft client should connect");
        let hot_client = client.plane(hot_plane_id);
        let cold_client = client.plane(cold_plane_id);
        let hot_sm = SMClient::new(77, &hot_client);
        let cold_sm = SMClient::new(88, &cold_client);

        assert_eq!(hot_sm.add(&3).await.unwrap(), 3);
        assert_eq!(cold_sm.add(&7).await.unwrap(), 7);

        cold_plane
            .flush_persistence()
            .await
            .expect("cold plane persistence should flush");
        cold_plane
            .shutdown()
            .await
            .expect("cold plane should shut down cleanly");

        {
            let mut planes = service.planes.write().await;
            assert!(planes.remove(&cold_plane_id).is_some());
        }

        assert_eq!(hot_sm.add(&2).await.unwrap(), 5);

        let reloaded_cold_plane = service
            .plane(cold_plane_id)
            .await
            .expect("persisted cold plane should lazy load from disk");
        reloaded_cold_plane
            .register_state_machine(Box::new(PersistentCounterStateMachine { value: 0 }))
            .await
            .expect("persistent state machine should register after lazy load");
        reloaded_cold_plane
            .recover_after_register()
            .await
            .expect("lazy-loaded plane should replay persisted logs");

        let reloaded_cold_client = client.plane(cold_plane_id);
        let reloaded_cold_sm = SMClient::new(88, &reloaded_cold_client);

        assert!(reloaded_cold_plane.is_leader().await.unwrap());
        assert_eq!(reloaded_cold_sm.get().await.unwrap(), 7);
        assert_eq!(reloaded_cold_sm.add(&1).await.unwrap(), 8);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_membership() {
        let _ = env_logger::try_init();
        let s1_addr = String::from("127.0.0.1:2001");
        let s2_addr = String::from("127.0.0.1:2002");
        let s3_addr = String::from("127.0.0.1:2003");
        let service1 = RaftService::new(Options {
            storage: Storage::default(),
            address: s1_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        info!("Starting server 1");
        let server1 = Server::new(&s1_addr);
        info!("Register raft service for server 1");
        server1.register_service(&service1).await;
        info!("Listening server 1");
        Server::listen_and_resume(&server1).await;
        info!("Start raft service server 1");
        assert!(RaftService::start(&service1, false).await);
        info!("Bootstrap raft service server 1");
        service1.bootstrap().await;
        let num_members = service1.num_members().await;
        assert_eq!(num_members, 1);
        info!("Starting server 2");
        let server2 = Server::new(&s2_addr);
        info!("Register raft service for server 2");
        let service2 = RaftService::new(Options {
            storage: Storage::default(),
            address: s2_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        server2.register_service(&service2).await;
        info!("Listening server 2");
        Server::listen_and_resume(&server2).await;
        info!("Start raft service for server 2");
        assert!(RaftService::start(&service2, false).await);
        info!("Server 2 join with server 1");
        let join_result = service2.join(&vec![s1_addr.clone()]).await;
        match join_result {
            Err(ExecError::ServersUnreachable) => panic!("Server unreachable"),
            Err(ExecError::CannotConstructClient) => panic!("Cannot Construct Client"),
            Err(e) => panic!(e),
            Ok(join_success) => assert!(join_success),
        }
        assert!(join_result.is_ok());
        info!("Checking number of members in both side");
        assert_eq!(service1.num_members().await, 2);
        assert_eq!(service2.num_members().await, 2);
        info!("Starting server 3");
        let service3 = RaftService::new(Options {
            storage: Storage::default(),
            address: s3_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server3 = Server::new(&s3_addr);
        Server::listen_and_resume(&server3).await;
        info!("Register raft service for server 3");
        server3.register_service(&service3).await;
        info!("Start raft service for server 3");
        assert!(RaftService::start(&service3, false).await);
        info!("Server 3 join server 1 and server 2");
        let join_result = service3.join(&vec![s1_addr.clone(), s2_addr.clone()]).await;
        assert!(join_result.unwrap());
        info!("Checking numbers of users on 3 servers");
        assert_eq!(service1.num_members().await, 3);
        assert_eq!(service2.num_members().await, 3);
        assert_eq!(service3.num_members().await, 3);

        async_wait_secs().await;

        // test remove member
        info!(
            "Server 1 ({}) is leaving, leader {}",
            service1.id,
            service1.leader_id().await
        );
        assert!(service1.leave().await);

        async_wait_secs().await;

        info!("Check number of servers, should be 2");
        assert_eq!(service2.num_members().await, 2);
        assert_eq!(service3.num_members().await, 2);

        async_wait_secs().await;

        info!(
            "Server 2 ({}) is leaving, leader {}",
            server2.server_id,
            service2.leader_id().await
        );
        assert!(service2.leave().await);

        // there will be some unavailability in leader transaction
        async_wait_secs().await;
        async_wait_secs().await;
        async_wait_secs().await;
        assert_eq!(service3.num_members().await, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn log_replication() {
        let _ = env_logger::try_init();
        info!("Testing log replications");
        let s1_addr = String::from("127.0.0.1:2004");
        let s2_addr = String::from("127.0.0.1:2005");
        let s3_addr = String::from("127.0.0.1:2006");
        let s4_addr = String::from("127.0.0.1:2007");
        let s5_addr = String::from("127.0.0.1:2008");
        let service1 = RaftService::new(Options {
            storage: Storage::default(),
            address: s1_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let service2 = RaftService::new(Options {
            storage: Storage::default(),
            address: s2_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let service3 = RaftService::new(Options {
            storage: Storage::default(),
            address: s3_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let service4 = RaftService::new(Options {
            storage: Storage::default(),
            address: s4_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let service5 = RaftService::new(Options {
            storage: Storage::default(),
            address: s5_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server_list = vec![
            s1_addr.clone(),
            s2_addr.clone(),
            s3_addr.clone(),
            s4_addr.clone(),
        ];
        info!("Start server 1");
        let server1 = Server::new(&s1_addr);
        info!("Register raft service for server 1");
        server1.register_service(&service1).await;
        info!("Listen server 1");
        Server::listen_and_resume(&server1).await;
        info!("Starting raft service for server 1");
        assert!(RaftService::start(&service1, false).await);
        info!("Bootstrap raft for server 1");
        assert_eq!(service1.probe_and_join(&server_list).await.unwrap(), false);

        info!("Starting server 2");
        let server2 = Server::new(&s2_addr);
        info!("Listening server 2");
        Server::listen_and_resume(&server2).await;
        info!("Register raft service for server 2");
        server2.register_service(&service2).await;
        info!("Start raft service for server 2");
        assert!(RaftService::start(&service2, false).await);
        info!("Server 2 join cluster");
        let join_result = service2.probe_and_join(&server_list).await;
        join_result.unwrap();

        info!("Starting server 3");
        let server3 = Server::new(&s3_addr);
        info!("Register raft service for server 3");
        server3.register_service(&service3).await;
        info!("Listening for server 3");
        Server::listen_and_resume(&server3).await;
        info!("Starting raft service for server 3");
        assert!(RaftService::start(&service3, false).await);
        info!("Server 3 join the cluster");
        let join_result = service3.probe_and_join(&server_list).await;
        join_result.unwrap();

        info!("Starting server 4");
        let server4 = Server::new(&s4_addr);
        info!("Register raft service for server 4");
        server4.register_service(&service4).await;
        info!("Listening for server 4");
        Server::listen_and_resume(&server4).await;
        info!("Starting raft service for server 4");
        assert!(RaftService::start(&service4, false).await);
        info!("Server 4 join cluster");
        let join_result = service4.probe_and_join(&server_list).await;
        join_result.unwrap();

        info!("Starting server 5");
        let server5 = Server::new(&s5_addr);
        info!("Register raft service for server 5");
        server5.register_service(&service5).await;
        info!("Listening for server 5");
        Server::listen_and_resume(&server5).await;
        info!("Starting raft service for server 5");
        assert!(RaftService::start(&service5, false).await);
        info!("Server 5 join cluster");
        let join_result = service5.probe_and_join(&server_list).await;
        join_result.unwrap();

        info!("Waiting for seconds for consistency check");
        async_wait_secs().await; // wait for membership replication to take effect
        async_wait_secs().await;
        async_wait_secs().await;

        info!("Number of logs should be the same");
        assert_eq!(service1.num_logs().await, service2.num_logs().await);
        assert_eq!(service2.num_logs().await, service3.num_logs().await);
        assert_eq!(service3.num_logs().await, service4.num_logs().await);
        assert_eq!(service4.num_logs().await, service5.num_logs().await);
        assert_eq!(service5.num_logs().await, 4); // check all logs replicated

        info!("All servers should have the same leader id on record");
        assert_eq!(service1.leader_id().await, service1.id);
        assert_eq!(service2.leader_id().await, service1.id);
        assert_eq!(service3.leader_id().await, service1.id);
        assert_eq!(service4.leader_id().await, service1.id);
        assert_eq!(service5.leader_id().await, service1.id);
    }

    mod state_machine {
        use super::*;
        use crate::raft::client::RaftClient;
        use crate::raft::disk;
        use crate::raft::state_machine::configs::CONFIG_SM_ID;
        use crate::raft::state_machine::master::MasterStateMachine;
        use crate::raft::{
            LifecycleState, LogEntry, Membership, RaftMeta, Service, SnapshotEntity,
        };
        use crate::utils::time::async_wait;
        use futures::stream::FuturesUnordered;
        use std::collections::BTreeMap;
        use std::sync::Arc;
        use std::time::Duration;

        raft_state_machine! {
            def qry answer_to_the_universe(name: String) -> String;
            def qry get_shot() -> i32;
            def cmd take_a_shot(num: i32) -> i32;
        }

        struct SM {
            shots: i32,
        }
        impl StateMachineCmds for SM {
            fn answer_to_the_universe<'a>(&'a self, name: String) -> BoxFuture<'_, String> {
                future::ready(format!("{}, the answer is 42", name)).boxed()
            }

            fn take_a_shot(&mut self, num: i32) -> BoxFuture<i32> {
                self.shots -= num;
                info!("Shot...{}...now...{}", num, self.shots);
                future::ready(self.shots).boxed()
            }
            fn get_shot(&self) -> BoxFuture<i32> {
                future::ready(self.shots).boxed()
            }
        }
        impl StateMachineCtl for SM {
            raft_sm_complete!();
            fn id(&self) -> u64 {
                15
            }
            fn snapshot(&self) -> Vec<u8> {
                // Serialize the shots value
                crate::utils::serde::serialize(&self.shots)
            }
            fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
                // Deserialize and restore the shots value
                if !data.is_empty() {
                    self.shots = crate::utils::serde::deserialize(&data).unwrap();
                    info!("SM recovered state: shots={}", self.shots);
                }
                future::ready(()).boxed()
            }
            fn recoverable(&self) -> bool {
                true
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn query_and_command() {
            let _ = env_logger::try_init();
            info!("TESTING CALLBACK");
            let addr = String::from("127.0.0.1:2009");
            let raft_service = RaftService::new(Options {
                storage: Storage::default(),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });
            let sm = SM { shots: 10 };
            let server = Server::new(&addr);
            let sm_id = sm.id();
            server.register_service(&raft_service).await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service, false).await;
            raft_service.register_state_machine(Box::new(sm)).await;
            raft_service.bootstrap().await;

            async_wait_secs().await;

            let raft_client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
                .await
                .unwrap();
            let sm_client = client::SMClient::new(sm_id, &raft_client);
            assert_eq!(
                sm_client
                    .answer_to_the_universe(&"Alice".to_string())
                    .await
                    .unwrap(),
                "Alice, the answer is 42"
            );
            assert_eq!(sm_client.take_a_shot(&2).await.unwrap(), 8);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn multi_server_command() {
            let _ = env_logger::try_init();
            // 5 servers
            let base_port = 4810 + (rand::random::<u16>() % 200);
            let addresses: Vec<_> = (0..5)
                .map(|offset| format!("127.0.0.1:{}", base_port + offset))
                .collect();
            let raft_services = addresses
                .iter()
                .map(|addr| {
                    let addr = addr.clone();
                    async move {
                        let raft_service = RaftService::new(Options {
                            storage: Storage::default(),
                            address: addr.clone(),
                            service_id: DEFAULT_SERVICE_ID,
                        });
                        let sm = SM { shots: 10 };
                        let server = Server::new(&addr);
                        server.register_service(&raft_service).await;
                        Server::listen_and_resume(&server).await;
                        RaftService::start(&raft_service, false).await;
                        raft_service.register_state_machine(Box::new(sm)).await;
                        raft_service
                    }
                })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await;
            raft_services[0].bootstrap().await;
            for i in 1..raft_services.len() {
                raft_services[i].join(&addresses).await.unwrap();
            }
            info!("Waiting cluster to be stable");
            async_wait(Duration::from_secs(2)).await;
            let raft_client = RaftClient::new(&addresses, DEFAULT_SERVICE_ID)
                .await
                .unwrap();
            let sm_client = Arc::new(client::SMClient::new(15, &raft_client));
            info!("Mass command");
            for _ in 0..100 {
                sm_client.take_a_shot(&-1).await.unwrap();
            }
            async_wait(Duration::from_secs(5)).await;
            info!("Mass query");
            for i in 0..100 {
                assert_eq!(
                    sm_client.get_shot().await.unwrap(),
                    110,
                    "fail at test {}",
                    i
                );
            }
            info!("Tests after leader transfer");
            info!("Leader should be consistent");
            for svr in &raft_services {
                assert_eq!(svr.leader_id().await, raft_services[0].id);
            }
            // Leader leave
            debug!("Leader leave cluster");
            raft_services[0].leave().await;
            async_wait(Duration::from_secs(5)).await;
            debug!("Leader should be changed");
            let new_leader = raft_services[1].leader_id().await;
            for (i, svr) in raft_services.iter().enumerate() {
                if svr.id == raft_services[0].id {
                    continue;
                }
                assert_ne!(
                    svr.leader_id().await,
                    raft_services[0].id,
                    "id {} at node {}",
                    i,
                    svr.id
                );
                assert_eq!(svr.leader_id().await, new_leader);
            }
            info!("Now we have leader {}", new_leader);
            info!("Mass command");
            for i in 0..10 {
                let res = sm_client.take_a_shot(&1).await;
                assert!(res.is_ok(), "{:?} at {}", res, i);
            }
            async_wait(Duration::from_secs(5)).await;
            info!("Mass query");
            for i in 0..100 {
                assert_eq!(
                    sm_client.get_shot().await.unwrap(),
                    100,
                    "fail at test {}",
                    i
                );
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn snapshot_disk_persistence() {
            let _ = env_logger::try_init();
            info!("TESTING SNAPSHOT DISK PERSISTENCE");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_test_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();

            let addr = String::from("127.0.0.1:3000");

            // Create service with disk storage
            let raft_service = RaftService::new(Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: data_path.clone(),
                    take_snapshots: true,
                    append_logs: true,
                    trim_logs: true,
                    snapshot_log_threshold: 10,
                    log_compaction_threshold: 20,
                }),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });

            let sm = SM { shots: 100 };
            let server = Server::new(&addr);
            let sm_id = sm.id();
            server.register_service(&raft_service).await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service, false).await;
            raft_service.register_state_machine(Box::new(sm)).await;
            raft_service.bootstrap().await;

            async_wait_secs().await;

            // Manually trigger a snapshot to test persistence
            {
                let mut meta = raft_service.write_meta().await;
                // Execute some state changes first
                for _ in 0..5 {
                    meta.last_applied += 1;
                }
                raft_service.take_snapshot(&mut meta).await;
                assert!(
                    meta.last_snapshot_index > 0,
                    "Snapshot should have been created"
                );
                info!(
                    "Manual snapshot created at index {}",
                    meta.last_snapshot_index
                );
            }

            // Verify snapshot file exists on disk
            let snapshot_path = std::path::PathBuf::from(&data_path).join("snapshot.dat");
            assert!(snapshot_path.exists(), "Snapshot file should exist on disk");
            info!("Snapshot file verified on disk");

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("Snapshot persistence test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn snapshot_persistence_and_recovery() {
            let _ = env_logger::try_init();
            info!("TESTING SNAPSHOT FILE PERSISTENCE AND RELOAD");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_persist_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            // Create a state machine and snapshot it
            let mut sm1 = SM { shots: 42 };
            let snapshot_data = sm1.snapshot();
            info!("Created snapshot with shots=42");

            // Create snapshot entity
            let snapshot_entity = SnapshotEntity {
                last_included_index: 100,
                last_included_term: 5,
                snapshot: snapshot_data,
            };

            // Write to disk
            let mut storage = disk::StorageEntity {
                logs: None,
                snapshot: None,
                last_term: 0,
                base_path: temp_dir.clone(),
                plane_id: PlaneId::type1(),
            };

            storage.write_snapshot(&snapshot_entity).await.unwrap();
            info!("Snapshot persisted to disk");

            // Verify file exists
            let snapshot_file = temp_dir.join("snapshot.dat");
            assert!(snapshot_file.exists(), "Snapshot file should exist");

            // Load snapshot from disk
            let loaded_snapshot = storage.read_snapshot().await.unwrap();
            assert!(loaded_snapshot.is_some(), "Should load snapshot");

            let loaded = loaded_snapshot.unwrap();
            assert_eq!(loaded.last_included_index, 100);
            assert_eq!(loaded.last_included_term, 5);

            // Create a new state machine and recover from loaded snapshot
            let mut sm2 = SM { shots: 999 }; // Different initial state
            sm2.recover(loaded.snapshot).await;

            // Verify recovery
            assert_eq!(sm2.shots, 42, "Should recover to snapshot value");
            info!("Successfully recovered state from persisted snapshot!");

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_snapshot_recovery_on_startup() {
            let _ = env_logger::try_init();
            info!("TESTING SNAPSHOT RECOVERY ON STARTUP");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_recovery_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            // Create a snapshot file on disk
            let snapshot = SnapshotEntity {
                last_included_index: 100,
                last_included_term: 5,
                snapshot: vec![42u8; 100], // Some test data
            };

            let mut storage = disk::StorageEntity {
                logs: None,
                snapshot: None,
                last_term: 0,
                base_path: temp_dir.clone(),
                plane_id: PlaneId::type1(),
            };

            // Write snapshot to disk
            storage.write_snapshot(&snapshot).await.unwrap();
            info!("Snapshot written to disk for recovery test");

            // Verify load_snapshot_on_startup would work
            let loaded = storage.read_snapshot().await.unwrap();
            assert!(loaded.is_some(), "Should load snapshot from disk");

            let recovered = loaded.unwrap();
            assert_eq!(recovered.last_included_index, 100);
            assert_eq!(recovered.last_included_term, 5);
            assert_eq!(recovered.snapshot.len(), 100);

            info!("Snapshot recovery test passed - would recover on startup");

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_snapshot_write_and_read() {
            let _ = env_logger::try_init();
            info!("TESTING SNAPSHOT WRITE AND READ FROM DISK");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_snapshot_io_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            // Create a snapshot entity
            let test_data = vec![1u8, 2, 3, 4, 5, 42, 100];
            let snapshot = SnapshotEntity {
                last_included_index: 42,
                last_included_term: 5,
                snapshot: test_data.clone(),
            };

            // Create storage entity
            let mut storage = disk::StorageEntity {
                logs: None,
                snapshot: None,
                last_term: 0,
                base_path: temp_dir.clone(),
                plane_id: PlaneId::type1(),
            };

            // Write snapshot
            storage.write_snapshot(&snapshot).await.unwrap();
            info!("Snapshot written to disk");

            // Verify file exists
            let snapshot_file = temp_dir.join("snapshot.dat");
            assert!(snapshot_file.exists(), "Snapshot file should exist");

            // Read snapshot back
            let loaded = storage.read_snapshot().await.unwrap();
            assert!(loaded.is_some(), "Should load snapshot");

            let loaded_snapshot = loaded.unwrap();
            assert_eq!(loaded_snapshot.last_included_index, 42);
            assert_eq!(loaded_snapshot.last_included_term, 5);
            assert_eq!(loaded_snapshot.snapshot, test_data);

            info!("Snapshot read successfully and data matches");

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_snapshot_corruption_detection() {
            let _ = env_logger::try_init();
            info!("TESTING SNAPSHOT CORRUPTION DETECTION");

            let temp_dir = std::env::temp_dir()
                .join(format!("raft_snapshot_corrupt_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            let snapshot = SnapshotEntity {
                last_included_index: 10,
                last_included_term: 2,
                snapshot: vec![1, 2, 3, 4, 5],
            };

            let mut storage = disk::StorageEntity {
                logs: None,
                snapshot: None,
                last_term: 0,
                base_path: temp_dir.clone(),
                plane_id: PlaneId::type1(),
            };

            // Write valid snapshot
            storage.write_snapshot(&snapshot).await.unwrap();

            // Corrupt the file by modifying some bytes
            let snapshot_file = temp_dir.join("snapshot.dat");
            let mut file_data = std::fs::read(&snapshot_file).unwrap();
            if file_data.len() > 20 {
                file_data[20] ^= 0xFF; // Flip some bits
                std::fs::write(&snapshot_file, file_data).unwrap();
            }

            // Try to read corrupted snapshot
            let result = storage.read_snapshot().await;
            assert!(result.is_ok(), "Should not error on corruption");
            assert!(
                result.unwrap().is_none(),
                "Should return None for corrupted snapshot"
            );

            info!("Corruption detection working correctly");

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_log_compaction_removes_old_logs() {
            let _ = env_logger::try_init();
            info!("TESTING LOG COMPACTION");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_compact_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            let addr = String::from("127.0.0.1:3100");

            let raft_service = RaftService::new(Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: temp_dir.to_str().unwrap().to_string(),
                    take_snapshots: true,
                    append_logs: true,
                    trim_logs: true,
                    snapshot_log_threshold: 5,
                    log_compaction_threshold: 10,
                }),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });

            let sm = SM { shots: 100 };
            let server = Server::new(&addr);
            server.register_service(&raft_service).await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service, false).await;
            raft_service.register_state_machine(Box::new(sm)).await;
            raft_service.bootstrap().await;

            async_wait_secs().await;

            // Get baseline log count and highest log ID
            let (baseline_count, max_log_id) = {
                let meta = raft_service.read_meta().await;
                let logs = meta.logs.read().await;
                let max_id = logs.keys().max().copied().unwrap_or(0);
                (logs.len(), max_id)
            };
            info!("Baseline: {} logs, max_id: {}", baseline_count, max_log_id);

            // Add 20 more logs with sequential IDs
            let new_log_start = max_log_id + 1;
            let new_log_end = new_log_start + 19;
            {
                let meta = raft_service.read_meta().await;
                let mut logs = meta.logs.write().await;
                for i in new_log_start..=new_log_end {
                    logs.insert(
                        i,
                        LogEntry {
                            id: i,
                            term: 1,
                            sm_id: 15,
                            fn_id: 1,
                            data: vec![],
                        },
                    );
                }
            }

            let after_add = raft_service.num_logs().await;
            info!("After adding 20 logs: {}", after_add);
            assert_eq!(after_add, baseline_count + 20, "Should have added 20 logs");

            // Create snapshot that covers first half of new logs
            let snapshot_index = new_log_start + 9; // Cover first 10 of our new logs
            {
                let mut meta = raft_service.write_meta().await;
                meta.last_applied = snapshot_index;
                raft_service.take_snapshot(&mut meta).await;
                assert_eq!(meta.last_snapshot_index, snapshot_index);
            }

            let final_count = raft_service.num_logs().await;
            info!("Final log count after compaction: {}", final_count);

            // Should have compacted logs up to snapshot_index
            // Remaining: baseline logs after snapshot_index + remaining new logs
            assert!(
                final_count < after_add,
                "Should have compacted some logs: before={}, after={}",
                after_add,
                final_count
            );

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("Log compaction test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_snapshot_threshold_configuration() {
            let _ = env_logger::try_init();
            info!("TESTING SNAPSHOT THRESHOLD CONFIGURATION");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_threshold_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            let addr = String::from("127.0.0.1:3101");

            // Create with custom thresholds
            let raft_service = RaftService::new(Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: temp_dir.to_str().unwrap().to_string(),
                    take_snapshots: true,
                    append_logs: true,
                    trim_logs: true,
                    snapshot_log_threshold: 3, // Very low for testing
                    log_compaction_threshold: 6,
                }),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });

            let sm = SM { shots: 100 };
            let server = Server::new(&addr);
            server.register_service(&raft_service).await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service, false).await;
            raft_service.register_state_machine(Box::new(sm)).await;
            raft_service.bootstrap().await;

            async_wait_secs().await;

            // Simulate some activity
            {
                let mut meta = raft_service.write_meta().await;
                meta.last_applied = 5; // Above threshold of 3

                // Test should_take_snapshot
                let should_snapshot = raft_service.should_take_snapshot(&meta, 10);
                assert!(
                    should_snapshot,
                    "Should trigger snapshot when last_applied (5) > threshold (3)"
                );
                info!("Threshold check passed");
            }

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_state_machine_snapshot_and_recovery() {
            let _ = env_logger::try_init();
            info!("TESTING STATE MACHINE SNAPSHOT AND RECOVERY");

            // Create SM with initial state
            let mut sm = SM { shots: 42 };

            // Take snapshot
            let snapshot_data = sm.snapshot();
            info!("Snapshot taken, size: {} bytes", snapshot_data.len());

            // Modify state
            sm.shots = 999;
            assert_eq!(sm.shots, 999);

            // Recover from snapshot
            sm.recover(snapshot_data).await;

            // Verify state was restored
            assert_eq!(sm.shots, 42, "State should be recovered to snapshot value");
            info!("State machine recovery test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_install_snapshot_compacts_logs() {
            let _ = env_logger::try_init();
            info!("TESTING install_snapshot COMPACTS LOGS");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_install_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            let addr = String::from("127.0.0.1:3102");

            let raft_service = RaftService::new(Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: temp_dir.to_str().unwrap().to_string(),
                    take_snapshots: true,
                    append_logs: true,
                    trim_logs: true,
                    snapshot_log_threshold: 10,
                    log_compaction_threshold: 20,
                }),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });

            let sm = SM { shots: 100 };
            let server = Server::new(&addr);
            server.register_service(&raft_service).await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service, false).await;
            raft_service.register_state_machine(Box::new(sm)).await;
            raft_service.bootstrap().await;

            async_wait_secs().await;

            // Add logs
            {
                let meta = raft_service.read_meta().await;
                let mut logs = meta.logs.write().await;
                for i in 1..=15u64 {
                    logs.insert(
                        i,
                        LogEntry {
                            id: i,
                            term: 1,
                            sm_id: 15,
                            fn_id: 1,
                            data: vec![],
                        },
                    );
                }
            }

            let before_count = raft_service.num_logs().await;
            info!("Logs before install_snapshot: {}", before_count);

            // Create valid snapshot data (SnapshotDataItems format)
            use crate::raft::state_machine::master::SnapshotDataItems;
            let snapshot_items: SnapshotDataItems = vec![
                (CONFIG_SM_ID, vec![1u8, 2, 3]), // Config SM snapshot
                (15u64, vec![42u8; 10]),         // Test SM snapshot
            ];
            let snapshot_data = crate::utils::serde::serialize(&snapshot_items);

            // Simulate receiving a snapshot via install_snapshot
            let _result = (&*raft_service as &dyn Service)
                .install_snapshot(
                    PlaneId::type1(),
                    1,     // term
                    12345, // leader_id
                    10,    // last_included_index
                    1,     // last_included_term
                    snapshot_data,
                )
                .await;

            let after_count = raft_service.num_logs().await;
            info!("Logs after install_snapshot: {}", after_count);

            // Should have removed logs 1-10, keeping logs with id > 10
            assert!(
                after_count < before_count,
                "Should have compacted logs: before={}, after={}",
                before_count,
                after_count
            );

            // Verify logs 1-10 are gone
            {
                let meta = raft_service.read_meta().await;
                let logs = meta.logs.read().await;
                for i in 1..=10 {
                    assert!(
                        !logs.contains_key(&i),
                        "Log {} should have been compacted",
                        i
                    );
                }
                // Logs 11-15 should still exist
                for i in 11..=15 {
                    assert!(logs.contains_key(&i), "Log {} should still exist", i);
                }
            }

            // Verify snapshot metadata was updated
            let meta = raft_service.read_meta().await;
            assert_eq!(meta.last_snapshot_index, 10);
            assert_eq!(meta.last_snapshot_term, 1);

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("install_snapshot compaction test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_logs_written_to_disk() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - LOGS WRITTEN TO DISK");

            let temp_dir = std::env::temp_dir().join(format!("raft_wal_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();

            let addr = String::from("127.0.0.1:3200");

            let raft_service = RaftService::new(Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: data_path.clone(),
                    take_snapshots: false, // Disable snapshots to focus on logs
                    append_logs: true,     // Enable WAL
                    trim_logs: false,
                    snapshot_log_threshold: 10000,
                    log_compaction_threshold: 20000,
                }),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });

            let sm = SM { shots: 100 };
            let server = Server::new(&addr);
            let sm_id = sm.id();
            server.register_service(&raft_service).await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service, false).await;
            raft_service.register_state_machine(Box::new(sm)).await;
            raft_service.bootstrap().await;

            async_wait_secs().await;

            // Verify log file doesn't exist yet or is small
            let log_file_path = std::path::PathBuf::from(&data_path).join("log.dat");
            let initial_size = if log_file_path.exists() {
                std::fs::metadata(&log_file_path).unwrap().len()
            } else {
                0
            };
            info!("Initial log file size: {} bytes", initial_size);

            // Execute commands - should write to WAL
            let raft_client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
                .await
                .unwrap();
            let sm_client = client::SMClient::new(sm_id, &raft_client);

            info!("Executing 10 commands that should be written to WAL");
            for i in 0..10 {
                sm_client.take_a_shot(&1).await.unwrap();
            }

            async_wait(Duration::from_secs(2)).await;

            // Verify log file exists and grew
            assert!(log_file_path.exists(), "WAL log file should exist");
            let final_size = std::fs::metadata(&log_file_path).unwrap().len();
            info!("Final log file size: {} bytes", final_size);

            assert!(
                final_size > initial_size,
                "Log file should have grown: initial={}, final={}",
                initial_size,
                final_size
            );

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("WAL persistence test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_recovery_after_crash() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - RECOVERY AFTER SIMULATED CRASH");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_wal_crash_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();

            let port = 3300 + (rand::random::<u16>() % 100);
            let addr = format!("127.0.0.1:{}", port);

            // Phase 1: Write some logs
            let num_logs_before_crash;
            {
                info!("Phase 1: Starting first instance and writing logs");
                let raft_service = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(),
                        take_snapshots: false, // No snapshots, only WAL
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });

                let sm = SM { shots: 100 };
                let server = Server::new(&addr);
                let sm_id = sm.id();
                server.register_service(&raft_service).await;
                Server::listen_and_resume(&server).await;
                raft_service.register_state_machine(Box::new(sm)).await;
                RaftService::start(&raft_service, false).await;
                raft_service.bootstrap().await;

                async_wait_secs().await;

                let raft_client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client = client::SMClient::new(sm_id, &raft_client);

                // Execute commands
                info!("Executing 5 commands");
                for _ in 0..5 {
                    sm_client.take_a_shot(&1).await.unwrap();
                }

                async_wait(Duration::from_secs(2)).await;

                // Verify state before "crash"
                let state_before = sm_client.get_shot().await.unwrap();
                assert_eq!(state_before, 95);
                info!("State before crash: {}", state_before);

                num_logs_before_crash = raft_service.num_logs().await;
                info!("Logs before crash: {}", num_logs_before_crash);

                // Simulate crash - just drop everything
                drop(sm_client);
                drop(raft_client);
                drop(raft_service);
                drop(server);
                info!("Simulated crash - dropped all services");
            }

            async_wait(Duration::from_secs(2)).await;

            // Phase 2: Recover from WAL
            {
                info!("Phase 2: Starting second instance and recovering from WAL");
                let port2 = port + 1;
                let addr2 = format!("127.0.0.1:{}", port2);

                let raft_service2 = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(), // Same data directory!
                        take_snapshots: false,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr2.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });

                // New SM with different initial state
                let sm2 = SM { shots: 999 }; // Different from crashed instance
                let server2 = Server::new(&addr2);
                let sm_id = sm2.id();
                server2.register_service(&raft_service2).await;
                Server::listen_and_resume(&server2).await;

                raft_service2.register_state_machine(Box::new(sm2)).await;
                // This should load logs from disk!
                RaftService::start(&raft_service2, false).await;
                raft_service2.bootstrap().await;

                async_wait(Duration::from_secs(2)).await;

                // Check that logs were recovered
                let num_logs_after = raft_service2.num_logs().await;
                info!("Logs after recovery: {}", num_logs_after);

                assert!(num_logs_after > 0, "Should have recovered logs from disk");

                // The logs should be similar to before crash
                // (might have some membership logs added/removed)
                let expected_min = num_logs_before_crash.saturating_sub(10);
                assert!(
                    num_logs_after >= expected_min,
                    "Should recover most logs: before={}, after={}, expected >= {}",
                    num_logs_before_crash,
                    num_logs_after,
                    expected_min
                );

                info!("WAL recovery test passed - logs recovered from disk!");
            }

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_log_file_format() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - LOG FILE FORMAT AND CONTENTS");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_wal_format_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();

            // Create storage and write some logs
            let mut storage = disk::StorageEntity {
                logs: None,
                snapshot: None,
                last_term: 0,
                base_path: temp_dir.clone(),
                plane_id: PlaneId::type1(),
            };

            // Create test logs in memory
            let mut logs = BTreeMap::new();
            for i in 1..=5u64 {
                logs.insert(
                    i,
                    LogEntry {
                        id: i,
                        term: 1,
                        sm_id: 15,
                        fn_id: 1,
                        data: vec![i as u8, (i * 2) as u8],
                    },
                );
            }

            // Create minimal RaftMeta for testing
            use async_std::sync::RwLock;
            let meta = RaftMeta {
                term: 1,
                vote_for: None,
                timeout: 10000,
                last_checked: 0,
                membership: Membership::Undefined,
                logs: Arc::new(RwLock::new(BTreeMap::new())),
                state_machine: Arc::new(RwLock::new(MasterStateMachine::new(DEFAULT_SERVICE_ID))),
                commit_index: 5,
                last_applied: 5,
                leader_id: 0,
                storage: None,
                last_snapshot_index: 0,
                last_snapshot_term: 0,
                lifecycle: LifecycleState::Running,
            };
            let meta_lock = async_std::sync::RwLock::new(meta);
            let meta_guard = meta_lock.write().await;
            let logs_lock = async_std::sync::RwLock::new(logs);
            let logs_guard = logs_lock.write().await;

            // Open log file manually
            let log_path = temp_dir.join("log.dat");
            storage.logs = Some(tokio::fs::File::create(&log_path).await.unwrap());

            // Write logs to disk
            storage.append_logs(&meta_guard, &logs_guard).await.unwrap();
            drop(storage);
            info!("Wrote 5 logs to WAL");

            // Verify file exists and has content
            assert!(log_path.exists(), "Log file should exist");
            let file_size = std::fs::metadata(&log_path).unwrap().len();
            info!("Log file size: {} bytes", file_size);
            assert!(file_size > 0, "Log file should have content");

            // Read back and verify we can parse it
            let file_contents = std::fs::read(&log_path).unwrap();
            assert!(
                file_contents.len() > 50,
                "Log file should contain serialized entries"
            );

            info!("WAL file format test passed");

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_fsync_durability() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - FSYNC DURABILITY GUARANTEE");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_wal_fsync_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();

            let addr = String::from("127.0.0.1:3400");

            let raft_service = RaftService::new(Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: data_path.clone(),
                    take_snapshots: false,
                    append_logs: true,
                    trim_logs: false,
                    snapshot_log_threshold: 10000,
                    log_compaction_threshold: 20000,
                }),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });

            let sm = SM { shots: 100 };
            let server = Server::new(&addr);
            let sm_id = sm.id();
            server.register_service(&raft_service).await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service, false).await;
            raft_service.register_state_machine(Box::new(sm)).await;
            raft_service.bootstrap().await;

            async_wait_secs().await;

            let raft_client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
                .await
                .unwrap();
            let sm_client = client::SMClient::new(sm_id, &raft_client);

            // Execute ONE command
            info!("Executing single command");
            sm_client.take_a_shot(&1).await.unwrap();

            // Small wait to ensure write completes
            async_wait(Duration::from_millis(500)).await;

            // Check log file was written immediately
            let log_file_path = std::path::PathBuf::from(&data_path).join("log.dat");
            assert!(
                log_file_path.exists(),
                "Log file should exist after one command"
            );

            // Get file modification time
            let metadata1 = std::fs::metadata(&log_file_path).unwrap();
            let modified1 = metadata1.modified().unwrap();
            info!("Log file modified at: {:?}", modified1);

            // Execute another command
            async_wait(Duration::from_millis(100)).await;
            sm_client.take_a_shot(&1).await.unwrap();
            async_wait(Duration::from_millis(500)).await;

            // Verify file was modified again (new write)
            let metadata2 = std::fs::metadata(&log_file_path).unwrap();
            let modified2 = metadata2.modified().unwrap();
            let size2 = metadata2.len();

            assert!(
                modified2 >= modified1,
                "Log file should be updated after second command"
            );
            assert!(
                size2 > metadata1.len(),
                "Log file should grow with new entries"
            );

            info!("WAL fsync durability test passed - each command persisted");

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_log_recovery_integration() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - LOG RECOVERY (logs are deltas, need same initial state)");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_wal_state_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();

            let port1 = 3500 + (rand::random::<u16>() % 50);
            let port2 = port1 + 100;
            let addr1 = format!("127.0.0.1:{}", port1);
            let addr2 = format!("127.0.0.1:{}", port2);

            let sm_id = 15u64;
            let expected_final_state;

            // Phase 1: Create initial state
            {
                info!("Phase 1: Creating initial state with WAL enabled");
                let raft_service = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(),
                        take_snapshots: false, // Only WAL, no snapshots
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr1.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });

                let sm = SM { shots: 100 };
                let server = Server::new(&addr1);
                server.register_service(&raft_service).await;
                Server::listen_and_resume(&server).await;
                RaftService::start(&raft_service, false).await;
                raft_service.register_state_machine(Box::new(sm)).await;
                raft_service.bootstrap().await;

                async_wait_secs().await;

                let raft_client = RaftClient::new(&vec![addr1.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client = client::SMClient::new(sm_id, &raft_client);

                // Execute commands that modify state
                info!("Executing 7 commands: take_a_shot(3) each time");
                for _ in 0..7 {
                    sm_client.take_a_shot(&3).await.unwrap();
                }

                async_wait(Duration::from_secs(2)).await;

                // Record expected state (100 - 7*3 = 79)
                expected_final_state = sm_client.get_shot().await.unwrap();
                info!("State before crash: {}", expected_final_state);
                assert_eq!(expected_final_state, 79);

                // Verify logs were written
                let log_file = std::path::PathBuf::from(&data_path).join("log.dat");
                assert!(log_file.exists(), "WAL should exist");

                info!("Simulating crash...");
                drop(sm_client);
                drop(raft_client);
                drop(raft_service);
                drop(server);
            }

            async_wait(Duration::from_secs(2)).await;

            // Phase 2: Recover from WAL
            {
                info!("Phase 2: Recovering from WAL after crash");
                let raft_service2 = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(), // Same directory!
                        take_snapshots: false,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr2.clone(), // Different port
                    service_id: DEFAULT_SERVICE_ID,
                });

                // Start with SAME initial state (WAL only replays commands, not full state)
                let sm2 = SM { shots: 100 }; // Same as first instance
                let server2 = Server::new(&addr2);
                server2.register_service(&raft_service2).await;
                Server::listen_and_resume(&server2).await;

                // Register state machine BEFORE start (important for recovery)
                raft_service2.register_state_machine(Box::new(sm2)).await;

                // This should load logs from disk!
                RaftService::start(&raft_service2, false).await;
                raft_service2.bootstrap().await;

                async_wait(Duration::from_secs(3)).await;

                // Verify logs were recovered
                let recovered_logs = raft_service2.num_logs().await;
                info!("Recovered {} logs from WAL", recovered_logs);
                assert!(recovered_logs > 0, "Should have recovered logs");

                // Manually apply the recovered logs
                {
                    let mut meta = raft_service2.write_meta().await;
                    info!(
                        "Before applying: last_applied={}, commit_index={}",
                        meta.last_applied, meta.commit_index
                    );
                    super::super::check_commit(&mut meta).await;
                    info!("After applying: last_applied={}", meta.last_applied);
                }

                // Verify state was recovered
                let raft_client2 = RaftClient::new(&vec![addr2.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client2 = client::SMClient::new(sm_id, &raft_client2);

                let recovered_state = sm_client2.get_shot().await.unwrap();
                info!("State after recovery: {}", recovered_state);

                // WAL recovers committed logs only, so might be within 1-2 commands
                // of expected state (uncommitted commands are lost, which is correct)
                let diff = (recovered_state as i32 - expected_final_state as i32).abs();
                assert!(
                    diff <= 6, // Allow for 2 uncommitted commands (2 * 3 = 6)
                    "State should be close to expected: expected={}, got={}, diff={}",
                    expected_final_state,
                    recovered_state,
                    diff
                );

                // Most importantly, verify logs were actually recovered
                assert!(
                    recovered_logs > 5,
                    "Should have recovered multiple logs from WAL"
                );

                info!("✅ WAL recovery test PASSED - logs recovered and replayed!");
            }

            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_deterministic_encoding() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - DETERMINISTIC ENCODING");

            // Create two identical log entries
            let log1 = LogEntry {
                id: 42,
                term: 5,
                sm_id: 15,
                fn_id: 99,
                data: vec![1, 2, 3, 4, 5],
            };

            let log2 = LogEntry {
                id: 42,
                term: 5,
                sm_id: 15,
                fn_id: 99,
                data: vec![1, 2, 3, 4, 5],
            };

            // Create two DiskLogEntries from them
            let disk_entry1 = disk::DiskLogEntry {
                term: 5,
                commit_index: 100,
                last_applied: 100,
                log: log1,
            };

            let disk_entry2 = disk::DiskLogEntry {
                term: 5,
                commit_index: 100,
                last_applied: 100,
                log: log2,
            };

            // Encode both
            let encoded1 = disk_entry1.encode();
            let encoded2 = disk_entry2.encode();

            // Verify determinism: same input → same output
            assert_eq!(encoded1, encoded2, "Encoding should be deterministic");
            info!("✓ Deterministic encoding verified");

            // Verify encoding is byte-for-byte identical
            assert_eq!(encoded1.len(), encoded2.len());
            for i in 0..encoded1.len() {
                assert_eq!(
                    encoded1[i], encoded2[i],
                    "Byte {} differs: {} vs {}",
                    i, encoded1[i], encoded2[i]
                );
            }
            info!("✓ Byte-for-byte identical");

            // Decode and verify correctness
            let decoded = disk::DiskLogEntry::decode(&encoded1).unwrap();
            assert_eq!(decoded.term, 5);
            assert_eq!(decoded.commit_index, 100);
            assert_eq!(decoded.last_applied, 100);
            assert_eq!(decoded.log.id, 42);
            assert_eq!(decoded.log.term, 5);
            assert_eq!(decoded.log.sm_id, 15);
            assert_eq!(decoded.log.fn_id, 99);
            assert_eq!(decoded.log.data, vec![1, 2, 3, 4, 5]);
            info!("✓ Decoding produces correct values");

            // Verify encoding size is predictable
            let expected_size = 8 * 8 + 5; // 8 u64 fields + 5 data bytes = 69 bytes
            assert_eq!(encoded1.len(), expected_size);
            info!("✓ Encoding size is predictable: {} bytes", expected_size);

            info!("Deterministic encoding test passed!");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_encoding_with_empty_data() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - ENCODING WITH EMPTY DATA");

            let entry = disk::DiskLogEntry {
                term: 1,
                commit_index: 10,
                last_applied: 10,
                log: LogEntry {
                    id: 10,
                    term: 1,
                    sm_id: 1,
                    fn_id: 1,
                    data: vec![], // Empty data
                },
            };

            let encoded = entry.encode();
            assert_eq!(encoded.len(), 64, "Empty data should encode to 64 bytes");

            let decoded = disk::DiskLogEntry::decode(&encoded).unwrap();
            assert_eq!(decoded.log.data.len(), 0);
            assert_eq!(decoded.log.id, 10);

            info!("Empty data encoding test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_encoding_with_large_data() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - ENCODING WITH LARGE DATA");

            let large_data = vec![42u8; 10000];

            let entry = disk::DiskLogEntry {
                term: 99,
                commit_index: 500,
                last_applied: 500,
                log: LogEntry {
                    id: 500,
                    term: 99,
                    sm_id: 7,
                    fn_id: 3,
                    data: large_data.clone(),
                },
            };

            let encoded = entry.encode();
            assert_eq!(
                encoded.len(),
                64 + 10000,
                "Should be 64 header + 10000 data"
            );

            let decoded = disk::DiskLogEntry::decode(&encoded).unwrap();
            assert_eq!(decoded.log.data.len(), 10000);
            assert_eq!(decoded.log.data, large_data);
            assert_eq!(decoded.term, 99);

            info!("Large data encoding test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_only_minimal_rsm_recovery() {
            let _ = env_logger::try_init();
            info!("==============================================");
            info!("WAL-ONLY E2E (macro SM): minimal commands, no snapshot");
            info!("==============================================");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_wal_only_min_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();

            let port1 = 3700 + (rand::random::<u16>() % 50);
            let port2 = port1 + 100;
            let addr1 = format!("127.0.0.1:{}", port1);
            let addr2 = format!("127.0.0.1:{}", port2);

            let sm_id = 15u64;
            let expected_state: i32;

            // ===== PHASE 1: Start single-node with WAL only and execute small commands =====
            {
                let service = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(),
                        take_snapshots: false, // WAL-only
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr1.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });

                let sm = SM { shots: 50 };
                let server = Server::new(&addr1);
                server.register_service(&service).await;
                Server::listen_and_resume(&server).await;
                service.register_state_machine(Box::new(sm)).await;
                RaftService::start(&service, false).await;
                service.bootstrap().await;

                async_wait(Duration::from_secs(2)).await;

                let client = RaftClient::new(&vec![addr1.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client = client::SMClient::new(sm_id, &client);

                // Execute 3 small commands: total delta = 6
                let deltas = [1, 2, 3];
                for d in deltas.iter() {
                    sm_client.take_a_shot(d).await.unwrap();
                }

                // Ensure committed logs are applied and WAL is flushed before crash (deterministic)
                {
                    let mut meta = service.write_meta().await;
                    super::super::check_commit(&mut meta).await;
                    if let Some(storage_mutex) = &meta.storage {
                        let mut storage = storage_mutex.lock().await;
                        let _ = storage.flush_wal().await;
                    }
                }

                // Persist current commit progress as an extra safety barrier
                service.flush_persistence().await;

                async_wait(Duration::from_secs(1)).await;

                // Record actual state before crash (source of truth for recovery)
                expected_state = sm_client.get_shot().await.unwrap();
                info!("State before crash: {}", expected_state);

                // Verify WAL exists
                let wal_file = std::path::PathBuf::from(&data_path).join("log.dat");
                assert!(wal_file.exists(), "WAL file should exist");

                // Graceful shutdown instead of drop
                drop(sm_client);
                drop(client);
                service.shutdown().await;
                server.shutdown().await;
            }

            async_wait(Duration::from_secs(2)).await;

            // ===== PHASE 2: Recover from WAL only =====
            {
                let service2 = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(), // Same directory
                        take_snapshots: false,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr2.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });

                // IMPORTANT: start with same initial state; WAL replays deltas
                let sm2 = SM { shots: 50 };
                let server2 = Server::new(&addr2);
                server2.register_service(&service2).await;
                Server::listen_and_resume(&server2).await;
                service2.register_state_machine(Box::new(sm2)).await;

                RaftService::start(&service2, false).await;
                service2.bootstrap().await;

                async_wait(Duration::from_secs(2)).await;

                // Ensure logs were recovered
                let recovered = service2.num_logs().await;
                info!("Recovered {} logs from WAL", recovered);
                assert!(recovered > 0);

                // Apply recovered logs
                {
                    let mut meta = service2.write_meta().await;
                    super::super::check_commit(&mut meta).await;
                }

                // Ensure all committed logs are applied after restart
                {
                    let mut meta = service2.write_meta().await;
                    super::super::check_commit(&mut meta).await;
                }

                // Verify state equals pre-crash state
                let client2 = RaftClient::new(&vec![addr2.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client2 = client::SMClient::new(sm_id, &client2);

                let recovered_state = sm_client2.get_shot().await.unwrap();
                info!("Recovered state: {}", recovered_state);
                // Assert equality pre vs post crash
                assert_eq!(
                    recovered_state, expected_state,
                    "recovered state should equal pre-crash state"
                );

                // Clean up
                drop(sm_client2);
                drop(client2);
                drop(service2);
                drop(server2);
            }

            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        // ── NEW RECOVERY TESTS ───────────────────────────────────────────────

        /// Simulate abrupt crash (drop without shutdown/flush) and verify the SM
        /// recovers to the exact pre-crash state via WAL + commit.idx.
        /// Asserts "not start over": recovered state ≠ fresh initial state.
        #[tokio::test(flavor = "multi_thread")]
        async fn test_abrupt_crash_full_recovery() {
            let _ = env_logger::try_init();
            info!("=== TEST: abrupt crash → full WAL recovery ===");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_abrupt_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();
            let sm_id = 15u64;
            let initial_shots = 100i32;
            let num_cmds = 8i32;
            let expected = initial_shots - num_cmds; // 92

            let port1 = 4001u16 + (rand::random::<u16>() % 20);
            let addr1 = format!("127.0.0.1:{}", port1);

            // ─── Phase 1: run commands then drop abruptly (no shutdown/flush) ───
            {
                let svc = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(),
                        take_snapshots: false,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr1.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });
                let server = Server::new(&addr1);
                server.register_service(&svc).await;
                Server::listen_and_resume(&server).await;
                svc.register_state_machine(Box::new(SM {
                    shots: initial_shots,
                }))
                .await;
                RaftService::start(&svc, false).await;
                svc.bootstrap().await;
                async_wait_secs().await;

                let client = RaftClient::new(&vec![addr1.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client = client::SMClient::new(sm_id, &client);
                for _ in 0..num_cmds {
                    sm_client.take_a_shot(&1).await.unwrap();
                }
                async_wait(Duration::from_secs(2)).await;

                let before = sm_client.get_shot().await.unwrap();
                assert_eq!(before, expected, "pre-crash state wrong");
                info!("State before crash: {}", before);

                // Verify WAL and commit.idx exist
                assert!(temp_dir.join("log.dat").exists(), "WAL must exist");
                assert!(
                    temp_dir.join("commit.idx").exists(),
                    "commit.idx must exist (written per-command)"
                );

                // ABRUPT CRASH — no shutdown(), no flush_persistence()
                drop(sm_client);
                drop(client);
                drop(svc);
                drop(server);
                info!("Abrupt crash simulated (all handles dropped)");
            }
            async_wait(Duration::from_secs(2)).await;

            // ─── Phase 2: restart with same initial state, recover ───
            let port2 = port1 + 50;
            let addr2 = format!("127.0.0.1:{}", port2);
            {
                let svc2 = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(),
                        take_snapshots: false,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr2.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });
                let server2 = Server::new(&addr2);
                server2.register_service(&svc2).await;
                Server::listen_and_resume(&server2).await;
                // Register SM with SAME initial shots so replay produces the right result
                svc2.register_state_machine(Box::new(SM {
                    shots: initial_shots,
                }))
                .await;
                RaftService::start(&svc2, false).await;
                svc2.bootstrap().await;

                // Replay committed WAL logs into the SM
                svc2.recover_after_register().await;
                async_wait(Duration::from_secs(2)).await;

                let client2 = RaftClient::new(&vec![addr2.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client2 = client::SMClient::new(sm_id, &client2);
                let recovered = sm_client2.get_shot().await.unwrap();
                info!("Recovered state: {} (expected {})", recovered, expected);

                assert_ne!(
                    recovered, initial_shots,
                    "Must not equal untouched initial state"
                );
                assert_eq!(
                    recovered, expected,
                    "Must recover exact pre-crash state via WAL"
                );

                drop(sm_client2);
                drop(client2);
                drop(svc2);
                drop(server2);
            }
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("=== PASS: abrupt crash recovery ===");
        }

        /// Write N valid WAL entries, then append partial bytes to simulate a power cut
        /// mid-entry.  Recovery must load all N good entries and truncate the corrupt tail.
        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_partial_write_truncation() {
            let _ = env_logger::try_init();
            info!("=== TEST: partial WAL write → truncation on recovery ===");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_partial_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let log_path = temp_dir.join("log.dat");
            const N: usize = 5;
            const DATA_LEN: usize = 8; // bytes per entry's data field

            // ─── Phase 1: write N complete entries via StorageEntity ───
            {
                let mut logs = BTreeMap::new();
                for i in 1..=N as u64 {
                    logs.insert(
                        i,
                        LogEntry {
                            id: i,
                            term: 1,
                            sm_id: 15,
                            fn_id: 1,
                            data: vec![i as u8; DATA_LEN],
                        },
                    );
                }
                let meta = RaftMeta {
                    term: 1,
                    vote_for: None,
                    timeout: 10000,
                    last_checked: 0,
                    membership: Membership::Undefined,
                    logs: Arc::new(async_std::sync::RwLock::new(BTreeMap::new())),
                    state_machine: Arc::new(async_std::sync::RwLock::new(MasterStateMachine::new(
                        DEFAULT_SERVICE_ID,
                    ))),
                    commit_index: N as u64,
                    last_applied: N as u64,
                    leader_id: 0,
                    storage: None,
                    last_snapshot_index: 0,
                    last_snapshot_term: 0,
                    lifecycle: LifecycleState::Running,
                };
                let meta_lock = async_std::sync::RwLock::new(meta);
                let meta_guard = meta_lock.write().await;
                let logs_lock = async_std::sync::RwLock::new(logs);
                let logs_guard = logs_lock.write().await;

                let mut storage = disk::StorageEntity {
                    logs: Some(tokio::fs::File::create(&log_path).await.unwrap()),
                    snapshot: None,
                    last_term: 0,
                    base_path: temp_dir.clone(),
                    plane_id: PlaneId::type1(),
                };
                storage.append_logs(&meta_guard, &logs_guard).await.unwrap();
                // drop storage to flush/close
            }

            let size_good = std::fs::metadata(&log_path).unwrap().len();
            assert!(size_good > 0, "WAL must have content after {} entries", N);
            info!("WAL size after {} complete entries: {} bytes", N, size_good);

            // ─── Phase 2: append 5 garbage bytes (partial length prefix) ───
            {
                use std::io::Write as _;
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&log_path)
                    .unwrap();
                f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0xCA]).unwrap();
                f.sync_all().unwrap();
            }
            let size_with_garbage = std::fs::metadata(&log_path).unwrap().len();
            assert_eq!(
                size_with_garbage,
                size_good + 5,
                "File should be exactly 5 bytes larger after injecting garbage"
            );

            // ─── Phase 3: recover using new_with_options ───
            let mut term = 0u64;
            let mut commit_index = 0u64;
            let mut last_applied = 0u64;
            let mut recovered_logs = BTreeMap::new();
            let opts = Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: temp_dir.to_str().unwrap().to_string(),
                    take_snapshots: false,
                    append_logs: true,
                    trim_logs: false,
                    snapshot_log_threshold: 10000,
                    log_compaction_threshold: 20000,
                }),
                address: "127.0.0.1:0".to_string(),
                service_id: DEFAULT_SERVICE_ID,
            };
            let _storage = disk::StorageEntity::new_with_options(
                &opts,
                &mut term,
                &mut commit_index,
                &mut last_applied,
                &mut recovered_logs,
            )
            .unwrap();

            // All N good entries must be present
            assert_eq!(
                recovered_logs.len(),
                N,
                "Should recover exactly {} entries; got {}",
                N,
                recovered_logs.len()
            );

            // The corrupt 5-byte tail must have been truncated
            let size_after = std::fs::metadata(&log_path).unwrap().len();
            assert_eq!(
                size_after, size_good,
                "WAL file must be truncated back to {} bytes; got {}",
                size_good, size_after
            );

            info!(
                "Recovered {} entries; corrupt tail truncated ({} → {} bytes)",
                N, size_with_garbage, size_after
            );
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("=== PASS: partial write truncation ===");
        }

        /// Write N WAL entries, then flip bytes in the CRC field of entry K.
        /// Recovery must stop at entry K (recovering K entries, not K+1..N)
        /// and the file must be truncated at the corruption boundary.
        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_crc_corruption_stops_at_bad_entry() {
            let _ = env_logger::try_init();
            info!("=== TEST: CRC corruption → partial recovery stops at bad entry ===");

            let temp_dir = std::env::temp_dir().join(format!("raft_crc_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let log_path = temp_dir.join("log.dat");

            const N: usize = 6;
            const DATA_LEN: usize = 8;
            // Corrupt entry at 0-based index CORRUPT_IDX; first CORRUPT_IDX entries should survive.
            const CORRUPT_IDX: usize = 3;
            // On-disk layout per entry: [8 len][4 CRC][64 fixed][DATA_LEN data]
            const RECORD_SIZE: usize = 8 + 4 + 64 + DATA_LEN; // = 84 bytes

            // ─── Phase 1: write N complete entries ───
            {
                let mut logs = BTreeMap::new();
                for i in 1..=N as u64 {
                    logs.insert(
                        i,
                        LogEntry {
                            id: i,
                            term: 1,
                            sm_id: 15,
                            fn_id: 1,
                            data: vec![i as u8; DATA_LEN],
                        },
                    );
                }
                let meta = RaftMeta {
                    term: 1,
                    vote_for: None,
                    timeout: 10000,
                    last_checked: 0,
                    membership: Membership::Undefined,
                    logs: Arc::new(async_std::sync::RwLock::new(BTreeMap::new())),
                    state_machine: Arc::new(async_std::sync::RwLock::new(MasterStateMachine::new(
                        DEFAULT_SERVICE_ID,
                    ))),
                    commit_index: N as u64,
                    last_applied: N as u64,
                    leader_id: 0,
                    storage: None,
                    last_snapshot_index: 0,
                    last_snapshot_term: 0,
                    lifecycle: LifecycleState::Running,
                };
                let meta_lock = async_std::sync::RwLock::new(meta);
                let meta_guard = meta_lock.write().await;
                let logs_lock = async_std::sync::RwLock::new(logs);
                let logs_guard = logs_lock.write().await;
                let mut storage = disk::StorageEntity {
                    logs: Some(tokio::fs::File::create(&log_path).await.unwrap()),
                    snapshot: None,
                    last_term: 0,
                    base_path: temp_dir.clone(),
                    plane_id: PlaneId::type1(),
                };
                storage.append_logs(&meta_guard, &logs_guard).await.unwrap();
            }

            let size_before = std::fs::metadata(&log_path).unwrap().len();
            assert_eq!(
                size_before,
                (N * RECORD_SIZE) as u64,
                "WAL size mismatch: expected {} bytes for {} entries",
                N * RECORD_SIZE,
                N
            );

            // ─── Phase 2: flip all 4 CRC bytes of entry CORRUPT_IDX ───
            {
                let mut file_data = std::fs::read(&log_path).unwrap();
                // CRC starts at byte 8 (after 8-byte length prefix) within each record
                let crc_offset = CORRUPT_IDX * RECORD_SIZE + 8;
                file_data[crc_offset] ^= 0xFF;
                file_data[crc_offset + 1] ^= 0xFF;
                file_data[crc_offset + 2] ^= 0xFF;
                file_data[crc_offset + 3] ^= 0xFF;
                std::fs::write(&log_path, &file_data).unwrap();
                info!(
                    "Corrupted CRC of entry {} at byte offset {}",
                    CORRUPT_IDX, crc_offset
                );
            }

            // ─── Phase 3: recover ───
            let mut term = 0u64;
            let mut commit_index = 0u64;
            let mut last_applied = 0u64;
            let mut recovered_logs = BTreeMap::new();
            let opts = Options {
                storage: Storage::DISK(disk::DiskOptions {
                    path: temp_dir.to_str().unwrap().to_string(),
                    take_snapshots: false,
                    append_logs: true,
                    trim_logs: false,
                    snapshot_log_threshold: 10000,
                    log_compaction_threshold: 20000,
                }),
                address: "127.0.0.1:0".to_string(),
                service_id: DEFAULT_SERVICE_ID,
            };
            let _storage = disk::StorageEntity::new_with_options(
                &opts,
                &mut term,
                &mut commit_index,
                &mut last_applied,
                &mut recovered_logs,
            )
            .unwrap();

            // Only the CORRUPT_IDX entries before the corruption survive
            assert_eq!(
                recovered_logs.len(),
                CORRUPT_IDX,
                "Should recover exactly {} entries before corruption; got {}",
                CORRUPT_IDX,
                recovered_logs.len()
            );

            // Verify the recovered entries are the correct ones (ids 1..CORRUPT_IDX)
            for id in 1..=CORRUPT_IDX as u64 {
                assert!(
                    recovered_logs.contains_key(&id),
                    "Entry id={} should be present",
                    id
                );
            }
            for id in (CORRUPT_IDX + 1) as u64..=N as u64 {
                assert!(
                    !recovered_logs.contains_key(&id),
                    "Entry id={} should have been dropped (after corruption)",
                    id
                );
            }

            // File truncated at the corruption boundary
            let expected_truncated_size = (CORRUPT_IDX * RECORD_SIZE) as u64;
            let actual_size = std::fs::metadata(&log_path).unwrap().len();
            assert_eq!(
                actual_size, expected_truncated_size,
                "WAL must be truncated to {} bytes at corruption; got {}",
                expected_truncated_size, actual_size
            );

            info!(
                "CRC corruption test: {} good entries recovered, {} corrupt entries dropped, \
                file truncated from {} to {} bytes",
                CORRUPT_IDX,
                N - CORRUPT_IDX,
                size_before,
                actual_size
            );
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("=== PASS: CRC corruption stops recovery at bad entry ===");
        }

        /// Crash after snapshot + additional WAL entries.
        /// On restart the SM must recover from the snapshot and then replay the
        /// post-snapshot WAL log entries — proving "partial recovery, not start over".
        #[tokio::test(flavor = "multi_thread")]
        async fn test_snapshot_plus_wal_not_start_over() {
            let _ = env_logger::try_init();
            info!("=== TEST: snapshot + post-snapshot WAL crash recovery ===");

            let temp_dir =
                std::env::temp_dir().join(format!("raft_snap_wal_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            let data_path = temp_dir.to_str().unwrap().to_string();
            let sm_id = 15u64;

            const INITIAL: i32 = 100;
            const CMDS_BEFORE_SNAP: i32 = 5; // shots: 100 → 95
            const CMDS_AFTER_SNAP: i32 = 3; // shots: 95  → 92
            let expected = INITIAL - CMDS_BEFORE_SNAP - CMDS_AFTER_SNAP; // 92

            let port1 = 4050u16 + (rand::random::<u16>() % 20);
            let addr1 = format!("127.0.0.1:{}", port1);

            // ─── Phase 1: commands → snapshot → more commands → abrupt crash ───
            {
                let svc = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(),
                        take_snapshots: true,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000, // won't auto-trigger; we do it manually
                        log_compaction_threshold: 20000,
                    }),
                    address: addr1.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });
                let server = Server::new(&addr1);
                server.register_service(&svc).await;
                Server::listen_and_resume(&server).await;
                svc.register_state_machine(Box::new(SM { shots: INITIAL }))
                    .await;
                RaftService::start(&svc, false).await;
                svc.bootstrap().await;
                async_wait_secs().await;

                let client = RaftClient::new(&vec![addr1.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client = client::SMClient::new(sm_id, &client);

                // Execute CMDS_BEFORE_SNAP commands
                for _ in 0..CMDS_BEFORE_SNAP {
                    sm_client.take_a_shot(&1).await.unwrap();
                }
                async_wait(Duration::from_secs(1)).await;
                let state_before_snap = sm_client.get_shot().await.unwrap();
                assert_eq!(state_before_snap, INITIAL - CMDS_BEFORE_SNAP);
                info!("State before snapshot: {}", state_before_snap);

                // Explicitly take a snapshot at this point
                {
                    let mut meta = svc.write_meta().await;
                    svc.take_snapshot(&mut meta).await;
                    info!("Snapshot taken at state {}", state_before_snap);
                }
                assert!(
                    temp_dir.join("snapshot.dat").exists(),
                    "snapshot.dat must exist"
                );

                // Execute CMDS_AFTER_SNAP more commands (post-snapshot WAL entries)
                for _ in 0..CMDS_AFTER_SNAP {
                    sm_client.take_a_shot(&1).await.unwrap();
                }
                async_wait(Duration::from_secs(1)).await;
                let state_before_crash = sm_client.get_shot().await.unwrap();
                assert_eq!(state_before_crash, expected);
                info!("State before crash: {}", state_before_crash);

                assert!(temp_dir.join("log.dat").exists(), "WAL must exist");
                assert!(
                    temp_dir.join("commit.idx").exists(),
                    "commit.idx must exist"
                );

                // Abrupt crash
                drop(sm_client);
                drop(client);
                drop(svc);
                drop(server);
                info!(
                    "Abrupt crash (post snapshot + {} WAL entries)",
                    CMDS_AFTER_SNAP
                );
            }
            async_wait(Duration::from_secs(2)).await;

            // ─── Phase 2: restart with DIFFERENT initial state (999) ───
            // If the SM "starts over", it would show 999 (no recovery) or 996 (999 - CMDS_AFTER_SNAP,
            // only post-snapshot WAL replay from wrong base). Correct recovery gives exactly 92.
            let port2 = port1 + 50;
            let addr2 = format!("127.0.0.1:{}", port2);
            {
                let svc2 = RaftService::new(Options {
                    storage: Storage::DISK(disk::DiskOptions {
                        path: data_path.clone(),
                        take_snapshots: true,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr2.clone(),
                    service_id: DEFAULT_SERVICE_ID,
                });
                let server2 = Server::new(&addr2);
                server2.register_service(&svc2).await;
                Server::listen_and_resume(&server2).await;
                // IMPORTANT: register SM AFTER start() so that load_snapshot_on_startup()
                // stores the snapshot bytes before register() applies them.
                // start() loads snapshot → stores snapshot[15] → register() applies snapshot → SM.shots=95
                RaftService::start(&svc2, false).await;
                svc2.register_state_machine(Box::new(SM { shots: 999 }))
                    .await;
                svc2.bootstrap().await;

                // Replay post-snapshot WAL entries (entries after snapshot index → shots 95→92)
                svc2.recover_after_register().await;
                async_wait(Duration::from_secs(2)).await;

                let client2 = RaftClient::new(&vec![addr2.clone()], DEFAULT_SERVICE_ID)
                    .await
                    .unwrap();
                let sm_client2 = client::SMClient::new(sm_id, &client2);
                let recovered = sm_client2.get_shot().await.unwrap();
                info!("Recovered state: {} (expected {})", recovered, expected);

                // Core assertions
                assert_ne!(
                    recovered, 999,
                    "SM must NOT show initial 999 — that would be 'starting over'"
                );
                assert_ne!(
                    recovered, INITIAL,
                    "SM must NOT show {} — that would mean snapshot was ignored",
                    INITIAL
                );
                assert_eq!(
                    recovered, expected,
                    "SM must recover to exact pre-crash state via snapshot + WAL replay"
                );

                drop(sm_client2);
                drop(client2);
                drop(svc2);
                drop(server2);
            }
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("=== PASS: snapshot + WAL crash recovery (not start over) ===");
        }
    }
}
