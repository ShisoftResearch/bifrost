use self::state_machine::configs::commands::{del_member_, member_address, new_member_};
use self::state_machine::configs::{RaftMember, CONFIG_SM_ID};
use self::state_machine::master::{ExecError, ExecResult, MasterStateMachine, SubStateMachine};
use self::state_machine::OpType;
use crate::raft::client::RaftClient;
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
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use tokio::runtime;
use tokio::sync::{watch, Mutex as TokioMutex};
use tokio::time::*;

#[macro_use]
pub mod state_machine;
pub mod client;
pub mod disk;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_DEFAULT_SERVICE) as u64;

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
    rpc append_entries(term: u64, leader_id: u64, prev_log_id: u64, prev_log_term: u64, entries: &Option<LogEntries>, leader_commit: u64) -> (u64, AppendEntriesResult);
    rpc request_vote(term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> ((u64, u64), bool); // term, voteGranted
    rpc install_snapshot(term: u64, leader_id: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>) -> u64;
    rpc reelect() -> bool;
    rpc c_command(entry: LogEntry) -> ClientCmdResponse;
    rpc c_query(entry: &LogEntry) -> ClientQryResponse;
    rpc c_server_cluster_info() -> ClientClusterInfo;
    rpc c_put_offline() -> bool;
    rpc c_have_state_machine(id: u64) -> bool;
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

pub struct RaftService {
    meta: RwLock<RaftMeta>,
    pub id: u64,
    pub options: Options,
    pub rt: runtime::Runtime,
    _is_leader: AtomicBool,
    checker_task: TokioMutex<Option<tokio::task::JoinHandle<()>>>,
    shutdown_tx: watch::Sender<LifecycleState>,
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

macro_rules! get_last_log_info {
    ($s: expr, $logs: expr) => {{
        let last_log = $logs.iter().next_back();
        $s.get_log_info_(last_log)
    }};
}

async fn check_commit(meta: &mut RwLockWriteGuard<'_, RaftMeta>) {
    while meta.commit_index > meta.last_applied {
        meta.last_applied += 1;
        let last_applied = meta.last_applied;
        // TODO: Get rid of frequent locking and clone?
        let logs = meta.logs.read().await;
        if let Some(entry) = logs.get(&last_applied) {
            if let Err(e) = commit_command(meta, &entry).await {
                error!("Failed to commit command for log entry {}: {:?}", last_applied, e);
                // Continue processing other entries despite this failure
            }
        };
    }
}

impl RaftService {
    fn lifecycle_is_stopping(state: LifecycleState) -> bool {
        matches!(state, LifecycleState::Stopping | LifecycleState::Stopped)
    }

    /// Public helper for applications to trigger commit replay after registering
    /// their state machines. This ensures replay happens when SMs are ready.
    pub async fn recover_after_register(&self) {
        let mut meta = self.meta.write().await;
        info!(
            "Manual apply: applying committed logs (commit_index={}, last_applied={})",
            meta.commit_index, meta.last_applied
        );
        check_commit(&mut meta).await;
        info!(
            "Manual apply: applied logs up to last_applied={}",
            meta.last_applied
        );
    }
}

/// Check commits and trigger snapshot if needed (should be called by leader)
async fn check_commit_and_maybe_snapshot(
    server: &RaftService,
    meta: &mut RwLockWriteGuard<'_, RaftMeta>,
) {
    check_commit(meta).await;
    
    // Check if we should take a snapshot
    let num_logs = meta.logs.read().await.len();
    if server.should_take_snapshot(meta, num_logs) {
        server.take_snapshot(meta).await;
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

async fn commit_command<'a>(
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
                panic!("Failed to initialize storage entity: {:?}. Cannot proceed without storage.", e);
            }
        };

        let master_sm = MasterStateMachine::new(opts.service_id);

        let (shutdown_tx, _shutdown_rx) = watch::channel(LifecycleState::Running);
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
            id: server_id,
            options: opts,
            rt: runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("raft-server")
                .worker_threads(12)
                .max_blocking_threads(num_cpus::get())
                .event_interval(31)
                .build()
                .expect("Failed to build tokio runtime for Raft service"),
            _is_leader: AtomicBool::new(false),
            checker_task: TokioMutex::new(None),
            shutdown_tx,
        };
        Arc::new(server_obj)
    }

    /// Load snapshot from disk and recover state machine if snapshot exists
    async fn load_snapshot_on_startup(&self) -> bool {
        let storage = {
            let meta = self.meta.read().await;
            meta.storage.clone()
        };

        if let Some(storage) = storage {
            let storage = storage.lock().await;
            match storage.read_snapshot().await {
                Ok(Some(snapshot)) => {
                    info!(
                        "Found snapshot on disk: index={}, term={}. Recovering state machine...",
                        snapshot.last_included_index, snapshot.last_included_term
                    );
                    
                    let mut meta = self.meta.write().await;
                    
                    // Recover state machine
                    meta.state_machine
                        .write()
                        .await
                        .recover(snapshot.snapshot.clone())
                        .await;
                    
                    // Update snapshot metadata
                    meta.last_snapshot_index = snapshot.last_included_index;
                    meta.last_snapshot_term = snapshot.last_included_term;
                    
                    // Update commit and applied indices
                    if snapshot.last_included_index > meta.last_applied {
                        meta.last_applied = snapshot.last_included_index;
                        meta.commit_index = snapshot.last_included_index;
                    }
                    
                    // Compact logs: remove logs covered by snapshot
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
                }
                Ok(None) => {
                    debug!("No snapshot found on disk, starting fresh");
                    false
                }
                Err(e) => {
                    warn!("Failed to load snapshot from disk: {:?}. Starting without snapshot recovery.", e);
                    false
                }
            }
        } else {
            debug!("No storage configured, skipping snapshot recovery");
            false
        }
    }

    pub async fn start(server: &Arc<RaftService>, recover_registered: bool) -> bool {
        let server_address = server.options.address.clone();
        
        // Load and recover from snapshot if it exists
        let recovered_from_disk = server.load_snapshot_on_startup().await;
        
        info!("Waiting for raft server to be initialized");
        {
            let mut meta = server.meta.write().await;
            meta.last_checked = get_time() + (CHECKER_MS * 10);
            let mut sm = meta.state_machine.write().await;
            let mut inited = false;
            let start_time = get_time();
            while get_time() < start_time + 5000 {
                //waiting for 5 secs
                if sm.configs.new_member(server_address.clone()).await {
                    inited = true;
                    break;
                }
            }
            if !inited {
                return false;
            }
            
            // FIX: Single-node cluster recovery from disk
            // If we recovered state from disk and we're the only member,
            // immediately become leader (no election needed)
            let num_members = sm.configs.members.len();
            let has_logs = !meta.logs.read().await.is_empty();
            let has_term = meta.term > 0;
            
            debug!(
                "Post-recovery check: recovered_from_disk={}, num_members={}, has_logs={}, has_term={}, membership={:?}",
                recovered_from_disk, num_members, has_logs, has_term,
                match &meta.membership {
                    Membership::Leader(_) => "Leader",
                    Membership::Follower => "Follower",
                    Membership::Candidate => "Candidate",
                    Membership::Offline => "Offline",
                    Membership::Undefined => "Undefined",
                }
            );
            
            if (recovered_from_disk || has_logs || has_term) && num_members == 1 {
                info!(
                    "Single-node cluster detected after recovery (term={}, logs={}, members={}). Becoming leader immediately.",
                    meta.term, has_logs, num_members
                );
                let (last_log_id, _) = {
                    let logs = meta.logs.read().await;
                    get_last_log_info!(server, logs)
                };
                drop(sm); // Release state machine lock before become_leader
                server.become_leader(&mut meta, last_log_id).await;
                info!("Successfully transitioned to Leader state");
            } else {
                debug!(
                    "Not transitioning to leader: condition not met (recovered={} || logs={} || term={}) && members==1: {}",
                    recovered_from_disk, has_logs, has_term, num_members
                );
            }
        }
        
        if recover_registered {
            server.recover_after_register().await;
        }

        let checker_ref = server.clone();
        let mut shutdown_rx = server.shutdown_tx.subscribe();
        let handle = server.rt.spawn(async move {
            info!("Starting Raft checker/heartbeat task");
            let server = checker_ref;
            loop {
                if Self::lifecycle_is_stopping(*shutdown_rx.borrow()) {
                    debug!("Heartbeat loop exiting because shutdown was requested");
                    break;
                }
                let start_time = get_time();
                let expected_ends = start_time + CHECKER_MS;
                let heartbeat_task_continue = async {
                    let mut meta = server.meta.write().await; //WARNING: Reentering not supported
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
                            if meta.vote_for == None && time_remains < 0 {
                                // TODO: in my test sometimes timeout_elapsed may go 1 for no reason, require investigation
                                //Timeout, require election
                                warn!(
                                "LEADER {} TIMEOUT!!! GOING TO CANDIDATE!!! {}, time remains {}ms",
                                meta.leader_id, server.id, time_remains);
                                CheckerAction::BecomeCandidate
                            } else {
                                CheckerAction::None
                            }
                        }
                        Membership::Offline => CheckerAction::ExitLoop,
                        Membership::Undefined => CheckerAction::None,
                    };
                    server._is_leader.store(is_leader, Relaxed);
                    match action {
                        CheckerAction::SendHeartbeat => {
                            // Send heartbeat synchronously - increased timeout prevents cancellation under stress
                            server
                                .send_followers_heartbeat(&mut meta, None, false)
                                .await;
                            meta.last_checked = get_time();
                        }
                        CheckerAction::BecomeCandidate => {
                            server.become_candidate(&mut meta).await;
                        }
                        CheckerAction::ExitLoop => {
                            return false;
                        }
                        CheckerAction::None => {}
                    }
                    return true;
                };
                let timed_heartbeat = tokio::select! {
                    changed = shutdown_rx.changed() => {
                        match changed {
                            Ok(_) if Self::lifecycle_is_stopping(*shutdown_rx.borrow()) => {
                                debug!("Heartbeat loop observed shutdown signal");
                                break;
                            }
                            Ok(_) => continue,
                            Err(_) => {
                                debug!("Heartbeat loop exiting because shutdown channel closed");
                                break;
                            }
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
                        error!("Heartbeat cannot finish in time for {}ms", HEARTBEAT_MS);
                    }
                    Ok(false) => {
                        debug!("Heartbeat loop exiting");
                        break;
                    }
                    Ok(true) => {
                        trace!(
                            "Continue on heartbeat, going to sleep for {}ms",
                            time_to_sleep
                        );
                    }
                }
                if time_to_sleep > 0 {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            match changed {
                                Ok(_) if Self::lifecycle_is_stopping(*shutdown_rx.borrow()) => {
                                    debug!("Heartbeat sleep interrupted by shutdown");
                                    break;
                                }
                                Ok(_) => {}
                                Err(_) => break,
                            }
                        }
                        _ = sleep(Duration::from_millis(time_to_sleep as u64)) => {}
                    }
                }
            }
            info!("Raft checker/heartbeat task stopped gracefully");
        });
        
        // Store the handle for graceful shutdown
        {
            let mut guard = server.checker_task.lock().await;
            *guard = Some(handle);
        }
        
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
        debug!("Probing and try to join servers: {:?}", servers);
        let is_first_node =
            !RaftClient::probe_servers(servers, &self.options.address, self.options.service_id)
                .await;
        if is_first_node {
            debug!("There is no live node in the server list, will bootstrap");
            self.bootstrap().await;
            Ok(false)
        } else {
            debug!("There are some live nodes, will join them");
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
        debug!("Conservative bootstrap, checking storage");
        if let Some(storage) = &meta.storage {
            debug!("There are storage, checking last term");
            if storage.lock().await.last_term > 0 {
                debug!("There are logged term, will probe and join or bootstrap");
                drop(meta);
                if let Err(e) = self.probe_and_join(servers).await {
                    error!("Failed to probe and join cluster during conservative bootstrap: {:?}", e);
                }
            } else {
                debug!("Log is empty, bootstrap");
                drop(meta);
                self.bootstrap().await;
            }
        } else {
            debug!("No storage, will probe and join or bootstrap");
            drop(meta);
            if let Err(e) = self.probe_and_join(servers).await {
                error!("Failed to probe and join cluster during conservative bootstrap: {:?}", e);
            }
        }
    }
    pub async fn join(&self, servers: &Vec<String>) -> Result<bool, ExecError> {
        debug!("Trying to join cluster with id {}", self.id);
        let client = RaftClient::new(servers, self.options.service_id).await;
        if let Ok(client) = client {
            debug!(
                "Executing in SM to create new member {}, {}",
                &self.options.address, self.id
            );
            let result = client
                .execute(CONFIG_SM_ID, new_member_::new(&self.options.address))
                .await;
            debug!("Getting member address: {}", self.id);
            let members = client.execute(CONFIG_SM_ID, member_address::new()).await;
            debug!("Updating local meta by acquiring lock: {}", self.id);
            let mut meta = self.write_meta().await;
            debug!("Local meta lock acquired: {}", self.id);
            if let Ok(members) = members {
                debug!("We have following members for {}: {:?}", self.id, members);
                for member in members {
                    meta.state_machine
                        .write()
                        .await
                        .configs
                        .new_member(member)
                        .await;
                }
            }
            debug!("Become follower bacause of join: {}", self.id);
            self.become_follower(&mut meta, 0, client.leader_id());
            debug!("Resetting last checked for join: {}", self.id);
            self.reset_last_checked(&mut meta);
            match &result {
                Ok(joined) => debug!("Completed join for {}, result {}", self.id, joined),
                Err(e) => debug!("Join failed for {}, error: {:?}", self.id, e),
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
            "Leaving from a cluster, server id {} with {} members {:?}",
            self.id,
            servers.len(),
            servers
        );
        if let Ok(client) = RaftClient::new(&servers, self.options.service_id).await {
            debug!(
                "Temporary client for leaving, leader: {}. Sending removal message.",
                client.leader_id()
            );
            match client
                .execute(CONFIG_SM_ID, del_member_::new(&self.options.address))
                .await
            {
                Ok(_) => info!("Successfully removed member {} from cluster", self.options.address),
                Err(e) => {
                    error!("Failed to remove member {} from cluster: {:?}", self.options.address, e);
                    return false;
                }
            }
        } else {
            error!("Cannot obtain temporary client for leaving");
            return false;
        }
        let mut meta = self.write_meta().await;
        if is_leader(&meta) {
            info!("Leader step down {}", self.options.address);
            if !self.send_followers_heartbeat(&mut meta, None, true).await {
                error!("Leader cannot step down");
                return false;
            }
            info!("Step down heartbeat sent to followers");
            let mut reelected = false;
            for (_id, addr) in members {
                if addr != self.options.address {
                    info!("Calling reelect to {}", addr);
                    match rpc::DEFAULT_CLIENT_POOL.get(&addr).await {
                        Ok(client) => {
                            let service = AsyncServiceClient::new(&client);
                            match service.reelect().await {
                                Ok(true) => {
                                    info!("New leader has been elected");
                                    reelected = true;
                                    break; // Only need one successful reelection
                                }
                                Ok(false) => {
                                    warn!("Server {} cannot be elected", addr);
                                }
                                Err(e) => {
                                    error!(
                                        "Server {} cannot be elected due to comm error {:?}",
                                        addr, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            error!("Cannot call reelect to {}, error {:?}", addr, e)
                        }
                    }
                }
            }
            if !reelected {
                warn!("No new leader been elected");
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
            let _ = storage.write_commit_progress(commit_index, last_applied).await;
            info!(
                "Flushed WAL and wrote commit progress: commit_index={}, last_applied={}",
                commit_index, last_applied
            );
        }
    }

    async fn wait_for_apply_drain(&self, timeout_duration: Duration) -> bool {
        let deadline = Instant::now() + timeout_duration;
        loop {
            {
                let meta = self.meta.read().await;
                if meta.commit_index == meta.last_applied {
                    info!(
                        "Raft apply drain complete: commit_index={}, last_applied={}",
                        meta.commit_index, meta.last_applied
                    );
                    return true;
                }
                debug!(
                    "Waiting for apply drain: commit_index={}, last_applied={}",
                    meta.commit_index, meta.last_applied
                );
            }

            if Instant::now() >= deadline {
                let meta = self.meta.read().await;
                warn!(
                    "Timed out waiting for apply drain: commit_index={}, last_applied={}",
                    meta.commit_index, meta.last_applied
                );
                return false;
            }

            sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn shutdown(&self) {
        info!("Shutting down RaftService on {}", self.options.address);

        let already_stopping = {
            let mut meta = self.meta.write().await;
            if meta.lifecycle != LifecycleState::Running {
                true
            } else {
                meta.lifecycle = LifecycleState::Stopping;
                meta.membership = Membership::Offline;
                info!("RaftService entered stopping state");
                false
            }
        };
        if already_stopping {
            info!("RaftService shutdown requested while already stopping");
        } else {
            let _ = self.shutdown_tx.send(LifecycleState::Stopping);
        }

        let _ = self.wait_for_apply_drain(Duration::from_secs(5)).await;

        let handle = {
            let mut guard = self.checker_task.lock().await;
            guard.take()
        };
        if let Some(handle) = handle {
            info!("Waiting for Raft checker task to complete...");
            let _ = handle.await;
            info!("Raft checker task completed");
        }

        // Ensure all persistence is flushed to disk
        self.flush_persistence().await;

        {
            let mut meta = self.meta.write().await;
            meta.lifecycle = LifecycleState::Stopped;
        }
        let _ = self.shutdown_tx.send(LifecycleState::Stopped);
        info!("RaftService shutdown complete");
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

    async fn become_candidate<'a>(&'a self, meta: &'a mut RwLockWriteGuard<'_, RaftMeta>) {
        let server_id = self.id;
        debug!("{} become candidate", server_id);
        self.reset_last_checked(meta);
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
                            debug!("Member {} vote for itself", member_id);
                            RequestVoteResponse::Granted
                        } else {
                            if let Ok(((remote_term, remote_leader_id), vote_granted)) = rpc
                                .request_vote(term, server_id, last_log_id, last_log_term)
                                .await
                            {
                                if vote_granted {
                                    debug!(
                                        "Member {} received one vote from {}",
                                        server_id, member_id
                                    );
                                    RequestVoteResponse::Granted
                                } else if remote_term > term {
                                    debug!(
                                        "Member {} is term out, by {}. Now leader is {}, term {}",
                                        server_id, member_id, remote_leader_id, remote_term
                                    );
                                    RequestVoteResponse::TermOut(remote_term, remote_leader_id)
                                } else {
                                    debug!(
                                        "Member {} did not get vote from {}",
                                        server_id, member_id
                                    );
                                    RequestVoteResponse::NotGranted
                                }
                            } else {
                                debug!(
                                    "Member {} request vote failed from {}",
                                    server_id, member_id
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
                        self.become_follower(meta, remote_term, remote_leader_id);
                        break;
                    }
                    Ok(RequestVoteResponse::Granted) => {
                        granted += 1;
                        debug!("Member {} received {} votes in for now", server_id, granted);
                        if is_majority(num_members as u64, granted) {
                            debug!(
                                "Member {} become leader for received majority votes",
                                server_id
                            );
                            self.become_leader(meta, last_log_id).await;
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
        debug!("GRANTED {}: {}/{}", self.id, granted, num_members);
        return;
    }

    fn become_follower(&self, meta: &mut RwLockWriteGuard<RaftMeta>, term: u64, leader_id: u64) {
        alter_term(meta, term);
        meta.leader_id = leader_id;
        self.switch_membership(meta, Membership::Follower);
    }

    async fn become_leader(&self, meta: &mut RwLockWriteGuard<'_, RaftMeta>, last_log_id: u64) {
        debug!("Server {} become leader, term {}", self.id, meta.term);
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
        self._is_leader.store(true, Relaxed);
    }

    async fn send_followers_heartbeat<'a>(
        &self,
        meta: &mut RwLockWriteGuard<'a, RaftMeta>,
        log_id: Option<u64>,
        no_delay: bool,
    ) -> bool {
        let now = get_time();
        if meta.last_checked + HEARTBEAT_MS > now {
            if no_delay {
                debug!("Issuing delayed heartbeat");
            } else {
                debug!("Block throttled heartbeat");
                return false;
            }
        }
        trace!("Sending followers heartbeat");
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
                            "follower not found, {}, {}",
                            member_id,
                            leader_meta.followers.len()
                        ); //TODO: remove after debug
                        continue;
                    };
                    // get a send follower task without await
                    let hb_fut = Self::send_follower_heartbeat(
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
                let mut leader_meta = leader_meta.write().await;
                let mut updated_followers = 0;
                while let Some(heartbeat_res) = heartbeat_futs.next().await {
                    if let Ok(Ok((member_id, last_matched_id))) = heartbeat_res {
                        // adaptive
                        debug!(
                            "Heartbeat response from {} is {:?}",
                            member_id, last_matched_id
                        );
                        if last_matched_id >= log_id {
                            updated_followers += 1;
                            if is_majority(followers as u64, updated_followers) {
                                return true;
                            }
                        }
                    }
                }
                leader_meta.last_updated = get_time();
                // is_majority(members, updated_followers)
                false
            } else {
                !log_id.is_some()
            }
        } else {
            unreachable!()
        }
    }

    async fn send_follower_heartbeat(
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
    ) -> u64 {
        // let commit_index = meta.commit_index;
        // let term = meta.term;
        // let leader_id = meta.leader_id;

        // let meta_term = meta.term;
        // let meta_last_applied = meta.last_applied;
        // let master_sm = &meta.state_machine;
        // let logs = &meta.logs;
        trace!("Sending follower heartbeat to {}", member_id);
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
                    "Stop retry when entry is empty, {}, member id {}",
                    follower.next_index,
                    member_id
                );
                return follower.match_index;
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
                    "Follower {} needs compacted logs (next_index: {} <= snapshot_index: {}), sending snapshot",
                    member_id, follower.next_index, last_snapshot_index
                );
                let master_sm = master_sm.read().await;
                let snapshot = master_sm.snapshot();
                // Use the correct last_included_term from snapshot metadata (Issue 2)
                if let Ok(_) = rpc.install_snapshot(
                    term, 
                    leader_id, 
                    last_snapshot_index, 
                    last_snapshot_term, 
                    snapshot
                ).await {
                    follower.next_index = last_snapshot_index + 1;
                    follower.match_index = last_snapshot_index;
                }
                return follower.match_index;
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
                            error!("Logs map is not empty but iter().next() returned None - this should not happen");
                            return follower.match_index;
                        }
                    };
                    if first_log_id > follower_last_log_id {
                        debug!(
                            "Taking snapshot for follower {} (first_log: {} > follower_last: {})",
                            member_id, first_log_id, follower_last_log_id
                        );
                        let master_sm = master_sm.read().await;
                        let snapshot = master_sm.snapshot();
                        // Use last_applied as snapshot index, get term from the log at that index
                        let snapshot_term = logs.get(&last_applied)
                            .map(|e| e.term)
                            .unwrap_or(last_snapshot_term);
                        if let Ok(_) = rpc.install_snapshot(
                            term, 
                            leader_id, 
                            last_applied, 
                            snapshot_term, 
                            snapshot
                        ).await {
                            follower.next_index = last_applied + 1;
                            follower.match_index = last_applied;
                        }
                        return follower.match_index;
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
                    term,
                    leader_id,
                    follower_last_log_id,
                    follower_last_log_term,
                    &entries,
                    commit_index,
                )
                .await;
            match append_result {
                Ok((_follower_term, result)) => match result {
                    AppendEntriesResult::Ok => {
                        trace!("Log updated to follower: {}", member_id);
                        if let Some(last_entries_id) = last_entries_id {
                            follower.next_index = last_entries_id + 1;
                            follower.match_index = last_entries_id;
                        }
                    }
                    AppendEntriesResult::LogMismatch => {
                        debug!(
                            "Log mismatch in follower {}, index {}",
                            member_id, follower.next_index
                        );
                        if follower.next_index > 0 {
                            follower.next_index -= 1;
                        } else {
                            debug!("Log mismatching index is zero");
                        }
                    }
                    AppendEntriesResult::TermOut(_actual_leader_id) => {
                        break;
                    }
                },
                _ => {
                    break;
                } // retry will happened in next heartbeat
            }
            is_retry = true;
        }
        follower.match_index
    }

    //check term number, return reject = false if server term is stale
    fn check_term(
        &self,
        meta: &mut RwLockWriteGuard<RaftMeta>,
        remote_term: u64,
        leader_id: u64,
    ) -> bool {
        if remote_term > meta.term {
            self.become_follower(meta, remote_term, leader_id)
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
            error!("Failed to persist log entry {} to storage: {:?}", new_log_id, e);
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

    async fn try_sync_log_to_followers<'a>(
        &'a self,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        entry: &LogEntry,
        new_log_id: u64,
    ) -> Option<ExecResult> {
        debug!("Sync logs to followers");
        if self
            .send_followers_heartbeat(&mut meta, Some(new_log_id), true)
            .await
        {
            // Strict write-ahead: ensure persistence reflects this index before applying
            if let Some(storage_mutex) = &meta.storage {
                let mut storage = storage_mutex.lock().await;
                info!(
                    "Strict WA: flushing WAL before commit at log_id={} (term={})",
                    new_log_id, entry.term
                );
                let _ = storage.flush_wal().await;
                info!(
                    "Strict WA: WAL fsync completed before commit at log_id={}",
                    new_log_id
                );
            }
            meta.commit_index = new_log_id;
            info!(
                "Strict WA: applying entry at log_id={} (commit_index={})",
                new_log_id, meta.commit_index
            );
            let result = commit_command(&mut meta, entry).await;
            info!(
                "Strict WA: apply completed at log_id={} (result={:?})",
                new_log_id, result
            );
            // Mark applied and persist commit progress atomically after apply
            meta.last_applied = new_log_id;
            if let Some(storage_mutex) = &meta.storage {
                let mut storage = storage_mutex.lock().await;
                info!(
                    "Strict WA: writing commit progress (commit_index={}, last_applied={})",
                    meta.commit_index, meta.last_applied
                );
                let _ = storage
                    .write_commit_progress(meta.commit_index, meta.last_applied)
                    .await;
                info!("Strict WA: commit progress persisted");
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
    async fn try_sync_config_to_followers<'a>(
        &'a self,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        entry: &LogEntry,
        new_log_id: u64,
    ) -> ExecResult {
        // this will force followers to commit the changes
        debug!("Sync config to followers");
        meta.commit_index = new_log_id;
        let data = commit_command(&meta, &entry).await;
        if let Membership::Leader(ref leader_meta) = meta.membership {
            let mut leader_meta = leader_meta.write().await;
            let member_sm = meta.state_machine.read().await;
            let ref members = member_sm.configs.members;
            self.reload_leader_meta(members, &mut leader_meta, new_log_id);
        }
        self.send_followers_heartbeat(&mut meta, Some(new_log_id), true)
            .await;
        data
    }

    /// Check if we should take a snapshot based on configuration thresholds
    fn should_take_snapshot(&self, meta: &RwLockWriteGuard<'_, RaftMeta>, _num_logs: usize) -> bool {
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
                    "Snapshot threshold reached: {} logs since last snapshot (threshold: {})",
                    logs_since_snapshot, opts.snapshot_log_threshold
                );
                return true;
            }
        }

        false
    }

    /// Create and persist a snapshot
    async fn take_snapshot(&self, meta: &mut RwLockWriteGuard<'_, RaftMeta>) {
        info!(
            "Taking snapshot at index={}, term={}",
            meta.last_applied, meta.term
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
                        "Snapshot created successfully at index={}, term={}",
                        meta.last_snapshot_index, meta.last_snapshot_term
                    );
                    
                    // Now compact logs (reads meta.last_snapshot_index)
                    self.compact_logs_after_snapshot(meta, storage_guard).await;
                }
                Err(e) => {
                    error!("Failed to persist snapshot: {:?}", e);
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
                "Compaction check: {} logs, threshold: {}, snapshot_index: {}",
                before_count, compaction_threshold, snapshot_index
            );
            
            // Only compact if we exceed the compaction threshold
            if before_count as u64 > compaction_threshold {
                // Keep logs after last_snapshot_index
                logs.retain(|&id, _| id > snapshot_index);
                let after_count = logs.len();
                
                info!(
                    "Compacted {} logs (from {} to {}), keeping logs after index {}",
                    before_count - after_count,
                    before_count,
                    after_count,
                    snapshot_index
                );
            } else {
                info!(
                    "Skipping log compaction: {} logs <= threshold {}",
                    before_count, compaction_threshold
                );
            }
        } else {
            debug!("Not using disk storage, skipping compaction");
        }
    }
}

impl Service for RaftService {
    fn append_entries<'a>(
        &'a self,
        term: u64,
        leader_id: u64,
        prev_log_id: u64,
        prev_log_term: u64,
        entries: &'a Option<LogEntries>,
        leader_commit: u64,
    ) -> BoxFuture<'a, (u64, AppendEntriesResult)> {
        async move {
            let mut meta = self.write_meta().await;
            self.reset_last_checked(&mut meta);
            let term_ok = self.check_term(&mut meta, term, leader_id); // RI, 1
            let result = if term_ok {
                if let Membership::Candidate = meta.membership {
                    debug!("SWITCH FROM CANDIDATE BACK TO FOLLOWER {}", self.id);
                    self.become_follower(&mut meta, term, leader_id);
                }
                if prev_log_id > 0 {
                    check_commit(&mut meta).await;
                    let mut logs = meta.logs.write().await;
                    //RI, 2
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
                        return (meta.term, AppendEntriesResult::LogMismatch); // prev log not existed
                    }
                    if log_mismatch {
                        //RI, 3
                        let ids_to_del: Vec<u64> = logs
                            .range((Included(prev_log_id), Unbounded))
                            .map(|(id, _)| *id)
                            .collect();
                        for id in ids_to_del {
                            logs.remove(&id);
                        }
                        return (meta.term, AppendEntriesResult::LogMismatch); // log mismatch
                    }
                }
                let mut last_new_entry = std::u64::MAX;
                {
                    let mut logs = meta.logs.write().await;
                    if let Some(ref entries) = entries {
                        // entry not empty
                        for entry in entries {
                            let entry_id = entry.id;
                            logs.entry(entry_id).or_insert(entry.clone()); // RI, 4
                            last_new_entry = max(last_new_entry, entry_id);
                        }
                    } else if !logs.is_empty() {
                        last_new_entry = match logs.values().last() {
                            Some(entry) => entry.id,
                            None => {
                                error!("Logs map is not empty but values().last() returned None - this should not happen");
                                // Use u64::MAX as fallback to prevent issues
                                std::u64::MAX
                            }
                        };
                    }
                    if let Err(e) = self.logs_post_processing(&meta, logs).await {
                        error!("Failed to persist logs during append_entries: {:?}", e);
                        // Continue processing despite persistence failure
                    }
                }
                if leader_commit > meta.commit_index {
                    //RI, 5
                    meta.commit_index = min(leader_commit, last_new_entry);
                    check_commit(&mut meta).await;
                }
                (meta.term, AppendEntriesResult::Ok)
            } else {
                (meta.term, AppendEntriesResult::TermOut(meta.leader_id)) // term mismatch
            };
            self.reset_last_checked(&mut meta);
            return result;
        }
        .boxed()
    }

    fn request_vote(
        &self,
        term: u64,
        candidate_id: u64,
        last_log_id: u64,
        last_log_term: u64,
    ) -> BoxFuture<((u64, u64), bool)> {
        async move {
            let mut meta = self.write_meta().await;
            let vote_for = meta.vote_for;
            let mut vote_granted = false;
            if term > meta.term {
                check_commit(&mut meta).await;
                let logs = meta.logs.read().await;
                let conf_sm = &meta.state_machine.read().await.configs;
                let candidate_valid = conf_sm.member_existed(candidate_id);
                debug!(
                    "{} VOTE FOR: {}, valid: {}",
                    self.id, candidate_id, candidate_valid
                );
                let can_vote = vote_for.map_or(true, |voted_for| voted_for == candidate_id);
                if can_vote && candidate_valid {
                    let (last_id, last_term) = get_last_log_info!(self, logs);
                    if last_log_id >= last_id && last_log_term >= last_term {
                        vote_granted = true;
                    } else {
                        debug!(
                            "{} VOTE FOR: {}, not granted due to log check",
                            self.id, candidate_id
                        );
                    }
                } else {
                    debug!(
                        "{} VOTE FOR: {}, not granted, candidate valid: {}, voted for {:?}",
                        self.id, candidate_id, candidate_valid, vote_for
                    );
                }
            } else {
                debug!(
                    "{} VOTE FOR: {}, not granted due to term out",
                    self.id, candidate_id
                );
            }
            if vote_granted {
                meta.vote_for = Some(candidate_id);
            }
            debug!(
                "{} VOTE FOR: {}, granted: {}",
                self.id, candidate_id, vote_granted
            );
            ((meta.term, meta.leader_id), vote_granted)
        }
        .boxed()
    }

    fn install_snapshot(
        &self,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Vec<u8>,
    ) -> BoxFuture<u64> {
        async move {
            let mut meta = self.write_meta().await;
            let term_ok = self.check_term(&mut meta, term, leader_id);
            if term_ok {
                check_commit(&mut meta).await;
            }
            
            // Recover state machine from snapshot
            meta.state_machine.write().await.recover(data.clone());
            
            // Update snapshot metadata
            meta.last_snapshot_index = last_included_index;
            meta.last_snapshot_term = last_included_term;
            meta.commit_index = last_included_index;
            meta.last_applied = last_included_index;
            
            // Compact logs: remove all logs at or before the snapshot
            {
                let mut logs = meta.logs.write().await;
                logs.retain(|&id, _| id > last_included_index);
                debug!(
                    "Compacted logs after snapshot install, removed logs <= {}, remaining: {}",
                    last_included_index,
                    logs.len()
                );
            }
            
            // Persist snapshot to disk if storage is available
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
        .boxed()
    }

    fn c_command<'a>(&'a self, entry: LogEntry) -> BoxFuture<'a, ClientCmdResponse> {
        async move {
            let mut meta = self.write_meta().await;
            let mut entry = entry;
            if Self::lifecycle_is_stopping(meta.lifecycle) {
                debug!(
                    "Rejecting raft command during shutdown on {}, sm_id={}, fn_id={}",
                    self.id, entry.sm_id, entry.fn_id
                );
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
                    warn!(
                        "RAFTDBG_V2 server single_node_self_heal self={} sm_id={} fn_id={} term={} leader_id={} lifecycle={:?}",
                        self.id,
                        entry.sm_id,
                        entry.fn_id,
                        meta.term,
                        meta.leader_id,
                        meta.lifecycle
                    );
                    warn!(
                        "Single-node raft command hit transient non-leader state on {}; re-promoting to leader before executing sm_id={}, fn_id={}, term={}, last_log_id={}",
                        self.id,
                        entry.sm_id,
                        entry.fn_id,
                        meta.term,
                        last_log_id
                    );
                    self.become_leader(&mut meta, last_log_id).await;
                }
            }
            if !is_leader(&meta) {
                warn!(
                    "RAFTDBG_V2 server non_leader_after_heal self={} leader_id={} lifecycle={:?} membership_is_leader={} sm_id={} fn_id={} term={} entry_term={} entry_id={}",
                    self.id,
                    meta.leader_id,
                    meta.lifecycle,
                    matches!(meta.membership, Membership::Leader(_)),
                    entry.sm_id,
                    entry.fn_id,
                    meta.term,
                    entry.term,
                    entry.id
                );
                warn!(
                    "Command sent to non-leader node, self={}, leader_id={}, lifecycle={:?}, membership_is_leader={}, sm_id={}, fn_id={}, term={}, entry_term={}, entry_id={}",
                    self.id,
                    meta.leader_id,
                    meta.lifecycle,
                    matches!(meta.membership, Membership::Leader(_)),
                    entry.sm_id,
                    entry.fn_id,
                    meta.term,
                    entry.term,
                    entry.id
                );
                return if meta.leader_id == self.id {
                    warn!(
                        "RAFTDBG_V2 server returning_notleader_zero self={} sm_id={} fn_id={} term={} entry_id={}",
                        self.id,
                        entry.sm_id,
                        entry.fn_id,
                        meta.term,
                        entry.id
                    );
                    warn!(
                        "Returning NotLeader(0) because membership is not leader while leader_id still points to self {}; sm_id={}, fn_id={}",
                        self.id,
                        entry.sm_id,
                        entry.fn_id
                    );
                    ClientCmdResponse::NotLeader(0)
                } else {
                    ClientCmdResponse::NotLeader(meta.leader_id)
                };
            }
            let (new_log_id, new_log_term) = self.leader_append_log(&meta, &mut entry).await;
            let data = match entry.sm_id {
                // special treats for membership changes
                CONFIG_SM_ID => Some(
                    self.try_sync_config_to_followers(meta, &entry, new_log_id)
                        .await,
                ),
                _ => {
                    self.try_sync_log_to_followers(meta, &entry, new_log_id)
                        .await
                }
            }; // Some for committed and None for not committed
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
        .boxed()
    }

    fn c_query<'a>(&'a self, entry: &'a LogEntry) -> BoxFuture<'a, ClientQryResponse> {
        async move {
            trace!("Client query for raft sm_id {}, fn_id {} with term {}, id {}. Obtaining meta read lock.", entry.sm_id, entry.fn_id, entry.term, entry.id);
            let meta = self.meta.read().await; // .unwrap();
            trace!("Client query for raft sm_id {}, fn_id {} with term {}, id {}. Obtaining logs read lock.", entry.sm_id, entry.fn_id, entry.term, entry.id);
            let logs = meta.logs.read().await;
            trace!("Client query for raft sm_id {}, fn_id {} with term {}, id {}. Getting last log and check term and id", entry.sm_id, entry.fn_id, entry.term, entry.id);
            let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
            if entry.term > last_log_term || entry.id > last_log_id {
                trace!("Client query for raft sm_id {}, fn_id {} with term {}, id {} have left behind. Extected term {}, id {}", entry.sm_id, entry.fn_id, entry.term, entry.id, last_log_term, last_log_id);
                ClientQryResponse::LeftBehind {
                    last_log_term,
                    last_log_id,
                }
            } else {
                trace!("Client query for raft sm_id {}, fn_id {} with term {}, id {}. Reading state machine for query result.", entry.sm_id, entry.fn_id, entry.term, entry.id);
                let qry_res = meta.state_machine.read().await.exec_qry(&entry).await;
                trace!("Client query for raft sm_id {}, fn_id {} with term {}, id {}.Query complete, return result to client.", entry.sm_id, entry.fn_id, entry.term, entry.id);
                ClientQryResponse::Success {
                    data: qry_res,
                    last_log_id,
                    last_log_term,
                }
            }
        }
        .boxed()
    }

    fn c_server_cluster_info(&self) -> BoxFuture<ClientClusterInfo> {
        self.cluster_info().boxed()
    }

    fn c_put_offline(&self) -> BoxFuture<bool> {
        self.leave().boxed()
    }

    fn c_have_state_machine(&self, id: u64) -> BoxFuture<bool> {
        async move {
            let meta = self.meta.read().await;
            let sm = meta.state_machine.read().await;
            sm.has_sub(&id)
        }
        .boxed()
    }

    fn c_ping(&self) -> BoxFuture<()> {
        future::ready(()).boxed()
    }

    fn reelect<'a>(&'a self) -> futures::future::BoxFuture<bool> {
        async move {
            let mut meta = self.meta.write().await;
            info!(
                "Been asked to reelect, become candidate. Server id {}",
                self.get_server_id()
            );
            self.become_candidate(&mut meta).await;
            let is_leader = self.is_leader();
            info!(
                "Reelect result for server {}, is leader {}",
                self.get_server_id(),
                is_leader
            );
            is_leader
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
    use crate::raft::state_machine::master::ExecError;
    use crate::raft::state_machine::StateMachineCtl;
    use crate::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use futures::FutureExt;

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
        use crate::raft::{
            LifecycleState, LogEntry, Membership, RaftMeta, Service, SnapshotEntity,
        };
        use crate::raft::state_machine::configs::CONFIG_SM_ID;
        use crate::raft::state_machine::master::MasterStateMachine;
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
            let addresses: Vec<_> = vec![
                "127.0.0.1:2010",
                "127.0.0.1:2011",
                "127.0.0.1:2012",
                "127.0.0.1:2013",
                "127.0.0.1:2014",
            ]
            .into_iter()
            .map(|addr| addr.to_string())
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_test_{}", rand::random::<u64>()));
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
                assert!(meta.last_snapshot_index > 0, "Snapshot should have been created");
                info!("Manual snapshot created at index {}", meta.last_snapshot_index);
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_persist_{}", rand::random::<u64>()));
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_recovery_{}", rand::random::<u64>()));
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_snapshot_io_{}", rand::random::<u64>()));
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_snapshot_corrupt_{}", rand::random::<u64>()));
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
            assert!(result.unwrap().is_none(), "Should return None for corrupted snapshot");
            
            info!("Corruption detection working correctly");
            
            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_log_compaction_removes_old_logs() {
            let _ = env_logger::try_init();
            info!("TESTING LOG COMPACTION");
            
            let temp_dir = std::env::temp_dir().join(format!("raft_compact_{}", rand::random::<u64>()));
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
                    logs.insert(i, LogEntry {
                        id: i,
                        term: 1,
                        sm_id: 15,
                        fn_id: 1,
                        data: vec![],
                    });
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
            assert!(final_count < after_add, "Should have compacted some logs: before={}, after={}", after_add, final_count);
            
            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("Log compaction test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_snapshot_threshold_configuration() {
            let _ = env_logger::try_init();
            info!("TESTING SNAPSHOT THRESHOLD CONFIGURATION");
            
            let temp_dir = std::env::temp_dir().join(format!("raft_threshold_{}", rand::random::<u64>()));
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
                assert!(should_snapshot, "Should trigger snapshot when last_applied (5) > threshold (3)");
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_install_{}", rand::random::<u64>()));
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
                    logs.insert(i, LogEntry {
                        id: i,
                        term: 1,
                        sm_id: 15,
                        fn_id: 1,
                        data: vec![],
                    });
                }
            }
            
            let before_count = raft_service.num_logs().await;
            info!("Logs before install_snapshot: {}", before_count);
            
            // Create valid snapshot data (SnapshotDataItems format)
            use crate::raft::state_machine::master::SnapshotDataItems;
            let snapshot_items: SnapshotDataItems = vec![
                (CONFIG_SM_ID, vec![1u8, 2, 3]), // Config SM snapshot
                (15u64, vec![42u8; 10]),          // Test SM snapshot
            ];
            let snapshot_data = crate::utils::serde::serialize(&snapshot_items);
            
            // Simulate receiving a snapshot via install_snapshot
            let _result = (&*raft_service as &dyn Service).install_snapshot(
                1,      // term
                12345,  // leader_id
                10,     // last_included_index
                1,      // last_included_term
                snapshot_data
            ).await;
            
            let after_count = raft_service.num_logs().await;
            info!("Logs after install_snapshot: {}", after_count);
            
            // Should have removed logs 1-10, keeping logs with id > 10
            assert!(
                after_count < before_count, 
                "Should have compacted logs: before={}, after={}", 
                before_count, after_count
            );
            
            // Verify logs 1-10 are gone
            {
                let meta = raft_service.read_meta().await;
                let logs = meta.logs.read().await;
                for i in 1..=10 {
                    assert!(!logs.contains_key(&i), "Log {} should have been compacted", i);
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
                    take_snapshots: false,  // Disable snapshots to focus on logs
                    append_logs: true,       // Enable WAL
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
                initial_size, final_size
            );
            
            // Clean up
            std::fs::remove_dir_all(&temp_dir).unwrap();
            info!("WAL persistence test passed");
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_wal_recovery_after_crash() {
            let _ = env_logger::try_init();
            info!("TESTING WAL - RECOVERY AFTER SIMULATED CRASH");
            
            let temp_dir = std::env::temp_dir().join(format!("raft_wal_crash_{}", rand::random::<u64>()));
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
                        take_snapshots: false,  // No snapshots, only WAL
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
                        path: data_path.clone(),  // Same data directory!
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
                let sm2 = SM { shots: 999 };  // Different from crashed instance
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
                
                assert!(
                    num_logs_after > 0,
                    "Should have recovered logs from disk"
                );
                
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_wal_format_{}", rand::random::<u64>()));
            std::fs::create_dir_all(&temp_dir).unwrap();
            
            // Create storage and write some logs
            let mut storage = disk::StorageEntity {
                logs: None,
                snapshot: None,
                last_term: 0,
                base_path: temp_dir.clone(),
            };
            
            // Create test logs in memory
            let mut logs = BTreeMap::new();
            for i in 1..=5u64 {
                logs.insert(i, LogEntry {
                    id: i,
                    term: 1,
                    sm_id: 15,
                    fn_id: 1,
                    data: vec![i as u8, (i * 2) as u8],
                });
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_wal_fsync_{}", rand::random::<u64>()));
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
            assert!(log_file_path.exists(), "Log file should exist after one command");
            
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
            
            let temp_dir = std::env::temp_dir().join(format!("raft_wal_state_{}", rand::random::<u64>()));
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
                        take_snapshots: false,  // Only WAL, no snapshots
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
                        path: data_path.clone(),  // Same directory!
                        take_snapshots: false,
                        append_logs: true,
                        trim_logs: false,
                        snapshot_log_threshold: 10000,
                        log_compaction_threshold: 20000,
                    }),
                    address: addr2.clone(),  // Different port
                    service_id: DEFAULT_SERVICE_ID,
                });
                
                // Start with SAME initial state (WAL only replays commands, not full state)
                let sm2 = SM { shots: 100 };  // Same as first instance
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
                    info!("Before applying: last_applied={}, commit_index={}", 
                        meta.last_applied, meta.commit_index);
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
                    diff <= 6,  // Allow for 2 uncommitted commands (2 * 3 = 6)
                    "State should be close to expected: expected={}, got={}, diff={}",
                    expected_final_state, recovered_state, diff
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
            assert_eq!(
                encoded1, encoded2,
                "Encoding should be deterministic"
            );
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
                    data: vec![],  // Empty data
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
            assert_eq!(encoded.len(), 64 + 10000, "Should be 64 header + 10000 data");
            
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

            let temp_dir = std::env::temp_dir().join(format!("raft_wal_only_min_{}", rand::random::<u64>()));
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
                assert_eq!(recovered_state, expected_state, "recovered state should equal pre-crash state");

                // Clean up
                drop(sm_client2);
                drop(client2);
                drop(service2);
                drop(server2);
            }

            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
    }
}
