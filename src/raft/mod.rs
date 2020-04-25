use self::state_machine::configs::commands::{del_member_, member_address, new_member_};
use self::state_machine::configs::{RaftMember, CONFIG_SM_ID};
use self::state_machine::master::{ExecError, ExecResult, MasterStateMachine, SubStateMachine};
use self::state_machine::OpType;
use crate::raft::client::RaftClient;
use crate::raft::state_machine::StateMachineCtl;
use async_std::sync::*;
use crate::utils::time::get_time;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::executor::*;
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use futures::task::SpawnExt;
use futures::FutureExt;
use rand;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::collections::Bound::{Included, Unbounded};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;
use std::{fs, thread};
use tokio::prelude::*;
use tokio::runtime;
use tokio::time::*;
use crate::raft::disk::*;

#[macro_use]
pub mod state_machine;
pub mod client;
pub mod disk;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_DEFAULT_SERVICE) as u64;
const THREAD_POOL_SIZE: usize = 10;

pub trait RaftMsg<R>: Send + Sync {
    fn encode(self) -> (u64, OpType, Vec<u8>);
    fn decode_return(data: &Vec<u8>) -> R;
}

const CHECKER_MS: i64 = 50;
const HEARTBEAT_MS: i64 = 200;

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
    NotCommitted,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientQryResponse {
    Success {
        data: ExecResult,
        last_log_term: u64,
        last_log_id: u64,
    },
    LeftBehind,
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

#[derive(Serialize, Deserialize)]
pub struct SnapshotEntity {
    term: u64,
    commit_index: u64,
    last_applied: u64,
    snapshot: Vec<u8>,
}

type LogEntries = Vec<LogEntry>;
type LogsMap = BTreeMap<u64, LogEntry>;

service! {
    rpc append_entries(term: u64, leader_id: u64, prev_log_id: u64, prev_log_term: u64, entries: Option<LogEntries>, leader_commit: u64) -> (u64, AppendEntriesResult);
    rpc request_vote(term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> ((u64, u64), bool); // term, voteGranted
    rpc install_snapshot(term: u64, leader_id: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>) -> u64;
    rpc c_command(entry: LogEntry) -> ClientCmdResponse;
    rpc c_query(entry: LogEntry) -> ClientQryResponse;
    rpc c_server_cluster_info() -> ClientClusterInfo;
    rpc c_put_offline() -> bool;
    rpc c_have_state_machine(id: u64) -> bool;
    rpc c_ping();
}

fn gen_rand(lower: i64, higher: i64) -> i64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(lower, higher)
}

fn gen_timeout() -> i64 {
    gen_rand(2000, 5000)
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
    rt: runtime::Runtime
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
            commit_command(meta, &entry).await;
        };
    }
}

fn is_majority(members: u64, granted: u64) -> bool {
    let required = members / 2 + 1;
    let majority = granted >= (required);
    debug!("Members {} granted {}, is majority: {}", members, granted, majority);
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

        let mut storage_entity = StorageEntity::new_with_options(
            &opts,
            &mut term,
            &mut commit_index,
            &mut last_applied,
            &mut logs
        ).unwrap();

        let mut master_sm = MasterStateMachine::new(opts.service_id);

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
                storage: storage_entity.map(|e| Arc::new(Mutex::new(e)))
            }),
            id: server_id,
            options: opts,
            rt: runtime::Builder::new()
                .enable_all()
                .core_threads(10)
                .thread_name("raft-server")
                .threaded_scheduler()
                .build()
                .unwrap()
        };
        Arc::new(server_obj)
    }
    pub async fn start(server: &Arc<RaftService>) -> bool {
        let server_address = server.options.address.clone();
        info!("Waiting for raft server to be initialized");
        let mut meta = server.meta.write().await;
        {
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
        }
        let checker_ref = server.clone();
        server.rt.spawn(async {
            let server = checker_ref;
            loop {
                let start_time = get_time();
                let expected_ends = start_time + CHECKER_MS;
                {
                    let mut meta = server.meta.write().await; //WARNING: Reentering not supported
                    let current_time = get_time();
                    let action = match meta.membership {
                        Membership::Leader(_) => {
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
                                debug!(
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
                    match action {
                        CheckerAction::SendHeartbeat => {
                            server.send_followers_heartbeat(&mut meta, None, false).await;
                        }
                        CheckerAction::BecomeCandidate => {
                            server.become_candidate(&mut meta).await;
                        }
                        CheckerAction::ExitLoop => {
                            break;
                        }
                        CheckerAction::None => {}
                    }
                }
                let end_time = get_time();
                let time_to_sleep = expected_ends - end_time - 1;
                if time_to_sleep > 0 {
                    // Use thread sleep here because we want system scheduler for precision
                    delay_for(Duration::from_millis(time_to_sleep as u64)).await;
                }
            }
        });
        meta.last_checked = get_time() + (CHECKER_MS * 10);
        return true;
    }
    pub async fn new_server(opts: Options) -> (bool, Arc<RaftService>, Arc<Server>) {
        let address = opts.address.clone();
        let svr_id = opts.service_id;
        let service = RaftService::new(opts);
        let server = Server::new(&address);
        Server::listen_and_resume(&server).await;
        server.register_service(svr_id, &service).await;
        (RaftService::start(&service).await, service, server)
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
                self.probe_and_join(servers).await;
            } else {
                debug!("Log is empty, bootstrap");
                drop(meta);
                self.bootstrap().await;
            }
        } else {
            debug!("No storage, will probe and join or bootstrap");
            drop(meta);
            self.probe_and_join(servers).await;
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
            debug!(
                "Completed join for {}, result {:}",
                self.id,
                result.is_ok() && *result.as_ref().unwrap()
            );
            result
        } else {
            Err(ExecError::CannotConstructClient)
        }
    }
    pub async fn leave(&self) -> bool {
        let servers = self
            .cluster_info()
            .await
            .members
            .iter()
            .map(|&(_, ref address)| address.clone())
            .collect();
        if let Ok(client) = RaftClient::new(&servers, self.options.service_id).await {
            client
                .execute(CONFIG_SM_ID, del_member_::new(&self.options.address))
                .await;
        } else {
            return false;
        }
        let mut meta = self.write_meta().await;
        if is_leader(&meta) {
            if !self.send_followers_heartbeat(&mut meta, None, true).await {
                return false;
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
    pub async fn is_leader(&self) -> bool {
        let meta = self.meta.read().await;
        match meta.membership {
            Membership::Leader(_) => true,
            _ => false,
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
                                    debug!("Member {} received one vote from {}", server_id, member_id);
                                    RequestVoteResponse::Granted
                                } else if remote_term > term {
                                    debug!("Member {} is term out, by {}. Now leader is {}, term {}",
                                           server_id, member_id, remote_leader_id, remote_term);
                                    RequestVoteResponse::TermOut(remote_term, remote_leader_id)
                                } else {
                                    debug!("Member {} did not get vote from {}", server_id, member_id);
                                    RequestVoteResponse::NotGranted
                                }
                            } else {
                                debug!("Member {} request vote failed from {}", server_id, member_id);
                                RequestVoteResponse::NotGranted // default for request failure
                            }
                        }
                    };
                    timeout(
                        Duration::from_millis(1500),
                        self.rt.spawn(vote_fut),
                    )
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
                            debug!("Member {} become leader for received majority votes", server_id);
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
    }

    async fn send_followers_heartbeat<'a>(
        &self,
        meta: &mut RwLockWriteGuard<'a, RaftMeta>,
        log_id: Option<u64>,
        no_delay: bool
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
                    follower.next_index, member_id
                );
                return follower.match_index;
            }
            let last_entries_id = match &entries {
                // get last entry id
                &Some(ref entries) => Some(entries.iter().last().unwrap().id),
                &None => None,
            };
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
                    // detect cleaned logs
                    let (first_log_id, _) = logs.iter().next().unwrap();
                    if *first_log_id > follower_last_log_id {
                        debug!(
                            "Taking snapshot of all state machines and install them on follower {}",
                            member_id
                        );
                        let master_sm = master_sm.read().await;
                        let snapshot = master_sm.snapshot().unwrap();
                        rpc.install_snapshot(term, leader_id, last_applied, term, snapshot)
                            .await
                            .unwrap();
                    }
                    let follower_last_entry = logs.get(&follower_last_log_id);
                    match follower_last_entry {
                        Some(entry) => (entry.id, entry.term),
                        None => {
                            panic!("Cannot find old logs for follower, first_id: {}, follower_last: {}");
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
                    entries,
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
        self.logs_post_processing(meta, logs).await;
        (new_log_id, new_log_term)
    }

    async fn logs_post_processing<'a>(
        &'a self,
        meta: &'a RwLockWriteGuard<'a, RaftMeta>,
        mut logs: RwLockWriteGuard<'a, LogsMap>,
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
            meta.commit_index = new_log_id;
            Some(commit_command(&mut meta, entry).await)
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
        let t = get_time();
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
}

impl Service for RaftService {
    fn append_entries(
        &self,
        term: u64,
        leader_id: u64,
        prev_log_id: u64,
        prev_log_term: u64,
        entries: Option<LogEntries>,
        leader_commit: u64,
    ) -> BoxFuture<(u64, AppendEntriesResult)> {
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
                    let mut log_mismatch = false;

                    if contains_prev_log {
                        let entry = logs.get(&prev_log_id).unwrap();
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
                            let sm_id = entry.sm_id;
                            logs.entry(entry_id).or_insert(entry.clone()); // RI, 4
                            last_new_entry = max(last_new_entry, entry_id);
                        }
                    } else if !logs.is_empty() {
                        last_new_entry = logs.values().last().unwrap().id;
                    }
                    self.logs_post_processing(&meta, logs).await;
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
                if (vote_for.is_none() || vote_for.unwrap() == candidate_id) && candidate_valid {
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
                        self.id,
                        candidate_id,
                        candidate_valid,
                        vote_for
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
            meta.state_machine.write().await.recover(data);
            meta.term = last_included_term;
            meta.commit_index = last_included_index;
            meta.last_applied = last_included_index;
            self.reset_last_checked(&mut meta);
            meta.term
        }
        .boxed()
    }

    fn c_command(&self, entry: LogEntry) -> BoxFuture<ClientCmdResponse> {
        async move {
            let meta = self.write_meta().await;
            let mut entry = entry;
            if !is_leader(&meta) {
                return ClientCmdResponse::NotLeader(meta.leader_id);
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
                ClientCmdResponse::NotCommitted
            }
        }
        .boxed()
    }

    fn c_query(&self, entry: LogEntry) -> BoxFuture<ClientQryResponse> {
        async move {
            let meta = self.meta.read().await;
            let logs = meta.logs.read().await;
            let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
            if entry.term > last_log_term || entry.id > last_log_id {
                ClientQryResponse::LeftBehind
            } else {
                ClientQryResponse::Success {
                    data: meta.state_machine.read().await.exec_qry(&entry).await,
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
    use crate::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use futures::FutureExt;
    use crate::raft::state_machine::StateMachineCtl;

    #[tokio::test(threaded_scheduler)]
    async fn startup() {
        let (success, _, _) = RaftService::new_server(Options {
            storage: Storage::default(),
            address: String::from("127.0.0.1:2000"),
            service_id: DEFAULT_SERVICE_ID,
        })
        .await;
        assert!(success);
    }

    #[tokio::test(threaded_scheduler)]
    async fn server_membership() {
        env_logger::try_init();
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
        server1
            .register_service(DEFAULT_SERVICE_ID, &service1)
            .await;
        info!("Listening server 1");
        Server::listen_and_resume(&server1).await;
        info!("Start raft service server 1");
        assert!(RaftService::start(&service1).await);
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
        server2
            .register_service(DEFAULT_SERVICE_ID, &service2)
            .await;
        info!("Listening server 2");
        Server::listen_and_resume(&server2).await;
        info!("Start raft service for server 2");
        assert!(RaftService::start(&service2).await);
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
        server3
            .register_service(DEFAULT_SERVICE_ID, &service3)
            .await;
        info!("Start raft service for server 3");
        assert!(RaftService::start(&service3).await);
        info!("Server 3 join server 1 and server 2");
        let join_result = service3.join(&vec![s1_addr.clone(), s2_addr.clone()]).await;
        assert!(join_result.unwrap());
        info!("Checking numbers of users on 3 servers");
        assert_eq!(service1.num_members().await, 3);
        assert_eq!(service2.num_members().await, 3);
        assert_eq!(service3.num_members().await, 3);

        async_wait_secs().await;

        // test remove member
        info!("Server 1 ({}) is leaving", service1.id);
        assert!(service1.leave().await);

        async_wait_secs().await;

        info!("Check number of servers, should be 2");
        assert_eq!(service2.num_members().await, 2);
        assert_eq!(service3.num_members().await, 2);

        async_wait_secs().await;

        info!("Server 2 ({}) is leaving", server2.server_id);
        assert!(service2.leave().await);

        // there will be some unavailability in leader transaction
        async_wait_secs().await;
        async_wait_secs().await;
        async_wait_secs().await;
        assert_eq!(service3.num_members().await, 1);
    }

    #[tokio::test(threaded_scheduler)]
    async fn log_replication() {
        env_logger::try_init();
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
        server1
            .register_service(DEFAULT_SERVICE_ID, &service1)
            .await;
        info!("Listen server 1");
        Server::listen_and_resume(&server1).await;
        info!("Starting raft service for server 1");
        assert!(RaftService::start(&service1).await);
        info!("Bootstrap raft for server 1");
        assert_eq!(service1.probe_and_join(&server_list).await.unwrap(), false);

        info!("Starting server 2");
        let server2 = Server::new(&s2_addr);
        info!("Listening server 2");
        Server::listen_and_resume(&server2).await;
        info!("Register raft service for server 2");
        server2
            .register_service(DEFAULT_SERVICE_ID, &service2)
            .await;
        info!("Start raft service for server 2");
        assert!(RaftService::start(&service2).await);
        info!("Server 2 join cluster");
        let join_result = service2.probe_and_join(&server_list).await;
        join_result.unwrap();

        info!("Starting server 3");
        let server3 = Server::new(&s3_addr);
        info!("Register raft service for server 3");
        server3
            .register_service(DEFAULT_SERVICE_ID, &service3)
            .await;
        info!("Listening for server 3");
        Server::listen_and_resume(&server3).await;
        info!("Starting raft service for server 3");
        assert!(RaftService::start(&service3).await);
        info!("Server 3 join the cluster");
        let join_result = service3.probe_and_join(&server_list).await;
        join_result.unwrap();

        info!("Starting server 4");
        let server4 = Server::new(&s4_addr);
        info!("Register raft service for server 4");
        server4
            .register_service(DEFAULT_SERVICE_ID, &service4)
            .await;
        info!("Listening for server 4");
        Server::listen_and_resume(&server4).await;
        info!("Starting raft service for server 4");
        assert!(RaftService::start(&service4).await);
        info!("Server 4 join cluster");
        let join_result = service4.probe_and_join(&server_list).await;
        join_result.unwrap();

        info!("Starting server 5");
        let server5 = Server::new(&s5_addr);
        info!("Register raft service for server 5");
        server5
            .register_service(DEFAULT_SERVICE_ID, &service5)
            .await;
        info!("Listening for server 5");
        Server::listen_and_resume(&server5).await;
        info!("Starting raft service for server 5");
        assert!(RaftService::start(&service5).await);
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
        use futures::stream::FuturesUnordered;
        use std::time::Duration;
        use crate::utils::time::async_wait;
        use std::sync::Arc;

        raft_state_machine! {
            def qry answer_to_the_universe(name: String) -> String;
            def qry get_shot() -> i32;
            def cmd take_a_shot(num: i32) -> i32;
        }

        struct SM {
            shots: i32
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
            fn snapshot(&self) -> Option<Vec<u8>> {
                None
            }
            fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
                future::ready(()).boxed()
            }
        }
    
        #[tokio::test(threaded_scheduler)]
        async fn query_and_command() {
            env_logger::try_init();
            println!("TESTING CALLBACK");
            let addr = String::from("127.0.0.1:2009");
            let raft_service = RaftService::new(Options {
                storage: Storage::default(),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });
            let sm = SM { shots: 10 };
            let server = Server::new(&addr);
            let sm_id = sm.id();
            server
                .register_service(DEFAULT_SERVICE_ID, &raft_service)
                .await;
            Server::listen_and_resume(&server).await;
            RaftService::start(&raft_service).await;
            raft_service
                .register_state_machine(Box::new(sm))
                .await;
            raft_service.bootstrap().await;
    
            async_wait_secs().await;
    
            let raft_client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
                .await
                .unwrap();
            let sm_client = client::SMClient::new(sm_id, &raft_client);
            assert_eq!(sm_client.answer_to_the_universe(&"Alice".to_string()).await.unwrap(), "Alice, the answer is 42");
            assert_eq!(sm_client.take_a_shot(&2).await.unwrap(), 8);
        }

        #[tokio::test(threaded_scheduler)]
        async fn multi_server_command() {
            env_logger::try_init();
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
                        server
                            .register_service(DEFAULT_SERVICE_ID, &raft_service)
                            .await;
                        Server::listen_and_resume(&server).await;
                        RaftService::start(&raft_service).await;
                        raft_service
                            .register_state_machine(Box::new(sm))
                            .await;
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
            async_wait(Duration::from_secs(5)).await;
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
                assert_eq!(sm_client.get_shot().await.unwrap(), 110, "fail at test {}", i);
            }
            async_wait(Duration::from_secs(5)).await;
        } 
    }
}
