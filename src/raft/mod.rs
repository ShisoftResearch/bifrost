use self::state_machine::configs::commands::{del_member_, member_address, new_member_};
use self::state_machine::configs::{RaftMember, CONFIG_SM_ID};
use self::state_machine::master::{ExecError, ExecResult, MasterStateMachine, SubStateMachine};
use self::state_machine::OpType;
use crate::raft::client::RaftClient;
use crate::raft::state_machine::StateMachineCtl;
use crate::utils::mutex::*;
use crate::utils::rwlock::*;
use crate::utils::time::get_time;
use bifrost_hasher::hash_str;
use bifrost_plugins::hash_ident;
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use rand;
use rand::Rng;
use std::cell::RefCell;
use std::cmp::{max, min};
use std::collections::Bound::{Included, Unbounded};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;
use tokio::time;
use serde::{Serialize, Deserialize};

#[macro_use]
pub mod state_machine;
pub mod client;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_DEFAULT_SERVICE) as u64;
const MAX_LOG_CAPACITY: usize = 10;

def_bindings! {
    bind val IS_LEADER: bool = false;
}

pub trait RaftMsg<R>: Send + Sync {
    fn encode(self) -> (u64, OpType, Vec<u8>);
    fn decode_return(data: &Vec<u8>) -> R;
}

const CHECKER_MS: i64 = 10;

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
    rpc append_entries(term: u64, leaderId: u64, prev_log_id: u64, prev_log_term: u64, entries: Option<LogEntries>, leader_commit: u64) -> (u64, AppendEntriesResult);
    rpc request_vote(term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> ((u64, u64), bool); // term, voteGranted
    rpc install_snapshot(term: u64, leader_id: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>) -> u64;
    rpc c_command(entry: LogEntry) -> ClientCmdResponse;
    rpc c_query(entry: LogEntry) -> ClientQryResponse;
    rpc c_server_cluster_info() -> ClientClusterInfo;
    rpc c_put_offline() -> bool;
}

fn gen_rand(lower: i64, higher: i64) -> i64 {
    let mut rng = rand::thread_rng();
    rng.gen_range(lower, higher)
}

fn gen_timeout() -> i64 {
    gen_rand(200, 500)
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
    storage: Option<Arc<RwLock<StorageEntity>>>,
}

#[derive(Clone)]
pub enum Storage {
    MEMORY,
    DISK(String),
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

struct StorageEntity {
    logs: File,
    snapshot: File,
}

pub struct RaftService {
    meta: RwLock<RaftMeta>,
    pub id: u64,
    pub options: Options,
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
    granted >= members / 2
}

async fn commit_command<'a>(
    meta: &'a RwLockWriteGuard<'a, RaftMeta>,
    entry: &'a LogEntry,
) -> ExecResult {
    with_bindings!(IS_LEADER: is_leader(meta) => {
        meta.state_machine.write().await.commit_cmd(&entry).await
    })
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
        let mut storage_entity = StorageEntity::new_with_options(&opts).unwrap();

        let mut term = 0;
        let mut logs = BTreeMap::new();
        let mut commit_index = 0;
        let mut last_applied = 0;
        let mut master_sm = MasterStateMachine::new(opts.service_id);

        if let &mut Some(ref mut storage) = &mut storage_entity {
            let mut snapshot_data = vec![];
            let mut log_data = vec![];
            storage.snapshot.read_to_end(&mut snapshot_data).unwrap();
            storage.logs.read_to_end(&mut log_data).unwrap();
            let snapshot: SnapshotEntity = bincode::deserialize(snapshot_data.as_slice()).unwrap();
            logs = bincode::deserialize(log_data.as_slice()).unwrap();
            term = snapshot.term;
            commit_index = snapshot.commit_index;
            last_applied = snapshot.last_applied;
            master_sm.recover(snapshot.snapshot);
        }

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
                storage: storage_entity.map(|e| Arc::new(RwLock::new(e))),
            }),
            id: server_id,
            options: opts,
        };
        Arc::new(server_obj)
    }
    pub async fn start(server: &Arc<RaftService>) -> bool {
        let server_address = server.options.address.clone();
        info!("Waiting for server to be initialized");
        {
            let meta = server.meta.write().await;
            let mut sm = meta.state_machine.write().await;
            sm.configs.new_member(server_address.clone());
        }
        let checker_ref = server.clone();
        tokio::spawn(async {
            let server = checker_ref;
            loop {
                let start_time = get_time();
                let expected_ends = start_time + CHECKER_MS;
                {
                    let mut meta = server.meta.write().await; //WARNING: Reentering not supported
                    let action = match meta.membership {
                        Membership::Leader(_) => CheckerAction::SendHeartbeat,
                        Membership::Follower | Membership::Candidate => {
                            let current_time = get_time();
                            let timeout_time = meta.timeout + meta.last_checked;
                            let timeout_elapsed = current_time - timeout_time;
                            if meta.vote_for == None && timeout_elapsed > 0 {
                                // TODO: in my test sometimes timeout_elapsed may go 1 for no reason, require investigation
                                //Timeout, require election
                                //debug!("TIMEOUT!!! GOING TO CANDIDATE!!! {}, {}", server_id, timeout_elapsed);
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
                            server.send_followers_heartbeat(&mut meta, None).await;
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
                    time::delay_for(Duration::from_millis(time_to_sleep as u64)).await;
                }
            }
        });
        {
            let mut meta = server.meta.write().await;
            meta.last_checked = get_time();
        }
        return true;
    }
    pub async fn new_server(opts: Options) -> (bool, Arc<RaftService>, Arc<Server>) {
        let address = opts.address.clone();
        let svr_id = opts.service_id;
        let service = RaftService::new(opts);
        let server = Server::new(&address);
        Server::listen_and_resume(&server);
        server.register_service(svr_id, &service);
        (RaftService::start(&service).await, service, server)
    }
    pub async fn bootstrap(&self) {
        let mut meta = self.write_meta().await;
        let (last_log_id, _) = {
            let logs = meta.logs.read().await;
            get_last_log_info!(self, logs)
        };
        self.become_leader(&mut meta, last_log_id).await;
    }
    pub async fn join(&self, servers: &Vec<String>) -> Result<bool, ExecError> {
        debug!("Trying to join cluster with id {}", self.id);
        let client = RaftClient::new(servers, self.options.service_id).await;
        if let Ok(client) = client {
            let result = client
                .execute(CONFIG_SM_ID, new_member_::new(&self.options.address))
                .await;
            let members = client.execute(CONFIG_SM_ID, member_address::new()).await;
            let mut meta = self.write_meta().await;
            if let Ok(members) = members {
                for member in members {
                    meta.state_machine.write().await.configs.new_member(member);
                }
            }
            self.reset_last_checked(&mut meta);
            self.become_follower(&mut meta, 0, client.leader_id());
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
            if !self.send_followers_heartbeat(&mut meta, None).await {
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
        //        let t = get_time();
        let lock_mon = self.meta.write();
        //        let acq_time = get_time() - t;
        //        println!("Meta write locked acquired for {}ms for {}, leader {}", acq_time, self.id, lock_mon.leader_id);
        lock_mon.await
    }
    pub async fn read_meta(&self) -> RwLockReadGuard<'_, RaftMeta> {
        self.meta.read().await
    }
    async fn become_candidate<'a>(&'a self, meta: &'a mut RwLockWriteGuard<'_, RaftMeta>) {
        self.reset_last_checked(meta);
        let term = meta.term;
        alter_term(meta, term + 1);
        meta.vote_for = Some(self.id);
        self.switch_membership(meta, Membership::Candidate);
        let term = meta.term;
        let server_id = self.id;
        let (last_log_id, last_log_term) = {
            let logs = meta.logs.read().await;
            get_last_log_info!(self, logs)
        };
        let (mut members_vote_response_stream, num_members) = {
            let members: Vec<_> = {
                let member_sm = meta.state_machine.read().await;
                let ref members = member_sm.configs.members;
                members.values().map(|member| (member.rpc.clone(), member.id)).collect()
            };
            let len = members.len();
            let futs: FuturesUnordered<_> = members
                .into_iter()
                .map(|(rpc, member_id)| {
                    let vote_fut = async move {
                        if member_id == server_id {
                            RequestVoteResponse::Granted
                        } else {
                            if let Ok(((remote_term, remote_leader_id), vote_granted)) = rpc
                                .request_vote(term, server_id, last_log_id, last_log_term)
                                .await
                            {
                                if vote_granted {
                                    RequestVoteResponse::Granted
                                } else if remote_term > term {
                                    RequestVoteResponse::TermOut(remote_term, remote_leader_id)
                                } else {
                                    RequestVoteResponse::NotGranted
                                }
                            } else {
                                RequestVoteResponse::NotGranted // default for request failure
                            }
                        }
                    };
                    time::timeout(Duration::from_millis(2000), tokio::spawn(vote_fut))
                })
                .collect();
            (futs, len)
        };
        let mut granted = 0;
        while let Some(vote_response) = members_vote_response_stream.next().await {
            if let Ok(Ok(res)) = vote_response {
                if meta.term != term {
                    break;
                }
                match res {
                    RequestVoteResponse::TermOut(remote_term, remote_leader_id) => {
                        self.become_follower(meta, remote_term, remote_leader_id);
                        break;
                    }
                    RequestVoteResponse::Granted => {
                        granted += 1;
                        if is_majority(num_members as u64, granted) {
                            self.become_leader(meta, last_log_id).await;
                            break;
                        }
                    }
                    _ => {}
                }
            }
        }
        debug!("GRANTED: {}/{}", granted, num_members);
        return;
    }

    fn become_follower(&self, meta: &mut RwLockWriteGuard<RaftMeta>, term: u64, leader_id: u64) {
        alter_term(meta, term);
        meta.leader_id = leader_id;
        self.switch_membership(meta, Membership::Follower);
    }

    async fn become_leader(&self, meta: &mut RwLockWriteGuard<'_, RaftMeta>, last_log_id: u64) {
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
    ) -> bool {
        if let Membership::Leader(ref leader_meta) = meta.membership {
            let leader_id = meta.leader_id;
            debug_assert_eq!(self.id, leader_id);
            let leader_meta = leader_meta.read().await;
            let mut heartbeat_futs = FuturesUnordered::new();
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
                let heartbeat_fut = Box::pin(self.send_follower_heartbeat(meta, follower, member.rpc.clone(), member_id));
                let task_spawned = heartbeat_fut; // tokio::spawn(heartbeat_fut);
                let timeout = 2000;
                let task_with_timeout = time::timeout(Duration::from_millis(timeout), task_spawned);
                heartbeat_futs.push(task_with_timeout);
            }
            let members = heartbeat_futs.len();
            if let (Some(log_id), &Membership::Leader(ref leader_meta)) = (log_id, &meta.membership) {
                let mut leader_meta = leader_meta.write().await;
                let mut updated_followers = 0;
                while let Some(heartbeat_res) = heartbeat_futs.next().await {
                    if let Ok(last_matched_id) = heartbeat_res {
                        // adaptive
                        //println!("{}, {}", last_matched_id, log_id);
                        if last_matched_id >= log_id {
                            updated_followers += 1;
                            if is_majority(members as u64, updated_followers) {
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
        &self,
        meta: &RwLockWriteGuard<'_, RaftMeta>,
        follower: &Mutex<FollowerStatus>,
        rpc: Arc<AsyncServiceClient>,
        member_id: u64
    ) -> u64 {
        let commit_index = meta.commit_index;
        let term = meta.term;
        let leader_id = meta.leader_id;

        let meta_term = meta.term;
        let meta_last_applied = meta.last_applied;
        let master_sm = &meta.state_machine;
        let logs = &meta.logs;

        let mut follower = follower.lock().await;
        let logs = logs.read().await;
        let mut is_retry = false;
        loop {
            let entries: Option<LogEntries> = { // extract logs to send to follower
                let list: LogEntries = logs.range(
                    (Included(&follower.next_index), Unbounded)
                ).map(|(_, entry)| entry.clone()).collect(); //TODO: avoid clone entry
                if list.is_empty() {None} else {Some(list)}
            };
            if is_retry && entries.is_none() { // break when retry and there is no entry
                debug!("stop retry when entry is empty, {}", follower.next_index);
                break;
            }
            let last_entries_id = match &entries { // get last entry id
                &Some(ref entries) => {
                    Some(entries.iter().last().unwrap().id)
                },
                &None => None
            };
            let (follower_last_log_id, follower_last_log_term) = { // extract follower last log info
                // assumed log ids are sequence of integers
                let follower_last_log_id = follower.next_index - 1;
                if follower_last_log_id == 0 || logs.is_empty() {
                    (0, 0) // 0 represents there is no logs in the leader
                } else {
                    // detect cleaned logs
                    let (first_log_id, _) = logs.iter().next().unwrap();
                    if *first_log_id > follower_last_log_id {
                        debug!("Taking snapshot of all state machines and install them on follower {}", member_id);
                        let master_sm = master_sm.read().await;
                        let snapshot = master_sm.snapshot().unwrap();
                        rpc.install_snapshot(meta_term, leader_id, meta_last_applied, meta_term, snapshot).await.unwrap();
                    }
                    let follower_last_entry = logs.get(&follower_last_log_id);
                    match follower_last_entry {
                        Some(entry) => {
                            (entry.id, entry.term)
                        },
                        None => {
                            panic!("Cannot find old logs for follower, first_id: {}, follower_last: {}");
                        }
                    }
                }
            };
            let append_result = rpc.append_entries(
                term,
                leader_id,
                follower_last_log_id,
                follower_last_log_term,
                entries,
                commit_index
            ).await;
            match append_result {
                Ok((follower_term, result)) => {
                    match result {
                        AppendEntriesResult::Ok => {
                            debug!("log updated");
                            if let Some(last_entries_id) = last_entries_id {
                                follower.next_index = last_entries_id + 1;
                                follower.match_index = last_entries_id;
                            }
                        },
                        AppendEntriesResult::LogMismatch => {
                            debug!("log mismatch, {}", follower.next_index);
                            follower.next_index -= 1;
                        },
                        AppendEntriesResult::TermOut(actual_leader_id) => {
                            break;
                        }
                    }
                },
                _ => {break;} // retry will happened in next heartbeat
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
        //println!("elapsed: {}, id: {}, term: {}", get_time() - meta.last_checked, self.id, meta.term);
        meta.last_checked = get_time();
        meta.timeout = gen_timeout();
    }

    async fn leader_append_log<'a>(
        &'a self,
        meta: &'a RwLockWriteGuard<'a, RaftMeta>,
        entry: &mut LogEntry,
    ) -> (u64, u64) {
        let mut logs = meta.logs.write().await;
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
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
    ) {
        let (last_log_id, _) = get_last_log_info!(self, logs);
        let expecting_oldest_log = if last_log_id > MAX_LOG_CAPACITY as u64 {
            last_log_id - MAX_LOG_CAPACITY as u64
        } else {
            0
        };
        let double_cap = MAX_LOG_CAPACITY << 1;
        if logs.len() > double_cap && meta.last_applied > expecting_oldest_log {
            debug!("trim logs");
            while logs.len() > MAX_LOG_CAPACITY {
                let first_key = *logs.iter().next().unwrap().0;
                logs.remove(&first_key).unwrap();
            }
            if let Some(ref storage) = meta.storage {
                let mut storage = storage.write().await;
                let snapshot = SnapshotEntity {
                    term: meta.term,
                    commit_index: meta.commit_index,
                    last_applied: meta.last_applied,
                    snapshot: meta.state_machine.read().await.snapshot().unwrap(),
                };
                storage
                    .snapshot
                    .write_all(bincode::serialize(&snapshot).unwrap().as_slice());
                storage.snapshot.sync_all().unwrap();
            }
        }
        if let Some(ref storage) = meta.storage {
            let mut storage = storage.write().await;
            let logs_data = bincode::serialize(&*meta.logs.read().await).unwrap();
            // TODO: async file system calls
            storage.logs.write_all(logs_data.as_slice());
            storage.logs.sync_all().unwrap();
        }
    }

    async fn try_sync_log_to_followers<'a>(
        &'a self,
        mut meta: RwLockWriteGuard<'a, RaftMeta>,
        entry: &LogEntry,
        new_log_id: u64,
    ) -> Option<ExecResult> {
        if self.send_followers_heartbeat(&mut meta, Some(new_log_id)).await {
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
        meta.commit_index = new_log_id;
        let data = commit_command(&meta, &entry).await;
        let t = get_time();
        if let Membership::Leader(ref leader_meta) = meta.membership {
            let mut leader_meta = leader_meta.write().await;
            let member_sm = meta.state_machine.read().await;
            let ref members = member_sm.configs.members;
            self.reload_leader_meta(members, &mut leader_meta, new_log_id);
        }
        self.send_followers_heartbeat(&mut meta, Some(new_log_id)).await;
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
                    check_commit(&mut meta);
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
                    let mut leader_commit = leader_commit;
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
                    self.logs_post_processing(&meta, logs);
                }
                if leader_commit > meta.commit_index {
                    //RI, 5
                    meta.commit_index = min(leader_commit, last_new_entry);
                    check_commit(&mut meta);
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
                check_commit(&mut meta);
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
                        "{} VOTE FOR: {}, not granted due to voted for {}",
                        self.id,
                        candidate_id,
                        vote_for.unwrap()
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
                check_commit(&mut meta);
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
            let mut meta = self.write_meta().await;
            let mut entry = entry;
            if !is_leader(&meta) {
                return ClientCmdResponse::NotLeader(meta.leader_id);
            }
            let (new_log_id, new_log_term) = self.leader_append_log(&meta, &mut entry).await;
            let mut data = match entry.sm_id {
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
            let mut meta = self.meta.read().await;
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

impl StorageEntity {
    pub fn new_with_options(opts: &Options) -> io::Result<Option<Self>> {
        Ok(match &opts.storage {
            &Storage::DISK(ref dir) => {
                let base_path = Path::new(dir);
                fs::create_dir_all(base_path);
                let log_path = base_path.with_file_name("log.dat");
                let snapshot_path = base_path.with_file_name("snapshot.dat");
                let mut open_opts = OpenOptions::new();
                open_opts
                    .write(true)
                    .create(true)
                    .read(true)
                    .truncate(false);
                Some(Self {
                    logs: open_opts.open(log_path.as_path())?,
                    snapshot: open_opts.open(snapshot_path.as_path())?,
                })
            }
            _ => None,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::raft::{RaftService, Storage, DEFAULT_SERVICE_ID, Options};
    use crate::raft::state_machine::master::ExecError;
    use crate::rpc::Server;
    use crate::utils::time::async_wait_5_secs;

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
        let s1_addr = String::from("127.0.0.1:2001");
        let s2_addr = String::from("127.0.0.1:2002");
        let s3_addr = String::from("127.0.0.1:2003");
        let service1 = RaftService::new(Options {
            storage: Storage::default(),
            address: s1_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server1 = Server::new(&s1_addr);
        server1.register_service(DEFAULT_SERVICE_ID, &service1);
        Server::listen_and_resume(&server1);
        assert!(RaftService::start(&service1).await);
        service1.bootstrap().await;
        assert_eq!(service1.num_members().await, 1);
        let service2 = RaftService::new(Options {
            storage: Storage::default(),
            address: s2_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server2 = Server::new(&s2_addr);
        server2.register_service(DEFAULT_SERVICE_ID, &service2);
        Server::listen_and_resume(&server2);
        assert!(RaftService::start(&service2).await);
        let join_result = service2.join(&vec![s1_addr.clone()]).await;
        match join_result {
            Err(ExecError::ServersUnreachable) => panic!("Server unreachable"),
            Err(ExecError::CannotConstructClient) => panic!("Cannot Construct Client"),
            Err(e) => panic!(e),
            Ok(_) => {}
        }
        assert!(join_result.is_ok());
        assert_eq!(service1.num_members().await, 2);
        assert_eq!(service2.num_members().await, 2);
        let service3 = RaftService::new(Options {
            storage: Storage::default(),
            address: s3_addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server3 = Server::new(&s3_addr);
        server3.register_service(DEFAULT_SERVICE_ID, &service3);
        Server::listen_and_resume(&server3);
        assert!(RaftService::start(&service3).await);
        let join_result = service3.join(&vec![s1_addr.clone(), s2_addr.clone()]).await;
        join_result.unwrap();
        assert_eq!(service1.num_members().await, 3);
        assert_eq!(service3.num_members().await, 3);

        async_wait_5_secs().await;

        // check in service2. Although it is a log replication problem but membership changes should take effect immediately
        assert_eq!(service2.num_members().await, 3);

        // test remove member
        assert!(service2.leave().await);
        assert_eq!(service1.num_members().await, 2);
        assert_eq!(service3.num_members().await, 2);

        //test remove leader
        assert_eq!(service1.leader_id().await, service1.id);
        assert!(service1.leave().await);

        async_wait_5_secs().await; // there will be some unavailability in leader transaction
        
        assert_eq!(service3.leader_id().await, service3.id);
        assert_eq!(service3.num_members().await, 1);
    }

    #[tokio::test(threaded_scheduler)]
    async fn log_replication() {
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

        let server1 = Server::new(&s1_addr);
        server1.register_service(DEFAULT_SERVICE_ID, &service1);
        Server::listen_and_resume(&server1);
        assert!(RaftService::start(&service1).await);
        service1.bootstrap().await;

        let server2 = Server::new(&s2_addr);
        server2.register_service(DEFAULT_SERVICE_ID, &service2);
        Server::listen_and_resume(&server2);
        assert!(RaftService::start(&service2).await);
        let join_result = service2.join(&vec![s1_addr.clone(), s2_addr.clone()]).await;
        join_result.unwrap();

        let server3 = Server::new(&s3_addr);
        server3.register_service(DEFAULT_SERVICE_ID, &service3);
        Server::listen_and_resume(&server3);
        assert!(RaftService::start(&service3).await);
        let join_result = service3.join(&vec![s1_addr.clone(), s2_addr.clone()]).await;
        join_result.unwrap();

        let server4 = Server::new(&s4_addr);
        server4.register_service(DEFAULT_SERVICE_ID, &service4);
        Server::listen_and_resume(&server4);
        assert!(RaftService::start(&service4).await);
        let join_result = service4
            .join(&vec![s1_addr.clone(), s2_addr.clone(), s3_addr.clone()])
            .await;
        join_result.unwrap();

        let server5 = Server::new(&s5_addr);
        server5.register_service(DEFAULT_SERVICE_ID, &service5);
        Server::listen_and_resume(&server5);
        assert!(RaftService::start(&service5).await);
        let join_result = service5
            .join(&vec![
                s1_addr.clone(),
                s2_addr.clone(),
                s3_addr.clone(),
                s4_addr.clone(),
            ])
            .await;
        join_result.unwrap();

        async_wait_5_secs().await; // wait for membership replication to take effect

        assert_eq!(service1.num_logs().await, service2.num_logs().await);
        assert_eq!(service2.num_logs().await, service3.num_logs().await);
        assert_eq!(service3.num_logs().await, service4.num_logs().await);
        assert_eq!(service4.num_logs().await, service5.num_logs().await);
        assert_eq!(service5.num_logs().await, 4); // check all logs replicated

        async_wait_5_secs().await;

        assert_eq!(service1.leader_id().await, service1.id);
        assert_eq!(service2.leader_id().await, service1.id);
        assert_eq!(service3.leader_id().await, service1.id);
        assert_eq!(service4.leader_id().await, service1.id);
        assert_eq!(service5.leader_id().await, service1.id);
    }
}