use rand;
use rand::distributions::{IndependentSample, Range};
use std::thread;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use utils::future_parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, HashMap};
use std::collections::Bound::{Included, Unbounded};
use std::cmp::{min, max};
use std::sync::mpsc::channel;
use std::rc::Rc;
use std::ops::Deref;
use self::state_machine::OpType;
use self::state_machine::master::{
    MasterStateMachine, ExecResult,
    ExecError, SubStateMachine};
use self::state_machine::configs::{CONFIG_SM_ID, RaftMember};
use self::state_machine::configs::commands::{new_member_, del_member_, member_address};
use self::client::RaftClient;
use bifrost_hasher::hash_str;
use utils::time::get_time;
use threadpool::ThreadPool;
use num_cpus;

use futures::prelude::*;

#[macro_use]
pub mod state_machine;
pub mod client;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_DEFAULT_SERVICE) as u64;

def_bindings! {
    bind val IS_LEADER: bool = false;
}

pub trait RaftMsg<R>: Send + Sync {
    fn encode(&self) -> (u64, OpType, &Vec<u8>);
    fn decode_return(&self, data: &Vec<u8>) -> R;
}

const CHECKER_MS: i64 = 10;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub id: u64,
    pub term: u64,
    pub sm_id: u64,
    pub fn_id: u64,
    pub data: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientCmdResponse {
    Success{
        data: ExecResult,
        last_log_term: u64,
        last_log_id: u64,
    },
    NotLeader(u64),
    NotCommitted,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientQryResponse {
    Success{
        data: ExecResult,
        last_log_term: u64,
        last_log_id: u64,
    },
    LeftBehind
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
    LogMismatch
}

type LogEntries = Vec<LogEntry>;
type LogsMap = BTreeMap<u64, LogEntry>;

service! {
    rpc append_entries(term: u64, leaderId: u64, prev_log_id: u64, prev_log_term: u64, entries: Option<LogEntries>, leader_commit: u64) -> (u64, AppendEntriesResult);
    rpc request_vote(term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> ((u64, u64), bool); // term, voteGranted
    rpc install_snapshot(term: u64, leader_id: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>, done: bool) -> u64;
    rpc c_command(entry: LogEntry) -> ClientCmdResponse;
    rpc c_query(entry: LogEntry) -> ClientQryResponse;
    rpc c_server_cluster_info() -> ClientClusterInfo;
    rpc c_put_offline() -> bool;
}

fn gen_rand(lower: i64, higher: i64) -> i64 {
    let between = Range::new(lower, higher);
    let mut rng = rand::thread_rng();
    between.ind_sample(&mut rng) + 1
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
    // this mutex is used to achieve parallel send heartbeat to followers
    followers: HashMap<u64, Arc<Mutex<FollowerStatus>>>
}

impl LeaderMeta {
    fn new() -> LeaderMeta {
        LeaderMeta{
            last_updated: get_time(),
            followers: HashMap::new(),
        }
    }
}

pub enum Membership {
    // this lock is to prevent conflicts from
    // new command logs and periodic heartbeats
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
    // this lock is for sending heartbeats in parallel
    logs: Arc<RwLock<LogsMap>>,
    // state machine map will not operate with raft,
    // it should have it's own lock
    state_machine: RwLock<MasterStateMachine>,
    commit_index: u64,
    last_applied: u64,
    leader_id: u64
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

pub struct RaftServiceInner {
    meta: RwLock<RaftMeta>,
    pub id: u64,
    pub options: Options,
}

pub struct RaftService {
    inner: Arc<RaftServiceInner>
}

dispatch_rpc_service_functions!(RaftService);

#[derive(Debug)]
enum CheckerAction {
    SendHeartbeat,
    BecomeCandidate,
    ExitLoop,
    None
}

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

macro_rules! members_from_meta {
    ($meta: expr) => {
        $meta.state_machine.read().configs.members
    };
}

fn check_commit(meta: &mut RwLockWriteGuard<RaftMeta>) {
    while meta.commit_index > meta.last_applied {
        meta.last_applied += 1;
        let last_applied = meta.last_applied;
        let logs = meta.logs.read();
        if let Some(entry) = logs.get(&last_applied) {
            commit_command(meta, &entry);
        };
    }
}

fn is_majority (members: u64, granted: u64) -> bool {
    granted >= members / 2
}

fn commit_command(meta: &RwLockWriteGuard<RaftMeta>, entry: &LogEntry) -> ExecResult {
    with_bindings!(IS_LEADER: is_leader(meta) => {
        meta.state_machine.write().commit_cmd(&entry)
    })
}

fn is_leader(meta: &RwLockWriteGuard<RaftMeta>) -> bool {
    match meta.membership {
        Membership::Leader(_) => {true},
        _ => {false}
    }
}
fn alter_term(meta: &mut RwLockWriteGuard<RaftMeta>, term: u64) {
    if meta.term != term {
        meta.term = term;
        meta.vote_for = None;
    }
}


impl RaftServiceInner {
    pub fn new(opts: Options) -> Arc<RaftServiceInner> {
        let server_address = opts.address.clone();
        let server_id = hash_str(&server_address);
        let server_obj = RaftServiceInner {
            meta: RwLock::new(
                RaftMeta {
                    term: 0, //TODO: read from persistent state
                    vote_for: None, //TODO: read from persistent state
                    timeout: gen_timeout(),
                    last_checked: get_time(),
                    membership: Membership::Undefined,
                    logs: Arc::new(RwLock::new(BTreeMap::new())), //TODO: read from persistent state
                    state_machine: RwLock::new(MasterStateMachine::new(opts.service_id)),
                    commit_index: 0,
                    last_applied: 0,
                    leader_id: 0
                }
            ),
            id: server_id,
            options: opts,
        };
        Arc::new(server_obj)
    }
    pub fn start(server: &Arc<RaftServiceInner>) -> bool {
        let server_address = server.options.address.clone();
        info!("Waiting for server to be initialized");
        {
            let start_time = get_time();
            let meta = server.meta.write();
            let mut sm = meta.state_machine.write();
            let mut inited = false;
            while get_time() < start_time + 5000 { //waiting for 5 secs
                if let Ok(_) = sm.configs.new_member(server_address.clone()) {
                    inited = true;
                    break;
                }
            }
            if !inited {
                return false;
            }
        }
        let checker_ref = server.clone();
        thread::spawn(move ||{
            let server = checker_ref;
            loop {
                let start_time = get_time();
                let expected_ends = start_time + CHECKER_MS;
                {
                    let mut meta = server.meta.write(); //WARNING: Reentering not supported
                    let action = match meta.membership {
                        Membership::Leader(_) => {
                            CheckerAction::SendHeartbeat
                        },
                        Membership::Follower | Membership::Candidate => {
                            let current_time = get_time();
                            let timeout_time = meta.timeout + meta.last_checked;
                            let timeout_elapsed = current_time - timeout_time;
                            if  meta.vote_for == None && timeout_elapsed > 0 { // TODO: in my test sometimes timeout_elapsed may go 1 for no reason, require investigation
                                //Timeout, require election
                                //debug!("TIMEOUT!!! GOING TO CANDIDATE!!! {}, {}", server_id, timeout_elapsed);
                                CheckerAction::BecomeCandidate
                            } else {
                                CheckerAction::None
                            }
                        },
                        Membership::Offline => {
                            CheckerAction::ExitLoop
                        },
                        Membership::Undefined => CheckerAction::None
                    };
                    match action {
                        CheckerAction::SendHeartbeat => {
                            server.send_followers_heartbeat(&mut meta, None);
                        },
                        CheckerAction::BecomeCandidate => {
                            Self::become_candidate(server.clone(), &mut meta);
                        },
                        CheckerAction::ExitLoop => {
                            break;
                        },
                        CheckerAction::None => {}
                    }
                }
                let end_time = get_time();
                let time_to_sleep = expected_ends - end_time - 1;
                if time_to_sleep > 0 {
                    thread::sleep(Duration::from_millis(time_to_sleep as u64));
                }
            }
        });
        {
            let mut meta = server.meta.write();
            meta.last_checked = get_time();
        }
        return true;
    }

    pub fn new_server(opts: Options) -> (bool, Arc<RaftService>, Arc<Server>) {
        let address = opts.address.clone();
        let svr_id = opts.service_id;
        let service = RaftService::new(opts);
        let server = Server::new(&address);
        Server::listen_and_resume(&server);
        server.register_service(, svr_id, &service);
        (service.start(), service, server)
    }
    pub fn bootstrap(&self) {
        let mut meta = self.write_meta();
        let (last_log_id, _) = {
            let logs = meta.logs.read();
            get_last_log_info!(self, logs)
        };
        self.become_leader(&mut meta, last_log_id);
    }
    pub fn join(&self, servers: &Vec<String>)
                -> Result<Result<(), ()>, ExecError> {
        debug!("Trying to join cluster with id {}", self.id);
        let client = RaftClient::new(servers, self.options.service_id);
        if let Ok(client) = client {
            let result = client.execute(
                CONFIG_SM_ID,
                &new_member_::new(&self.options.address)
            );
            let members = client.execute(
                CONFIG_SM_ID,
                &member_address::new()
            );
            let mut meta = self.write_meta();
            if let Ok(Ok(members)) = members {
                for member in members {
                    meta.state_machine.write().configs.new_member(member);
                }
            }
            self.reset_last_checked(&mut meta);
            self.become_follower(&mut meta, 0, client.leader_id());
            result
        } else {
            Err(ExecError::CannotConstructClient)
        }
    }
    pub fn leave(&self) -> bool {
        let servers = self.cluster_info().members.iter()
            .map(|&(_, ref address)|{
                address.clone()
            }).collect();
        if let Ok(client) = RaftClient::new(&servers, self.options.service_id) {
            client.execute(
                CONFIG_SM_ID,
                &del_member_::new(&self.options.address)
            );
        } else {
            return false
        }
        let mut meta = self.write_meta();
        if is_leader(&meta) {
            if !self.send_followers_heartbeat(&mut meta, None) {
                return false;
            }
        }
        meta.membership = Membership::Offline;
        let mut sm = meta.state_machine.write();
        sm.clear_subs();
        return true;
    }
    pub fn cluster_info(&self) -> ClientClusterInfo {
        let meta = self.meta.read();
        let logs = meta.logs.read();
        let sm = &meta.state_machine.read();
        let sm_members = sm.members();
        let mut members = Vec::new();
        for (id, member) in sm_members.iter(){
            members.push((*id, member.address.clone()))
        }
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
        ClientClusterInfo{
            members,
            last_log_id,
            last_log_term,
            leader_id: meta.leader_id,
        }
    }
    pub fn num_members(&self) -> usize {
        let meta = self.meta.read();
        let ref members = members_from_meta!(meta);
        members.len()
    }
    pub fn num_logs(&self) -> usize {
        let meta = self.meta.read();
        let logs = meta.logs.read();
        logs.len()
    }
    pub fn last_log_id(&self) -> Option<u64> {
        let meta = self.meta.read();
        let logs = meta.logs.read();
        logs.keys().cloned().last()
    }
    pub fn leader_id(&self) -> u64 {
        let meta = self.meta.read();
        meta.leader_id
    }
    pub fn is_leader(&self) -> bool {
        let meta = self.meta.read();
        match meta.membership {
            Membership::Leader(_) => {true},
            _ => {false}
        }
    }
    pub fn register_state_machine(&self, state_machine: SubStateMachine) {
        let meta = self.meta.read();
        let mut master_sm = meta.state_machine.write();
        master_sm.register(state_machine);
    }
    fn switch_membership(&self, meta: &mut RwLockWriteGuard<RaftMeta>, membership: Membership) {
        self.reset_last_checked(meta);
        meta.membership = membership;
    }
    fn get_log_info_(&self, log: Option<(&u64, &LogEntry)>) -> (u64, u64) {
        match log {
            Some((last_log_id, last_log_item)) => {
                (*last_log_id, last_log_item.term)
            },
            None => (0, 0)
        }
    }
    fn insert_leader_follower_meta(
        &self,
        leader_meta: &mut RwLockWriteGuard<LeaderMeta>,
        last_log_id: u64,
        member_id: u64
    ){
        // the leader itself will not be consider as a follower when sending heartbeat
        if member_id == self.id {return;}
        leader_meta.followers.entry(member_id).or_insert_with(|| {
            Arc::new(Mutex::new(FollowerStatus {
                next_index: last_log_id + 1,
                match_index: 0
            }))
        });
    }
    fn reload_leader_meta(
        &self,
        member_map: &HashMap<u64, RaftMember>,
        leader_meta: &mut RwLockWriteGuard<LeaderMeta>,
        last_log_id: u64
    ) {
        for member in member_map.values() {
            self.insert_leader_follower_meta(leader_meta, last_log_id, member.id);
        }
    }
    fn write_meta(&self) -> RwLockWriteGuard<RaftMeta> {
        //        let t = get_time();
        let lock_mon = self.meta.write();
        //        let acq_time = get_time() - t;
        //        println!("Meta write locked acquired for {}ms for {}, leader {}", acq_time, self.id, lock_mon.leader_id);
        lock_mon
    }
    pub fn read_meta(&self) -> RwLockReadGuard<RaftMeta> {
        self.meta.read()
    }
    fn become_candidate(server: Arc<RaftServiceInner>, meta: &mut RwLockWriteGuard<RaftMeta>) {
        server.reset_last_checked(meta);
        let term = meta.term;
        alter_term(meta, term + 1);
        meta.vote_for = Some(server.id);
        server.switch_membership(meta, Membership::Candidate);
        let term = meta.term;
        let id = server.id;
        let logs = meta.logs.read();
        let (last_log_id, last_log_term) = get_last_log_info!(server, logs);
        let (tx, rx) = channel();
        let mut members = 0;
        for member in members_from_meta!(meta).values() {
            let rpc = member.rpc.clone();
            let tx = tx.clone();
            members += 1;
            if member.id == server.id {
                tx.send(RequestVoteResponse::Granted);
            } else {
                meta.workers.lock().execute(move||{
                    if let Ok(Ok(((remote_term, remote_leader_id), vote_granted))) = rpc.request_vote(&term, &id, &last_log_id, &last_log_term).wait() {
                        if vote_granted {
                            tx.send(RequestVoteResponse::Granted);
                        } else if remote_term > term {
                            tx.send(RequestVoteResponse::TermOut(remote_term, remote_leader_id));
                        } else {
                            tx.send(RequestVoteResponse::NotGranted);
                        }
                    }
                });
            }
        }
        meta.workers.lock().execute(move ||{
            let mut granted = 0;
            let mut timeout = 2000;
            for _ in 0..members {
                if timeout <= 0 {break;}
                if let Ok(res)= rx.recv_timeout(Duration::from_millis(timeout as u64)) {
                    let mut meta = server.meta.write();
                    if meta.term != term {break;}
                    match res {
                        RequestVoteResponse::TermOut(remote_term, remote_leader_id) => {
                            server.become_follower(&mut meta, remote_term, remote_leader_id);
                            break;
                        },
                        RequestVoteResponse::Granted => {
                            granted += 1;
                            if is_majority(members, granted) {
                                server.become_leader(&mut meta, last_log_id);
                                break;
                            }
                        },
                        _ => {}
                    }
                }
                let curr_time = get_time();
                timeout -= get_time() - curr_time;
            }
            debug!("GRANTED: {}/{}", granted, members);
        });
    }

    fn become_follower(&self, meta: &mut RwLockWriteGuard<RaftMeta>, term: u64, leader_id: u64) {
        alter_term(meta, term);
        meta.leader_id = leader_id;
        self.switch_membership(meta, Membership::Follower);
    }

    fn become_leader(&self, meta: &mut RwLockWriteGuard<RaftMeta>, last_log_id: u64) {
        let leader_meta = RwLock::new(LeaderMeta::new());
        {
            let mut guard = leader_meta.write();
            self.reload_leader_meta(&members_from_meta!(meta), &mut guard, last_log_id);
            guard.last_updated = get_time();
        }
        meta.leader_id = self.id;
        self.switch_membership(meta, Membership::Leader(leader_meta));
    }

    fn send_followers_heartbeat(&self, meta: &mut RwLockWriteGuard<RaftMeta>, log_id: Option<u64>) -> bool {
        let (tx, rx) = channel();
        let mut members = 0;
        let commit_index = meta.commit_index;
        let term = meta.term;
        let leader_id = meta.leader_id;
        {
            let workers = meta.workers.lock();
            if let Membership::Leader(ref leader_meta) = meta.membership {
                let leader_meta = leader_meta.read();
                for member in members_from_meta!(meta).values() {
                    let id = member.id;
                    if id == self.id {continue;}
                    let tx = tx.clone();
                    let logs = meta.logs.clone();
                    let rpc = member.rpc.clone();
                    let follower = {
                        if let Some(follower) = leader_meta.followers.get(&id) {
                            follower.clone()
                        } else {
                            debug!(
                                "follower not found, {}, {}",
                                id,
                                leader_meta.followers.len()
                            ); //TODO: remove after debug
                            continue;
                        }
                    };
                    workers.execute(move||{
                        let mut follower = follower.lock();
                        let mut is_retry = false;
                        let logs = logs.read();
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
                                        panic!("TODO: deal with snapshot or other situations may remove old logs {}, {}", *first_log_id, follower_last_log_id)
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
                                &term,
                                &leader_id,
                                &follower_last_log_id,
                                &follower_last_log_term,
                                &entries,
                                &commit_index
                            ).wait();
                            match append_result {
                                Ok(Ok((follower_term, result))) => {
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
                                            //                                            let actual_leader = actual_leader_id.clone();
                                            //                                            println!(
                                            //                                                "term out, new term from follower is {} but this leader is {}",
                                            //                                                follower_term, term
                                            //                                            );
                                            break;
                                        }
                                    }
                                },
                                _ => {break;} // retry will happened in next heartbeat
                            }
                            is_retry = true;
                        } // append entries to followers
                        tx.send(follower.match_index);
                    });
                    members += 1;
                }
            }
        }
        match log_id {
            Some(log_id) => {
                if let Membership::Leader(ref leader_meta) = meta.membership{
                    let mut leader_meta = leader_meta.write();
                    let mut updated_followers = 0;
                    let mut timeout = 2000 as i64; // assume client timeout is more than 2s　(5 by default)
                    for _ in 0..members {
                        if timeout <= 0 {break;}
                        if let Ok(last_matched_id) = rx.recv_timeout(Duration::from_millis(timeout as u64)) { // adaptive
                            //println!("{}, {}", last_matched_id, log_id);
                            if last_matched_id >= log_id {
                                updated_followers += 1;
                                if is_majority(members, updated_followers) {break;}
                            }
                        }
                        let current_time = get_time();
                        timeout -= get_time() - current_time;
                    }
                    leader_meta.last_updated = get_time();
                    is_majority(members, updated_followers)
                } else {false}
            },
            None => true
        }
    }
    //check term number, return reject = false if server term is stale
    fn check_term(&self, meta: &mut RwLockWriteGuard<RaftMeta>, remote_term: u64, leader_id: u64) -> bool {
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
    fn append_log(&self, meta: &RwLockWriteGuard<RaftMeta>, entry: &mut LogEntry) -> (u64, u64) {
        let mut logs = meta.logs.write();
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
        let new_log_id = last_log_id + 1;
        let new_log_term = meta.term;
        entry.term = new_log_term;
        entry.id = new_log_id;
        logs.insert(entry.id, entry.clone());
        (new_log_id, new_log_term)
    }
    fn try_sync_log_to_followers(
        &self, meta: &mut RwLockWriteGuard<RaftMeta>,
        entry: &LogEntry, new_log_id: u64
    ) -> Option<ExecResult> {
        if self.send_followers_heartbeat(meta, Some(new_log_id)) {
            meta.commit_index = new_log_id;
            Some(commit_command(meta, entry))
        } else {
            None
        }
    }
    fn try_sync_config_to_followers(
        &self, meta: &mut RwLockWriteGuard<RaftMeta>,
        entry: &LogEntry, new_log_id: u64
    ) -> ExecResult {
        // this will force followers to commit the changes
        meta.commit_index = new_log_id;
        let data = commit_command(&meta, &entry);
        let t = get_time();
        if let Membership::Leader(ref leader_meta) = meta.membership {//  ||| TODO: New member should install newest snapshot
            let mut leader_meta = leader_meta.write();
            self.reload_leader_meta( //                                   |||       and logs to get updated first before leader
                                     &members_from_meta!(meta), //                             |||       add it to member list in configuration
                                     &mut leader_meta, new_log_id
            );
        }
        self.send_followers_heartbeat(meta, Some(new_log_id));
        data
    }
    fn append_entries(
        &self,
        term: u64, leader_id: u64,
        prev_log_id: u64, prev_log_term: u64,
        entries: Option<LogEntries>,
        leader_commit: u64
    ) -> Box<Future<Item = (u64, AppendEntriesResult), Error = ()>>  {
        // TODO: check if locks can be async friendly
        let mut meta = self.write_meta();
        self.reset_last_checked(&mut meta);
        let term_ok = self.check_term(&mut meta, term, leader_id); // RI, 1
        let result = if term_ok {
            if let Membership::Candidate = meta.membership {
                debug!("SWITCH FROM CANDIDATE BACK TO FOLLOWER {}", self.id);
                self.become_follower(&mut meta, term, leader_id);
            }
            if prev_log_id > 0 {
                check_commit(&mut meta);
                let mut logs = meta.logs.write();
                //RI, 2
                let contains_prev_log = logs.contains_key(&prev_log_id);
                let mut log_mismatch = false;

                if contains_prev_log {
                    let entry = logs.get(&prev_log_id).unwrap();
                    log_mismatch = entry.term != prev_log_term;
                } else {
                    return box future::finished((
                        meta.term,
                        AppendEntriesResult::LogMismatch
                    )) // prev log not existed
                }
                if log_mismatch {
                    //RI, 3
                    let ids_to_del: Vec<u64> = logs.range(
                        (Included(prev_log_id), Unbounded)
                    ).map(|(id, _)| *id).collect();
                    for id in ids_to_del {
                        logs.remove(&id);
                    }
                    return box future::finished((
                        meta.term,
                        AppendEntriesResult::LogMismatch
                    )) // log mismatch
                }
            }
            let mut last_new_entry = std::u64::MAX;
            {
                let mut logs = meta.logs.write();
                let mut leader_commit = leader_commit;
                if let Some(ref entries) = entries { // entry not empty
                    for entry in entries {
                        let entry_id = entry.id;
                        let sm_id = entry.sm_id;
                        logs.entry(entry_id).or_insert(entry.clone());// RI, 4
                        last_new_entry = max(last_new_entry, entry_id);
                    }
                } else if !logs.is_empty() {
                    last_new_entry = logs.values().last().unwrap().id;
                }
            }
            if leader_commit > meta.commit_index { //RI, 5
                meta.commit_index = min(leader_commit, last_new_entry);
                check_commit(&mut meta);
            }
            Ok((meta.term, AppendEntriesResult::Ok))
        } else {
            Ok((meta.term, AppendEntriesResult::TermOut(meta.leader_id))) // term mismatch
        };
        self.reset_last_checked(&mut meta);
        box future::result(result)
    }

    fn request_vote(
        &self,
        term: u64, candidate_id: u64,
        last_log_id: u64, last_log_term: u64
    ) -> Box<Future<Item = ((u64, u64), bool), Error = ()>> {
        let mut meta = self.write_meta();
        let vote_for = meta.vote_for;
        let mut vote_granted = false;
        if term > meta.term {
            check_commit(&mut meta);
            let logs = meta.logs.read();
            let conf_sm = &meta.state_machine.read().configs;
            let candidate_valid = conf_sm.member_existed(candidate_id);
            debug!("{} VOTE FOR: {}, valid: {}", self.id, candidate_id, candidate_valid);
            if (vote_for.is_none() || vote_for.unwrap() == candidate_id) && candidate_valid{
                let (last_id, last_term) = get_last_log_info!(self, logs);
                if last_log_id >= last_id && last_log_term >= last_term {
                    vote_granted = true;
                } else {
                    debug!("{} VOTE FOR: {}, not granted due to log check", self.id, candidate_id);
                }
            } else {
                debug!(
                    "{} VOTE FOR: {}, not granted due to voted for {}",
                    self.id, candidate_id, vote_for.unwrap()
                );
            }
        } else {
            debug!("{} VOTE FOR: {}, not granted due to term out", self.id, candidate_id);
        }
        if vote_granted {
            meta.vote_for = Some(candidate_id);
        }
        debug!("{} VOTE FOR: {}, granted: {}", self.id, candidate_id, vote_granted);
        box future::finished(((meta.term, meta.leader_id), vote_granted))
    }

    fn install_snapshot(
        &self,
        term: u64, leader_id: u64, last_included_index: u64,
        last_included_term: u64, data: Vec<u8>, done: bool
    ) -> Box<Future<Item = u64, Error = ()>> {
        let mut meta = self.write_meta();
        let term_ok = self.check_term(&mut meta, term, leader_id);
        if term_ok {
            check_commit(&mut meta);
        }
        box future::finished(meta.term)
    }

    fn c_command(&self, entry: LogEntry)
                 -> Box<Future<Item = ClientCmdResponse, Error = ()>>
    {
        let mut meta = self.write_meta();
        let mut entry = entry;
        if !is_leader(&meta) {
            return box future::finished(ClientCmdResponse::NotLeader(meta.leader_id));
        }
        let (new_log_id, new_log_term) = self.append_log(&meta, &mut entry);
        let mut data = match entry.sm_id {
            // special treats for membership changes
            CONFIG_SM_ID => Some(self.try_sync_config_to_followers(&mut meta, &entry, new_log_id)),
            _ => self.try_sync_log_to_followers(&mut meta, &entry, new_log_id)
        }; // Some for committed and None for not committed
        if let Some(data) = data {
            box future::finished(ClientCmdResponse::Success{
                data,
                last_log_id: new_log_id,
                last_log_term: new_log_term,
            })
        } else {
            box future::finished(ClientCmdResponse::NotCommitted)
        }
    }

    fn c_query(&self, entry: LogEntry) -> Box<Future<Item = ClientQryResponse, Error = ()>> {
        let mut meta = self.meta.read();
        let logs = meta.logs.read();
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
        if entry.term > last_log_term || entry.id > last_log_id {
            box future::finished(ClientQryResponse::LeftBehind)
        } else {
            box future::finished(ClientQryResponse::Success{
                data: meta.state_machine.read().exec_qry(&entry),
                last_log_id,
                last_log_term,
            })
        }
    }

    fn c_server_cluster_info(&self) -> Box<Future<Item = ClientClusterInfo, Error = ()>> {
        box future::finished(self.cluster_info())
    }

    fn c_put_offline(&self) -> Box<Future<Item = bool, Error = ()>> {
        box future::finished(self.leave())
    }
}

impl Service for RaftService {
    fn append_entries(
        &self,
        term: u64, leader_id: u64,
        prev_log_id: u64, prev_log_term: u64,
        entries: Option<LogEntries>,
        leader_commit: u64
    ) -> Box<Future<Item = (u64, AppendEntriesResult), Error = ()>>  {
        unimplemented!()
    }

    fn request_vote(
        &self,
        term: u64, candidate_id: u64,
        last_log_id: u64, last_log_term: u64
    ) -> Box<Future<Item = ((u64, u64), bool), Error = ()>> {
        unimplemented!()
    }

    fn install_snapshot(
        &self,
        term: u64, leader_id: u64, last_included_index: u64,
        last_included_term: u64, data: Vec<u8>, done: bool
    ) -> Box<Future<Item = u64, Error = ()>> {
        unimplemented!()
    }

    fn c_command(&self, entry: LogEntry)
                 -> Box<Future<Item = ClientCmdResponse, Error = ()>>
    {
        unimplemented!()
    }

    fn c_query(&self, entry: LogEntry) -> Box<Future<Item = ClientQryResponse, Error = ()>> {
        unimplemented!()
    }

    fn c_server_cluster_info(&self) -> Box<Future<Item = ClientClusterInfo, Error = ()>> {
        unimplemented!()
    }

    fn c_put_offline(&self) -> Box<Future<Item = bool, Error = ()>> {
        unimplemented!()
    }
}

impl RaftService {
    pub fn new(opts: Options) -> Arc<RaftService> {
        Arc::new(
            RaftService {
                inner: RaftServiceInner::new(opts)
            }
        )
    }
    pub fn start(&self) -> bool {
        RaftServiceInner::start(&self.inner)
    }
    pub fn register_state_machine(&self, state_machine: SubStateMachine) {
        let meta = self.meta.read();
        let mut master_sm = meta.state_machine.write();
        master_sm.register(state_machine);
    }
}

impl Deref for RaftService {
    type Target = RaftServiceInner;

    fn deref(&self) -> &RaftServiceInner {
        &*self.inner
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
            name: name.clone()
        }
    }
}
