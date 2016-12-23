use rand;
use rand::Rng;
use rand::distributions::{IndependentSample, Range};
use std::thread;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Mutex, MutexGuard};
use std::collections::{BTreeMap, HashMap};
use std::collections::Bound::{Included, Unbounded};
use self::state_machine::OpType;
use self::state_machine::master::{MasterStateMachine, StateMachineCmds, ExecResult};
use std::cmp::{min, max};
use std::cell::RefCell;
use bifrost_plugins::hash_str;
use utils::time::get_time;
use threadpool::ThreadPool;
use num_cpus;
use std::sync::mpsc::channel;

#[macro_use]
mod state_machine;
pub mod client;

pub trait RaftMsg<R> {
    fn encode(&self) -> (u64, OpType, Vec<u8>);
    fn decode_return(&self, data: &Vec<u8>) -> R;
}

const CHECKER_MS: u64 = 50;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    id: u64,
    term: u64,
    sm_id: u64,
    fn_id: u64,
    data: Vec<u8>
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ClientCmdResponse {
    Success{
        data: ExecResult,
        last_log_term: u64,
        last_log_id: u64,
    },
    NotLeader(u64),
    NotUpdated,
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
type LogEntries = Vec<LogEntry>;
type LogsMap = BTreeMap<u64, LogEntry>;

service! {
    rpc append_entries(term: u64, leaderId: u64, prev_log_id: u64, prev_log_term: u64, entries: Option<LogEntries>, leader_commit: u64) -> (u64, bool);
    rpc request_vote(term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> ((u64, u64), bool); // term, voteGranted
    rpc install_snapshot(term: u64, leader_id: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>, done: bool) -> u64;
    rpc c_command(entries: LogEntry) -> ClientCmdResponse;
    rpc c_query(entries: LogEntry) -> ClientQryResponse;
    rpc c_server_cluster_info() -> ClientClusterInfo;
}

fn gen_rand(lower: u64, higher: u64) -> u64 {
    let between = Range::new(lower, higher);
    let mut rng = rand::thread_rng();
    between.ind_sample(&mut rng) + 1
}

fn gen_timeout() -> u64 {
    gen_rand(100, 500)
}

struct FollowerStatus {
    next_index: u64,
    match_index: u64,
}

pub struct LeaderMeta {
    last_updated: u64,
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
    Leader(LeaderMeta),
    Follower,
    Candidate,
    Offline,
}

pub struct RaftMeta {
    term: u64,
    vote_for: Option<u64>,
    timeout: u64,
    last_checked: u64,
    membership: Membership,
    logs: Arc<RwLock<LogsMap>>,
    state_machine: RwLock<MasterStateMachine>,
    commit_index: u64,
    last_applied: u64,
    leader_id: u64,
    workers: Mutex<ThreadPool>,
}

#[derive(Clone)]
pub enum Storage {
    MEMORY,
    DISK(String),
}

impl Storage {
    pub fn Default() -> Storage {
        Storage::MEMORY
    }
}

#[derive(Clone)]
pub struct Options {
    pub storage: Storage,
    pub address: String,
}

pub struct RaftServer {
    meta: RwLock<RaftMeta>,
    pub id: u64,
    pub options: Options,
}

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

macro_rules! check_commit {
    ($meta: expr) => {{
        while $meta.commit_index > $meta.last_applied {
            (*$meta).last_applied += 1;
            let last_applied = $meta.last_applied;
            let logs = $meta.logs.read().unwrap();
            if let Some(entry) = logs.get(&last_applied) {
                $meta.state_machine.write().unwrap().commit_cmd(&entry);
            };
        }
    }};
}

impl RaftServer {
    pub fn new(opts: Options) -> Arc<RaftServer> {
        let server_obj = RaftServer {
            meta: RwLock::new(
                RaftMeta {
                    term: 0, //TODO: read from persistent state
                    vote_for: None, //TODO: read from persistent state
                    timeout: gen_timeout(), // 10~500 ms for timeout
                    last_checked: get_time(),
                    membership: Membership::Follower,
                    logs: Arc::new(RwLock::new(BTreeMap::new())), //TODO: read from persistent state
                    state_machine: RwLock::new(MasterStateMachine::new()),
                    commit_index: 0,
                    last_applied: 0,
                    leader_id: 0,
                    workers: Mutex::new(ThreadPool::new(num_cpus::get())),
                }
            ),
            options: opts.clone(),
            id: hash_str(opts.address.clone()),
        };
        let server = Arc::new(server_obj);
        let svr_ref = server.clone();
        thread::spawn(move ||{
            listen(svr_ref, &opts.address);
        });
        let checker_ref = server.clone();
        thread::spawn(move ||{
            let server = checker_ref;
            loop {
                {
                    let mut meta = server.meta.write().unwrap(); //WARNING: Reentering not supported
                    let action = match meta.membership {
                        Membership::Leader(ref leader_meta) => {
                            if get_time() > (leader_meta.last_updated + CHECKER_MS) {
                                CheckerAction::SendHeartbeat
                            } else {
                                CheckerAction::None
                            }
                        },
                        Membership::Follower | Membership::Candidate => {
                            if get_time() > (meta.timeout + meta.last_checked) && meta.vote_for == None {
                                //Timeout, require election
                                CheckerAction::BecomeCandidate
                            } else {
                                CheckerAction::None
                            }
                        },
                        Membership::Offline => {
                            CheckerAction::ExitLoop
                        }
                    };
                    match action {
                        CheckerAction::SendHeartbeat => {
                            let (last_log_id, _) = {
                                let logs = meta.logs.read().unwrap();
                                get_last_log_info!(server, logs)
                            };
                            server.send_followers_heartbeat(&mut meta, last_log_id);
                        },
                        CheckerAction::BecomeCandidate => {
                            RaftServer::become_candidate(server.clone(), &mut meta);
                        },
                        CheckerAction::ExitLoop => {
                            break;
                        },
                        CheckerAction::None => {}
                    }
                }
                thread::sleep(Duration::from_millis(CHECKER_MS));
            }
        });
        server
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
    fn become_candidate(server: Arc<RaftServer>, meta: &mut RwLockWriteGuard<RaftMeta>) {
        server.reset_last_checked(meta);
        meta.term += 1;
        meta.vote_for = Some(server.id);
        server.switch_membership(meta, Membership::Candidate);
        let term = meta.term;
        let id = server.id;
        let logs = meta.logs.read().unwrap();
        let (last_log_id, last_log_term) = get_last_log_info!(server, logs);
        let (tx, rx) = channel();
        let mut members = 0;
        let timeout = meta.timeout;
        for member in meta.state_machine.read().unwrap().configs.members.values() {
            let rpc = member.rpc.clone();
            let tx = tx.clone();
            members += 1;
            meta.workers.lock().unwrap().execute(move||{
                let mut rpc = rpc.lock().unwrap();
                if let Some(Ok(((remote_term, remote_leader_id), vote_granted))) = rpc.request_vote(term, id, last_log_id, last_log_term) {
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
        meta.workers.lock().unwrap().execute(move ||{
            let mut granted = 0;
            for _ in 0..members {
                let received = rx.recv();
                let mut meta = server.meta.write().unwrap();
                if meta.term != term {break;}
                match received {
                    Ok(res) => {
                        match res {
                            RequestVoteResponse::TermOut(remote_term, remote_leader_id) => {
                                server.become_follower(&mut meta, remote_term, remote_leader_id);
                                break;
                            },
                            RequestVoteResponse::Granted => {
                                granted += 1;
                                if granted > (members / 2 + 1) {
                                    server.become_leader(&mut meta, last_log_id);
                                    break;
                                }
                            },
                            _ => {}
                        }
                    },
                    Err(_) => {}
                }
            }
        });
    }
    fn become_follower(
        &self,
        meta: &mut RwLockWriteGuard<RaftMeta>,
        term: u64, leader_id: u64,
    ) {
        meta.term = term;
        meta.leader_id = leader_id;
        self.switch_membership(meta, Membership::Follower);
    }
    fn become_leader(
        &self,
        meta: &mut RwLockWriteGuard<RaftMeta>,
        last_log_id: u64
    ) {
        let mut leader_meta = LeaderMeta::new();
        for member in meta.state_machine.read().unwrap().configs.members.values() {
            let id = member.id;
            leader_meta.followers.insert(id, Arc::new(Mutex::new(FollowerStatus {
                next_index: last_log_id + 1,
                match_index: 0
            })));
        }
        leader_meta.last_updated = get_time();
        self.switch_membership(meta, Membership::Leader(leader_meta));
    }

    fn send_followers_heartbeat(
        &self, meta: &mut RwLockWriteGuard<RaftMeta>, last_log_id: u64
    ) {
        let mut log_id = 0;
        let (tx, rx) = channel();
        let mut members = 0;
        let timeout = meta.timeout;
        let commit_index = meta.commit_index;
        let term = meta.term;
        let leader_id = meta.leader_id;
        {
            let workers = meta.workers.lock().unwrap();
            for member in meta.state_machine.read().unwrap().configs.members.values() {
                let id = member.id;
                let tx = tx.clone();
                members += 1;
                let logs = meta.logs.clone();
                let rpc = member.rpc.clone();
                let follower = {
                    if let Membership::Leader(ref leader_meta) = meta.membership {
                        leader_meta.followers.get(&id)
                    } else {
                        None
                    }}.unwrap().clone();
                workers.execute(move||{
                    let mut rpc = rpc.lock().unwrap();
                    let mut follower = follower.lock().unwrap();
                    let logs = logs.read().unwrap();
                    loop {
                        let entries: Option<LogEntries> = {
                            let list: LogEntries = logs.range(
                                Included(&follower.next_index), Included(&last_log_id)
                            ).map(|(_, entry)| entry.clone()).collect(); //TODO: avoid clone entry
                            if list.is_empty() {None} else {Some(list)}
                        };
                        let (follower_last_log_id, follower_last_log_term) = {
                            if logs.is_empty() {
                                (0, 0) // 0 represents there is no logs in the leader
                            } else {
                                let (first_log_id, _) = logs.iter().next().unwrap();
                                let follower_last_log_id = last_log_id - 1;
                                // Assumed log ids are sequence of integers
                                if *first_log_id > follower_last_log_id {
                                    panic!("TODO: deal with snapshot or other situations may remove old logs")
                                }
                                let follower_last_entry = logs.get(&follower_last_log_id);
                                match follower_last_entry {
                                    Some(entry) => {
                                        (entry.id, entry.term)
                                    },
                                    None => {
                                        panic!("Cannot find old logs for follower, first_id: {}, follower_last: {}");
                                        (0, 0)
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
                        );
                        match append_result {
                            Some(Ok((term, ok))) => {
                                if ok {
                                    tx.send(());
                                    break;
                                } else {
                                    follower.next_index -= 1;
                                }
                            },
                            _ => {break;} // retry will happened in next heartbeat
                        }
                    }
                });
            }
        }
        if let Membership::Leader(ref mut leader_meta) = meta.membership{
            for _ in 0..members {

            }
            leader_meta.last_updated = get_time();
        }
    }

    //check term number, return reject = false if server term is stale
    fn check_term(&self, meta: &mut RwLockWriteGuard<RaftMeta>, remote_term: u64, leader_id: u64) -> bool {
        if remote_term > meta.term {
            meta.term = remote_term;
            meta.vote_for = None;
            self.become_follower(meta, remote_term, leader_id)
        } else if remote_term < meta.term {
            return false;
        }
        return true;
    }

    fn reset_last_checked(&self, meta: &mut RwLockWriteGuard<RaftMeta>) {
        meta.last_checked = get_time();
        meta.timeout = gen_timeout();
    }
}

impl Server for RaftServer {
    fn append_entries(
        &self,
        term: u64, leader_id: u64,
        prev_log_id: u64, prev_log_term: u64,
        entries: Option<LogEntries>,
        leader_commit: u64
    ) -> Result<(u64, bool), ()>  {
        let mut meta = self.meta.write().unwrap();
        let term_ok = self.check_term(&mut meta, term, leader_id); // RI, 1
        self.reset_last_checked(&mut meta);
        if term_ok {
            if let Membership::Candidate = meta.membership {
                self.become_follower(&mut meta, term, leader_id);
            }
            {
                check_commit!(meta);

                if prev_log_id == 0 {
                    return Ok((meta.term, true));
                } //  if leader logs empty, return immediately

                let mut logs = meta.logs.write().unwrap();
                //RI, 2
                let contains_prev_log = logs.contains_key(&prev_log_id);
                let mut log_mismatch = false;
                if contains_prev_log {
                    let entry = logs.get(&prev_log_id).unwrap();
                    log_mismatch = entry.term != prev_log_term;
                } else {
                    return Ok((meta.term, false)) // prev log not existed
                }
                if log_mismatch { //RI, 3
                    let ids_to_del: Vec<u64> = logs.range(
                        Included(&prev_log_id), Unbounded
                    ).map(|(id, _)| *id).collect();
                    for id in ids_to_del {
                        logs.remove(&id);
                    }
                    return Ok((meta.term, false)) // log mismatch
                }
            }
            if let Some(entries) = entries { // entry not empty
                let mut last_new_entry = std::u64::MAX;
                {
                    let mut logs = meta.logs.write().unwrap();
                    for entry in entries {
                        let entry_id = entry.id;
                        logs.entry(entry_id).or_insert(entry);// RI, 4
                        last_new_entry = max(last_new_entry, entry_id);
                    }
                }
                if leader_commit > meta.commit_index { //RI, 5
                    meta.commit_index = min(leader_commit, last_new_entry);
                }
            }
            Ok((meta.term, true))
        } else {
            Ok((meta.term, false)) // term mismatch
        }
    }

    fn request_vote(
        &self,
        term: u64, candidate_id: u64,
        last_log_id: u64, last_log_term: u64
    ) -> Result<((u64, u64), bool), ()> {
        let mut meta = self.meta.write().unwrap();
        let vote_for = meta.vote_for;
        let mut vote_granted = false;
        if term >= meta.term {
            check_commit!(meta);
            let logs = meta.logs.read().unwrap();
            if vote_for.is_none() || vote_for.unwrap() == candidate_id {
                if !logs.is_empty() {
                    let (last_id, last_term) = get_last_log_info!(self, logs);
                    if last_log_id >= last_id && last_log_term >= last_term {
                        vote_granted = true;
                    }
                } else {
                    vote_granted = false;
                }
            }
        }
        if vote_granted {
            meta.vote_for = Some(candidate_id);
        }
        Ok(((meta.term, meta.leader_id), vote_granted))
    }

    fn install_snapshot(
        &self,
        term: u64, leader_id: u64, last_included_index: u64,
        last_included_term: u64, data: Vec<u8>, done: bool
    ) -> Result<u64, ()> {
        let mut meta = self.meta.write().unwrap();
        let term_ok = self.check_term(&mut meta, term, leader_id);
        if term_ok {
            check_commit!(meta);
        }
        Ok(meta.term)
    }

    fn c_command(&self, mut entry: LogEntry) -> Result<ClientCmdResponse, ()> {
        let mut meta = self.meta.write().unwrap();
        let is_leader = match meta.membership {
            Membership::Leader(_) => {true},
            _ => {false}
        };
        if !is_leader {
            Ok(ClientCmdResponse::NotLeader(meta.leader_id))
        } else {
            let
            (
                entry,
                last_log_id, last_log_term,
                new_log_id, new_log_term

            ) = {
                let mut logs = meta.logs.write().unwrap();
                let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
                let new_log_id = last_log_id + 1;
                let new_log_term = meta.term;
                entry.term = new_log_term;
                entry.id = new_log_id;
                logs.insert(entry.id, entry.clone());
                (
                    entry,
                    last_log_id, last_log_term,
                    new_log_id, new_log_term
                )
            };
            self.send_followers_heartbeat(&mut meta, new_log_id);
            meta.commit_index = new_log_id;
            Ok(ClientCmdResponse::Success{
                data: meta.state_machine.write().unwrap().commit_cmd(
                    &entry
                ),
                last_log_id: new_log_id,
                last_log_term: new_log_term,
            })
        }
    }
    fn c_query(&self, entry: LogEntry) -> Result<ClientQryResponse, ()> {
        let mut meta = self.meta.read().unwrap();
        let logs = meta.logs.read().unwrap();
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
        if entry.term > last_log_term || entry.id > last_log_id {
            Ok(ClientQryResponse::LeftBehind)
        } else {
            Ok(ClientQryResponse::Success{
                data: meta.state_machine.read().unwrap().exec_qry(&entry),
                last_log_id: last_log_id,
                last_log_term: last_log_term,
            })
        }
    }
    fn c_server_cluster_info(&self) -> Result<ClientClusterInfo, ()> {
        let mut meta = self.meta.read().unwrap();
        let logs = meta.logs.read().unwrap();
        let sm = &meta.state_machine.read().unwrap();
        let sm_members = sm.members();
        let mut members = Vec::new();
        for (id, member) in sm_members.iter(){
            members.push((*id, member.address.clone()))
        }
        let (last_log_id, last_log_term) = get_last_log_info!(self, logs);
        Ok(ClientClusterInfo{
            members: members,
            last_log_id: last_log_id,
            last_log_term: last_log_term,
            leader_id: meta.leader_id,
        })
    }
}

pub struct RaftStateMachine {
    pub id: u64,
    pub name: String,
}

impl RaftStateMachine {
    pub fn new(name: String) -> RaftStateMachine {
        RaftStateMachine {
            id: hash_str(name.clone()),
            name: name
        }
    }

}
