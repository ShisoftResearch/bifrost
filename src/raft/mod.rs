use rand;
use rand::Rng;
use rand::distributions::{IndependentSample, Range};
use std::thread;
use std::sync::{Mutex, MutexGuard};
use time;
use std::time::Duration;
use std::collections::{BTreeMap, HashMap};
use self::state_machine::master::{MasterStateMachine, StateMachineCmds};
use std::cmp::min;
use std::cell::RefCell;
use bifrost_plugins::hash_str;

#[macro_use]
mod state_machine;

trait RaftMsg {
    fn encode(&self) -> (usize, Vec<u8>);
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

type LogEntries = Vec<LogEntry>;

service! {
    rpc append_entries(term: u64, leaderId: u64, prev_log_id: u64, prev_log_term: u64, entries: Option<LogEntries>, leader_commit: u64) -> (u64, bool);
    rpc request_vote(term: u64, candidate_id: u64, last_log_id: u64, last_log_term: u64) -> (u64, bool); // term, voteGranted
    rpc install_snapshot(term: u64, leader_id: u64, last_included_index: u64, last_included_term: u64, data: Vec<u8>, done: bool) -> u64;
}

fn gen_rand(lower: u64, higher: u64) -> u64 {
    let between = Range::new(lower, higher);
    let mut rng = rand::thread_rng();
    between.ind_sample(&mut rng) + 1
}

fn get_time() -> u64 {
    let timespec = time::get_time();
    let mills: f64 = timespec.sec as f64 + (timespec.nsec as f64 / 1000.0 / 1000.0 / 1000.0 );
    mills as u64
}

fn gen_timeout() -> u64 {
    gen_rand(100, 500)
}

pub struct LeaderMeta {
    last_updated: u64,
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
}

impl LeaderMeta {
    fn new() -> LeaderMeta {
        LeaderMeta{
            last_updated: get_time(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }
    }
}

pub struct FollowerMeta {
    leader_id: u64,
}

impl FollowerMeta {
    fn new() -> FollowerMeta {
        FollowerMeta{
            leader_id: 0
        }
    }
    fn with_leader(id: u64) -> FollowerMeta {
        FollowerMeta{
            leader_id: id
        }
    }
}

pub struct CandidateMeta {

}

impl CandidateMeta {
    fn  new() -> CandidateMeta {
        CandidateMeta{

        }
    }
}

pub enum Membership {
    Leader(LeaderMeta),
    Follower(FollowerMeta),
    Candidate(CandidateMeta),
    Offline,
}

pub struct RaftMeta {
    term: u64,
    vote_for: Option<u64>,
    timeout: u64,
    last_checked: u64,
    membership: Membership,
    logs: BTreeMap<u64, LogEntry>,
    state_machine: RefCell<MasterStateMachine>,
    commit_index: u64,
    last_applied: u64,
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
    meta: Arc<Mutex<RaftMeta>>,
    pub id: u64,
    pub options: Options,
}

enum CheckerAction {
    SendHeartbeat,
    BecomeCandidate,
    ExitLoop,
    None
}
impl RaftServer {
    pub fn new(opts: Options) -> Arc<RaftServer> {
        let server = Arc::new(RaftServer {
            meta: Arc::new(Mutex::new(
                RaftMeta {
                    term: 0, //TODO: read from persistent state
                    vote_for: None, //TODO: read from persistent state
                    timeout: gen_timeout(), // 10~500 ms for timeout
                    last_checked: get_time(),
                    membership: Membership::Follower(FollowerMeta::new()),
                    logs: BTreeMap::new(), //TODO: read from persistent state
                    state_machine: RefCell::new(MasterStateMachine::new()),
                    commit_index: 0,
                    last_applied: 0,
                }
            )),
            options: opts.clone(),
            id: hash_str(opts.address.clone()),
        });
        let svr_ref = server.clone();
        thread::spawn(move ||{
            listen(svr_ref, &opts.address);
        });
        let checker_ref = server.clone();
        thread::spawn(move ||{
            let server = checker_ref;
            loop {
                {
                    let mut meta = server.meta.lock().unwrap(); //WARNING: Reentering not supported
                    let action = match meta.membership {
                        Membership::Leader(ref leader_meta) => {
                            if get_time() > (leader_meta.last_updated + CHECKER_MS) {
                                CheckerAction::SendHeartbeat
                            } else {
                                CheckerAction::None
                            }
                        },
                        Membership::Follower(_) | Membership::Candidate(_) => {
                            if get_time() > (meta.timeout + meta.last_checked) {
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
                            server.send_heartbeat(&mut meta, None);
                        },
                        CheckerAction::BecomeCandidate => {
                            server.become_candidate(&mut meta);
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
    fn become_candidate(&self, meta: &mut MutexGuard<RaftMeta>) {
        self.reset_last_checked(meta);
        meta.term += 1;
        meta.vote_for = Some(self.id);
        meta.membership = Membership::Candidate(CandidateMeta::new());

    }
    fn become_follower(&self, meta: &mut MutexGuard<RaftMeta>, leader_id: u64) {
        meta.membership = Membership::Follower(FollowerMeta::with_leader(leader_id));
    }
    fn become_leader(&self, meta: &mut MutexGuard<RaftMeta>) {
        meta.membership = Membership::Leader(LeaderMeta::new());
    }
    fn send_heartbeat(&self, meta: &mut MutexGuard<RaftMeta>, entries: Option<LogEntries>) {

    }

    //check term number, return reject = false if server term is stale
    fn check_term(&self, meta: &mut MutexGuard<RaftMeta>, remote_term: u64, leader_id: u64) -> bool {
        if remote_term > meta.term {
            meta.term = remote_term;
            meta.vote_for = None;
            self.become_follower(meta, leader_id)
        } else if remote_term < meta.term {
            return false;
        }
        return true;
    }

    fn check_commit(&self, meta: &mut MutexGuard<RaftMeta>) { //not need to return result for follower
        while meta.commit_index > meta.last_applied {
            (*meta).last_applied += 1;
            let last_applied = meta.last_applied;
            if let Some(entry) = meta.logs.get(&last_applied) {
                meta.state_machine.borrow_mut().commit(&entry);
            };
        }
    }
    fn reset_last_checked(&self, meta: &mut MutexGuard<RaftMeta>) {
        meta.last_checked = get_time();
        meta.timeout = gen_timeout();
    }
}

impl Server for RaftServer {
    fn append_entries(
        &self,
        term: u64, leader_id: u64, prev_log_id: u64,
        prev_log_term: u64, entries: Option<LogEntries>,
        leader_commit: u64
    ) -> Result<(u64, bool), ()>  {
        let mut meta = self.meta.lock().unwrap();
        let term_ok = self.check_term(&mut meta, term, leader_id); // RI, 1
        self.reset_last_checked(&mut meta);
        if term_ok {
            self.check_commit(&mut meta);
            { //RI, 2
                let contains_prev_log = (*meta).logs.contains_key(&prev_log_id);
                let mut log_mismatch = false;
                if contains_prev_log {
                    let entry = (*meta).logs.get(&prev_log_id).unwrap();
                    log_mismatch = entry.term != prev_log_term;
                } else {
                    return Ok((meta.term, false))
                }
                if log_mismatch { //RI, 3
                    let keys: Vec<u64> = (*meta).logs.keys().cloned().collect();
                    for id in keys {
                        if id >= prev_log_id {
                            (*meta).logs.remove(&id);
                        }
                    }
                    return Ok((meta.term, false))
                }
            }
            if let Some(entries) = entries { // entry not empty
                let entries_len = entries.len();
                for entry in entries {
                    meta.logs.entry(entry.id).or_insert(entry);// RI, 4
                }
                if leader_commit > meta.commit_index { //RI, 5
                    meta.commit_index = min(leader_commit, prev_log_id + entries_len as u64);
                }
            }
            Ok((meta.term, true))
        } else {
            Ok((meta.term, false))
        }
    }

    fn request_vote(
        &self,
        term: u64, candidate_id: u64,
        last_log_id: u64, last_log_term: u64
    ) -> Result<(u64, bool), ()> {
        let mut meta = self.meta.lock().unwrap();
        let vote_for = meta.vote_for;
        let mut vote_granted = false;
        if term >= meta.term {
            self.check_commit(&mut meta);
            if vote_for.is_none() || vote_for.unwrap() == candidate_id {
                if !meta.logs.is_empty() {
                    let (last_id, _) = meta.logs.iter().next_back().unwrap();
                    let last_term = meta.logs.get(last_id).unwrap().term;
                    if last_log_id >= *last_id && last_log_term >= last_term {
                        vote_granted = true;
                    }
                } else {
                    vote_granted = true;
                }
            }
        }
        if vote_granted {
            meta.vote_for = Some(candidate_id);
        }
        Ok((meta.term, vote_granted))
    }

    fn install_snapshot(
        &self,
        term: u64, leader_id: u64, last_included_index: u64,
        last_included_term: u64, data: Vec<u8>, done: bool
    ) -> Result<u64, ()> {
        let mut meta = self.meta.lock().unwrap();
        let term_ok = self.check_term(&mut meta, term, leader_id);
        if term_ok {
            self.check_commit(&mut meta);
        }
        Ok(meta.term)
    }
}
