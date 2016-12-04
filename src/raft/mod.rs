use rand;
use rand::Rng;
use rand::distributions::{IndependentSample, Range};
use std::thread;
use std::sync::{Mutex, MutexGuard};
use time;
use std::time::Duration;
use std::collections::BTreeMap;
use self::state_machine::master::MasterStateMachine;
use std::cmp::min;

#[macro_use]
mod state_machine;

trait RaftMsg {
    fn encode(&self) -> (usize, Vec<u8>);
}

const CHECKER_MS: u64 = 50;

#[derive(Serialize, Deserialize, Debug)]
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

pub enum Membership {
    LEADER,
    FOLLOWER,
    CANDIDATE,
    OFFLINE,
}

pub struct RaftMeta {
    term: u64,
    voted: bool,
    timeout: u64,
    last_checked: u64,
    last_updated: u64,
    membership: Membership,
    leader_id: u64,
    logs: BTreeMap<u64, LogEntry>,
    state_machine: MasterStateMachine,
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
    pub options: Options,
}

impl RaftServer {
    pub fn new(opts: Options) -> Arc<RaftServer> {
        let server = Arc::new(RaftServer {
            meta: Arc::new(Mutex::new(
                RaftMeta {
                    term: 0, //TODO: read from persistent state
                    voted: false, //TODO: read from persistent state
                    timeout: gen_rand(100, 500), // 10~500 ms for timeout
                    last_checked: get_time(),
                    last_updated: get_time(),
                    membership: Membership::FOLLOWER,
                    leader_id: 0,
                    logs: BTreeMap::new(), //TODO: read from persistent state
                    state_machine: MasterStateMachine::new(),
                    commit_index: 0,
                    last_applied: 0,
                }
            )),
            options: opts.clone(),
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
                    match meta.membership {
                        Membership::LEADER => {
                            if get_time() > (meta.last_updated + CHECKER_MS) {
                                server.send_heartbeat(&mut meta, None);
                            }
                        },
                        Membership::FOLLOWER | Membership::CANDIDATE => {
                            if get_time() > (meta.timeout + meta.last_checked) { //Timeout, require election
                                server.become_candidate(&mut meta);
                            }
                        },
                        Membership::OFFLINE => {
                            break;
                        }
                    }
                }
                thread::sleep(Duration::from_millis(CHECKER_MS));
            }
        });
        server
    }
    fn become_candidate(&self, meta: &mut MutexGuard<RaftMeta>) {
        meta.membership = Membership::CANDIDATE;
    }
    fn become_follower(&self, meta: &mut MutexGuard<RaftMeta>) {
        meta.membership = Membership::FOLLOWER;
    }
    fn send_heartbeat(&self, meta: &mut MutexGuard<RaftMeta>, entries: Option<LogEntries>) {

    }

    //check term number, return reject = false if server term is stale
    fn check_term(&self, meta: &mut MutexGuard<RaftMeta>, remote_term: u64) -> bool {
        if remote_term > meta.term {
            (*meta).term = remote_term;
            self.become_follower(meta)
        } else if remote_term < meta.term {
            return false;
        }
        return true;
    }
}

impl Server for RaftServer {
    fn append_entries(
        &self,
        term: u64, leaderId: u64, prev_log_id: u64,
        prev_log_term: u64, entries: Option<LogEntries>,
        leader_commit: u64
    ) -> Result<(u64, bool), ()>  {
        let mut meta = self.meta.lock().unwrap();
        let term_ok = self.check_term(&mut meta, term); // RI, 1
        if term_ok {
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
        let term_ok = self.check_term(&mut meta, term);
        let voted = meta.voted;
        let mut vote_granted = false;
        if term_ok && !voted {
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
        if vote_granted {
            meta.voted = true;
        }
        Ok((meta.term, vote_granted))
    }

    fn install_snapshot(
        &self,
        term: u64, leader_id: u64, last_included_index: u64,
        last_included_term: u64, data: Vec<u8>, done: bool
    ) -> Result<u64, ()> {
        let mut meta = self.meta.lock().unwrap();
        let term_ok = self.check_term(&mut meta, term);
        Ok(meta.term)
    }
}
