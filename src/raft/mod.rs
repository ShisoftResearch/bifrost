use rand;
use rand::Rng;
use rand::distributions::{IndependentSample, Range};
use std::thread;
use std::sync::{Mutex, MutexGuard};
use time;
use std::time::Duration;
use std::collections::HashMap;
use self::state_machine::master::MasterStateMachine;

#[macro_use]
mod state_machine;

trait RaftMsg {
    fn encode(&self) -> (usize, Vec<u8>);
}

const CHECKER_MS: u64 = 50;

//               sm , fn , data
type LogEntry = (u64, u64, Vec<u8>);
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
    log: u64,
    voted: bool,
    timeout: u64,
    last_checked: u64,
    last_updated: u64,
    membership: Membership,
    leader_id: u64,
    logs: LogEntries,
    state_machine: MasterStateMachine
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
                    term: 0,
                    log: 0,
                    voted: false,
                    timeout: gen_rand(100, 500), // 10~500 ms for timeout
                    last_checked: get_time(),
                    last_updated: get_time(),
                    membership: Membership::FOLLOWER,
                    leader_id: 0,
                    logs: Vec::new(),
                    state_machine: MasterStateMachine::new(),
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
        let term_ok = self.check_term(&mut meta, term);
        if !term_ok {
            Ok((meta.term, false))
        } else {
            Ok((meta.term, true))
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
        if term_ok && !voted {

        }
        Ok((meta.term, !voted))
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
