use utils::time;
use super::heartbeat_rpc::*;
use super::raft::*;
use super::*;
use raft::RaftService;
use raft::state_machine::StateMachineCtl;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{thread, time as std_time};

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_MEMBERSHIP_SERVICE) as u64;

static MAX_TIMEOUT: i64 = 5000; //5 secs for 500ms heartbeat

struct HBStatus {
    alive: bool,
    last_updated: i64,
    last_checked: i64,
}

pub struct HeartbeatService {
    status: Mutex<HashMap<u64, HBStatus>>,
    member_addresses: Mutex<HashMap<String, u64>>,
    raft_service: Arc<RaftService>,
    closed: AtomicBool,
}

impl Service for HeartbeatService {
    fn ping(&self, id: u64) -> Result<(), ()> {
        Ok(())
    }
}
impl HeartbeatService {

}
dispatch_rpc_service_functions!(HeartbeatService);

pub struct MemberGroup {
    name: String,
    id: u64,
    members: HashSet<u64>
}

pub struct Membership {
    heartbeat: Arc<HeartbeatService>,
    groups: HashMap<u64, MemberGroup>,
}
impl Drop for Membership {
    fn drop(&mut self) {
        self.heartbeat.closed.store(true, Ordering::Relaxed)
    }
}

impl Membership {
    fn new(raft_service: Arc<RaftService>) {
        let service = Arc::new(HeartbeatService {
            status: Mutex::new(HashMap::new()),
            member_addresses: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            raft_service: raft_service.clone(),
        });
        let service_clone = service.clone();
        thread::spawn(move || {
            while !service_clone.closed.load(Ordering::Relaxed) {
                if service_clone.raft_service.is_leader() {
                    let current_time = time::get_time();
                    let mut status_map = service_clone.status.lock();
                    let mut outdated_members: Vec<u64> = Vec::new();
                    let mut backedin_members: Vec<u64> = Vec::new();
                    let mut members_to_update: HashMap<u64, bool> = HashMap::new();
                    for (id, status) in status_map.iter() {
                        let alive = (current_time - status.last_updated) < MAX_TIMEOUT;
                        if status.alive && !alive {
                            outdated_members.push(id.clone());
                            members_to_update.insert(id.clone(), alive);
                        }
                        if !status.alive && alive {
                            backedin_members.push(id.clone());
                            members_to_update.insert(id.clone(), alive);
                        }
                    }
                    for (id, alive) in members_to_update.iter() {
                        let mut status = status_map.get_mut(&id).unwrap();
                        status.alive = alive.clone();
                    }
                    // TODO: Update members in raft_service
                }
                thread::sleep(std_time::Duration::from_secs(1));
            }
        });
        raft_service.register_state_machine(Box::new(Membership {
            heartbeat: service,
            groups: HashMap::new()
        }))
    }
}

impl StateMachineCmds for Membership {
    fn offline(&mut self, ids: Vec<u64>) -> Result<(), ()> {
        Err(())
    }
    fn online(&mut self, ids: Vec<u64>) -> Result<(), ()> {
        Err(())
    }
    fn join(&mut self, group: u64, address: String) -> Result<(), ()> {
        Err(())
    }
    fn leave(&mut self, group: u64, address: String) -> Result<(), ()> {
        Err(())
    }
    fn members(&self, group: u64) -> Result<Vec<Member>, ()> {
        Err(())
    }
    fn leader(&self, group: u64) -> Result<Member, ()> {
        Err(())
    }
    fn group_members(&self, group: u64) -> Result<Vec<Member>, ()> {
        Err(())
    }
    fn all_members(&self) -> Result<Vec<Member>, ()> {
        Err(())
    }
}
impl StateMachineCtl for Membership {
    sm_complete!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        //Some(serialize!(&self.map))
        None // TODO: Backup members
    }
    fn recover(&mut self, data: Vec<u8>) {
        //self.map = deserialize!(&data);
    }
    fn id(&self) -> u64 {DEFAULT_SERVICE_ID}
}