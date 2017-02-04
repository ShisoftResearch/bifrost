use utils::time;
use super::heartbeat_rpc::*;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

static MAX_TIMEOUT: i64 = 5000; //5 secs for 500ms heartbeat

struct HBStatus {
    alive: bool,
    last_updated: i64,
    last_checked: i64,
}

pub struct HeartbeatService {
    status: Mutex<HashMap<u64, HBStatus>>,
    member_addresses: Mutex<HashMap<String, u64>>,
    closed: AtomicBool,
}

impl Drop for HeartbeatService {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Relaxed)
    }
}

impl Service for HeartbeatService {
    fn ping(&self, id: u64) -> Result<(), ()> {
        Ok(())
    }
}
impl HeartbeatService {
    fn new() -> Arc<HeartbeatService> {
        let service = Arc::new(HeartbeatService {
            status: Mutex::new(HashMap::new()),
            member_addresses: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        });
        let service_clone = service.clone();
        thread::spawn(move || {
            while !service_clone.closed.load(Ordering::Relaxed) {
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

            }
        });
        return service;
    }
}

dispatch_rpc_service_functions!(HeartbeatService);