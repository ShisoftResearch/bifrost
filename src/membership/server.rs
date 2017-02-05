use utils::time;
use super::heartbeat_rpc::*;
use super::raft::*;
use super::*;
use raft::{RaftService, LogEntry, RaftMsg, Service as raft_svr_trait};
use raft::state_machine::StateMachineCtl;
use parking_lot::{RwLock};
use std::collections::{HashMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{thread, time as std_time};
use bifrost_hasher::hash_str;
use membership::client::{Group as ClientGroup, Member as ClientMember};


static MAX_TIMEOUT: i64 = 5000; //5 secs for 500ms heartbeat

struct HBStatus {
    online: bool,
    last_updated: i64,
}

pub struct HeartbeatService {
    status: RwLock<HashMap<u64, HBStatus>>,
    raft_service: Arc<RaftService>,
    closed: AtomicBool,
    was_leader: AtomicBool,
}

impl Service for HeartbeatService {
    fn ping(&self, id: u64) -> Result<(), ()> {
        let mut stat_map = self.status.write();
        let current_time = time::get_time();
        let mut stat = stat_map.entry(id).or_insert_with(|| HBStatus {
            online: false,
            last_updated: current_time,
            //orthodoxy info will trigger the watcher thread to update
        });
        stat.last_updated = current_time;
        // only update the timestamp, let the watcher thread to decide
        Ok(())
    }
}
impl HeartbeatService {
    fn update_raft(&self, online: Vec<u64>, offline: Vec<u64>) {
        let log = commands::hb_online_changed {
            online: online,
            offline: offline
        };
        let (fn_id, _, data) = log.encode();
        self.raft_service.c_command(LogEntry {
            id: 0,
            term: 0,
            sm_id: DEFAULT_SERVICE_ID,
            fn_id: fn_id,
            data: data
        });
    }
    fn transfer_leadership(&self) { //update timestamp for every alive server
        let mut stat_map = self.status.write();
        let current_time = time::get_time();
        for stat in stat_map.values_mut() {
            if stat.online {
                stat.last_updated = current_time;
            }
        }
    }
}
dispatch_rpc_service_functions!(HeartbeatService);

pub struct MemberGroup {
    name: String,
    id: u64,
    members: BTreeSet<u64>
}

pub struct Membership {
    heartbeat: Arc<HeartbeatService>,
    groups: HashMap<u64, MemberGroup>,
    members: HashMap<u64, Member>
}
impl Drop for Membership {
    fn drop(&mut self) {
        self.heartbeat.closed.store(true, Ordering::Relaxed)
    }
}

impl Membership {
    pub fn new(raft_service: Arc<RaftService>) {
        let service = Arc::new(HeartbeatService {
            status: RwLock::new(HashMap::new()),
            closed: AtomicBool::new(false),
            raft_service: raft_service.clone(),
            was_leader: AtomicBool::new(false),
        });
        let service_clone = service.clone();
        thread::spawn(move || {
            while !service_clone.closed.load(Ordering::Relaxed) {
                let is_leader = service_clone.raft_service.is_leader();
                let was_leader = service_clone.was_leader.load(Ordering::Relaxed);
                if !was_leader && is_leader {service_clone.transfer_leadership()}
                if was_leader != is_leader {service_clone.was_leader.store(is_leader, Ordering::Relaxed);}
                if is_leader {
                    let current_time = time::get_time();
                    let mut outdated_members: Vec<u64> = Vec::new();
                    let mut backedin_members: Vec<u64> = Vec::new();
                    {
                        let mut status_map = service_clone.status.write();
                        let mut members_to_update: HashMap<u64, bool> = HashMap::new();
                        for (id, status) in status_map.iter() {
                            let alive = (current_time - status.last_updated) < MAX_TIMEOUT;
                            if status.online && !alive {
                                outdated_members.push(id.clone());
                                members_to_update.insert(id.clone(), alive);
                            }
                            if !status.online && alive {
                                backedin_members.push(id.clone());
                                members_to_update.insert(id.clone(), alive);
                            }
                        }
                        for (id, alive) in members_to_update.iter() {
                            let mut status = status_map.get_mut(&id).unwrap();
                            status.online = alive.clone();
                        }

                    }
                    service_clone.update_raft(backedin_members, outdated_members);
                }
                thread::sleep(std_time::Duration::from_secs(1));
            }
        });
        raft_service.register_state_machine(Box::new(Membership {
            heartbeat: service,
            groups: HashMap::new(),
            members: HashMap::new(),
        }))
    }
    fn compose_client_member(&self, id: u64) -> ClientMember {
        let member = self.members.get(&id).unwrap();
        let stat_map = self.heartbeat.status.read();
        ClientMember {
            id: id,
            address: member.address.clone(),
            online: stat_map.get(&id).unwrap().online
        }
    }
}

impl StateMachineCmds for Membership {
    fn hb_online_changed(&mut self, online: Vec<u64>, offline: Vec<u64>) -> Result<(), ()> {
        let mut stat_map = self.heartbeat.status.write();
        for id in online {
            if let Some(ref mut stat) = stat_map.get_mut(&id) {
                stat.online = true;
            }
        }
        for id in offline {
            if let Some(ref mut stat) = stat_map.get_mut(&id) {
                stat.online = false;
            }
        }
        Ok(())
    }
    fn join(&mut self, address: String) -> Result<u64, ()> {
        let id = hash_str(address.clone());
        let mut stat_map = self.heartbeat.status.write();
        self.members.entry(id).or_insert_with(|| {
            let current_time = time::get_time();
            let mut stat = stat_map.entry(id).or_insert_with(|| HBStatus {
                online: true,
                last_updated: current_time
            });
            stat.online = true;
            stat.last_updated = current_time;
            Member {
                id: id,
                address: address.clone(),
                groups: Vec::new(),
            }
        });
        Ok(id)
    }
    fn leave(&mut self, id: u64) -> Result<(), ()> {
        let mut groups:Vec<u64> = Vec::new();
        {
            let mut stat_map = self.heartbeat.status.write();
            stat_map.remove(&id);
        }
        if let Some(member) = self.members.get(&id) {
            for group in &member.groups {
                groups.push(group.clone());
            }
        }
        for group_id in groups {
            self.leave_group(group_id, id);
        }
        self.members.remove(&id);
        Ok(())
    }
    fn join_group(&mut self, group: u64, id: u64) -> Result<(), ()> {
        if let Some(ref mut group) = self.groups.get_mut(&group) {
            group.members.insert(id);
            Ok(())
        } else {
            Err(())
        }
    }
    fn leave_group(&mut self, group: u64, id: u64) -> Result<(), ()> {
        if let Some(ref mut group) = self.groups.get_mut(&group) {
            group.members.remove(&id);
            Ok(())
        } else {
            Err(())
        }
    }
    fn group_leader(&self, group: u64) -> Result<Option<ClientMember>, ()> {
        if let Some(group) = self.groups.get(&group) {
            let first_member = group.members.iter().next();
            if let Some(first_member) = first_member {
                Ok(Some(self.compose_client_member(first_member.clone())))
            } else {
                Ok(None)
            }
        } else {
            Err(())
        }
    }
    fn group_members(&self, group: u64, online_only: bool) -> Result<Vec<ClientMember>, ()> {
        if let Some(group) = self.groups.get(&group) {
            Ok(group.members.iter()
                .map(|id| self.compose_client_member(id.clone()))
                .filter(|member| !online_only || member.online)
                .collect())
        } else {
            Err(())
        }
    }
    fn all_members(&self, online_only: bool) -> Result<Vec<ClientMember>, ()> {
        Ok(self.members.iter()
            .map(|(id, _)| self.compose_client_member(id.clone()))
            .filter(|member| !online_only || member.online)
            .collect())
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