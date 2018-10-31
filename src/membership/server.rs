use super::heartbeat_rpc::*;
use super::raft::*;
use super::*;
use bifrost_hasher::hash_str;
use futures::future;
use futures::prelude::*;
use membership::client::{Group as ClientGroup, Member as ClientMember};
use parking_lot::RwLock;
use raft::state_machine::callback::server::{notify as cb_notify, SMCallback};
use raft::state_machine::StateMachineCtl;
use raft::{LogEntry, RaftMsg, RaftService, Service as raft_svr_trait};
use rpc::Server;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::{thread, time as std_time};
use utils::time;

static MAX_TIMEOUT: i64 = 1000; //5 secs for 500ms heartbeat

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
    fn ping(&self, id: u64) -> Box<Future<Item = (), Error = ()>> {
        let mut stat_map = self.status.write();
        let current_time = time::get_time();
        let mut stat = stat_map.entry(id).or_insert_with(|| HBStatus {
            online: false,
            last_updated: current_time,
            //orthodoxy info will trigger the watcher thread to update
        });
        stat.last_updated = current_time;
        // only update the timestamp, let the watcher thread to decide
        box future::finished(())
    }
}
impl HeartbeatService {
    fn update_raft(&self, online: &Vec<u64>, offline: &Vec<u64>) {
        let log = commands::hb_online_changed::new(online, offline);
        let (fn_id, _, data) = log.encode();
        self.raft_service.c_command(LogEntry {
            id: 0,
            term: 0,
            sm_id: DEFAULT_SERVICE_ID,
            fn_id,
            data,
        });
    }
    fn transfer_leadership(&self) {
        //update timestamp for every alive server
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

#[derive(Debug)]
struct Member {
    pub id: u64,
    pub address: String,
    pub groups: HashSet<u64>,
}

struct MemberGroup {
    name: String,
    id: u64,
    members: BTreeSet<u64>,
    leader: Option<u64>,
}

pub struct Membership {
    heartbeat: Arc<HeartbeatService>,
    groups: HashMap<u64, MemberGroup>,
    members: HashMap<u64, Member>,
    callback: Option<SMCallback>,
    version: u64,
}
impl Drop for Membership {
    fn drop(&mut self) {
        self.heartbeat.closed.store(true, Ordering::Relaxed)
    }
}

impl Membership {
    pub fn new(server: &Arc<Server>, raft_service: &Arc<RaftService>) {
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
                if !was_leader && is_leader {
                    service_clone.transfer_leadership()
                }
                if was_leader != is_leader {
                    service_clone.was_leader.store(is_leader, Ordering::Relaxed);
                }
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
                                outdated_members.push(*id);
                                members_to_update.insert(*id, alive);
                            }
                            if !status.online && alive {
                                backedin_members.push(*id);
                                members_to_update.insert(*id, alive);
                            }
                        }
                        for (id, alive) in members_to_update.iter() {
                            let mut status = status_map.get_mut(&id).unwrap();
                            status.online = *alive;
                        }
                    }
                    if backedin_members.len() + outdated_members.len() > 0 {
                        service_clone.update_raft(&backedin_members, &outdated_members);
                    }
                }
                thread::sleep(std_time::Duration::from_secs(1));
            }
        });
        let mut membership_service = Membership {
            heartbeat: service.clone(),
            groups: HashMap::new(),
            members: HashMap::new(),
            callback: None,
            version: 0,
        };
        membership_service.init_callback(raft_service);
        raft_service.register_state_machine(Box::new(membership_service));
        server.register_service(DEFAULT_SERVICE_ID, &service);
    }
    fn compose_client_member(&self, id: u64) -> ClientMember {
        let member = self.members.get(&id).unwrap();
        let stat_map = self.heartbeat.status.read();
        ClientMember {
            id,
            address: member.address.clone(),
            online: stat_map.get(&id).unwrap().online,
        }
    }
    fn init_callback(&mut self, raft_service: &Arc<RaftService>) {
        self.callback = Some(SMCallback::new(self.id(), raft_service.clone()));
    }
    fn notify_for_member_online(&self, id: u64) {
        let client_member = self.compose_client_member(id);
        let version = self.version;
        cb_notify(
            &self.callback,
            commands::on_any_member_online::new(),
            || Ok((client_member.clone(), version)),
        );
        if let Some(ref member) = self.members.get(&id) {
            for group in &member.groups {
                cb_notify(
                    &self.callback,
                    commands::on_group_member_online::new(group),
                    || Ok((client_member.clone(), version)),
                );
            }
        }
    }
    fn notify_for_member_offline(&self, id: u64) {
        let client_member = self.compose_client_member(id);
        let version = self.version;
        cb_notify(
            &self.callback,
            commands::on_any_member_offline::new(),
            || Ok((client_member.clone(), version)),
        );
        if let Some(ref member) = self.members.get(&id) {
            for group in &member.groups {
                cb_notify(
                    &self.callback,
                    commands::on_group_member_offline::new(group),
                    || Ok((client_member.clone(), version)),
                );
            }
        }
    }
    fn notify_for_member_left(&self, id: u64) {
        let client_member = self.compose_client_member(id);
        let version = self.version;
        cb_notify(&self.callback, commands::on_any_member_left::new(), || {
            Ok((client_member.clone(), version))
        });
        if let Some(ref member) = self.members.get(&id) {
            for group in &member.groups {
                self.notify_for_group_member_left(*group, &client_member)
            }
        }
    }
    fn notify_for_group_member_left(&self, group: u64, member: &ClientMember) {
        cb_notify(
            &self.callback,
            commands::on_group_member_left::new(&group),
            || Ok((member.clone(), self.version)),
        );
    }
    fn leave_group_(&mut self, group_id: u64, id: u64, need_notify: bool) -> Result<(), ()> {
        let mut success = false;
        let client_member: Option<ClientMember> = None;
        if let Some(ref mut group) = self.groups.get_mut(&group_id) {
            if let Some(ref mut member) = self.members.get_mut(&id) {
                group.members.remove(&id);
                member.groups.remove(&group_id);
                success = true;
            }
        }
        if success {
            if need_notify {
                self.notify_for_group_member_left(group_id, &self.compose_client_member(id));
            }
            self.group_leader_candidate_unavailable(group_id, id);
            return Ok(());
        } else {
            return Err(());
        }
    }
    fn member_groups(&self, member: u64) -> Option<HashSet<u64>> {
        if let Some(member) = self.members.get(&member) {
            Some(member.groups.clone())
        } else {
            None
        }
    }
    fn group_first_online_member_id(&self, group: u64) -> Result<Option<u64>, ()> {
        if let Some(group) = self.groups.get(&group) {
            let stat_map = self.heartbeat.status.read();
            for member in group.members.iter() {
                if let Some(member_stat) = stat_map.get(&member) {
                    if member_stat.online {
                        return Ok(Some(*member));
                    }
                }
            }
            Ok(None)
        } else {
            Err(())
        }
    }
    fn change_leader(&mut self, group_id: u64, new: Option<u64>) -> Result<(), ()> {
        let mut old: Option<u64> = None;
        let mut changed = false;
        if let Some(mut group) = self.groups.get_mut(&group_id) {
            old = group.leader;
            if old != new {
                group.leader = new;
                changed = true;
            }
        }
        if changed {
            let convert = |id_opt| {
                if let Some(id_opt) = id_opt {
                    Some(self.compose_client_member(id_opt))
                } else {
                    None
                }
            };
            cb_notify(
                &self.callback,
                commands::on_group_leader_changed::new(&group_id),
                || Ok((convert(old), convert(new), self.version)),
            );
            Ok(())
        } else {
            Err(())
        }
    }
    fn group_leader_candidate_available(&mut self, group_id: u64, member: u64) {
        // if the group does not have a leader, assign the member
        let mut leader_changed = false;
        if let Some(group) = self.groups.get_mut(&group_id) {
            if group.leader == None {
                leader_changed = true;
            }
        }
        if leader_changed {
            self.change_leader(group_id, Some(member));
        }
    }
    fn group_leader_candidate_unavailable(&mut self, group_id: u64, member: u64) {
        // if the group have a leader that is the same as the member, reelect
        let mut reelected = false;
        if let Some(group) = self.groups.get_mut(&group_id) {
            if group.leader == Some(member) {
                reelected = true;
            }
        }
        if reelected {
            let online_id = self.group_first_online_member_id(group_id).unwrap();
            self.change_leader(group_id, online_id);
        }
    }
    fn leader_candidate_available(&mut self, member: u64) {
        if let Some(groups) = self.member_groups(member) {
            for group in groups {
                self.group_leader_candidate_available(group, member)
            }
        }
    }
    fn leader_candidate_unavailable(&mut self, member: u64) {
        if let Some(groups) = self.member_groups(member) {
            for group in groups {
                self.group_leader_candidate_unavailable(group, member)
            }
        }
    }
}

impl StateMachineCmds for Membership {
    fn hb_online_changed(&mut self, online: Vec<u64>, offline: Vec<u64>) -> Result<(), ()> {
        self.version += 1;
        {
            let mut stat_map = self.heartbeat.status.write();
            for id in &online {
                if let Some(ref mut stat) = stat_map.get_mut(&id) {
                    stat.online = true;
                }
            }
            for id in &offline {
                if let Some(ref mut stat) = stat_map.get_mut(&id) {
                    stat.online = false;
                }
            }
        }
        for id in online {
            self.notify_for_member_online(id);
            self.leader_candidate_available(id);
        }
        for id in offline {
            self.notify_for_member_offline(id);
            self.leader_candidate_unavailable(id);
        }
        Ok(())
    }
    fn join(&mut self, address: String) -> Result<u64, ()> {
        self.version += 1;
        let id = hash_str(&address);
        let mut joined = false;
        {
            let mut stat_map = self.heartbeat.status.write();
            self.members.entry(id).or_insert_with(|| {
                let current_time = time::get_time();
                let mut stat = stat_map.entry(id).or_insert_with(|| HBStatus {
                    online: true,
                    last_updated: current_time,
                });
                stat.online = true;
                stat.last_updated = current_time;
                joined = true;
                Member {
                    id,
                    address: address.clone(),
                    groups: HashSet::new(),
                }
            });
        }
        if joined {
            cb_notify(
                &self.callback,
                commands::on_any_member_joined::new(),
                || Ok((self.compose_client_member(id), self.version)),
            );
            Ok(id)
        } else {
            Err(())
        }
    }
    fn leave(&mut self, id: u64) -> Result<(), ()> {
        if !self.members.contains_key(&id) {
            return Err(());
        };
        self.version += 1;
        let mut groups: Vec<u64> = Vec::new();
        if let Some(member) = self.members.get(&id) {
            for group in &member.groups {
                groups.push(*group);
            }
        }
        self.notify_for_member_left(id);
        for group_id in groups {
            self.leave_group_(group_id, id, false);
        }
        // in this part we will not do leader_candidate_unavailable
        // because it have already been triggered by leave_group_
        // in the loop above
        {
            let mut stat_map = self.heartbeat.status.write();
            stat_map.remove(&id);
        }
        self.members.remove(&id);
        Ok(())
    }
    fn join_group(&mut self, group_name: String, id: u64) -> Result<(), ()> {
        let group_id = hash_str(&group_name);
        self.version += 1;
        let mut success = false;
        if !self.groups.contains_key(&group_id) {
            self.new_group(group_name);
        } // create group if not exists
        if let Some(ref mut group) = self.groups.get_mut(&group_id) {
            if let Some(ref mut member) = self.members.get_mut(&id) {
                group.members.insert(id);
                member.groups.insert(group_id);
                success = true;
            }
        }
        if success {
            cb_notify(
                &self.callback,
                commands::on_group_member_joined::new(&group_id),
                || Ok((self.compose_client_member(id), self.version)),
            );
            self.group_leader_candidate_available(group_id, id);
            return Ok(());
        } else {
            return Err(());
        }
    }
    fn leave_group(&mut self, group_id: u64, id: u64) -> Result<(), ()> {
        self.version += 1;
        self.leave_group_(group_id, id, true)
    }
    fn new_group(&mut self, name: String) -> Result<u64, u64> {
        self.version += 1;
        let id = hash_str(&name);
        let mut inserted = false;
        self.groups.entry(id).or_insert_with(|| {
            inserted = true;
            MemberGroup {
                name,
                id,
                members: BTreeSet::new(),
                leader: None,
            }
        });
        if inserted {
            Ok(id)
        } else {
            Err(id)
        }
    }
    fn del_group(&mut self, id: u64) -> Result<(), ()> {
        self.version += 1;
        let mut members: Option<BTreeSet<u64>> = None;
        if let Some(group) = self.groups.get(&id) {
            members = Some(group.members.clone());
        }
        if let Some(members) = members {
            for member_id in members {
                if let Some(ref mut member) = self.members.get_mut(&member_id) {
                    member.groups.remove(&id);
                }
            }
            self.groups.remove(&id);
            return Ok(());
        } else {
            return Err(());
        }
    }
    fn group_leader(&self, group_id: u64) -> Result<(Option<ClientMember>, u64), ()> {
        if let Some(group) = self.groups.get(&group_id) {
            Ok((
                match group.leader {
                    Some(id) => Some(self.compose_client_member(id)),
                    None => None,
                },
                self.version,
            ))
        } else {
            Err(())
        }
    }
    fn group_members(&self, group: u64, online_only: bool) -> Result<(Vec<ClientMember>, u64), ()> {
        if let Some(group) = self.groups.get(&group) {
            Ok((
                group
                    .members
                    .iter()
                    .map(|id| self.compose_client_member(*id))
                    .filter(|member| !online_only || member.online)
                    .collect(),
                self.version,
            ))
        } else {
            Err(())
        }
    }
    fn all_members(&self, online_only: bool) -> Result<(Vec<ClientMember>, u64), ()> {
        Ok((
            self.members
                .iter()
                .map(|(id, _)| self.compose_client_member(*id))
                .filter(|member| !online_only || member.online)
                .collect(),
            self.version,
        ))
    }
}
impl StateMachineCtl for Membership {
    raft_sm_complete!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        //Some(serialize!(&self.map))
        None // TODO: Backup members
    }
    fn recover(&mut self, data: Vec<u8>) {
        //self.map = deserialize!(&data);
    }
    fn id(&self) -> u64 {
        DEFAULT_SERVICE_ID
    }
}
