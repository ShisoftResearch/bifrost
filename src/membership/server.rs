use super::heartbeat_rpc::*;
use super::raft::*;
use super::*;
use crate::membership::client::Member as ClientMember;
use crate::raft::state_machine::callback::server::{notify as cb_notify, SMCallback};
use crate::raft::state_machine::StateMachineCtl;
use crate::raft::{LogEntry, PlaneId, RaftMsg, RaftService, Service as raft_svr_trait};
use crate::rpc::Server;
use crate::utils::time;
use crate::utils::time::get_time;
use bifrost_hasher::hash_str;
use futures::prelude::future::*;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use lightning::map::Map;
use lightning::map::PtrHashMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time as std_time;
use tokio::time as async_time;

static MAX_TIMEOUT: i64 = 10_000; // 10 secs timeout before considering a member potentially offline
static OFFLINE_GRACE_CHECKS: u8 = 3; // Number of consecutive timeout checks before marking offline
static ONLINE_GRACE_CHECKS: u8 = 2; // Number of consecutive successful checks before marking back online
static MIN_STATE_CHANGE_INTERVAL: i64 = 5_000; // Minimum 5 seconds between state changes (anti-flapping)

#[derive(Clone, Copy)]
struct HBStatus {
    last_updated: i64,
    online: bool,
    consecutive_failures: u8, // Count of consecutive timeout checks while supposedly online
    consecutive_successes: u8, // Count of consecutive successful checks while supposedly offline
    last_state_change: i64,   // Timestamp of last online/offline state change
}

pub struct HeartbeatService {
    status: PtrHashMap<u64, HBStatus>,
    raft_service: Weak<RaftService>,
    closed: AtomicBool,
    was_leader: AtomicBool,
    watcher_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Service for HeartbeatService {
    fn ping(&self, id: u64) -> BoxFuture<PingReply> {
        async move {
            // Only the raft leader keeps the liveness of members. A follower
            // says so and names the leader it knows, so the sender can route
            // the next ping there (the sender's cache of the leader can be
            // stale after a leader change). The check reads one atomic.
            match self.raft_service.upgrade() {
                Some(raft_service) if raft_service.is_leader() => {}
                Some(raft_service) => {
                    return PingReply::NotLeader {
                        leader_hint: raft_service.leader_id().await,
                    }
                }
                None => return PingReply::NotLeader { leader_hint: 0 },
            }
            let current_time = time::get_time();
            // Update existing status or create new one
            let old_status = self.status.get(&id);
            let new_status = if let Some(mut status) = old_status {
                let elapsed = current_time - status.last_updated;
                status.last_updated = current_time;
                // Reset failure counter on successful ping, but keep other fields
                status.consecutive_failures = 0;
                if !status.online {
                    // Member is recovering, increment success counter
                    status.consecutive_successes += 1;
                }
                trace!("Updated heartbeat for member {}, elapsed {}ms", id, elapsed);
                status
            } else {
                // First time seeing this member
                trace!("First heartbeat from member {}", id);
                HBStatus {
                    online: true,
                    last_updated: current_time,
                    consecutive_failures: 0,
                    consecutive_successes: 0,
                    last_state_change: current_time,
                }
            };
            self.status.insert(id, new_status);
            PingReply::Accepted
        }
        .boxed()
    }
}
impl HeartbeatService {
    async fn update_raft(
        &self,
        raft_service: &Arc<RaftService>,
        online: &Vec<u64>,
        offline: &Vec<u64>,
    ) {
        let log = commands::hb_online_changed::new(online, offline);
        // Encode to state machine command
        let (fn_id, _, data) = log.encode();
        raft_service
            .c_command(
                PlaneId::type1(),
                LogEntry {
                    id: 0,
                    term: 0,
                    sm_id: DEFAULT_SERVICE_ID,
                    fn_id,
                    data,
                },
            )
            .await;
    }
    async fn transfer_leadership(&self) {
        //update timestamp for every alive server to give them a grace period
        let all_entries = self.status.entries();
        let current_time = get_time();
        let mut online_count = 0;
        for (id, mut stat) in all_entries {
            if stat.online {
                stat.last_updated = current_time;
                // Reset counters to give all members a fresh start under new leader
                stat.consecutive_failures = 0;
                stat.consecutive_successes = 0;
                self.status.insert(id, stat);
                online_count += 1;
            }
        }
        info!(
            "Leadership transferred, reset heartbeat status for {} online members",
            online_count
        );
    }

    pub async fn shutdown(&self) {
        info!("Shutting down heartbeat service");
        self.closed.store(true, Ordering::Relaxed);

        // Wait for the watcher task to complete
        match self.watcher_handle.lock() {
            Ok(mut guard) => {
                if let Some(handle) = guard.take() {
                    let _ = handle.await;
                }
            }
            Err(e) => {
                error!(
                    "Failed to acquire watcher handle lock during shutdown: {}",
                    e
                );
            }
        }
    }
}
dispatch_rpc_service_functions!(HeartbeatService);
service_with_id!(HeartbeatService, DEFAULT_SERVICE_ID);
/// A member of the roster. `online` is part of the replicated state: it
/// changes only through the logged command `hb_online_changed` (and `join`),
/// so a node that replays the log or loads a snapshot rebuilds it exactly.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Member {
    pub address: String,
    pub groups: HashSet<u64>,
    pub online: bool,
}

/// The persisted image of the roster. The heartbeat timers are not part of it:
/// they are local and start at the time of recovery.
#[derive(Serialize, Deserialize)]
struct MembershipImage {
    members: BTreeMap<u64, Member>,
    groups: BTreeMap<u64, MemberGroup>,
    version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MemberGroup {
    members: BTreeSet<u64>,
    leader: Option<u64>,
    name: String,
}

/// Membership service manages member groups and heartbeat status.
///
/// The roster (members, groups, version and the `online` flag of each member)
/// is replicated state: its log entries stay in the WAL and its snapshot holds
/// the roster, so a restarted node rebuilds it from the log. Only the heartbeat
/// timers are local; they start at the time of recovery, and a new leader gives
/// every member a grace period.
pub struct Membership {
    heartbeat: Arc<HeartbeatService>,
    groups: BTreeMap<u64, MemberGroup>,
    members: BTreeMap<u64, Member>,
    callback: Option<SMCallback>,
    version: u64,
}
impl Drop for Membership {
    fn drop(&mut self) {
        self.heartbeat.closed.store(true, Ordering::Relaxed)
    }
}

impl Membership {
    /// Replace the roster by a persisted image. The heartbeat timers start now.
    fn restore_image(
        &mut self,
        data: &Vec<u8>,
    ) -> Result<(), crate::raft::state_machine::RecoverError> {
        let image = crate::utils::serde::deserialize::<MembershipImage>(data).ok_or_else(|| {
            crate::raft::state_machine::RecoverError {
                sm_id: DEFAULT_SERVICE_ID,
                reason: format!("the roster image ({} bytes) does not decode", data.len()),
            }
        })?;
        self.members = image.members;
        self.groups = image.groups;
        self.version = image.version;
        let now = time::get_time();
        for (id, _) in self.heartbeat.status.entries() {
            self.heartbeat.status.remove(&id);
        }
        for (id, member) in self.members.iter() {
            self.heartbeat.status.insert(
                *id,
                HBStatus {
                    last_updated: now,
                    online: member.online,
                    consecutive_failures: 0,
                    consecutive_successes: 0,
                    last_state_change: now,
                },
            );
        }
        Ok(())
    }

    /// Creates a new Membership service with an empty roster. The roster is
    /// filled by log replay or by the snapshot at recovery, and by commands
    /// afterwards. Register it before the raft service starts.
    pub async fn new(server: &Arc<Server>, raft_service: &Arc<RaftService>) {
        let service = Arc::new(HeartbeatService {
            status: PtrHashMap::with_capacity(32),
            closed: AtomicBool::new(false),
            raft_service: Arc::downgrade(raft_service),
            was_leader: AtomicBool::new(false),
            watcher_handle: std::sync::Mutex::new(None),
        });
        let service_clone = service.clone();
        let service_for_task = service.clone();
        let handle = raft_service.rt.spawn(async move {
            info!("Starting membership heartbeat watcher");
            while !service_for_task.closed.load(Ordering::Relaxed) {
                let service = &service_for_task;
                let raft_service = match service.raft_service.upgrade() {
                    Some(raft_service) => raft_service,
                    None => break,
                };
                let start_time = get_time();
                let is_leader = raft_service.is_leader();
                let was_leader = service.was_leader.load(Ordering::Relaxed);
                if !was_leader && is_leader {
                    // Transferred leader will skip checking all member timeout for once
                    service.transfer_leadership().await
                }
                if was_leader != is_leader {
                    service.was_leader.store(is_leader, Ordering::Relaxed);
                }
                if is_leader {
                    trace!("Resync Membership as leader id {}", raft_service.id);
                    // The leader needs no RPC to know that it is alive.
                    if let Some(mut own) = service.status.get(&raft_service.id) {
                        own.last_updated = start_time;
                        own.consecutive_failures = 0;
                        service.status.insert(raft_service.id, own);
                    }
                    let mut outdated_members: Vec<u64> = Vec::new();
                    let mut back_in_members: Vec<u64> = Vec::new();
                    {
                        let all_entries = service.status.entries();
                        let mut members_to_update = vec![];
                        for (id, mut status) in all_entries {
                            let last_updated = status.last_updated;
                            let alive = (start_time < last_updated)
                                || ((start_time - last_updated) < MAX_TIMEOUT);
                            let time_since_last_change = start_time - status.last_state_change;

                            // Finding new offline servers (with grace period)
                            if status.online && !alive {
                                status.consecutive_failures += 1;
                                status.consecutive_successes = 0;

                                // Only mark offline after multiple consecutive failures AND minimum interval
                                if status.consecutive_failures >= OFFLINE_GRACE_CHECKS
                                    && time_since_last_change >= MIN_STATE_CHANGE_INTERVAL {
                                    warn!(
                                        "Marking member {} as offline after {} consecutive timeout checks ({}ms since last update)",
                                        id, status.consecutive_failures, start_time - last_updated
                                    );
                                    status.online = false;
                                    status.last_state_change = start_time;
                                    status.consecutive_failures = 0;
                                    outdated_members.push(id);
                                } else {
                                    debug!(
                                        "Member {} timeout check {}/{} ({}ms since last update, {}ms since last state change)",
                                        id, status.consecutive_failures, OFFLINE_GRACE_CHECKS,
                                        start_time - last_updated, time_since_last_change
                                    );
                                }
                                members_to_update.push((id, status));
                            }
                            // Finding new online servers (with grace period)
                            else if !status.online && alive {
                                status.consecutive_successes += 1;
                                status.consecutive_failures = 0;

                                // Only mark online after multiple consecutive successes AND minimum interval
                                if status.consecutive_successes >= ONLINE_GRACE_CHECKS
                                    && time_since_last_change >= MIN_STATE_CHANGE_INTERVAL {
                                    info!(
                                        "Marking member {} as back online after {} consecutive successful checks",
                                        id, status.consecutive_successes
                                    );
                                    status.online = true;
                                    status.last_state_change = start_time;
                                    status.consecutive_successes = 0;
                                    back_in_members.push(id);
                                } else {
                                    debug!(
                                        "Member {} recovery check {}/{} ({}ms since last state change)",
                                        id, status.consecutive_successes, ONLINE_GRACE_CHECKS,
                                        time_since_last_change
                                    );
                                }
                                members_to_update.push((id, status));
                            }
                            // Member is consistently online or offline
                            else if alive {
                                // Member is online and responsive - reset counters
                                if status.consecutive_failures > 0 || status.consecutive_successes > 0 {
                                    status.consecutive_failures = 0;
                                    status.consecutive_successes = 0;
                                    members_to_update.push((id, status));
                                }
                            }
                        }
                        for (id, s) in members_to_update {
                            service.status.insert(id, s);
                        }
                    }
                    if back_in_members.len() + outdated_members.len() > 0 {
                        debug!(
                            "Update member state machine for {} online, {} offline",
                            back_in_members.len(),
                            outdated_members.len()
                        );
                        service
                            .update_raft(
                                &raft_service,
                                &back_in_members,
                                &outdated_members,
                            )
                            .await;
                    }
                }
                let end_time = get_time();
                let time_took = end_time - start_time;
                let interval = 500; // in ms
                drop(raft_service);
                if time_took < interval {
                    let time_to_wait = interval - time_took;
                    trace!(
                        "Membership resync completed, waiting for {}ms for next resync",
                        time_to_wait
                    );
                    async_time::sleep(std_time::Duration::from_millis(time_to_wait as u64)).await
                } else {
                    trace!(
                        "Membership resync completed, left behine {}ms for next resync",
                        time_took - interval
                    );
                }
            }
            info!("Membership heartbeat watcher stopped gracefully");
        });

        // Store the handle for graceful shutdown
        match service.watcher_handle.lock() {
            Ok(mut guard) => {
                *guard = Some(handle);
            }
            Err(e) => {
                error!(
                    "Failed to acquire watcher handle lock during initialization: {}",
                    e
                );
            }
        }

        // Create membership service with EMPTY state.
        // It will learn all membership from the network through:
        // 1. join() commands as members join
        // 2. ping() heartbeats for liveness tracking
        // 3. Group operations (join_group, leave_group, etc.)
        let mut membership_service = Membership {
            heartbeat: service_clone.clone(),
            groups: BTreeMap::new(),
            members: BTreeMap::new(),
            callback: None,
            version: 0,
        };
        membership_service.init_callback(raft_service).await;
        raft_service
            .register_state_machine(Box::new(membership_service))
            .await;
        server.register_service(&service_clone).await;
    }
    async fn compose_client_member(&self, id: u64) -> Option<ClientMember> {
        let member = self.members.get(&id)?;
        Some(ClientMember {
            id,
            address: member.address.clone(),
            online: member.online,
        })
    }
    async fn init_callback(&mut self, raft_service: &Arc<RaftService>) {
        self.callback = Some(SMCallback::new(self.id(), raft_service.clone()).await);
    }
    async fn notify_for_member_online(&self, id: u64) {
        debug!("Notifying member {} online", id);
        let client_member = match self.compose_client_member(id).await {
            Some(member) => member,
            None => {
                error!(
                    "Failed to compose client member {} for online notification",
                    id
                );
                return;
            }
        };
        let version = self.version;
        cb_notify(
            &self.callback,
            commands::on_any_member_online::new(),
            || (client_member.clone(), version),
        )
        .await;
        if let Some(ref member) = self.members.get(&id) {
            for group in &member.groups {
                cb_notify(
                    &self.callback,
                    commands::on_group_member_online::new(group),
                    || (client_member.clone(), version),
                )
                .await;
            }
        }
    }
    async fn notify_for_member_offline(&self, id: u64) {
        debug!("Notifying member {} offline", id);
        let client_member = match self.compose_client_member(id).await {
            Some(member) => member,
            None => {
                error!(
                    "Failed to compose client member {} for offline notification",
                    id
                );
                return;
            }
        };
        let version = self.version;
        cb_notify(
            &self.callback,
            commands::on_any_member_offline::new(),
            || (client_member.clone(), version),
        )
        .await;
        if let Some(ref member) = self.members.get(&id) {
            for group in &member.groups {
                cb_notify(
                    &self.callback,
                    commands::on_group_member_offline::new(group),
                    || (client_member.clone(), version),
                )
                .await;
            }
        }
    }
    async fn notify_for_member_left(&self, id: u64) {
        debug!("Notifying member {} left", id);
        let client_member = match self.compose_client_member(id).await {
            Some(member) => member,
            None => {
                error!(
                    "Failed to compose client member {} for left notification",
                    id
                );
                return;
            }
        };
        let version = self.version;
        cb_notify(&self.callback, commands::on_any_member_left::new(), || {
            (client_member.clone(), version)
        })
        .await;
        if let Some(ref member) = self.members.get(&id) {
            for group in &member.groups {
                self.notify_for_group_member_left(*group, &client_member)
                    .await
            }
        }
    }
    async fn notify_for_group_member_left(&self, group: u64, member: &ClientMember) {
        debug!("Notifying member {:?} left group {}", member, group);
        cb_notify(
            &self.callback,
            commands::on_group_member_left::new(&group),
            || (member.clone(), self.version),
        )
        .await;
    }
    async fn leave_group_(&mut self, group_id: u64, id: u64, need_notify: bool) -> bool {
        let mut success = false;
        if let Some(ref mut group) = self.groups.get_mut(&group_id) {
            if let Some(ref mut member) = self.members.get_mut(&id) {
                group.members.remove(&id);
                member.groups.remove(&group_id);
                success = true;
            }
        }
        if success {
            if need_notify {
                if let Some(client_member) = self.compose_client_member(id).await {
                    self.notify_for_group_member_left(group_id, &client_member)
                        .await;
                } else {
                    error!(
                        "Failed to compose client member {} for group {} leave notification",
                        id, group_id
                    );
                }
            }
            self.group_leader_candidate_unavailable(group_id, id).await;
            true
        } else {
            false
        }
    }
    fn member_groups(&self, member: u64) -> Option<HashSet<u64>> {
        if let Some(member) = self.members.get(&member) {
            Some(member.groups.clone())
        } else {
            None
        }
    }
    async fn group_first_online_member_id(&self, group: u64) -> Result<Option<u64>, ()> {
        if let Some(group) = self.groups.get(&group) {
            for member in group.members.iter() {
                if self.members.get(member).map_or(false, |m| m.online) {
                    return Ok(Some(*member));
                }
            }
            Ok(None)
        } else {
            Err(())
        }
    }
    async fn change_leader(&mut self, group_id: u64, new: Option<u64>) -> Result<(), ()> {
        let mut old: Option<u64> = None;
        let mut changed = false;
        if let Some(group) = self.groups.get_mut(&group_id) {
            old = group.leader;
            if old != new {
                group.leader = new;
                changed = true;
            }
        }
        if changed {
            let version = self.version;
            let old_leader = if let Some(id_opt) = old {
                self.compose_client_member(id_opt).await
            } else {
                None
            };
            let new_leader = if let Some(id_opt) = new {
                self.compose_client_member(id_opt).await
            } else {
                None
            };
            cb_notify(
                &self.callback,
                commands::on_group_leader_changed::new(&group_id),
                move || (old_leader, new_leader, version),
            )
            .await;
            Ok(())
        } else {
            Err(())
        }
    }
    async fn group_leader_candidate_available(&mut self, group_id: u64, member: u64) {
        // if the group does not have a leader, assign the available member
        let mut leader_changed = false;
        if let Some(group) = self.groups.get_mut(&group_id) {
            if group.leader == None {
                leader_changed = true;
            }
        }
        if leader_changed {
            if let Err(_) = self.change_leader(group_id, Some(member)).await {
                error!(
                    "Failed to change leader for group {} to member {}",
                    group_id, member
                );
            }
        }
    }
    async fn group_leader_candidate_unavailable(&mut self, group_id: u64, member: u64) {
        // if the group have a leader that is the same as the member, reelect
        let mut reelected = false;
        if let Some(group) = self.groups.get_mut(&group_id) {
            if group.leader == Some(member) {
                reelected = true;
            }
        }
        if reelected {
            match self.group_first_online_member_id(group_id).await {
                Ok(online_id) => {
                    if let Err(_) = self.change_leader(group_id, online_id).await {
                        error!("Failed to change leader for group {} after member {} became unavailable", group_id, member);
                    }
                }
                Err(_) => {
                    error!("Failed to find online member for group {} after member {} became unavailable", group_id, member);
                }
            }
        }
    }
    async fn leader_candidate_available(&mut self, member: u64) {
        if let Some(groups) = self.member_groups(member) {
            for group in groups {
                self.group_leader_candidate_available(group, member).await
            }
        }
    }
    async fn leader_candidate_unavailable(&mut self, member: u64) {
        if let Some(groups) = self.member_groups(member) {
            for group in groups {
                self.group_leader_candidate_unavailable(group, member).await
            }
        }
    }
}

impl StateMachineCmds for Membership {
    fn hb_online_changed(&mut self, online: Vec<u64>, offline: Vec<u64>) -> BoxFuture<()> {
        debug!(
            "Member status changed, back online  {}, gone offline {}",
            online.len(),
            offline.len()
        );
        async move {
            self.version += 1;
            let current_time = time::get_time();
            {
                for id in &online {
                    if let Some(member) = self.members.get_mut(id) {
                        member.online = true;
                    }
                    if let Some(mut stat) = self.heartbeat.status.get(&id) {
                        stat.online = true;
                        stat.last_state_change = current_time;
                        // Reset counters after state change is confirmed
                        stat.consecutive_failures = 0;
                        stat.consecutive_successes = 0;
                        self.heartbeat.status.insert(*id, stat);
                    }
                }
                for id in &offline {
                    if let Some(member) = self.members.get_mut(id) {
                        member.online = false;
                    }
                    if let Some(mut stat) = self.heartbeat.status.get(&id) {
                        stat.online = false;
                        stat.last_state_change = current_time;
                        // Reset counters after state change is confirmed
                        stat.consecutive_failures = 0;
                        stat.consecutive_successes = 0;
                        self.heartbeat.status.insert(*id, stat);
                    }
                }
            }
            for id in online {
                self.notify_for_member_online(id).await;
                self.leader_candidate_available(id).await;
            }
            for id in offline {
                self.notify_for_member_offline(id).await;
                self.leader_candidate_unavailable(id).await;
            }
        }
        .boxed()
    }
    fn join(&mut self, address: String) -> BoxFuture<Option<u64>> {
        async move {
            self.version += 1;
            let id = hash_str(&address);
            let mut joined = false;
            {
                let current_time = time::get_time();
                let member = self.members.entry(id).or_insert_with(|| {
                    joined = true;
                    Member {
                        address: address.clone(),
                        groups: HashSet::new(),
                        online: true,
                    }
                });
                member.online = true;
                self.heartbeat.status.insert(
                    id,
                    HBStatus {
                        last_updated: current_time,
                        online: true,
                        consecutive_failures: 0,
                        consecutive_successes: 0,
                        last_state_change: current_time,
                    },
                );
            }
            if joined {
                match self.compose_client_member(id).await {
                    Some(composed_client_member) => {
                        cb_notify(
                            &self.callback,
                            commands::on_any_member_joined::new(),
                            || (composed_client_member, self.version),
                        )
                        .await;
                        Some(id)
                    }
                    None => {
                        error!("Failed to compose client member {} after join", id);
                        None
                    }
                }
            } else {
                None
            }
        }
        .boxed()
    }
    fn leave(&mut self, id: u64) -> BoxFuture<bool> {
        async move {
            if !self.members.contains_key(&id) {
                return false;
            };
            self.version += 1;
            let mut groups: Vec<u64> = Vec::new();
            if let Some(member) = self.members.get(&id) {
                for group in &member.groups {
                    groups.push(*group);
                }
            }
            self.notify_for_member_left(id).await;
            for group_id in groups {
                self.leave_group_(group_id, id, false).await;
            }
            // in this part we will not do leader_candidate_unavailable
            // because it have already been triggered by leave_group_
            // in the loop above
            self.heartbeat.status.remove(&id);
            self.members.remove(&id);
            true
        }
        .boxed()
    }
    fn join_group(&mut self, group_name: String, id: u64) -> BoxFuture<bool> {
        async move {
            let group_id = hash_str(&group_name);
            self.version += 1;
            let mut success = false;
            if !self.groups.contains_key(&group_id) {
                if let Err(existing_id) = self.new_group(group_name.clone()).await {
                    debug!(
                        "Group {} already exists with id {}",
                        group_name, existing_id
                    );
                }
            } // create group if not exists
            if let Some(ref mut group) = self.groups.get_mut(&group_id) {
                if let Some(ref mut member) = self.members.get_mut(&id) {
                    group.members.insert(id);
                    member.groups.insert(group_id);
                    success = true;
                }
            }
            if success {
                match self.compose_client_member(id).await {
                    Some(composed_member) => {
                        cb_notify(
                            &self.callback,
                            commands::on_group_member_joined::new(&group_id),
                            || (composed_member, self.version),
                        )
                        .await;
                        self.group_leader_candidate_available(group_id, id).await;
                        true
                    }
                    None => {
                        error!(
                            "Failed to compose client member {} for group {} join notification",
                            id, group_id
                        );
                        false
                    }
                }
            } else {
                false
            }
        }
        .boxed()
    }
    fn leave_group(&mut self, group_id: u64, id: u64) -> BoxFuture<bool> {
        async move {
            self.version += 1;
            self.leave_group_(group_id, id, true).await
        }
        .boxed()
    }
    fn new_group(&mut self, name: String) -> BoxFuture<Result<u64, u64>> {
        async move {
            self.version += 1;
            let id = hash_str(&name);
            let mut inserted = false;
            self.groups.entry(id).or_insert_with(|| {
                inserted = true;
                MemberGroup {
                    members: BTreeSet::new(),
                    leader: None,
                    name: name.clone(),
                }
            });
            if inserted {
                Ok(id)
            } else {
                Err(id)
            }
        }
        .boxed()
    }
    fn del_group(&mut self, id: u64) -> BoxFuture<bool> {
        async move {
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
                true
            } else {
                false
            }
        }
        .boxed()
    }
    fn group_leader(&self, group_id: u64) -> BoxFuture<Option<(Option<ClientMember>, u64)>> {
        async move {
            if let Some(group) = self.groups.get(&group_id) {
                Some((
                    match group.leader {
                        Some(id) => self.compose_client_member(id).await,
                        None => None,
                    },
                    self.version,
                ))
            } else {
                None
            }
        }
        .boxed()
    }
    fn group_members(
        &self,
        group: u64,
        online_only: bool,
    ) -> BoxFuture<Option<(Vec<ClientMember>, u64)>> {
        async move {
            if let Some(group) = self.groups.get(&group) {
                let futs: FuturesUnordered<_> = group
                    .members
                    .iter()
                    .map(|id| self.compose_client_member(*id))
                    .collect();
                let members: Vec<_> = futs.collect().await;
                Some((
                    members
                        .into_iter()
                        .filter_map(|member| member)
                        .filter(|member| !online_only || member.online)
                        .collect(),
                    self.version,
                ))
            } else {
                None
            }
        }
        .boxed()
    }
    fn all_members(&self, online_only: bool) -> BoxFuture<(Vec<ClientMember>, u64)> {
        async move {
            let futs: FuturesUnordered<_> = self
                .members
                .iter()
                .map(|(id, _)| self.compose_client_member(*id))
                .collect();
            let members: Vec<_> = futs.collect().await;
            (
                members
                    .into_iter()
                    .filter_map(|member| member)
                    .filter(|member| !online_only || member.online)
                    .collect(),
                self.version,
            )
        }
        .boxed()
    }
    fn all_groups(&self) -> BoxFuture<BTreeMap<u64, MemberGroup>> {
        future::ready(self.groups.clone()).boxed()
    }
}
impl StateMachineCtl for Membership {
    #[cfg(multisite_test)]
    fn msraft_diagnostic(&self) -> serde_json::Value {
        serde_json::json!({
            "members": self.members.iter().map(|(id, m)| serde_json::json!({
                "id": id, "address": m.address, "groups": m.groups,
                "online": self.heartbeat.status.get(id).map(|s| s.online),
                "last_updated": self.heartbeat.status.get(id).map(|s| s.last_updated),
            })).collect::<Vec<_>>(),
            "groups": self.groups, "version": self.version, "recoverable": self.recoverable()
        })
    }

    raft_sm_complete!();
    fn id(&self) -> u64 {
        DEFAULT_SERVICE_ID
    }
    fn snapshot(&self) -> Vec<u8> {
        crate::utils::serde::serialize(&MembershipImage {
            members: self.members.clone(),
            groups: self.groups.clone(),
            version: self.version,
        })
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        // Raft recovery calls `try_recover`; this path cannot report a failure.
        if let Err(e) = self.restore_image(&data) {
            error!("{}; roster unchanged", e);
        }
        future::ready(()).boxed()
    }
    fn try_recover(
        &mut self,
        data: Vec<u8>,
    ) -> BoxFuture<Result<(), crate::raft::state_machine::RecoverError>> {
        future::ready(self.restore_image(&data)).boxed()
    }
    fn recoverable(&self) -> bool {
        true
    }
}
