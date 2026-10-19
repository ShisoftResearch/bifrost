use futures::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::conshash::weights::client::SMClient as WeightSMClient;
use crate::conshash::weights::DEFAULT_SERVICE_ID;
use crate::membership::client::{Member, ObserverClient as MembershipClient};
use crate::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use crate::raft::state_machine::master::ExecError;
use crate::utils::serde::serialize;
use bifrost_hasher::{hash_bytes, hash_str};
use parking_lot::*;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const SYNC_ATTEMPTS: usize = 10;

pub mod fill;
pub mod slots;
pub mod weights;

#[derive(Debug)]
pub enum Action {
    Joined,
    Left,
}

#[derive(Debug)]
pub enum InitTableError {
    GroupNotExisted,
    /// The group reads older than the version that a notice or the caller
    /// needs, after the retries.
    StaleRead {
        read: u64,
        wanted: u64,
    },
    NoWeightService(ExecError),
    NoWeightGroup,
    NoWeightInfo,
    Unknown,
}

/// No source knows the address of a server. Names the server and every source
/// that was asked, with its answer.
#[derive(Debug)]
pub struct AddressSearchError {
    pub server_id: u64,
    pub tried: Vec<(&'static str, String)>,
}

impl std::fmt::Display for AddressSearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no address known for server id {}:", self.server_id)?;
        for (source, answer) in &self.tried {
            write!(f, " [{}: {}]", source, answer)?;
        }
        Ok(())
    }
}

impl std::error::Error for AddressSearchError {}

#[derive(Debug)]
pub enum CHError {
    WatchError(Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>),
    InitTableError(InitTableError),
}

struct LookupTables {
    nodes: Vec<u64>,
    addrs: HashMap<u64, String>,
}

#[derive(Default)]
struct SlotOverrides {
    owners: Option<Vec<u64>>,
    /// Raft log index that established each cached owner.
    versions: Vec<u64>,
    /// Greatest complete snapshot this cache has observed. It also versions
    /// slots that are absent from `owners`.
    snapshot_index: u64,
}

impl SlotOverrides {
    fn install(&mut self, incoming: Option<Vec<u64>>, applied_index: u64) -> bool {
        if applied_index < self.snapshot_index {
            return false;
        }
        self.snapshot_index = applied_index;

        match incoming {
            Some(incoming) => {
                let target_len = self
                    .owners
                    .as_ref()
                    .map_or(incoming.len(), |owners| owners.len().max(incoming.len()));
                let owners = self.owners.get_or_insert_with(|| vec![0; target_len]);
                owners.resize(target_len, 0);
                self.versions.resize(target_len, 0);

                for slot in 0..target_len {
                    if self.versions[slot] <= applied_index {
                        owners[slot] = incoming.get(slot).copied().unwrap_or(0);
                        self.versions[slot] = applied_index;
                    }
                }
            }
            None => {
                if let Some(owners) = self.owners.as_mut() {
                    self.versions.resize(owners.len(), 0);
                    for (owner, version) in owners.iter_mut().zip(self.versions.iter_mut()) {
                        if *version <= applied_index {
                            *owner = 0;
                            *version = applied_index;
                        }
                    }
                    if owners.iter().all(|owner| *owner == 0) {
                        self.owners = None;
                        self.versions.clear();
                    }
                }
            }
        }
        true
    }

    fn note_owner(&mut self, slot: u64, owner: u64, slot_count: usize, applied_index: u64) -> bool {
        if applied_index < self.snapshot_index {
            return false;
        }
        let slot = slot as usize;
        let required_len = slot_count.max(slot.saturating_add(1));
        let owners = self.owners.get_or_insert_with(|| vec![0; required_len]);
        owners.resize(required_len, 0);
        self.versions.resize(required_len, self.snapshot_index);
        if applied_index < self.versions[slot] {
            return false;
        }
        owners[slot] = owner;
        self.versions[slot] = applied_index;
        true
    }

    fn has_table(&self) -> bool {
        self.owners.is_some()
    }

    fn owner(&self, slot: u64) -> Option<u64> {
        self.owners
            .as_ref()
            .and_then(|owners| owners.get(slot as usize).copied())
            .filter(|owner| *owner != 0)
    }

    fn owner_with_index(&self, slot: u64) -> Option<(u64, u64)> {
        let slot = slot as usize;
        let owner = self
            .owners
            .as_ref()
            .and_then(|owners| owners.get(slot).copied())
            .filter(|owner| *owner != 0)?;
        let index = self
            .versions
            .get(slot)
            .copied()
            .unwrap_or(self.snapshot_index);
        Some((owner, index))
    }
}

pub struct ConsistentHashing {
    tables: RwLock<LookupTables>,
    /// Stored slot ownership that overrides the computed answer, indexed by slot.
    ///
    /// `None` means this group has no slot table and placement is purely derived,
    /// which is the behaviour every caller had before this existed. A zero entry
    /// means that one slot is unplaced and falls back to the ring, which is a
    /// different statement from "the table says nobody owns anything".
    ///
    /// It lives here, next to the derived table, for one reason: a system that
    /// stores data needs placement to have exactly **one** answer. Keeping the
    /// override in each caller instead means every code path that asks "who owns
    /// this?" is a separate chance to ask the wrong oracle — and a write and a
    /// read that disagree do not fail loudly, they silently lose the data.
    slot_overrides: RwLock<SlotOverrides>,
    membership: Arc<MembershipClient>,
    weight_sm_client: WeightSMClient,
    group_name: String,
    watchers: RwLock<Vec<Box<dyn Fn(&Member, &Action, &Vec<u64>, &Vec<u64>) + Send + Sync>>>,
    update_lock: async_std::sync::Mutex<()>,
    version: AtomicU64,
    num_addrs: AtomicUsize,
    /// The greatest membership version that a notice announced.
    wanted_version: AtomicU64,
    raft_client: Arc<RaftClient>,
    /// Address searches that no source could answer.
    address_search_failures: AtomicU64,
    /// Test hook: the previous successful read, and how many of the next
    /// reads are answered from it (a member that lags behind a notice).
    #[cfg(any(test, multisite_test))]
    last_read: Mutex<Option<(Vec<Member>, u64)>>,
    #[cfg(any(test, multisite_test))]
    stale_reads: AtomicUsize,
}

impl ConsistentHashing {
    pub async fn new_with_id(
        id: u64,
        group: &str,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        let ch = Arc::new(ConsistentHashing {
            tables: RwLock::new(LookupTables {
                nodes: Vec::new(),
                addrs: HashMap::new(),
            }),
            slot_overrides: RwLock::new(SlotOverrides::default()),
            membership: membership_client.clone(),
            weight_sm_client: WeightSMClient::new(id, &raft_client),
            group_name: group.to_string(),
            watchers: RwLock::new(Vec::new()),
            version: AtomicU64::new(0),
            update_lock: async_std::sync::Mutex::new(()),
            num_addrs: AtomicUsize::new(0),
            wanted_version: AtomicU64::new(0),
            raft_client: raft_client.clone(),
            address_search_failures: AtomicU64::new(0),
            #[cfg(any(test, multisite_test))]
            last_read: Mutex::new(None),
            #[cfg(any(test, multisite_test))]
            stale_reads: AtomicUsize::new(0),
        });
        {
            let ch = ch.clone();
            let res = membership_client
                .on_group_member_joined(
                    move |(member, version)| {
                        let ch = ch.clone();
                        server_joined(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        {
            let ch = ch.clone();
            let res = membership_client
                .on_group_member_online(
                    move |(member, version)| {
                        let ch = ch.clone();
                        server_joined(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        {
            let ch = ch.clone();
            let res = membership_client
                .on_group_member_left(
                    move |(member, version)| {
                        let ch = ch.clone();
                        server_left(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        {
            let ch = ch.clone();
            let res = membership_client
                .on_group_member_offline(
                    move |(member, version)| {
                        debug!(
                            "Dected server member {:?} offline at version {}",
                            member, version
                        );
                        let ch = ch.clone();
                        server_left(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        Self::spawn_reconcile(&ch);
        Ok(ch)
    }

    /// Heal a table that missed an event: every `RECONCILE_INTERVAL` the ring
    /// reads the group and installs a newer result. One task per ring object;
    /// it ends when the ring is dropped.
    fn spawn_reconcile(ch: &Arc<ConsistentHashing>) {
        let weak = Arc::downgrade(ch);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(RECONCILE_INTERVAL).await;
                let ch = match weak.upgrade() {
                    Some(ch) => ch,
                    None => break,
                };
                let before = ch.version.load(Ordering::Relaxed);
                if before == 0 {
                    // Never initialised: the owner of the ring has not asked
                    // for a table yet.
                    continue;
                }
                let old_online = ch.online_ids();
                match ch.init_table().await {
                    Ok(version) if version > before => {
                        warn!(
                            "Ring of group {} reconciled from version {} to {}",
                            ch.group_name, before, version
                        );
                        ch.notify_watchers_of_difference(old_online);
                    }
                    Ok(_) => {}
                    Err(e) => debug!("Ring reconcile of group {} failed: {:?}", ch.group_name, e),
                }
            }
        });
    }

    fn online_ids(&self) -> std::collections::BTreeSet<u64> {
        self.tables.read().nodes.iter().copied().collect()
    }

    /// Tell the watchers about members whose routing state changed without a
    /// notice (a reconcile found them).
    fn notify_watchers_of_difference(&self, old_online: std::collections::BTreeSet<u64>) {
        let new_nodes = self.tables.read().nodes.clone();
        let new_online: std::collections::BTreeSet<u64> = new_nodes.iter().copied().collect();
        let old_nodes: Vec<u64> = old_online.iter().copied().collect();
        let changed: Vec<(u64, bool)> = new_online
            .difference(&old_online)
            .map(|id| (*id, true))
            .chain(old_online.difference(&new_online).map(|id| (*id, false)))
            .collect();
        for (id, joined) in changed {
            let member = Member {
                id,
                address: self.try_server_name(id).unwrap_or_default(),
                online: joined,
            };
            let action = if joined { Action::Joined } else { Action::Left };
            for watch in self.watchers.read().iter() {
                watch(&member, &action, &new_nodes, &old_nodes);
            }
        }
    }

    /// The number of address searches that failed in every source.
    pub fn address_search_failures(&self) -> u64 {
        self.address_search_failures.load(Ordering::Relaxed)
    }

    /// The address of a server, searched in order: the ring (all members, also
    /// the offline ones), the roster of the membership service, and the raft
    /// configuration. Each remote source has a bound of one second. The error
    /// names the server and every source with its answer.
    pub async fn resolve_address(&self, server_id: u64) -> Result<String, AddressSearchError> {
        if let Some(address) = self.try_server_name(server_id) {
            return Ok(address);
        }
        let mut tried: Vec<(&'static str, String)> =
            vec![("ring", "the address table has no entry".to_string())];
        match tokio::time::timeout(Duration::from_secs(1), self.membership.all_members(false)).await
        {
            Ok(Ok((members, _))) => match members.iter().find(|m| m.id == server_id) {
                Some(member) => {
                    self.tables
                        .write()
                        .addrs
                        .insert(server_id, member.address.clone());
                    return Ok(member.address.clone());
                }
                None => tried.push(("roster", "the server is not in the roster".to_string())),
            },
            Ok(Err(e)) => tried.push(("roster", format!("read failed: {:?}", e))),
            Err(_) => tried.push(("roster", "read timed out after 1 s".to_string())),
        }
        match tokio::time::timeout(
            Duration::from_secs(1),
            self.raft_client
                .cluster_info_on_plane(crate::raft::PlaneId::type1()),
        )
        .await
        {
            Ok(Ok(info)) => {
                match info
                    .member_addresses()
                    .into_iter()
                    .find(|address| hash_str(address) == server_id)
                {
                    Some(address) => {
                        self.tables.write().addrs.insert(server_id, address.clone());
                        return Ok(address);
                    }
                    None => tried.push((
                        "raft configuration",
                        "the server is not a voter".to_string(),
                    )),
                }
            }
            Ok(Err(e)) => tried.push(("raft configuration", format!("read failed: {:?}", e))),
            Err(_) => tried.push(("raft configuration", "read timed out after 1 s".to_string())),
        }
        self.address_search_failures.fetch_add(1, Ordering::Relaxed);
        Err(AddressSearchError { server_id, tried })
    }

    /// The version of the membership group that the table reflects.
    pub fn table_version(&self) -> u64 {
        self.version.load(Ordering::Relaxed)
    }

    /// Read until the table reflects `wanted` or a later version. The group
    /// answers from any member, so a read right after a notice can be older
    /// than the notice (the answering member has applied fewer entries).
    pub async fn sync_to(&self, wanted: u64) -> Result<u64, InitTableError> {
        let mut delay = Duration::from_millis(100);
        let mut read = self.version.load(Ordering::Relaxed);
        for _ in 0..SYNC_ATTEMPTS {
            if read >= wanted {
                return Ok(read);
            }
            // Raise the client cursor to the leader's log position first: the
            // query gate then refuses answers from members that lag behind it.
            let _ = self.raft_client.refresh_leader(0).await;
            match self.init_table().await {
                Ok(version) => {
                    read = version;
                    if read >= wanted {
                        return Ok(read);
                    }
                }
                Err(InitTableError::StaleRead { read: stale, .. }) => read = stale,
                Err(e) => debug!("Ring read for version {} failed: {:?}", wanted, e),
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(2));
        }
        Err(InitTableError::StaleRead { read, wanted })
    }
    pub async fn new(
        group: &str,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_with_id(DEFAULT_SERVICE_ID, group, raft_client, membership_client).await
    }
    pub async fn new_client(
        group: &str,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_client_with_id(DEFAULT_SERVICE_ID, group, raft_client, membership_client).await
    }
    pub async fn new_client_with_id(
        id: u64,
        group: &str,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        match ConsistentHashing::new_with_id(id, group, raft_client, membership_client).await {
            Err(e) => Err(e),
            Ok(ch) => {
                // Read the group after the cursor reached the leader's log
                // position (a start has no notice to wait for).
                let _ = ch.raft_client.refresh_leader(0).await;
                match ch.init_table().await {
                    Err(e) => Err(CHError::InitTableError(e)),
                    Ok(_) => Ok(ch.clone()),
                }
            }
        }
    }
    /// The address of a member, or `None` if this ring has never seen it.
    ///
    /// Prefer this over [`Self::to_server_name`]. An unknown server id is a
    /// *routine* input, not a bug: a stored placement table can name a member that
    /// has since left the ring, an operator can ask to drain onto a member that is
    /// not there, and a stale watcher can hand over an id from a previous
    /// membership. All of those want an error the caller can handle.
    pub fn try_server_name(&self, server_id: u64) -> Option<String> {
        self.tables.read().addrs.get(&server_id).cloned()
    }

    /// **Panics** on an unknown server id. Prefer [`Self::try_server_name`].
    ///
    /// Kept for callers that have already established the member exists. It was
    /// once the only option, and being the only option meant a placement table
    /// naming a departed member took the whole process down instead of failing one
    /// request -- which is exactly what a stored placement table makes possible.
    pub fn to_server_name(&self, server_id: u64) -> String {
        match self.try_server_name(server_id) {
            Some(name) => name,
            None => panic!("Cannot find server name for server id {}", server_id),
        }
    }
    pub fn to_server_name_option(&self, server_id: Option<u64>) -> Option<String> {
        if let Some(sid) = server_id {
            let lookup_table = self.tables.read();
            lookup_table.addrs.get(&sid).cloned()
        } else {
            None
        }
    }
    pub fn get_server_id(&self, hash: u64) -> Option<u64> {
        let lookup_table = self.tables.read();
        let nodes = &lookup_table.nodes;
        let slot_count = nodes.len();
        if slot_count == 0 {
            return None;
        }
        let result = nodes.get(self.jump_hash(slot_count, hash));
        // trace!("Hash {} have been point to {:?}", hash, result);
        result.cloned()
    }
    /// Who owns a slot: the stored table if there is one, the ring otherwise.
    ///
    /// This is the call every consumer that stores or locates *data* should
    /// make, rather than `get_server_id`. The distinction is not stylistic:
    /// `get_server_id` answers "which member does this hash land on", which is
    /// the right question for stateless routing and the wrong one for a stored
    /// cell, because the answer changes the moment membership does.
    ///
    /// Deliberately falls back rather than failing, so a group that never seeded
    /// a table behaves exactly as it did before slot ownership existed.
    pub fn get_server_id_for_slot(&self, slot: u64) -> Option<u64> {
        if let Some(owner) = self.slot_overrides.read().owner(slot) {
            return Some(owner);
        }
        self.get_server_id(slot)
    }

    /// Install (or clear) a complete stored slot table at its applied Raft log
    /// index. A lagging snapshot is ignored, and a newer push for one slot is
    /// preserved even while the snapshot advances the rest of the table.
    pub fn set_slot_overrides(&self, owners: Option<Vec<u64>>, applied_index: u64) -> bool {
        self.slot_overrides.write().install(owners, applied_index)
    }

    /// Record one slot's owner without re-reading the whole table.
    ///
    /// For a caller that has just committed a placement change and therefore
    /// already knows the answer authoritatively. With no table installed this
    /// creates one holding just this slot, which is honest: every other slot
    /// stays 0, reads as unplaced, and falls back to the ring as before.
    pub fn note_slot_owner(
        &self,
        slot: u64,
        owner: u64,
        slot_count: usize,
        applied_index: u64,
    ) -> bool {
        self.slot_overrides
            .write()
            .note_owner(slot, owner, slot_count, applied_index)
    }

    /// Whether a stored table is installed at all.
    ///
    /// Callers that *refuse* work on the strength of the table have to check
    /// this first. With no table, placement is derived and members can
    /// legitimately disagree while the ring forms; refusing then converts a
    /// placement question into an availability failure.
    pub fn has_slot_overrides(&self) -> bool {
        self.slot_overrides.read().has_table()
    }

    /// The stored owner of a slot, without the ring fallback.
    ///
    /// `None` means either no table or no entry for that slot — in both cases
    /// the answer would have come from the ring, so a caller deciding whether it
    /// is *authoritatively* not the owner must treat it as "cannot tell" rather
    /// than as "somebody else".
    pub fn slot_override(&self, slot: u64) -> Option<u64> {
        self.slot_overrides.read().owner(slot)
    }

    /// The stored owner and the Raft log index that established it.
    pub fn slot_override_with_index(&self, slot: u64) -> Option<(u64, u64)> {
        self.slot_overrides.read().owner_with_index(slot)
    }

    pub fn jump_hash(&self, slot_count: usize, hash: u64) -> usize {
        let mut b: i64 = -1;
        let mut j: i64 = 0;
        let mut h = hash;
        while j < (slot_count as i64) {
            b = j;
            h = h.wrapping_mul(2862933555777941757).wrapping_add(1);
            j = (((b.wrapping_add(1)) as f64) * ((1i64 << 31) as f64)
                / (((h >> 33).wrapping_add(1)) as f64)) as i64;
        }
        // trace!(
        //     "Jump hash point to index {} for {}, with slots {}",
        //     b,
        //     hash,
        //     slot_count
        // );
        b as usize
    }
    pub fn get_server(&self, hash: u64) -> Option<String> {
        self.to_server_name_option(self.get_server_id(hash))
    }
    pub fn get_server_by_string(&self, string: &String) -> Option<String> {
        self.get_server(hash_str(string))
    }
    pub fn get_server_by<T>(&self, obj: &T) -> Option<String>
    where
        T: serde::Serialize,
    {
        self.get_server(hash_bytes(serialize(obj).as_slice()))
    }
    pub fn get_server_id_by_string(&self, string: &String) -> Option<u64> {
        self.get_server_id(hash_str(string))
    }
    pub fn get_server_id_by<T>(&self, obj: &T) -> Option<u64>
    where
        T: serde::Serialize,
    {
        self.get_server_id(hash_bytes(serialize(obj).as_slice()))
    }
    pub fn rand_server(&self) -> Option<String> {
        let rand = rand::random::<u64>();
        self.get_server(rand)
    }
    pub fn rand_server_id(&self) -> Option<u64> {
        let rand = rand::random::<u64>();
        self.get_server_id(rand)
    }
    pub fn nodes_count(&self) -> usize {
        let lookup_table = self.tables.read();
        return lookup_table.nodes.len();
    }
    pub fn server_count(&self) -> usize {
        return self.num_addrs.load(Ordering::Relaxed);
    }
    pub async fn set_weight(&self, server_name: &String, weight: u64) -> Result<(), ExecError> {
        let group_id = hash_str(&self.group_name);
        let server_id = hash_str(server_name);
        self.weight_sm_client
            .set_weight(&group_id, &server_id, &weight)
            .await
    }
    fn watch_all_actions<F>(&self, f: F)
    where
        F: Fn(&Member, &Action, &Vec<u64>, &Vec<u64>) + 'static + Send + Sync,
    {
        let mut watchers = self.watchers.write();
        watchers.push(Box::new(f));
    }
    pub fn watch_server_nodes_range_changed<F>(&self, server: &String, f: F)
    // return ranges [...,...)
    where
        F: Fn((usize, u32)) + 'static + Send + Sync,
    {
        let server_id = hash_str(server);
        let wrapper = move |_: &Member, _: &Action, nodes: &Vec<u64>, _: &Vec<u64>| {
            let node_len = nodes.len();
            let mut weight = 0;
            let mut start = None;
            for ni in 0..node_len {
                let node = nodes[ni];
                if node == server_id {
                    weight += 1;
                    if start.is_none() {
                        start = Some(ni)
                    }
                }
            }
            if let Some(start_node) = start {
                f((start_node, weight));
            } else {
                warn!("No node exists for watch");
            }
        };
        self.watch_all_actions(wrapper);
    }

    /// Test hook: the next `count` reads of the group answer from the previous
    /// successful read (an older version than the notice that caused them).
    #[cfg(any(test, multisite_test))]
    pub fn inject_stale_reads(&self, count: usize) {
        self.stale_reads.store(count, Ordering::Relaxed);
    }

    /// Install the group as the table. Returns the installed version. A read
    /// older than the installed table installs nothing (monotonic install).
    /// The address table holds every member of the group, online or not, and
    /// is never pruned; routing uses the online members only.
    pub async fn init_table(&self) -> Result<u64, InitTableError> {
        let group_name = &self.group_name;
        let _lock = self.update_lock.lock().await;
        debug!(
            "Initializing table from membership group members for {}",
            group_name
        );
        debug!("Get group members for {}", group_name);
        let group_members = self.membership.group_members(group_name, false).await;
        #[cfg(any(test, multisite_test))]
        let group_members = match group_members {
            Ok(Some(read)) => {
                if self
                    .stale_reads
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                    .is_ok()
                {
                    Ok(Some(self.last_read.lock().clone().unwrap_or(read)))
                } else {
                    *self.last_read.lock() = Some(read.clone());
                    Ok(Some(read))
                }
            }
            other => other,
        };
        debug!("Got group members for {}", group_name);
        if let Ok(Some((all_members, version))) = group_members {
            let installed = self.version.load(Ordering::Relaxed);
            if version < installed {
                debug!(
                    "Read of group {} at version {} is older than the table at {}; not installed",
                    group_name, version, installed
                );
                return Err(InitTableError::StaleRead {
                    read: installed,
                    wanted: installed.max(self.wanted_version.load(Ordering::Relaxed)),
                });
            }
            let members: Vec<Member> = all_members.iter().filter(|m| m.online).cloned().collect();
            let group_id = hash_str(group_name);
            debug!("Getting weights for {}", group_name);
            match self.weight_sm_client.get_weights(&group_id).await {
                Ok(Some(weights)) => {
                    debug!("Group {} have {} weights", group_name, weights.len());
                    if let Some(min_weight) = weights.values().min() {
                        let mut factors: BTreeMap<u64, u32> = BTreeMap::new();
                        let min_weight = *min_weight as f64;
                        for member in members.iter() {
                            let k = member.id;
                            let w = match weights.get(&k) {
                                Some(w) => *w as f64,
                                None => min_weight,
                            };
                            factors.insert(k, (w / min_weight) as u32);
                        }
                        let factor_sum: u32 = factors.values().sum();
                        let mut lookup_table = self.tables.write();
                        lookup_table.nodes = Vec::with_capacity(factor_sum as usize);
                        for member in all_members.iter() {
                            lookup_table.addrs.insert(member.id, member.address.clone());
                        }
                        for (server_id, weight) in factors.into_iter() {
                            for _ in 0..weight {
                                lookup_table.nodes.push(server_id);
                            }
                        }
                        self.num_addrs.store(members.len(), Ordering::Relaxed);
                        self.version.store(version, Ordering::Relaxed);
                        Ok(version)
                    } else {
                        Err(InitTableError::NoWeightInfo)
                    }
                }
                Err(e) => {
                    error!("No weright service for group {}", group_name);
                    Err(InitTableError::NoWeightService(e))
                }
                Ok(None) => {
                    error!("No weight group for group {}", group_name);
                    Err(InitTableError::NoWeightGroup)
                }
            }
        } else {
            error!(
                "No group {} existed in table, groups {:?}",
                group_name,
                self.membership.all_groups().await
            );
            Err(InitTableError::GroupNotExisted)
        }
    }

    #[inline]
    pub fn membership(&self) -> &Arc<MembershipClient> {
        &self.membership
    }
}

async fn server_joined(ch: Arc<ConsistentHashing>, member: Member, version: u64) {
    server_changed(ch, member, Action::Joined, version).await;
}
async fn server_left(ch: Arc<ConsistentHashing>, member: Member, version: u64) {
    server_changed(ch, member, Action::Left, version).await;
}
async fn server_changed(ch: Arc<ConsistentHashing>, member: Member, action: Action, version: u64) {
    warn!(
        "Detected server membership change, member {:?}, action {:?}, version {}",
        member, action, version
    );
    ch.wanted_version.fetch_max(version, Ordering::Relaxed);
    let ch_version = ch.version.load(Ordering::Relaxed);
    if ch_version <= version {
        let old_nodes = (&*ch.tables.read()).nodes.clone();
        // The read must reach the version of the notice; it can come from a
        // member that has applied fewer entries.
        if ch_version < version {
            if let Err(e) = ch.sync_to(version).await {
                error!(
                    "Cannot bring the table to version {}: {:?}, member {:?}, action {:?}, was {}",
                    version, e, member, action, ch_version
                );
            }
        } else {
            // The table is at this version already; read once more for the
            // changed online flags.
            let _ = ch.init_table().await;
        }
        debug!("Triggering conshash watchers");
        let new_nodes = (&*ch.tables.read()).nodes.clone();
        for watch in ch.watchers.read().iter() {
            watch(&member, &action, &new_nodes, &old_nodes);
        }
        debug!("Server change processing completed");
    } else {
        warn!("Server membership change too old to follow, member {:?}, action {:?}, version {}, expect {}", member, action, version, ch_version);
    }
}

#[cfg(test)]
mod test {
    use crate::conshash::weights::Weights;
    use crate::conshash::{ConsistentHashing, SlotOverrides};
    use crate::membership::client::ObserverClient;
    use crate::membership::member::MemberService;
    use crate::membership::server::Membership;
    use crate::raft::client::RaftClient;
    use crate::raft::{Options, RaftService, Storage};
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use bifrost_hasher::hash_str;
    use std::collections::HashMap;
    use std::sync::atomic::*;
    use std::sync::Arc;

    #[test]
    fn older_slot_snapshot_is_refused() {
        let mut slots = SlotOverrides::default();
        assert!(slots.install(Some(vec![10, 20]), 12));
        assert!(!slots.install(Some(vec![1, 2]), 11));
        assert_eq!(slots.owner_with_index(0), Some((10, 12)));
        assert_eq!(slots.owner_with_index(1), Some((20, 12)));
    }

    #[test]
    fn snapshot_preserves_a_newer_owner_push() {
        let mut slots = SlotOverrides::default();
        assert!(slots.install(Some(vec![10, 20]), 10));
        assert!(slots.note_owner(0, 99, 2, 12));

        assert!(slots.install(Some(vec![11, 21]), 11));

        assert_eq!(slots.owner_with_index(0), Some((99, 12)));
    }

    #[test]
    fn snapshot_still_updates_slots_without_a_newer_push() {
        let mut slots = SlotOverrides::default();
        assert!(slots.install(Some(vec![10, 20]), 10));
        assert!(slots.note_owner(0, 99, 2, 12));

        assert!(slots.install(Some(vec![11, 21]), 11));

        assert_eq!(slots.owner_with_index(1), Some((21, 11)));
    }

    #[test]
    fn older_owner_push_is_refused() {
        let mut slots = SlotOverrides::default();
        assert!(slots.install(Some(vec![10, 20]), 10));
        assert!(slots.note_owner(0, 99, 2, 12));

        assert!(!slots.note_owner(0, 11, 2, 11));

        assert_eq!(slots.owner_with_index(0), Some((99, 12)));
    }

    /// F (RC7, RC8): a notice for version N, a read that answers an older
    /// version, and the table must still reach N. `stale_reads` makes the next
    /// read(s) answer from the previous result.
    async fn ring_after_a_notice(stale: bool) -> Arc<ConsistentHashing> {
        ring_with_address(stale).await.0
    }

    /// `ring_after_a_notice`, and the address of the raft member.
    async fn ring_with_address(stale: bool) -> (Arc<ConsistentHashing>, String) {
        let _ = env_logger::try_init();
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reserved.local_addr().unwrap().to_string();
        drop(reserved);
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: 0,
        });
        let server = Server::new(&addr);
        let _membership = Membership::new(&server, &raft_service).await;
        server.register_service_with_id(0, &raft_service).await;
        Server::listen_and_resume(&server).await;
        RaftService::start(&raft_service, false).await;
        raft_service.bootstrap().await;
        let group = String::from("ring_stale_group");
        let client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        let observer = Arc::new(ObserverClient::new(&client));
        RaftClient::prepare_subscription(&server).await;
        observer.new_group(&group).await.unwrap().unwrap();
        Weights::new(&raft_service).await;
        let member1_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        let member1 = MemberService::new(&"ring_server_1".to_string(), &member1_client, &raft_service).await;
        member1.join_group(&group).await.unwrap();
        let ch = ConsistentHashing::new(&group, &client, &observer).await.unwrap();
        ch.set_weight(&"ring_server_1".to_string(), 1).await.unwrap();
        ch.set_weight(&"ring_server_2".to_string(), 1).await.unwrap();
        ch.init_table().await.unwrap();
        assert_eq!(ch.server_count(), 1);
        if stale {
            ch.stale_reads.store(1, Ordering::Relaxed);
        }
        let member2_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        let member2 = MemberService::new(&"ring_server_2".to_string(), &member2_client, &raft_service).await;
        member2.join_group(&group).await.unwrap();
        // The notice arrives; the table must reach the new version.
        for _ in 0..100 {
            if ch.server_count() == 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        // Keep the services alive until the caller is done.
        std::mem::forget((raft_service, server, member1, member2));
        (ch, addr)
    }

    /// Control: the read answers the version of the notice.
    #[tokio::test(flavor = "multi_thread")]
    async fn control_a_ring_follows_a_notice_when_the_read_is_current() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let ch = ring_after_a_notice(false).await;
        assert_eq!(ch.server_count(), 2);
    }

    /// The first read after the notice answers the older version. The ring
    /// reads again until it reaches the notified version.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_ring_follows_a_notice_when_the_first_read_is_older() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let ch = ring_after_a_notice(true).await;
        assert_eq!(ch.server_count(), 2, "the ring kept the older read");
        assert!(ch.try_server_name(hash_str("ring_server_2")).is_some());
    }

    /// RC13: a server that the ring does not know is searched in the roster,
    /// then in the raft configuration. The error names every source.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_address_is_searched_in_the_ring_the_roster_and_the_raft_configuration() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let (ch, raft_address) = ring_with_address(false).await;
        // 1. The ring has the entry.
        let member = hash_str("ring_server_2");
        assert!(ch.try_server_name(member).is_some());
        assert_eq!(
            ch.resolve_address(member).await.unwrap(),
            ch.try_server_name(member).unwrap()
        );
        // 2. The ring has no entry; the roster has it.
        ch.tables.write().addrs.remove(&member);
        assert!(ch.try_server_name(member).is_none());
        let from_roster = ch.resolve_address(member).await.unwrap();
        assert_eq!(hash_str(&from_roster), member);
        // 3. Neither the ring nor the roster has it; the raft configuration has.
        let voter = hash_str(&raft_address);
        ch.tables.write().addrs.remove(&voter);
        assert_eq!(ch.resolve_address(voter).await.unwrap(), raft_address);
        // 4. No source has it: the error names each source.
        let unknown = hash_str("nobody");
        let before = ch.address_search_failures();
        let error = ch.resolve_address(unknown).await.unwrap_err();
        let sources: Vec<&str> = error.tried.iter().map(|(source, _)| *source).collect();
        assert_eq!(sources, vec!["ring", "roster", "raft configuration"], "{}", error);
        assert_eq!(ch.address_search_failures(), before + 1);
    }

    /// A late older read installs nothing (monotonic install).
    #[tokio::test(flavor = "multi_thread")]
    async fn a_late_older_read_does_not_replace_a_newer_table() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let ch = ring_after_a_notice(false).await;
        let installed = ch.table_version();
        ch.stale_reads.store(1, Ordering::Relaxed);
        // The stale answer is the previous successful read, one version back.
        {
            let mut last = ch.last_read.lock();
            if let Some((members, version)) = last.as_mut() {
                *version = installed - 1;
                members.truncate(1);
            }
        }
        assert!(ch.init_table().await.is_err());
        assert_eq!(ch.table_version(), installed);
        assert_eq!(ch.server_count(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn primary() {
        let _ = env_logger::try_init();
        // Installs its own callback into the process-global slot and then
        // depends on it staying there, so this is a destructive callback test in
        // the sense `CALLBACK_TEST_GUARD` documents: the lifecycle tests take
        // `write` and call `reset_callback`, which would otherwise pull this
        // test's subscription out from under it mid-run. That is what the
        // intermittent "Too many retry on command for plane 0" failures were.
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;

        info!("Creating raft service");
        let addr = String::from("127.0.0.1:2200");
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: 0,
        });

        info!("Creating server");
        let server = Server::new(&addr);
        info!("Creating membership service");
        let _membership = Membership::new(&server, &raft_service).await;
        server.register_service_with_id(0, &raft_service).await;
        Server::listen_and_resume(&server).await;
        RaftService::start(&raft_service, false).await;
        raft_service.bootstrap().await;

        let group_1 = String::from("test_group_1");
        let group_2 = String::from("test_group_2");
        let group_3 = String::from("test_group_3");

        let server_1 = String::from("server1");
        let server_2 = String::from("server2");
        let server_3 = String::from("server3");

        info!("Create raft client");
        let wild_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("Create observer");
        let observer_client = Arc::new(ObserverClient::new(&wild_raft_client));

        info!("Create subscription");
        RaftClient::prepare_subscription(&server).await;

        info!("New group 1");
        observer_client.new_group(&group_1).await.unwrap().unwrap();
        info!("New group 2");
        observer_client.new_group(&group_2).await.unwrap().unwrap();
        info!("New group 3");
        observer_client.new_group(&group_3).await.unwrap().unwrap();

        info!("New raft client for member 1");
        let member1_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("New member service 1");
        let member1_svr = MemberService::new(&server_1, &member1_raft_client, &raft_service).await;

        info!("New raft client for member 2");
        let member2_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("New member service 2");
        let member2_svr = MemberService::new(&server_2, &member2_raft_client, &raft_service).await;

        info!("New raft client for member 3");
        let member3_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("New member service 3");
        let member3_svr = MemberService::new(&server_3, &member3_raft_client, &raft_service).await;

        info!("Member 1 join group 1");
        member1_svr.join_group(&group_1).await.unwrap();
        info!("Member 1 join group 2");
        member2_svr.join_group(&group_1).await.unwrap();
        info!("Member 1 join group 3");
        member3_svr.join_group(&group_1).await.unwrap();

        info!("Member 1 join group 2");
        member1_svr.join_group(&group_2).await.unwrap();
        info!("Member 2 join group 2");
        member2_svr.join_group(&group_2).await.unwrap();

        info!("Member 1 join group 3");
        member1_svr.join_group(&group_3).await.unwrap();

        info!("New weight service");
        Weights::new(&raft_service).await;

        info!("New conshash for group 1");
        let ch1 = ConsistentHashing::new(&group_1, &wild_raft_client, &observer_client)
            .await
            .unwrap();

        info!("New conshash for group 2");
        let ch2 = ConsistentHashing::new(&group_2, &wild_raft_client, &observer_client)
            .await
            .unwrap();

        info!("New conshash for group 3");
        let ch3 = ConsistentHashing::new(&group_3, &wild_raft_client, &observer_client)
            .await
            .unwrap();

        info!("Set server 1 in group 1 to 1");
        ch1.set_weight(&server_1, 1).await.unwrap();
        info!("Set server 2 in group 1 to 2");
        ch1.set_weight(&server_2, 2).await.unwrap();
        info!("Set server 3 in group 1 to 3");
        ch1.set_weight(&server_3, 3).await.unwrap();

        info!("Set server 1 in group 2 to 1");
        ch2.set_weight(&server_1, 1).await.unwrap();
        info!("Set server 2 in group 2 to 1");
        ch2.set_weight(&server_2, 1).await.unwrap();

        info!("Set server 1 in group 3 to 2");
        ch3.set_weight(&server_1, 2).await.unwrap();

        info!("Init table for conshash 1");
        ch1.init_table().await.unwrap();
        info!("Init table for conshash 2");
        ch2.init_table().await.unwrap();
        info!("Init table for conshash 3");
        ch3.init_table().await.unwrap();

        let ch1_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch1_server_node_changes_count_clone = ch1_server_node_changes_count.clone();
        info!("Watch node change from conshash 1");
        ch1.watch_server_nodes_range_changed(&server_2, move |_| {
            ch1_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        let ch2_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch2_server_node_changes_count_clone = ch2_server_node_changes_count.clone();
        info!("Watch node change from conshash 2");
        ch2.watch_server_nodes_range_changed(&server_2, move |_| {
            ch2_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        let ch3_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch3_server_node_changes_count_clone = ch3_server_node_changes_count.clone();
        info!("Watch node change from conshash 3");
        ch3.watch_server_nodes_range_changed(&server_2, move |_| {
            ch3_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        info!("Counting nodes for conshash 1");
        assert_eq!(ch1.nodes_count(), 6);
        info!("Counting nodes for conshash 2");
        assert_eq!(ch2.nodes_count(), 2);
        info!("Counting nodes for conshash 3");
        assert_eq!(ch3.nodes_count(), 1);

        info!("Batch get server by string from conshash 1");
        let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
        let data_set_size: usize = 30000;

        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch1.get_server_by_string(&k).unwrap();
            *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Counting distribution for conshash 1");
        assert_eq!(ch_1_mapping.get(&server_1).unwrap(), &4936);
        assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &9923);
        assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &15141); // hard coded due to constant

        info!("Batch get server by string from conshash 2");
        let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch2.get_server_by_string(&k).unwrap();
            *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Counting distribution for conshash 2");
        assert_eq!(ch_2_mapping.get(&server_1).unwrap(), &14967);
        assert_eq!(ch_2_mapping.get(&server_2).unwrap(), &15033);

        info!("Batch get server by string from conshash 3");
        let mut ch_3_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch3.get_server_by_string(&k).unwrap();
            *ch_3_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Counting distribution for conshash 3");
        assert_eq!(ch_3_mapping.get(&server_1).unwrap(), &30000);

        info!("Close member 1");
        member1_svr.close();
        info!("Waiting");

        for i in 0..10 {
            async_wait_secs().await;
        }

        let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
        info!("Recheck get server by string for conshash 1");
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch1.get_server_by_string(&k).unwrap();
            *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Recount distribution for conshash 1");
        assert_eq!(
            ch_1_mapping.get(&server_2).unwrap() + ch_1_mapping.get(&server_3).unwrap(),
            data_set_size as u64
        );
        assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &11932);
        assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &18068);

        let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
        info!("Recheck get server by string for conshash 2");
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch2.get_server_by_string(&k).unwrap();
            *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Recount distribution for conshash 2");
        assert_eq!(
            ch_2_mapping.get(&server_2).unwrap(),
            &(data_set_size as u64)
        );

        info!("Cheching conshash 3 with no members");
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            assert!(ch3.get_server_by_string(&k).is_none()); // no member
        }

        info!("Waiting");
        async_wait_secs().await;
        async_wait_secs().await;
        info!("Testing callback counter");
        assert_eq!(ch1_server_node_changes_count.load(Ordering::Relaxed), 1);
        assert_eq!(ch2_server_node_changes_count.load(Ordering::Relaxed), 1);
        assert_eq!(ch3_server_node_changes_count.load(Ordering::Relaxed), 0);
        info!("Membership tests all done !");
    }
}
