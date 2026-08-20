//! Consensus-backed slot ownership: which member of a group holds which slot.
//!
//! `ConsistentHashing` derives placement from the current ring —
//! `nodes[jump_hash(slot_count, hash)]`. That is correct and cheap for
//! stateless routing, and it cannot support **moving stored data**, for a
//! reason that is structural rather than incidental: jump hash reassigns keys
//! as an atomic consequence of the bucket count. The instant a member joins,
//! a function-chosen ~1/(n+1) of keys point at it — all at once, with whatever
//! they name still sitting on the previous owner. There is no way to say
//! "reassign this one slot now", and no way to choose which slots move.
//!
//! This state machine is the alternative for callers that store data: slot
//! ownership becomes replicated state they edit deliberately. A joining member
//! owns nothing until something assigns it, so nothing becomes unreachable by
//! the mere fact of joining, and slots move one at a time under a caller's
//! control.
//!
//! **This module is bookkeeping only — it moves no data.** It records who owns
//! what and which slots are in flight; transferring the contents of a slot is
//! the caller's job, and `SlotState::Migrating` is the interlock it uses to do
//! that safely.
//!
//! What a slot *means* is deliberately not defined here. Callers map their keys
//! onto a slot space however they like — a hash, a key prefix, an id field —
//! and only need the mapping to be stable for a given key.

use crate::raft::state_machine::StateMachineCtl;
use crate::raft::RaftService;
use bifrost_plugins::hash_ident;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_DHT_SLOTS) as u64;

/// Who owns a slot, and whether it is currently moving.
///
/// `Migrating` is the state a pure hash function has nowhere to put, and the
/// reason ownership is stored rather than computed. The donor stays
/// authoritative for the whole transfer: the step to `Stable { owner: to }` is
/// a migration's single commit point, so there is no instant at which a slot is
/// owned by neither member or by both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotState {
    Stable { owner: u64 },
    Migrating { from: u64, to: u64 },
}

impl SlotState {
    /// The member that answers for this slot right now.
    ///
    /// During a migration that is still the donor — which is what makes an
    /// interrupted transfer leave the group correct rather than ambiguous.
    pub fn serving_owner(&self) -> u64 {
        match self {
            SlotState::Stable { owner } => *owner,
            SlotState::Migrating { from, .. } => *from,
        }
    }

    pub fn is_migrating(&self) -> bool {
        matches!(self, SlotState::Migrating { .. })
    }
}

/// Sparse per group: a slot with no entry has no owner yet. An empty map is the
/// honest representation of "this group has not been placed", which a map
/// pre-filled with some default member would hide.
type GroupSlots = HashMap<u32, SlotState>;

/// Operator- and balancer-facing controls over one group's migrations.
///
/// Both live in the state machine rather than in whoever runs the balancer,
/// for the same reason the table itself does: enforcement at the SM means an
/// automatic balancer and an operator's concurrent manual reshard **jointly**
/// cannot exceed the budget, and the kill switch survives a restart because it
/// rides in the raft snapshot -- a freeze that lives in a CLI flag is off
/// again the moment the process bounces.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupControl {
    /// Maximum slots allowed in `Migrating` at once. 0 means no cap, which is
    /// the default -- existing operator flows see no behaviour change until a
    /// cap is explicitly set.
    pub migration_budget: u32,
    /// While set, nothing may START a migration; in-flight ones may finish or
    /// abort, because freezing a cluster should drain its work, not strand it.
    pub frozen: bool,
}

/// One group's control state plus what it currently governs, in one answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationControlView {
    pub budget: u32,
    pub frozen: bool,
    pub in_flight: u32,
}

pub struct Slots {
    pub groups: HashMap<u64, GroupSlots>,
    pub controls: HashMap<u64, GroupControl>,
    pub id: u64,
}

raft_state_machine! {
    def cmd set_slot_owner(group: u64, slot: u32, owner: u64);
    def cmd adopt_slots(group: u64, assignments: Vec<(u32, u64)>) -> usize;
    def cmd reassign_slots(group: u64, assignments: Vec<(u32, u64)>, from: u64) -> usize;
    def cmd begin_slot_migration(group: u64, slot: u32, from: u64, to: u64) -> Result<(), String>;
    def cmd begin_slot_migrations(group: u64, moves: Vec<(u32, u64, u64)>) -> Vec<(u32, String)>;
    def cmd complete_slot_migrations(group: u64, slots: Vec<u32>) -> Vec<(u32, u64)>;
    def cmd complete_slot_migration(group: u64, slot: u32) -> Result<u64, String>;
    def cmd abort_slot_migration(group: u64, slot: u32) -> Result<u64, String>;
    def cmd all_slots_consistent(group: u64) -> Option<HashMap<u32, SlotState>>;
    def cmd set_migration_budget(group: u64, budget: u32);
    def cmd set_migration_freeze(group: u64, frozen: bool);
    def qry migration_control(group: u64) -> MigrationControlView;
    def qry slot_state(group: u64, slot: u32) -> Option<SlotState>;
    def qry all_slots(group: u64) -> Option<HashMap<u32, SlotState>>;
    def qry slots_owned_by(group: u64, server: u64) -> Vec<u32>;
    def qry placed_slot_count(group: u64) -> usize;
}

impl client::SMClient {
    pub async fn all_slots_consistent_with_index(
        &self,
        group: &u64,
    ) -> Result<(Option<HashMap<u32, SlotState>>, u64), crate::raft::state_machine::master::ExecError>
    {
        self.execute_command_with_index(commands::all_slots_consistent::new(group))
            .await
    }

    pub async fn complete_slot_migration_with_index(
        &self,
        group: &u64,
        slot: &u32,
    ) -> Result<(Result<u64, String>, u64), crate::raft::state_machine::master::ExecError> {
        self.execute_command_with_index(commands::complete_slot_migration::new(group, slot))
            .await
    }

    pub async fn complete_slot_migrations_with_index(
        &self,
        group: &u64,
        slots: &Vec<u32>,
    ) -> Result<(Vec<(u32, u64)>, u64), crate::raft::state_machine::master::ExecError> {
        self.execute_command_with_index(commands::complete_slot_migrations::new(group, slots))
            .await
    }
}

/// Slots currently mid-migration in one group. Counted from the table rather
/// than tracked in a counter on purpose: several commands can end a migration
/// (`complete`, `abort`, and any future overwrite), and a counter that each of
/// them must remember to decrement drifts the first time one forgets. The scan
/// is a few tens of microseconds against the ~88 ms consensus round trip every
/// command already pays.
fn in_flight_migrations(slots: &GroupSlots) -> u32 {
    slots.values().filter(|state| state.is_migrating()).count() as u32
}

/// Whether one more migration may START, given a group's controls and what is
/// already moving. Counted from the table at each admission, which is also
/// what makes a batch honest: every entry it admits lands in the table before
/// the next entry is judged, so a batch is capped exactly as a stream of
/// single commands would be.
fn admit_new_migration(control: &GroupControl, slots: &GroupSlots) -> Result<(), String> {
    if control.frozen {
        return Err("migrations are frozen for this group (kill switch set)".to_string());
    }
    if control.migration_budget > 0 {
        let moving = in_flight_migrations(slots);
        if moving >= control.migration_budget {
            return Err(format!(
                "migration budget exhausted: {} already in flight against a cap of {}",
                moving, control.migration_budget
            ));
        }
    }
    Ok(())
}

impl StateMachineCmds for Slots {
    fn set_slot_owner(&mut self, group: u64, slot: u32, owner: u64) -> BoxFuture<()> {
        self.groups
            .entry(group)
            .or_insert_with(HashMap::new)
            .insert(slot, SlotState::Stable { owner });
        future::ready(()).boxed()
    }

    /// Claim only the slots that have no owner yet; report how many were taken.
    ///
    /// This is how a group already running on derived placement adopts the
    /// table without appearing to move anything: every member proposes the
    /// placement it already computes, and whoever commits first wins. Never
    /// overwriting an existing entry is what makes that safe to run
    /// concurrently on every member and safe to repeat. The alternative is
    /// electing a populator, and an election is one more thing that can fail
    /// during exactly the window this is meant to make safe.
    fn adopt_slots(&mut self, group: u64, assignments: Vec<(u32, u64)>) -> BoxFuture<usize> {
        let slots = self.groups.entry(group).or_insert_with(HashMap::new);
        let mut adopted = 0usize;
        for (slot, owner) in assignments {
            slots.entry(slot).or_insert_with(|| {
                adopted += 1;
                SlotState::Stable { owner }
            });
        }
        future::ready(adopted).boxed()
    }

    /// Hand a batch of slots from one member to others in a single command.
    ///
    /// For slots with **nothing in them**. Draining a member means moving every
    /// slot it owns, which in a small cluster is the whole 32768-slot space, and
    /// the great majority of those hold no data at all. Walking them through the
    /// full migration sequence costs six round trips each to move nothing --
    /// measured at ~200k operations for one drain, which is correct and unusable.
    ///
    /// Safe as a bulk operation precisely because it is narrow. It moves a slot
    /// only if that slot is `Stable` on `from`, so it cannot steal a slot from a
    /// third member and cannot disturb one that is mid-migration -- the two ways
    /// a bulk placement edit could otherwise strand data. Anything it declines is
    /// simply not counted, and the caller sees the shortfall.
    ///
    /// It moves no data and must only be used where there is none to move.
    fn reassign_slots(
        &mut self,
        group: u64,
        assignments: Vec<(u32, u64)>,
        from: u64,
    ) -> BoxFuture<usize> {
        let slots = self.groups.entry(group).or_insert_with(HashMap::new);
        let mut reassigned = 0usize;
        for (slot, owner) in assignments {
            match slots.get(&slot) {
                Some(SlotState::Stable { owner: current }) if *current == from => {
                    slots.insert(slot, SlotState::Stable { owner });
                    reassigned += 1;
                }
                _ => {}
            }
        }
        future::ready(reassigned).boxed()
    }

    fn begin_slot_migration(
        &mut self,
        group: u64,
        slot: u32,
        from: u64,
        to: u64,
    ) -> BoxFuture<Result<(), String>> {
        let control = self.controls.get(&group).copied().unwrap_or_default();
        let slots = self.groups.entry(group).or_insert_with(HashMap::new);
        let result = match slots.get(&slot) {
            Some(SlotState::Stable { owner }) if *owner == from => {
                if from == to {
                    Err(format!("slot {slot} migration from {from} to itself"))
                } else if let Err(refusal) = admit_new_migration(&control, slots) {
                    // Only a Stable -> Migrating transition consults the
                    // controls: the idempotent-retry branch below stays
                    // admitted under freeze and budget alike, because it
                    // starts nothing -- refusing it would strand a transfer
                    // that already began.
                    Err(format!("slot {slot} refused: {refusal}"))
                } else {
                    slots.insert(slot, SlotState::Migrating { from, to });
                    Ok(())
                }
            }
            Some(SlotState::Stable { owner }) => Err(format!(
                "slot {slot} is owned by {owner}, not by the claimed donor {from}"
            )),
            // Retrying the same migration after a lost response must be safe.
            // Redirecting an in-flight one must not be: whatever the first
            // transfer already copied would be stranded.
            Some(SlotState::Migrating {
                from: active_from,
                to: active_to,
            }) => {
                if *active_from == from && *active_to == to {
                    Ok(())
                } else {
                    Err(format!(
                        "slot {slot} is already migrating {active_from} -> {active_to}"
                    ))
                }
            }
            None => Err(format!("slot {slot} has no owner to migrate from")),
        };
        future::ready(result).boxed()
    }

    /// Begin many migrations in one command; report only the refusals.
    ///
    /// Same guards as `begin_slot_migration`, applied per entry -- this is a
    /// batching of that command, not a weaker version of it.
    ///
    /// Batching matters more than it looks. A migration costs two commands and a
    /// query per slot, and the raft leader serialises log appends however many
    /// callers there are, so per-slot commands put a consensus round trip in the
    /// critical path of every slot: measured ~88 ms of fixed cost per slot even
    /// with 32-way concurrency on the caller side, dominating a reshard of a
    /// 1 GB store. Concurrency cannot fix a serialised path; fewer commands can.
    fn begin_slot_migrations(
        &mut self,
        group: u64,
        moves: Vec<(u32, u64, u64)>,
    ) -> BoxFuture<Vec<(u32, String)>> {
        let control = self.controls.get(&group).copied().unwrap_or_default();
        let slots = self.groups.entry(group).or_insert_with(HashMap::new);
        let mut refused = Vec::new();
        for (slot, from, to) in moves {
            let outcome = match slots.get(&slot) {
                Some(SlotState::Stable { owner }) if *owner == from => {
                    if from == to {
                        Err(format!("slot {slot} migration from {from} to itself"))
                    } else if let Err(refusal) = admit_new_migration(&control, slots) {
                        Err(format!("slot {slot} refused: {refusal}"))
                    } else {
                        slots.insert(slot, SlotState::Migrating { from, to });
                        Ok(())
                    }
                }
                Some(SlotState::Stable { owner }) => Err(format!(
                    "slot {slot} is owned by {owner}, not by the claimed donor {from}"
                )),
                Some(SlotState::Migrating {
                    from: active_from,
                    to: active_to,
                }) => {
                    if *active_from == from && *active_to == to {
                        Ok(())
                    } else {
                        Err(format!(
                            "slot {slot} is already migrating {active_from} -> {active_to}"
                        ))
                    }
                }
                None => Err(format!("slot {slot} has no owner to migrate from")),
            };
            if let Err(reason) = outcome {
                refused.push((slot, reason));
            }
        }
        future::ready(refused).boxed()
    }

    /// Commit many migrations in one command; report each slot's new owner.
    ///
    /// A slot absent from the result did not commit, and the caller must treat it
    /// as still the donor's. Because this is a *command*, its return value is
    /// produced by its own apply and is authoritative — so a bulk caller does not
    /// need to query the table afterwards to learn what happened, which also
    /// sidesteps a query being served by a member that has not applied this yet.
    fn complete_slot_migrations(
        &mut self,
        group: u64,
        slots: Vec<u32>,
    ) -> BoxFuture<Vec<(u32, u64)>> {
        let table = self.groups.entry(group).or_insert_with(HashMap::new);
        let mut committed = Vec::new();
        for slot in slots {
            if let Some(SlotState::Migrating { to, .. }) = table.get(&slot) {
                let owner = *to;
                table.insert(slot, SlotState::Stable { owner });
                committed.push((slot, owner));
            }
        }
        future::ready(committed).boxed()
    }

    /// A migration's commit point. Returns the new owner.
    fn complete_slot_migration(&mut self, group: u64, slot: u32) -> BoxFuture<Result<u64, String>> {
        let slots = self.groups.entry(group).or_insert_with(HashMap::new);
        let result = match slots.get(&slot) {
            Some(SlotState::Migrating { to, .. }) => {
                let owner = *to;
                slots.insert(slot, SlotState::Stable { owner });
                Ok(owner)
            }
            Some(SlotState::Stable { owner }) => Err(format!(
                "slot {slot} is not migrating; it is stable on {owner}"
            )),
            None => Err(format!("slot {slot} has no placement")),
        };
        future::ready(result).boxed()
    }

    /// Give up on a transfer and leave the slot with its donor. Safe precisely
    /// because the donor never stopped being authoritative.
    fn abort_slot_migration(&mut self, group: u64, slot: u32) -> BoxFuture<Result<u64, String>> {
        let slots = self.groups.entry(group).or_insert_with(HashMap::new);
        let result = match slots.get(&slot) {
            Some(SlotState::Migrating { from, .. }) => {
                let owner = *from;
                slots.insert(slot, SlotState::Stable { owner });
                Ok(owner)
            }
            Some(SlotState::Stable { owner }) => Err(format!(
                "slot {slot} is not migrating; it is stable on {owner}"
            )),
            None => Err(format!("slot {slot} has no placement")),
        };
        future::ready(result).boxed()
    }

    fn set_migration_budget(&mut self, group: u64, budget: u32) -> BoxFuture<()> {
        self.controls.entry(group).or_default().migration_budget = budget;
        future::ready(()).boxed()
    }

    fn set_migration_freeze(&mut self, group: u64, frozen: bool) -> BoxFuture<()> {
        self.controls.entry(group).or_default().frozen = frozen;
        future::ready(()).boxed()
    }

    fn all_slots_consistent(&mut self, group: u64) -> BoxFuture<Option<HashMap<u32, SlotState>>> {
        future::ready(self.groups.get(&group).cloned()).boxed()
    }

    fn migration_control(&self, group: u64) -> BoxFuture<MigrationControlView> {
        let control = self.controls.get(&group).copied().unwrap_or_default();
        let in_flight = self
            .groups
            .get(&group)
            .map(|slots| in_flight_migrations(slots))
            .unwrap_or(0);
        future::ready(MigrationControlView {
            budget: control.migration_budget,
            frozen: control.frozen,
            in_flight,
        })
        .boxed()
    }

    fn slot_state(&self, group: u64, slot: u32) -> BoxFuture<Option<SlotState>> {
        future::ready(
            self.groups
                .get(&group)
                .and_then(|slots| slots.get(&slot).copied()),
        )
        .boxed()
    }

    fn all_slots(&self, group: u64) -> BoxFuture<Option<HashMap<u32, SlotState>>> {
        future::ready(self.groups.get(&group).cloned()).boxed()
    }

    /// Slots this member answers for, migrations included — a donor still owns
    /// what it is sending until the transfer commits.
    fn slots_owned_by(&self, group: u64, server: u64) -> BoxFuture<Vec<u32>> {
        let mut owned: Vec<u32> = self
            .groups
            .get(&group)
            .map(|slots| {
                slots
                    .iter()
                    .filter(|(_, state)| state.serving_owner() == server)
                    .map(|(slot, _)| *slot)
                    .collect()
            })
            .unwrap_or_default();
        owned.sort_unstable();
        future::ready(owned).boxed()
    }

    fn placed_slot_count(&self, group: u64) -> BoxFuture<usize> {
        future::ready(
            self.groups
                .get(&group)
                .map(|slots| slots.len())
                .unwrap_or(0),
        )
        .boxed()
    }
}

impl StateMachineCtl for Slots {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.id
    }
    fn snapshot(&self) -> Vec<u8> {
        crate::utils::serde::serialize(&(self.groups.clone(), self.controls.clone()))
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        // Current format first, then the pre-control format (a bare groups
        // map). The fallback is not optional politeness: a persisted snapshot
        // from before the controls existed that failed to decode would leave
        // this member's table EMPTY, which claims nothing is placed anywhere
        // -- the exact placement wipe the loud error below exists to prevent.
        type WithControls = (HashMap<u64, GroupSlots>, HashMap<u64, GroupControl>);
        if let Some((groups, controls)) =
            crate::utils::serde::deserialize::<WithControls>(data.as_slice())
        {
            self.groups = groups;
            self.controls = controls;
            return future::ready(()).boxed();
        }
        match crate::utils::serde::deserialize::<HashMap<u64, GroupSlots>>(data.as_slice()) {
            Some(groups) => {
                self.groups = groups;
                self.controls = HashMap::new();
            }
            None => {
                // Deliberately loud and deliberately NOT silently emptied the
                // way a weights table can be: an empty placement table claims
                // nothing is placed anywhere, which would send every lookup to
                // the wrong member. Keep what we have and let the caller see a
                // stale table rather than an empty one.
                error!(
                    "Failed to deserialize slot placement snapshot; keeping the current table. \
                     Placement may be stale on this member."
                );
            }
        }
        future::ready(()).boxed()
    }
    fn recoverable(&self) -> bool {
        // Members must converge on ONE placement table. A member that joins
        // after slots were adopted has to pull this snapshot; deriving its own
        // is precisely the divergence the table exists to remove.
        true
    }
}

impl Slots {
    pub async fn new_with_id(id: u64, raft_service: &Arc<RaftService>) {
        raft_service
            .register_state_machine(Box::new(Slots {
                groups: HashMap::new(),
                controls: HashMap::new(),
                id,
            }))
            .await
    }
    pub async fn new(raft_service: &Arc<RaftService>) {
        Self::new_with_id(DEFAULT_SERVICE_ID, raft_service).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    const G: u64 = 7;
    const OTHER: u64 = 8;
    const A: u64 = 100;
    const B: u64 = 200;
    const C: u64 = 300;

    fn new_sm() -> Slots {
        Slots {
            groups: HashMap::new(),
            controls: HashMap::new(),
            id: 1,
        }
    }

    #[test]
    fn a_slot_has_no_owner_until_one_is_assigned() {
        // Callers must be able to tell "not placed yet" from "placed on some
        // default member"; a pre-filled table would make that impossible.
        let sm = new_sm();
        assert_eq!(block_on(sm.slot_state(G, 7)), None);
        assert_eq!(block_on(sm.placed_slot_count(G)), 0);
        assert_eq!(block_on(sm.all_slots(G)), None);
    }

    #[test]
    fn adoption_never_overwrites_an_existing_placement() {
        // Every member proposes what it already computes, concurrently and
        // repeatedly. First writer wins and re-running changes nothing --
        // otherwise adopting the table would itself relocate data.
        let mut sm = new_sm();
        assert_eq!(block_on(sm.adopt_slots(G, vec![(1, A), (2, B)])), 2);
        assert_eq!(block_on(sm.adopt_slots(G, vec![(1, C), (2, C), (3, C)])), 1);

        assert_eq!(
            block_on(sm.slot_state(G, 1)),
            Some(SlotState::Stable { owner: A })
        );
        assert_eq!(
            block_on(sm.slot_state(G, 2)),
            Some(SlotState::Stable { owner: B })
        );
        assert_eq!(
            block_on(sm.slot_state(G, 3)),
            Some(SlotState::Stable { owner: C })
        );
    }

    #[test]
    fn consistent_snapshot_returns_stable_and_migrating_states_without_mutation() {
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 1, A));
        block_on(sm.set_slot_owner(G, 2, B));
        block_on(sm.begin_slot_migration(G, 2, B, C)).expect("migration should begin");
        let before = sm.groups.clone();

        let snapshot = block_on(sm.all_slots_consistent(G)).expect("group should be placed");

        assert_eq!(snapshot.get(&1), Some(&SlotState::Stable { owner: A }));
        assert_eq!(
            snapshot.get(&2),
            Some(&SlotState::Migrating { from: B, to: C })
        );
        assert_eq!(sm.groups, before, "consistent read must not mutate slots");
    }

    #[test]
    fn groups_are_independent() {
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 1, A));
        assert_eq!(block_on(sm.slot_state(OTHER, 1)), None);
        assert_eq!(block_on(sm.placed_slot_count(OTHER)), 0);
    }

    #[test]
    fn the_donor_answers_for_the_whole_migration() {
        // The property that makes an interrupted transfer safe: no instant at
        // which the slot is owned by neither member, or by both.
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 5, A));

        block_on(sm.begin_slot_migration(G, 5, A, B)).expect("migration should begin");
        assert_eq!(
            block_on(sm.slot_state(G, 5)).unwrap().serving_owner(),
            A,
            "reads must still resolve at the donor while the transfer is in flight"
        );
        assert_eq!(block_on(sm.slots_owned_by(G, A)), vec![5]);
        assert!(block_on(sm.slots_owned_by(G, B)).is_empty());

        assert_eq!(block_on(sm.complete_slot_migration(G, 5)), Ok(B));
        assert_eq!(block_on(sm.slot_state(G, 5)).unwrap().serving_owner(), B);
        assert!(block_on(sm.slots_owned_by(G, A)).is_empty());
        assert_eq!(block_on(sm.slots_owned_by(G, B)), vec![5]);
    }

    #[test]
    fn an_aborted_migration_leaves_the_slot_with_its_donor() {
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 5, A));
        block_on(sm.begin_slot_migration(G, 5, A, B)).expect("begin");

        assert_eq!(block_on(sm.abort_slot_migration(G, 5)), Ok(A));
        assert_eq!(
            block_on(sm.slot_state(G, 5)),
            Some(SlotState::Stable { owner: A })
        );
    }

    #[test]
    fn migration_rejects_a_donor_that_does_not_own_the_slot() {
        // A planner working from a stale table would otherwise "move" a slot
        // out from under its real owner and strand what it holds.
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 5, A));
        assert!(block_on(sm.begin_slot_migration(G, 5, B, C)).is_err());
        assert_eq!(
            block_on(sm.slot_state(G, 5)),
            Some(SlotState::Stable { owner: A })
        );

        assert!(block_on(sm.begin_slot_migration(G, 9, A, B)).is_err());
    }

    #[test]
    fn a_repeated_migration_is_idempotent_but_a_conflicting_one_is_not() {
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 5, A));
        block_on(sm.begin_slot_migration(G, 5, A, B)).expect("begin");

        assert!(block_on(sm.begin_slot_migration(G, 5, A, B)).is_ok());
        assert!(block_on(sm.begin_slot_migration(G, 5, A, C)).is_err());
        assert_eq!(
            block_on(sm.slot_state(G, 5)),
            Some(SlotState::Migrating { from: A, to: B })
        );
    }

    #[test]
    fn bulk_begin_and_complete_match_the_single_slot_guards() {
        // The batched commands must be a batching of the single-slot ones, not a
        // weaker version: same refusals, same commit point.
        let mut sm = new_sm();
        block_on(sm.adopt_slots(G, vec![(1, A), (2, A), (3, B)]));

        let refused = block_on(sm.begin_slot_migrations(
            G,
            vec![
                (1, A, B), // fine
                (2, A, A), // to itself
                (3, A, B), // A does not own it
                (9, A, B), // unplaced
            ],
        ));
        let refused_slots: Vec<u32> = refused.iter().map(|(slot, _)| *slot).collect();
        assert_eq!(refused_slots, vec![2, 3, 9]);
        assert_eq!(
            block_on(sm.slot_state(G, 1)),
            Some(SlotState::Migrating { from: A, to: B })
        );
        assert_eq!(
            block_on(sm.slot_state(G, 3)),
            Some(SlotState::Stable { owner: B }),
            "a refused entry must not disturb the slot"
        );

        // Only the slot that actually began commits, and the command reports it.
        let committed = block_on(sm.complete_slot_migrations(G, vec![1, 2, 3, 9]));
        assert_eq!(committed, vec![(1, B)]);
        assert_eq!(
            block_on(sm.slot_state(G, 1)),
            Some(SlotState::Stable { owner: B })
        );
        assert_eq!(
            block_on(sm.slot_state(G, 2)),
            Some(SlotState::Stable { owner: A }),
            "a slot that never began must not be committed by a bulk complete"
        );
    }

    #[test]
    fn bulk_reassignment_only_moves_slots_the_named_member_holds() {
        // The narrowness is the safety argument: a bulk placement edit that could
        // touch a third member's slot, or one mid-migration, could strand data
        // with no migration sequence to protect it.
        let mut sm = new_sm();
        block_on(sm.adopt_slots(G, vec![(1, A), (2, A), (3, B), (4, A)]));
        block_on(sm.begin_slot_migration(G, 4, A, C)).expect("begin");

        let moved = block_on(sm.reassign_slots(
            G,
            vec![(1, B), (2, B), (3, C), (4, C), (9, C)],
            A,
        ));
        assert_eq!(moved, 2, "only the two stable slots owned by A may move");

        assert_eq!(
            block_on(sm.slot_state(G, 1)),
            Some(SlotState::Stable { owner: B })
        );
        assert_eq!(
            block_on(sm.slot_state(G, 2)),
            Some(SlotState::Stable { owner: B })
        );
        assert_eq!(
            block_on(sm.slot_state(G, 3)),
            Some(SlotState::Stable { owner: B }),
            "a slot owned by somebody else must not be stolen"
        );
        assert_eq!(
            block_on(sm.slot_state(G, 4)),
            Some(SlotState::Migrating { from: A, to: C }),
            "a migrating slot must not be redirected out from under its transfer"
        );
        assert_eq!(
            block_on(sm.slot_state(G, 9)),
            None,
            "an unplaced slot is not claimed by a reassignment"
        );
    }

    #[test]
    fn bulk_reassignment_empties_a_member() {
        let mut sm = new_sm();
        block_on(sm.adopt_slots(G, (0..64u32).map(|slot| (slot, A)).collect()));
        let assignments: Vec<(u32, u64)> = (0..64u32).map(|slot| (slot, B)).collect();
        assert_eq!(block_on(sm.reassign_slots(G, assignments, A)), 64);
        assert!(block_on(sm.slots_owned_by(G, A)).is_empty());
        assert_eq!(block_on(sm.slots_owned_by(G, B)).len(), 64);
    }

    #[test]
    fn completing_or_aborting_a_settled_slot_is_an_error() {
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 5, A));
        assert!(block_on(sm.complete_slot_migration(G, 5)).is_err());
        assert!(block_on(sm.abort_slot_migration(G, 5)).is_err());
        assert!(block_on(sm.complete_slot_migration(G, 9)).is_err());
    }

    #[test]
    fn state_survives_a_snapshot_round_trip() {
        // Members converge by pulling this snapshot. If it did not round-trip,
        // each would keep its own divergent table -- the exact failure the
        // table exists to remove.
        let mut sm = new_sm();
        block_on(sm.adopt_slots(G, vec![(1, A), (2, B)]));
        block_on(sm.begin_slot_migration(G, 2, B, C)).expect("begin");

        let snapshot = sm.snapshot();
        let mut restored = new_sm();
        block_on(restored.recover(snapshot));

        assert_eq!(
            block_on(restored.slot_state(G, 1)),
            Some(SlotState::Stable { owner: A })
        );
        assert_eq!(
            block_on(restored.slot_state(G, 2)),
            Some(SlotState::Migrating { from: B, to: C })
        );
    }

    #[test]
    fn a_corrupt_snapshot_keeps_the_current_table_rather_than_emptying_it() {
        // An empty placement table asserts that nothing is placed anywhere,
        // which would route every lookup to the wrong member. Stale beats that.
        let mut sm = new_sm();
        block_on(sm.set_slot_owner(G, 1, A));
        block_on(sm.recover(vec![0xff, 0x00, 0xff]));
        assert_eq!(
            block_on(sm.slot_state(G, 1)),
            Some(SlotState::Stable { owner: A }),
            "a table that cannot be read must not be replaced by an empty one"
        );
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use futures::executor::block_on;

    const G: u64 = 7;
    const A: u64 = 100;
    const B: u64 = 200;
    const C: u64 = 300;

    fn placed_sm(slots: u32) -> Slots {
        let mut sm = Slots {
            groups: HashMap::new(),
            controls: HashMap::new(),
            id: 1,
        };
        for slot in 0..slots {
            block_on(sm.set_slot_owner(G, slot, A));
        }
        sm
    }

    #[test]
    fn the_budget_caps_what_may_move_at_once() {
        let mut sm = placed_sm(4);
        block_on(sm.set_migration_budget(G, 2));

        block_on(sm.begin_slot_migration(G, 0, A, B)).expect("first within budget");
        block_on(sm.begin_slot_migration(G, 1, A, B)).expect("second within budget");
        let refused = block_on(sm.begin_slot_migration(G, 2, A, B))
            .expect_err("third must exceed the budget");
        assert!(refused.contains("budget exhausted"), "{refused}");

        // The idempotent retry of an in-flight migration starts nothing, so
        // the budget must not refuse it -- a caller retrying a lost response
        // would otherwise be wedged by its own earlier success.
        block_on(sm.begin_slot_migration(G, 1, A, B))
            .expect("retrying an in-flight migration is not a new one");

        // Finishing one frees its budget slot.
        block_on(sm.complete_slot_migration(G, 0)).expect("commit");
        block_on(sm.begin_slot_migration(G, 2, A, B)).expect("room again after a commit");

        assert_eq!(
            block_on(sm.migration_control(G)),
            MigrationControlView {
                budget: 2,
                frozen: false,
                in_flight: 2,
            }
        );
    }

    #[test]
    fn a_batch_cannot_slip_past_the_budget_by_being_one_command() {
        let mut sm = placed_sm(5);
        block_on(sm.set_migration_budget(G, 3));
        block_on(sm.begin_slot_migration(G, 0, A, B)).expect("one in flight already");

        let refused = block_on(sm.begin_slot_migrations(
            G,
            vec![(1, A, B), (2, A, B), (3, A, B), (4, A, B)],
        ));
        // 1 in flight + a cap of 3 leaves room for exactly 2 of the 4.
        assert_eq!(refused.len(), 2, "refusals: {refused:?}");
        assert!(refused.iter().all(|(_, reason)| reason.contains("budget exhausted")));
        assert_eq!(block_on(sm.migration_control(G)).in_flight, 3);
    }

    #[test]
    fn the_kill_switch_stops_new_migrations_and_drains_old_ones() {
        let mut sm = placed_sm(3);
        block_on(sm.begin_slot_migration(G, 0, A, B)).expect("in flight before the freeze");
        block_on(sm.set_migration_freeze(G, true));

        let refused =
            block_on(sm.begin_slot_migration(G, 1, A, B)).expect_err("frozen must refuse");
        assert!(refused.contains("frozen"), "{refused}");
        let batch = block_on(sm.begin_slot_migrations(G, vec![(1, A, B), (2, A, C)]));
        assert_eq!(batch.len(), 2, "a freeze refuses the whole batch");

        // Draining is allowed: retries of in-flight work, commits, and aborts
        // all still function, because a freeze should empty the cluster of
        // moving slots, not strand them mid-transfer.
        block_on(sm.begin_slot_migration(G, 0, A, B)).expect("retry survives the freeze");
        block_on(sm.complete_slot_migration(G, 0)).expect("commit survives the freeze");

        block_on(sm.set_migration_freeze(G, false));
        block_on(sm.begin_slot_migration(G, 1, A, B)).expect("unfrozen works again");
    }

    #[test]
    fn controls_survive_the_snapshot_and_default_for_old_snapshots() {
        let mut sm = placed_sm(2);
        block_on(sm.set_migration_budget(G, 5));
        block_on(sm.set_migration_freeze(G, true));
        block_on(sm.begin_slot_migration(G, 0, A, B))
            .expect_err("frozen before the snapshot, so this must refuse");

        let snapshot = sm.snapshot();
        let mut restored = Slots {
            groups: HashMap::new(),
            controls: HashMap::new(),
            id: 1,
        };
        block_on(restored.recover(snapshot));
        assert_eq!(
            block_on(restored.migration_control(G)),
            MigrationControlView {
                budget: 5,
                frozen: true,
                in_flight: 0,
            },
            "the kill switch must survive a restart -- that is why it lives in the SM"
        );
        assert_eq!(restored.groups, sm.groups, "the table itself must round-trip");

        // A snapshot from before the controls existed is a bare groups map.
        // It must recover the TABLE -- losing it claims nothing is placed
        // anywhere -- with default controls.
        let old_format = crate::utils::serde::serialize(&sm.groups);
        let mut upgraded = Slots {
            groups: HashMap::new(),
            controls: HashMap::new(),
            id: 1,
        };
        block_on(upgraded.recover(old_format));
        assert_eq!(upgraded.groups, sm.groups, "pre-control snapshot must keep the table");
        assert_eq!(
            block_on(upgraded.migration_control(G)),
            MigrationControlView {
                budget: 0,
                frozen: false,
                in_flight: 0,
            }
        );
    }
}
