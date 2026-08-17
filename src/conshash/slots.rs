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

pub struct Slots {
    pub groups: HashMap<u64, GroupSlots>,
    pub id: u64,
}

raft_state_machine! {
    def cmd set_slot_owner(group: u64, slot: u32, owner: u64);
    def cmd adopt_slots(group: u64, assignments: Vec<(u32, u64)>) -> usize;
    def cmd begin_slot_migration(group: u64, slot: u32, from: u64, to: u64) -> Result<(), String>;
    def cmd complete_slot_migration(group: u64, slot: u32) -> Result<u64, String>;
    def cmd abort_slot_migration(group: u64, slot: u32) -> Result<u64, String>;
    def qry slot_state(group: u64, slot: u32) -> Option<SlotState>;
    def qry all_slots(group: u64) -> Option<HashMap<u32, SlotState>>;
    def qry slots_owned_by(group: u64, server: u64) -> Vec<u32>;
    def qry placed_slot_count(group: u64) -> usize;
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

    fn begin_slot_migration(
        &mut self,
        group: u64,
        slot: u32,
        from: u64,
        to: u64,
    ) -> BoxFuture<Result<(), String>> {
        let slots = self.groups.entry(group).or_insert_with(HashMap::new);
        let result = match slots.get(&slot) {
            Some(SlotState::Stable { owner }) if *owner == from => {
                if from == to {
                    Err(format!("slot {slot} migration from {from} to itself"))
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

    fn slot_state(&self, group: u64, slot: u32) -> BoxFuture<Option<SlotState>> {
        future::ready(self.groups.get(&group).and_then(|slots| slots.get(&slot).copied())).boxed()
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
        future::ready(self.groups.get(&group).map(|slots| slots.len()).unwrap_or(0)).boxed()
    }
}

impl StateMachineCtl for Slots {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.id
    }
    fn snapshot(&self) -> Vec<u8> {
        crate::utils::serde::serialize(&self.groups)
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        match crate::utils::serde::deserialize::<HashMap<u64, GroupSlots>>(data.as_slice()) {
            Some(groups) => self.groups = groups,
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
