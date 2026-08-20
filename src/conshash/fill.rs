//! Filling a joining member: the one rebalancing trigger with an unambiguous
//! right answer.
//!
//! Under stored slot placement a joiner owns **nothing** — that is the
//! orphaning fix — so without a fill a new machine stays empty forever. This
//! module is the coordinator half of the Phase 4 node manager: it decides
//! *which* slots move. What a slot's bytes are, whether a recipient can take
//! more, and how cells actually travel are questions for the storage layer,
//! reached through [`FillFabric`] — this crate cannot and must not know what a
//! cell is.
//!
//! The policy is fixed by decision record
//! (`docs/superpowers/specs/2026-08-19-phase-4-node-manager-decision.md` in
//! the consumer repo):
//!
//! - **"Filled" means stopping BEFORE the move that would push the joiner
//!   past the cluster mean.** No tunable threshold, and self-hysteretic: a
//!   fill can never overshoot, so a fill can never trigger another.
//! - **Largest slots first, from the currently-largest holder**, skipping any
//!   slot bigger than the remaining gap; the donor is re-picked every round
//!   so the fill drains the actually-largest holder, not a snapshot of it.
//! - A fill stops when the gap is closed, no remaining slot fits it, the
//!   recipient declines, the group is frozen, or a move fails (which is how
//!   the state machine's budget refusal arrives here).

use super::slots::client::SMClient as SlotsSMClient;
use super::slots::MigrationControlView;
use crate::raft::state_machine::master::ExecError;
use futures::future::BoxFuture;
use futures::FutureExt;
use std::sync::Arc;

/// The storage layer's side of a fill. Implemented by the consumer (for Neb:
/// over its cell-service RPCs and its migration module).
pub trait FillFabric: Send + Sync + 'static {
    /// Total live bytes each of `members` holds, positionally.
    fn member_totals(&self, members: &[u64]) -> BoxFuture<'_, Result<Vec<u64>, String>>;
    /// Live bytes for each of `slots` on `member`, positionally.
    fn slot_bytes(&self, member: u64, slots: &[u32]) -> BoxFuture<'_, Result<Vec<u64>, String>>;
    /// May `recipient` take roughly `bytes` more right now? The recipient's
    /// veto: a member over its tier limit or low on disk answers no and the
    /// fill stops rather than pushing it over.
    fn admit(&self, recipient: u64, bytes: u64) -> BoxFuture<'_, Result<bool, String>>;
    /// Move `slots` from `from` to `to`: transfer, commit, reclaim. An error
    /// stops the fill — this is also how the SM's budget refusal surfaces.
    fn move_slots(&self, slots: &[u32], from: u64, to: u64)
        -> BoxFuture<'_, Result<(), String>>;
}

/// The placement questions the driver asks, factored out of [`SlotsSMClient`]
/// so the driver is testable without standing up a raft cluster.
pub trait PlacementView: Send + Sync + 'static {
    fn slots_owned_by(&self, group: u64, member: u64) -> BoxFuture<'_, Result<Vec<u32>, String>>;
    fn migration_control(&self, group: u64)
        -> BoxFuture<'_, Result<MigrationControlView, String>>;
}

impl PlacementView for SlotsSMClient {
    fn slots_owned_by(&self, group: u64, member: u64) -> BoxFuture<'_, Result<Vec<u32>, String>> {
        async move {
            SlotsSMClient::slots_owned_by(self, &group, &member)
                .await
                .map_err(describe_exec_error)
        }
        .boxed()
    }
    fn migration_control(
        &self,
        group: u64,
    ) -> BoxFuture<'_, Result<MigrationControlView, String>> {
        async move {
            SlotsSMClient::migration_control(self, &group)
                .await
                .map_err(describe_exec_error)
        }
        .boxed()
    }
}

fn describe_exec_error(e: ExecError) -> String {
    format!("{e:?}")
}

/// Why a fill ended. Every variant is a legitimate end state — `Filled` is the
/// goal, and the rest say precisely what to look at before running again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FillStop {
    /// The joiner reached the mean, or came as close as whole slots permit:
    /// the decided stop rule is "stop BEFORE the move that would overshoot",
    /// so a residual gap smaller than every remaining slot is a completed
    /// fill, not a failed one.
    Filled,
    /// The fill could move NOTHING: every candidate slot is bigger than the
    /// entire gap (one hub slot holding most of the data), so moving any of
    /// them would just hand the imbalance to the joiner.
    NoSlotFits,
    /// The recipient declined the next batch.
    RecipientDeclined,
    /// The group's kill switch is set.
    Frozen,
    /// A move failed; the error is preserved. The SM's budget refusal lands
    /// here too, which is deliberate: the budget is enforced THERE so this
    /// driver and a concurrent operator cannot jointly exceed it.
    MoveFailed(String),
    /// The defensive round cap tripped; nothing converging takes this long.
    RoundLimit,
}

#[derive(Debug, Clone)]
pub struct FillReport {
    pub moved_slots: usize,
    pub moved_bytes: u64,
    pub rounds: usize,
    pub stopped: FillStop,
}

/// One round's picks from one donor: largest first, each capped by what is
/// left of the gap, at most `round_batch` slots. Pure, so the policy is
/// testable as arithmetic.
fn plan_round(gap: u64, donor_slots: &mut Vec<(u32, u64)>, round_batch: usize) -> Vec<(u32, u64)> {
    donor_slots.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut remaining = gap;
    let mut picks = Vec::new();
    for (slot, bytes) in donor_slots.iter() {
        if picks.len() >= round_batch {
            break;
        }
        // An empty slot moves no bytes toward the mean; filling is not the
        // place to shuffle empties around.
        if *bytes == 0 || *bytes > remaining {
            continue;
        }
        picks.push((*slot, *bytes));
        remaining -= *bytes;
    }
    picks
}

pub struct JoinFiller {
    placement: Arc<dyn PlacementView>,
    fabric: Arc<dyn FillFabric>,
    group: u64,
    /// Most slots moved per round; bounds how much is in flight between
    /// re-reads of the cluster's totals.
    pub round_batch: usize,
    /// Defensive bound on rounds.
    pub max_rounds: usize,
}

impl JoinFiller {
    pub fn new(placement: Arc<dyn PlacementView>, fabric: Arc<dyn FillFabric>, group: u64) -> Self {
        Self {
            placement,
            fabric,
            group,
            round_batch: 16,
            max_rounds: 4096,
        }
    }

    /// Fill `joiner` toward the mean of `members` (which must include it).
    ///
    /// Callers gate on leadership; this gates on the kill switch. Safe to
    /// re-run at any time: a filled member sits at the mean, so a second call
    /// is a no-op — which is also the hysteresis story, since re-running on
    /// every ring twitch moves nothing once the member is full.
    pub async fn fill_joiner(&self, joiner: u64, members: &[u64]) -> Result<FillReport, String> {
        let mut report = FillReport {
            moved_slots: 0,
            moved_bytes: 0,
            rounds: 0,
            stopped: FillStop::RoundLimit,
        };
        if !members.contains(&joiner) {
            return Err(format!("joiner {joiner} is not among the members"));
        }
        while report.rounds < self.max_rounds {
            report.rounds += 1;

            // Checked every round, not once: an operator freezing the group
            // mid-fill means "stop moving things", not "finish your plan".
            let control = self.placement.migration_control(self.group).await?;
            if control.frozen {
                report.stopped = FillStop::Frozen;
                return Ok(report);
            }

            let totals = self.fabric.member_totals(members).await?;
            if totals.len() != members.len() {
                return Err(format!(
                    "member_totals answered {} entries for {} members",
                    totals.len(),
                    members.len()
                ));
            }
            let mean = totals.iter().sum::<u64>() / members.len() as u64;
            let joiner_idx = members.iter().position(|m| *m == joiner).unwrap();
            let gap = mean.saturating_sub(totals[joiner_idx]);
            if gap == 0 {
                report.stopped = FillStop::Filled;
                return Ok(report);
            }

            // The CURRENTLY largest holder, re-picked every round.
            let (donor_idx, _) = totals
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != joiner_idx)
                .max_by_key(|(_, total)| **total)
                .ok_or_else(|| "a fill needs at least one other member".to_string())?;
            let donor = members[donor_idx];

            let owned = self.placement.slots_owned_by(self.group, donor).await?;
            let bytes = self.fabric.slot_bytes(donor, &owned).await?;
            if bytes.len() != owned.len() {
                return Err(format!(
                    "slot_bytes answered {} entries for {} slots",
                    bytes.len(),
                    owned.len()
                ));
            }
            let mut donor_slots: Vec<(u32, u64)> =
                owned.into_iter().zip(bytes.into_iter()).collect();
            let picks = plan_round(gap, &mut donor_slots, self.round_batch);
            if picks.is_empty() {
                // No further slot fits the remaining gap. If earlier rounds
                // made progress this is the stop rule doing its job; only a
                // fill that could move nothing at all reports NoSlotFits.
                report.stopped = if report.moved_slots > 0 {
                    FillStop::Filled
                } else {
                    FillStop::NoSlotFits
                };
                return Ok(report);
            }
            let batch_bytes: u64 = picks.iter().map(|(_, b)| *b).sum();

            if !self.fabric.admit(joiner, batch_bytes).await? {
                report.stopped = FillStop::RecipientDeclined;
                return Ok(report);
            }

            let slots: Vec<u32> = picks.iter().map(|(s, _)| *s).collect();
            if let Err(error) = self.fabric.move_slots(&slots, donor, joiner).await {
                report.stopped = FillStop::MoveFailed(error);
                return Ok(report);
            }
            report.moved_slots += slots.len();
            report.moved_bytes += batch_bytes;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    const G: u64 = 7;
    const A: u64 = 100; // loaded member
    const B: u64 = 200; // second member
    const J: u64 = 300; // joiner

    /// An in-memory cluster: members hold slots holding bytes, moves actually
    /// move them, and every fabric answer is derived from this one state --
    /// so the driver is tested against a world that reacts to it.
    struct FakeCluster {
        state: Mutex<HashMap<u64, HashMap<u32, u64>>>,
        control: Mutex<MigrationControlView>,
        admit_up_to: Mutex<u64>,
        moves: Mutex<Vec<(Vec<u32>, u64, u64)>>,
        fail_moves: Mutex<bool>,
    }

    impl FakeCluster {
        fn new(holdings: &[(u64, &[(u32, u64)])]) -> Arc<Self> {
            let mut state = HashMap::new();
            for (member, slots) in holdings {
                state.insert(*member, slots.iter().copied().collect());
            }
            Arc::new(Self {
                state: Mutex::new(state),
                control: Mutex::new(MigrationControlView {
                    budget: 0,
                    frozen: false,
                    in_flight: 0,
                }),
                admit_up_to: Mutex::new(u64::MAX),
                moves: Mutex::new(Vec::new()),
                fail_moves: Mutex::new(false),
            })
        }
        fn total(&self, member: u64) -> u64 {
            self.state
                .lock()
                .get(&member)
                .map(|slots| slots.values().sum())
                .unwrap_or(0)
        }
    }

    impl PlacementView for FakeCluster {
        fn slots_owned_by(
            &self,
            _group: u64,
            member: u64,
        ) -> BoxFuture<'_, Result<Vec<u32>, String>> {
            let owned = self
                .state
                .lock()
                .get(&member)
                .map(|slots| slots.keys().copied().collect())
                .unwrap_or_default();
            async move { Ok(owned) }.boxed()
        }
        fn migration_control(
            &self,
            _group: u64,
        ) -> BoxFuture<'_, Result<MigrationControlView, String>> {
            let control = *self.control.lock();
            async move { Ok(control) }.boxed()
        }
    }

    impl FillFabric for FakeCluster {
        fn member_totals(&self, members: &[u64]) -> BoxFuture<'_, Result<Vec<u64>, String>> {
            let totals = members.iter().map(|m| self.total(*m)).collect();
            async move { Ok(totals) }.boxed()
        }
        fn slot_bytes(
            &self,
            member: u64,
            slots: &[u32],
        ) -> BoxFuture<'_, Result<Vec<u64>, String>> {
            let state = self.state.lock();
            let holdings = state.get(&member);
            let bytes = slots
                .iter()
                .map(|slot| holdings.and_then(|h| h.get(slot)).copied().unwrap_or(0))
                .collect();
            async move { Ok(bytes) }.boxed()
        }
        fn admit(&self, _recipient: u64, bytes: u64) -> BoxFuture<'_, Result<bool, String>> {
            let ok = bytes <= *self.admit_up_to.lock();
            async move { Ok(ok) }.boxed()
        }
        fn move_slots(
            &self,
            slots: &[u32],
            from: u64,
            to: u64,
        ) -> BoxFuture<'_, Result<(), String>> {
            if *self.fail_moves.lock() {
                return async move { Err("budget exhausted (from the SM)".to_string()) }.boxed();
            }
            let mut state = self.state.lock();
            for slot in slots {
                if let Some(bytes) = state.get_mut(&from).and_then(|h| h.remove(slot)) {
                    state.entry(to).or_default().insert(*slot, bytes);
                }
            }
            self.moves.lock().push((slots.to_vec(), from, to));
            async move { Ok(()) }.boxed()
        }
    }

    fn filler(cluster: &Arc<FakeCluster>) -> JoinFiller {
        JoinFiller::new(
            cluster.clone() as Arc<dyn PlacementView>,
            cluster.clone() as Arc<dyn FillFabric>,
            G,
        )
    }

    #[test]
    fn plan_takes_largest_first_and_never_overshoots() {
        let mut slots = vec![(1, 10), (2, 40), (3, 25), (4, 0), (5, 100)];
        let picks = plan_round(80, &mut slots, 16);
        // 100 exceeds the gap and is skipped; 40 then 25 then 10 fit (75).
        assert_eq!(picks, vec![(2, 40), (3, 25), (1, 10)]);
        let total: u64 = picks.iter().map(|(_, b)| b).sum();
        assert!(total <= 80, "a round must never plan past the gap");
    }

    #[test]
    fn a_joiner_is_filled_to_the_mean_and_a_rerun_moves_nothing() {
        // A holds everything; the mean of (1000, 0, 0) is 333.
        let holdings: Vec<(u32, u64)> = (0..10).map(|slot| (slot, 100)).collect();
        let cluster = FakeCluster::new(&[(A, &holdings), (B, &[]), (J, &[])]);
        let report = futures::executor::block_on(
            filler(&cluster).fill_joiner(J, &[A, B, J]),
        )
        .unwrap();

        assert_eq!(report.stopped, FillStop::Filled);
        assert_eq!(report.moved_bytes, 300, "3 slots of 100 reach the mean of 333");
        assert_eq!(cluster.total(J), 300);
        assert!(cluster.total(A) >= 700);

        // Hysteresis is the stop rule itself: run it again, nothing moves.
        let again =
            futures::executor::block_on(filler(&cluster).fill_joiner(J, &[A, B, J])).unwrap();
        assert_eq!(again.moved_slots, 0, "a filled member must not be refilled");
        assert!(matches!(again.stopped, FillStop::Filled | FillStop::NoSlotFits));
    }

    #[test]
    fn the_donor_is_repicked_each_round() {
        // Two loaded members; with round_batch=1 the fill must alternate
        // between them rather than draining the first it saw.
        let a_slots: Vec<(u32, u64)> = (0..4).map(|slot| (slot, 100)).collect();
        let b_slots: Vec<(u32, u64)> = (10..14).map(|slot| (slot, 100)).collect();
        let cluster = FakeCluster::new(&[(A, &a_slots), (B, &b_slots), (J, &[])]);
        let mut filler = filler(&cluster);
        filler.round_batch = 1;
        let report =
            futures::executor::block_on(filler.fill_joiner(J, &[A, B, J])).unwrap();
        assert_eq!(report.stopped, FillStop::Filled);

        let moves = cluster.moves.lock();
        let donors: Vec<u64> = moves.iter().map(|(_, from, _)| *from).collect();
        assert!(
            donors.contains(&A) && donors.contains(&B),
            "both loaded members must donate; donors were {donors:?}"
        );
        // Neither donor may end up below the joiner: taking from the largest
        // each round is what prevents creating a new imbalance.
        assert!(cluster.total(A) >= cluster.total(J) - 100);
        assert!(cluster.total(B) >= cluster.total(J) - 100);
    }

    #[test]
    fn a_declined_batch_stops_the_fill() {
        let holdings: Vec<(u32, u64)> = (0..10).map(|slot| (slot, 100)).collect();
        let cluster = FakeCluster::new(&[(A, &holdings), (J, &[])]);
        *cluster.admit_up_to.lock() = 0;
        let report =
            futures::executor::block_on(filler(&cluster).fill_joiner(J, &[A, J])).unwrap();
        assert_eq!(report.stopped, FillStop::RecipientDeclined);
        assert_eq!(report.moved_slots, 0);
        assert_eq!(cluster.total(J), 0, "a declined fill must move nothing");
    }

    #[test]
    fn a_frozen_group_fills_nothing() {
        let holdings: Vec<(u32, u64)> = (0..4).map(|slot| (slot, 100)).collect();
        let cluster = FakeCluster::new(&[(A, &holdings), (J, &[])]);
        cluster.control.lock().frozen = true;
        let report =
            futures::executor::block_on(filler(&cluster).fill_joiner(J, &[A, J])).unwrap();
        assert_eq!(report.stopped, FillStop::Frozen);
        assert_eq!(report.moved_slots, 0);
    }

    #[test]
    fn a_failed_move_stops_and_preserves_the_error() {
        let holdings: Vec<(u32, u64)> = (0..4).map(|slot| (slot, 100)).collect();
        let cluster = FakeCluster::new(&[(A, &holdings), (J, &[])]);
        *cluster.fail_moves.lock() = true;
        let report =
            futures::executor::block_on(filler(&cluster).fill_joiner(J, &[A, J])).unwrap();
        match report.stopped {
            FillStop::MoveFailed(reason) => assert!(reason.contains("budget exhausted")),
            other => panic!("expected MoveFailed, got {other:?}"),
        }
    }

    #[test]
    fn one_giant_slot_cannot_be_moved_without_overshooting() {
        // A single slot holding everything: moving it would swap the imbalance
        // to the joiner, so the answer is to move nothing and say why.
        let cluster = FakeCluster::new(&[(A, &[(1, 1000)]), (J, &[])]);
        let report =
            futures::executor::block_on(filler(&cluster).fill_joiner(J, &[A, J])).unwrap();
        assert_eq!(report.stopped, FillStop::NoSlotFits);
        assert_eq!(report.moved_slots, 0);
    }
}
