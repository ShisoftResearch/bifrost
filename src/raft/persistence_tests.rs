//! What a member says is what it has made durable.
//!
//! A member never acknowledges, commits or votes on the strength of a write
//! that failed: the failure surfaces as a refusal (a follower's
//! AppendEntries answer, a leader's command answer, a denied vote), and the
//! state a restart recovers agrees with every answer given. Failures are
//! injected through `disk::failpoints`, exactly where the disk call would fail.
//!
//! The hard state (term, vote) survives a restart, so a member neither votes
//! twice in one term nor comes back at an inflated term that deposes the
//! leader.

use super::disk::failpoints::{self, Fail};
use super::multi_member_tests::{add_entry, cluster, wait_for_values, Member, NEVER};
use super::*;
use crate::utils::time::async_wait_secs;

fn acknowledged(answer: &ClientCmdResponse) -> bool {
    matches!(answer, ClientCmdResponse::Success { .. })
}

/// Restart one member in place: same directory, same address.
async fn restart(member: Member, compaction_threshold: u64) -> Member {
    let (dir, addr) = member.stop().await;
    async_wait_secs().await;
    Member::launch(&dir, &addr, compaction_threshold).await
}

/// AppendEntries straight to `follower` as from `leader_id` in `term`,
/// carrying one new entry after the follower's last position and a commit
/// index that covers it.
async fn append_one(follower: &Member, term: u64, leader_id: u64) -> (u64, AppendEntriesResult) {
    let (prev_id, prev_term) = follower.position().await;
    Service::append_entries(
        follower.svc.as_ref(),
        PlaneId::type1(),
        term,
        leader_id,
        prev_id,
        prev_term,
        &Some(vec![add_entry(prev_id + 1, term, 1)]),
        prev_id + 1,
    )
    .await
}

/// A follower whose WAL write (or its fsync) fails must not answer Ok, and
/// must not commit or apply the entry: its leader would count a copy that a
/// crash erases.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_refuses_entries_it_failed_to_persist() {
    let _ = env_logger::try_init();
    for point in [Fail::WalAppend, Fail::WalSync] {
        let mut members = cluster(&format!("follower_{:?}", point), 2, NEVER).await;
        members[0].add(1).await;
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 1).await, vec![1, 1]);
        // Stop the leader so only this test's AppendEntries reaches B.
        let leader = members.remove(0);
        let leader_id = leader.svc.id;
        let term = leader.svc.read_meta().await.term;
        leader.stop().await;
        let follower = members.remove(0);
        let commit_before = follower.svc.read_meta().await.commit_index;

        failpoints::arm(&follower.dir, point);
        let (_, answer) = append_one(&follower, term, leader_id).await;
        failpoints::disarm(&follower.dir, point);
        assert!(
            !matches!(answer, AppendEntriesResult::Ok),
            "{:?}: B acknowledged an entry it failed to persist",
            point
        );
        assert_eq!(
            follower.svc.read_meta().await.commit_index,
            commit_before,
            "{:?}: B committed past what it persisted",
            point
        );
        assert_eq!(follower.value().await, 1, "{:?}: B applied it", point);
        follower.stop().await;
    }
}

/// A leader whose WAL write (or fsync) fails must answer the client with a
/// failure, not commit, not apply -- and a restart must not bring the
/// command back.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_refuses_a_command_it_failed_to_persist() {
    let _ = env_logger::try_init();
    for point in [Fail::WalAppend, Fail::WalSync] {
        let mut members = cluster(&format!("leader_{:?}", point), 1, NEVER).await;
        let mut node = members.remove(0);
        node.add(1).await;
        let commit_before = node.svc.read_meta().await.commit_index;

        failpoints::arm(&node.dir, point);
        let answer = node.command(10).await;
        failpoints::disarm(&node.dir, point);
        assert!(
            !acknowledged(&answer),
            "{:?}: the leader acknowledged a command it failed to persist: {:?}",
            point,
            answer
        );
        assert_eq!(node.svc.read_meta().await.commit_index, commit_before);
        assert_eq!(node.value().await, 1, "{:?}: the refused command applied", point);

        node = restart(node, NEVER).await;
        assert_eq!(
            node.value().await,
            1,
            "{:?}: the refused command came back after a restart",
            point
        );
        node.stop().await;
    }
}

/// The commit index is the replay bound after a restart: a leader that
/// cannot persist it must not commit (or answer Success), or an acknowledged
/// command is invisible after the next restart.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_does_not_commit_past_an_unpersisted_commit_index() {
    let _ = env_logger::try_init();
    let mut members = cluster("commit_progress", 1, NEVER).await;
    let node = members.remove(0);
    node.add(1).await;
    let commit_before = node.svc.read_meta().await.commit_index;

    failpoints::arm(&node.dir, Fail::CommitProgress);
    let answer = node.command(10).await;
    failpoints::disarm(&node.dir, Fail::CommitProgress);
    assert!(
        !acknowledged(&answer),
        "the leader acknowledged a commit it could not persist: {:?}",
        answer
    );
    assert_eq!(node.svc.read_meta().await.commit_index, commit_before);
    assert_eq!(node.value().await, 1, "the uncommitted command applied");

    // The entry itself is durable in the log, so the next commit commits it
    // too (Log Matching): its outcome was unknown to the client, never
    // acknowledged, and it is applied once, everywhere.
    let (value, _) = node.add(1).await;
    assert_eq!(value, 12);
    node.stop().await;
}

/// Failing again and again, a leader stops leading instead of carrying on:
/// a member whose disk works can then take over.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_persistence_failures_make_the_leader_step_down() {
    let _ = env_logger::try_init();
    let mut members = cluster("step_down", 1, NEVER).await;
    let node = members.remove(0);
    node.add(1).await;
    failpoints::arm(&node.dir, Fail::WalAppend);
    for _ in 0..3 {
        let answer = node.command(1).await;
        assert!(!acknowledged(&answer), "acknowledged: {:?}", answer);
    }
    failpoints::disarm(&node.dir, Fail::WalAppend);
    assert!(
        !node.is_leader().await,
        "the leader kept leading through repeated persistence failures"
    );
    // With the disk back it can win again.
    assert!(Service::reelect(node.svc.as_ref(), PlaneId::type1()).await);
    let (value, _) = node.add(1).await;
    assert_eq!(value, 2);
    node.stop().await;
}

/// A vote is part of the durable state: a member that voted for A in term
/// T, then restarted, must refuse B in T (Raft Figure 2: votedFor is
/// persistent). Otherwise A and B can both win T.
#[tokio::test(flavor = "multi_thread")]
async fn a_vote_is_not_forgotten_by_a_restart() {
    let _ = env_logger::try_init();
    let mut members = cluster("double_vote", 3, NEVER).await;
    members[0].add(1).await;
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 1).await, vec![1, 1, 1]);
    }
    let (last_id, last_term) = members[0].position().await;
    // Far above the plane's term, so no ordinary election overtakes it
    // during the test.
    let term = members[2].svc.read_meta().await.term + 1000;
    let (a, b) = (members[0].svc.id, members[1].svc.id);
    let voter = members.remove(2);
    let first = Service::request_vote(
        voter.svc.as_ref(),
        PlaneId::type1(),
        term,
        a,
        last_id,
        last_term,
    )
    .await;
    assert!(first.1, "the voter must grant A its vote in term {}", term);

    let voter = restart(voter, NEVER).await;
    let second = Service::request_vote(
        voter.svc.as_ref(),
        PlaneId::type1(),
        term,
        b,
        last_id,
        last_term,
    )
    .await;
    assert!(
        !second.1,
        "after a restart the voter voted again in term {}, for B",
        term
    );
    voter.stop().await;
    for member in members {
        member.stop().await;
    }
}

/// A restarted follower comes back at the plane's term, so the leader keeps
/// leading: no step-down, no election, no new term.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_follower_rejoins_without_an_election() {
    let _ = env_logger::try_init();
    let mut members = cluster("no_churn", 3, NEVER).await;
    for _ in 0..5 {
        members[0].add(1).await;
    }
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 5).await, vec![5, 5, 5]);
    }
    let term = members[0].svc.read_meta().await.term;

    let follower = members.remove(2);
    let follower = restart(follower, NEVER).await;
    assert_eq!(
        follower.svc.read_meta().await.term,
        term,
        "the follower restarted at another term than the plane's"
    );
    // Several heartbeats.
    async_wait_secs().await;
    async_wait_secs().await;
    assert!(
        members[0].is_leader().await,
        "the leader was deposed by a restarted follower"
    );
    assert_eq!(members[0].svc.read_meta().await.term, term, "a new term began");
    members[0].add(1).await;
    members.push(follower);
    let all: Vec<&Member> = members.iter().collect();
    assert_eq!(wait_for_values(&all, 6).await, vec![6, 6, 6]);
    for member in members {
        member.stop().await;
    }
}
