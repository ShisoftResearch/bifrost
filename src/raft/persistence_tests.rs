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
use super::disk::{self, HardState, StorageEntity};
use super::state_machine::master::SnapshotDataItems;
use super::multi_member_tests::{add_entry, cluster, wait_for_values, Member, NEVER};
use super::*;
use crate::utils::time::async_wait_secs;

/// Stop a member's raft but keep its RPC server listening. A restarting
/// member replays its "add member" entries by connecting to each member, and
/// silently drops one it cannot reach -- with every peer gone it would come
/// back as a single-member plane and promote itself (a pre-existing gap in
/// membership recovery, reported separately). Paused peers stay reachable
/// and run no elections.
async fn pause(member: &Member) {
    member.svc.shutdown().await;
}

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
    let term = members[2].svc.read_meta().await.term + 1000;
    let (a, b) = (members[0].svc.id, members[1].svc.id);
    let voter = members.remove(2);
    // Only the voter runs raft from here: no election of A's or B's can move
    // its term under the test.
    for member in members.iter() {
        pause(member).await;
    }
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

const HARD_STATE_POINTS: [Fail; 3] = [
    Fail::HardStateWrite,
    Fail::HardStateRename,
    Fail::HardStateDirSync,
];

/// Whether a failed hard-state write at `point` already replaced the file
/// (the rename happened; only the directory fsync was missing). A crash there
/// may leave either state on disk; in-process, the new one is visible.
fn renamed_before(point: Fail) -> bool {
    point == Fail::HardStateDirSync
}

/// Each point of the write sequence, failing: the file holds the old state or
/// the new one, never a mix, and a leftover temp file changes nothing.
#[test]
fn a_failed_hard_state_write_leaves_the_old_or_the_new_state() {
    let dir = super::compaction_tests::scratch_dir("hard_state_points");
    let old = HardState {
        term: 5,
        voted_for: Some(11),
    };
    let new = HardState {
        term: 6,
        voted_for: Some(22),
    };
    for point in HARD_STATE_POINTS {
        disk::write_hard_state(&dir, &old).unwrap();
        failpoints::arm(&dir, point);
        assert!(disk::write_hard_state(&dir, &new).is_err());
        failpoints::disarm(&dir, point);
        let read = disk::read_hard_state(&dir).unwrap().unwrap();
        let expected = if renamed_before(point) { new } else { old };
        assert_eq!(read, expected, "{:?}", point);
        // Whatever was left behind, the next write wins.
        disk::write_hard_state(&dir, &new).unwrap();
        assert_eq!(disk::read_hard_state(&dir).unwrap(), Some(new));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A damaged hard-state file, or one of another format version, stops the
/// member from starting with a typed error: guessing a term or forgetting a
/// vote is how a member votes twice, and there is no reader for old versions.
#[test]
fn a_damaged_hard_state_refuses_to_start() {
    let dir = super::compaction_tests::scratch_dir("hard_state_damaged");
    disk::write_hard_state(&dir, &HardState { term: 9, voted_for: None }).unwrap();
    let path = dir.join("hardstate.dat");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[6] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    let problem = |dir: &std::path::Path| {
        let opts = super::compaction_tests::disk_options(dir, NEVER, NEVER);
        let (mut term, mut commit, mut applied) = (0, 0, 0);
        let mut logs = LogsMap::new();
        match StorageEntity::new_with_options(&opts, &mut term, &mut commit, &mut applied, &mut logs) {
            Err(e) => e
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<disk::BadHardState>())
                .map(|bad| bad.problem.clone())
                .expect("a typed BadHardState"),
            Ok(_) => panic!("an unreadable hard state was accepted"),
        }
    };
    assert!(matches!(problem(&dir), disk::HardStateProblem::Damaged(_)));
    // Another format version is refused as such, not read or reset.
    let mut other_version = std::fs::read(&path).unwrap();
    other_version[6] ^= 0xff; // restore the byte damaged above
    // `RHS1` is the store before the seed became a voter in the log: refused.
    other_version[3] = b'1';
    std::fs::write(&path, &other_version).unwrap();
    assert_eq!(problem(&dir), disk::HardStateProblem::Version(b'1'));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A vote whose persistence fails at any point is answered "not granted", and
/// after a restart the member's durable state is the old one or the full new
/// one (term and vote together) -- never a vote it could deny having cast.
#[tokio::test(flavor = "multi_thread")]
async fn a_vote_is_granted_only_once_durable() {
    let _ = env_logger::try_init();
    // Four members: the voter's configuration after a restart is replayed
    // from its log, which holds the members that JOINED (B and C) but not the
    // bootstrap member A (a pre-existing gap in membership recovery). The
    // candidates are B and C.
    let mut members = cluster("vote_points", 4, NEVER).await;
    members[0].add(1).await;
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 1).await, vec![1, 1, 1, 1]);
    }
    let (last_id, last_term) = members[0].position().await;
    let (a, b) = (members[1].svc.id, members[2].svc.id);
    let mut voter = members.remove(3);
    // Only the voter runs raft from here: no election of A's or B's can move
    // its term under the test.
    for member in members.iter() {
        pause(member).await;
    }
    let base = voter.svc.read_meta().await.term + 1000;
    for (round, point) in HARD_STATE_POINTS.iter().copied().enumerate() {
        let term = base + 10 * round as u64;
        let before = {
            let meta = voter.svc.read_meta().await;
            (meta.term, meta.vote_for)
        };
        failpoints::arm(&voter.dir, point);
        let answer = Service::request_vote(
            voter.svc.as_ref(),
            PlaneId::type1(),
            term,
            a,
            last_id,
            last_term,
        )
        .await;
        failpoints::disarm(&voter.dir, point);
        assert!(!answer.1, "{:?}: a vote that was not durable was granted", point);

        voter = restart(voter, NEVER).await;
        let durable = voter.svc.read_meta().await;
        let (durable_term, durable_vote) = (durable.term, durable.vote_for);
        drop(durable);
        if renamed_before(point) {
            assert_eq!((durable_term, durable_vote), (term, Some(a)), "{:?}", point);
        } else {
            assert_eq!((durable_term, durable_vote), before, "{:?}", point);
        }
        // Either way exactly one candidate can hold this member's vote in
        // `term`: A if the vote reached the disk, otherwise whoever asks.
        let other = Service::request_vote(
            voter.svc.as_ref(),
            PlaneId::type1(),
            term,
            b,
            last_id,
            last_term,
        )
        .await;
        assert_eq!(other.1, !renamed_before(point), "{:?}", point);
    }
    voter.stop().await;
    for member in members {
        member.stop().await;
    }
}

/// A term adopted from a leader is durable before the follower answers in it:
/// if persisting fails at any point the AppendEntries is not acknowledged.
#[tokio::test(flavor = "multi_thread")]
async fn a_term_is_answered_in_only_once_durable() {
    let _ = env_logger::try_init();
    let mut members = cluster("term_points", 3, NEVER).await;
    members[0].add(1).await;
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 1).await, vec![1, 1, 1]);
    }
    let leader_id = members[0].svc.id;
    let mut follower = members.remove(2);
    for member in members.iter() {
        pause(member).await;
    }
    for (round, point) in HARD_STATE_POINTS.iter().copied().enumerate() {
        let before = follower.svc.read_meta().await.term;
        let term = before + 5 + round as u64;
        failpoints::arm(&follower.dir, point);
        let (_, answer) = append_one(&follower, term, leader_id).await;
        failpoints::disarm(&follower.dir, point);
        assert!(
            matches!(answer, AppendEntriesResult::NotPersisted),
            "{:?}: answered {:?} in a term it could not persist",
            point,
            answer
        );
        follower = restart(follower, NEVER).await;
        let durable_term = follower.svc.read_meta().await.term;
        let expected = if renamed_before(point) { term } else { before };
        assert_eq!(durable_term, expected, "{:?}", point);
        assert_eq!(follower.value().await, 1, "{:?}: an entry applied", point);
    }
    follower.stop().await;
    for member in members {
        member.stop().await;
    }
}

/// A store with history but no hard state cannot know its term or vote: it
/// refuses to start with a typed error (no derivation -- such stores are
/// reimported). A brand-new store creates the file at term 0.
#[tokio::test(flavor = "multi_thread")]
async fn a_store_with_history_but_no_hard_state_refuses_to_start() {
    let _ = env_logger::try_init();
    let mut members = cluster("no_hard_state", 1, NEVER).await;
    let node = members.remove(0);
    for _ in 0..4 {
        node.add(1).await;
    }
    let (dir, _addr) = node.stop().await;
    std::fs::remove_file(dir.join("hardstate.dat")).expect("the store had a hard state");

    let opts = super::compaction_tests::disk_options(&dir, NEVER, NEVER);
    let (mut term, mut commit, mut applied) = (0, 0, 0);
    let mut logs = LogsMap::new();
    let err = match StorageEntity::new_with_options(&opts, &mut term, &mut commit, &mut applied, &mut logs) {
        Err(e) => e,
        Ok(_) => panic!("a store with a log but no hard state started"),
    };
    assert!(
        err.get_ref()
            .and_then(|inner| inner.downcast_ref::<disk::MissingHardState>())
            .is_some(),
        "expected MissingHardState, got {:?}",
        err
    );
    assert!(
        !dir.join("hardstate.dat").exists(),
        "the refusal must not invent a hard state"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_new_store_creates_its_hard_state() {
    let dir = super::compaction_tests::scratch_dir("fresh_hard_state");
    let opts = super::compaction_tests::disk_options(&dir, NEVER, NEVER);
    let (mut term, mut commit, mut applied) = (0, 0, 0);
    let mut logs = LogsMap::new();
    let storage =
        StorageEntity::new_with_options(&opts, &mut term, &mut commit, &mut applied, &mut logs)
            .expect("a new store starts")
            .expect("disk storage");
    assert_eq!(term, 0);
    assert_eq!(storage.hard_state, HardState::default());
    assert_eq!(disk::read_hard_state(&dir).unwrap(), Some(HardState::default()));
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Replace the persisted snapshot of the store at `dir` with `image` (a valid
/// file around it -- checksum and framing intact -- so only the image is bad).
async fn persist_snapshot_image(dir: &std::path::Path, index: u64, image: Vec<u8>) {
    let mut storage = StorageEntity {
        logs: None,
        snapshot: None,
        last_term: 0,
        base_path: dir.to_path_buf(),
        plane_id: PlaneId::type1(),
        wal_index: Default::default(),
        poisoned: None,
        hard_state: Default::default(),
    };
    storage
        .write_snapshot(&SnapshotEntity {
            last_included_index: index,
            last_included_term: 0,
            snapshot: image,
        })
        .await
        .unwrap();
}

/// A persisted snapshot whose image does not restore stops startup: the
/// master image itself, or a part of it (here the member list). It never
/// starts from an empty or stale state.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_that_does_not_restore_stops_startup() {
    let _ = env_logger::try_init();
    let bad_parts: SnapshotDataItems = vec![(
        crate::raft::state_machine::configs::CONFIG_SM_ID,
        vec![0xff, 0x00, 0x13],
    )];
    let images = [
        ("master", vec![0xff, 0x00, 0x13, 0x37]),
        ("member_list", crate::utils::serde::serialize(&bad_parts)),
    ];
    for (what, image) in images {
        let mut members = cluster(&format!("bad_snapshot_{}", what), 1, NEVER).await;
        let node = members.remove(0);
        node.add(1).await;
        let applied = node.svc.read_meta().await.last_applied;
        let (dir, addr) = node.stop().await;
        persist_snapshot_image(&dir, applied, image).await;

        let mut opts = super::compaction_tests::disk_options(&dir, NEVER, NEVER);
        opts.address = addr.clone();
        let svc = RaftService::new(opts);
        assert!(
            !RaftService::start(&svc, false).await,
            "{}: started on a snapshot it cannot restore",
            what
        );
        svc.shutdown().await;
    }
}

/// InstallSnapshot of an image that does not decode is refused before
/// anything is persisted or replaced; one whose part cannot be restored stops
/// the member's plane (it acknowledges nothing more) instead of running on
/// state it does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn an_unrestorable_installed_snapshot_is_refused_or_stops_the_plane() {
    let _ = env_logger::try_init();
    let mut members = cluster("bad_install", 3, NEVER).await;
    members[0].add(1).await;
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 1).await, vec![1, 1, 1]);
    }
    let leader_id = members[0].svc.id;
    let follower = members.remove(2);
    for member in members.iter() {
        pause(member).await;
    }
    let (term, applied) = {
        let meta = follower.svc.read_meta().await;
        (meta.term, meta.last_applied)
    };
    let snapshot_file = follower.dir.join("snapshot.dat");
    let before = std::fs::read(&snapshot_file).unwrap_or_default();

    let answer = Service::install_snapshot(
        follower.svc.as_ref(),
        PlaneId::type1(),
        term,
        leader_id,
        applied + 10,
        term,
        vec![0xff, 0x00, 0x13, 0x37],
    )
    .await;
    assert_eq!(answer, InstallSnapshotResult::Unrestorable);
    assert_eq!(follower.svc.read_meta().await.last_applied, applied);
    assert_eq!(
        std::fs::read(&snapshot_file).unwrap_or_default(),
        before,
        "an undecodable snapshot was persisted"
    );
    assert_eq!(follower.value().await, 1);

    let bad_parts: SnapshotDataItems = vec![(
        crate::raft::state_machine::configs::CONFIG_SM_ID,
        vec![0xff, 0x00, 0x13],
    )];
    let answer = Service::install_snapshot(
        follower.svc.as_ref(),
        PlaneId::type1(),
        term,
        leader_id,
        applied + 10,
        term,
        crate::utils::serde::serialize(&bad_parts),
    )
    .await;
    assert_eq!(answer, InstallSnapshotResult::Unrestorable);
    let (_, after) = append_one(&follower, term, leader_id).await;
    assert!(
        matches!(after, AppendEntriesResult::NotPersisted),
        "a member that stopped on an unrestorable snapshot acknowledged {:?}",
        after
    );
    follower.stop().await;
    for member in members {
        member.stop().await;
    }
}
