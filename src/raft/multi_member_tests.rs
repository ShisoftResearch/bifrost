//! Multi-member planes: what a member acknowledges is what it holds, in memory
//! and on disk, across leader changes, conflicting suffixes, snapshot
//! installation, compaction and restarts.
//!
//! Every member runs on disk storage with snapshots taken only when a test asks
//! for one, so each scenario controls exactly where the snapshot boundary is.

use super::compaction_tests::commands::{add, get};
use super::compaction_tests::{
    disk_options, scratch_dir, snapshot_and_compact, Ledger, LEDGER_SM_ID,
};
use super::disk::{DiskLogEntry, StorageEntity};
use super::*;
use crate::raft::client::RaftClient;
use crate::rpc::Server;
use crate::utils::time::async_wait_secs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Snapshot threshold that is never reached: snapshots happen on request only.
pub(super) const NEVER: u64 = u64::MAX / 4;

pub(super) struct Member {
    pub(super) svc: Arc<RaftService>,
    pub(super) server: Arc<Server>,
    pub(super) addr: String,
    pub(super) dir: PathBuf,
}

impl Member {
    pub(super) async fn launch(dir: &Path, addr: &str, compaction_threshold: u64) -> Member {
        let mut opts = disk_options(dir, NEVER, compaction_threshold);
        opts.address = addr.to_string();
        let svc = RaftService::new(opts);
        let server = Server::new(&addr.to_string());
        server.register_service(&svc).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&svc, false).await);
        svc.register_state_machine(Box::new(Ledger { value: 0 }))
            .await;
        svc.recover_after_register()
            .await
            .expect("recover the registered state machines");
        Member {
            svc,
            server,
            addr: addr.to_string(),
            dir: dir.to_path_buf(),
        }
    }

    /// Stop the member. `shutdown` persists the WAL and the commit cursor the
    /// member already has, nothing more.
    pub(super) async fn stop(self) -> (PathBuf, String) {
        self.svc.shutdown().await;
        self.server.shutdown().await;
        (self.dir, self.addr)
    }

    pub(super) async fn value(&self) -> u64 {
        let (fn_id, _, data) = get::new().encode();
        let entry = LogEntry {
            id: 0,
            term: 0,
            sm_id: LEDGER_SM_ID,
            fn_id,
            data,
        };
        match Service::c_query(self.svc.as_ref(), PlaneId::type1(), &entry).await {
            ClientQryResponse::Success { data: Ok(data), .. } => get::decode_return(&data),
            other => panic!("local query on {} failed: {:?}", self.addr, other),
        }
    }

    /// Command on this member, which must be the leader. Returns (value, log id).
    pub(super) async fn add(&self, value: u64) -> (u64, u64) {
        match Service::c_command(self.svc.as_ref(), PlaneId::type1(), add_entry(0, 0, value))
            .await
        {
            ClientCmdResponse::Success {
                data: Ok(data),
                last_log_id,
                ..
            } => (add::decode_return(&data), last_log_id),
            other => panic!("command on {} failed: {:?}", self.addr, other),
        }
    }

    /// Command on this member, whatever the answer.
    pub(super) async fn command(&self, value: u64) -> ClientCmdResponse {
        Service::c_command(self.svc.as_ref(), PlaneId::type1(), add_entry(0, 0, value)).await
    }

    pub(super) async fn position(&self) -> (u64, u64) {
        let meta = self.svc.read_meta().await;
        let logs = meta.logs.read().await;
        last_log_position(&meta, &logs)
    }

    pub(super) async fn is_leader(&self) -> bool {
        matches!(self.svc.read_meta().await.membership, Membership::Leader(_))
    }

    /// Entries this member holds above its snapshot, as (id, term, data).
    pub(super) async fn log(&self) -> Vec<(u64, u64, Vec<u8>)> {
        let meta = self.svc.read_meta().await;
        let logs = meta.logs.read().await;
        logs.values()
            .filter(|e| e.id > meta.last_snapshot_index)
            .map(|e| (e.id, e.term, e.data.clone()))
            .collect()
    }
}

pub(super) fn add_entry(id: u64, term: u64, value: u64) -> LogEntry {
    let (fn_id, _, data) = add::new(&value).encode();
    LogEntry {
        id,
        term,
        sm_id: LEDGER_SM_ID,
        fn_id,
        data,
    }
}

pub(super) fn addresses(n: usize) -> Vec<String> {
    let base = 20_000 + (rand::random::<u16>() % 30_000);
    (0..n)
        .map(|i| format!("127.0.0.1:{}", base + i as u16))
        .collect()
}

pub(super) async fn cluster(tag: &str, n: usize, compaction_threshold: u64) -> Vec<Member> {
    let addrs = addresses(n);
    let mut members = Vec::with_capacity(n);
    let first = Member::launch(
        &scratch_dir(&format!("{}_0", tag)),
        &addrs[0],
        compaction_threshold,
    )
    .await;
    first.svc.bootstrap().await;
    members.push(first);
    for (i, addr) in addrs.iter().enumerate().skip(1) {
        let member = Member::launch(
            &scratch_dir(&format!("{}_{}", tag, i)),
            addr,
            compaction_threshold,
        )
        .await;
        member
            .svc
            .join(&vec![addrs[0].clone()])
            .await
            .expect("join the plane");
        members.push(member);
    }
    async_wait_secs().await;
    members
}

/// Poll until every member's ledger reads `expected` (or time out); returns
/// the last values read so the caller's assertion names them.
pub(super) async fn wait_for_values(members: &[&Member], expected: u64) -> Vec<u64> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut values = Vec::with_capacity(members.len());
        for member in members {
            values.push(member.value().await);
        }
        if values.iter().all(|v| *v == expected) || Instant::now() > deadline {
            return values;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Entries the leader of a term appended but never replicated beyond this
/// member: written straight into its log and its WAL, as an append_entries
/// from that leader would have.
async fn hold_unreplicated(member: &Member, entries: Vec<LogEntry>) {
    let meta = member.svc.write_meta().await;
    let mut logs = meta.logs.write().await;
    for entry in entries {
        logs.insert(entry.id, entry);
    }
    member
        .svc
        .logs_post_processing(&meta, logs)
        .await
        .expect("persist the unreplicated entries");
}

/// What a restart would load from this member's WAL, as (id, term, data).
fn durable_log(dir: &Path, above: u64) -> Vec<(u64, u64, Vec<u8>)> {
    let opts = disk_options(dir, NEVER, NEVER);
    let (mut term, mut commit, mut applied) = (0, 0, 0);
    let mut logs = LogsMap::new();
    StorageEntity::new_with_options(&opts, &mut term, &mut commit, &mut applied, &mut logs)
        .expect("open the WAL")
        .expect("disk storage");
    logs.values()
        .filter(|e| e.id > above)
        .map(|e| (e.id, e.term, e.data.clone()))
        .collect()
}

/// Member B holds an entry of term T that T's leader (A) appended and never
/// replicated further. C, without it, wins term T+1 and sends B its own
/// entries for those ids in ONE message (it holds two it has not shipped yet,
/// then commits a third), so B's consistency check at prev passes and the
/// next message's prev lands on an entry B does share. Returns (members, n,
/// t): the shared position before the divergence and the stale entry's term.
async fn diverged_follower(tag: &str) -> (Vec<Member>, u64, u64) {
    let members = cluster(tag, 3, NEVER).await;
    for _ in 0..3 {
        members[0].add(1).await;
    }
    let all: Vec<&Member> = members.iter().collect();
    assert_eq!(wait_for_values(&all, 3).await, vec![3, 3, 3]);
    let (n, t) = members[0].position().await;
    assert_eq!(members[1].position().await, (n, t));
    assert_eq!(members[2].position().await, (n, t));

    hold_unreplicated(&members[1], vec![add_entry(n + 1, t, 100)]).await;
    assert!(
        Service::reelect(members[2].svc.as_ref(), PlaneId::type1()).await,
        "C must win term {} with A's vote (B's longer log refuses it)",
        t + 1
    );
    {
        // Two entries C appended but has not replicated yet (a slow round, or
        // commands answered NotCommitted): they travel with the next one.
        let meta = members[2].svc.write_meta().await;
        for _ in 0..2 {
            let mut entry = add_entry(0, 0, 1);
            members[2]
                .svc
                .leader_append_log(&meta, &mut entry)
                .await
                .expect("append to the leader's log");
        }
    }
    members[2].add(1).await;
    (members, n, t)
}

/// Raft Figure 2, AppendEntries step 3: an existing entry that conflicts with
/// a new one (same index, different term) is deleted with everything after
/// it. Keeping it instead (`or_insert`) acknowledges the leader's entry while
/// holding -- and applying -- another one.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_replaces_a_conflicting_suffix() {
    let _ = env_logger::try_init();
    let (members, n, t) = diverged_follower("conflict").await;
    let all: Vec<&Member> = members.iter().collect();
    // Let a few heartbeats carry the commit index over the batch.
    let values = wait_for_values(&all, 6).await;
    let leader_log = members[2].log().await;
    let follower_log = members[1].log().await;
    assert_eq!(follower_log, leader_log, "B's log must equal C's");
    assert_eq!(
        values,
        vec![6, 6, 6],
        "B applied the deposed leader's entry instead of C's"
    );
    assert!(
        follower_log
            .iter()
            .filter(|(id, _, _)| *id > n)
            .all(|(_, term, _)| *term > t),
        "B still holds entries of the deposed term {} above {}",
        t,
        n
    );
    for member in members {
        member.stop().await;
    }
}

/// B holds two stale entries of term T; C holds one entry of term T+1 at the
/// first of those ids (it led term T+1 briefly) and wins term T+1 -- B grants
/// it, the later last term wins (section 5.4.1). C's first message names prev
/// = that entry, so B finds the conflict in the consistency check and
/// discards from prev on. Returns the members; the ledger then reads 6.
async fn diverged_at_prev(tag: &str) -> Vec<Member> {
    let members = cluster(tag, 3, NEVER).await;
    for _ in 0..3 {
        members[0].add(1).await;
    }
    let all: Vec<&Member> = members.iter().collect();
    assert_eq!(wait_for_values(&all, 3).await, vec![3, 3, 3]);
    let (n, t) = members[0].position().await;
    hold_unreplicated(
        &members[1],
        vec![add_entry(n + 1, t, 100), add_entry(n + 2, t, 100)],
    )
    .await;
    hold_unreplicated(&members[2], vec![add_entry(n + 1, t + 1, 1)]).await;
    assert!(
        Service::reelect(members[2].svc.as_ref(), PlaneId::type1()).await,
        "C must win term {}",
        t + 1
    );
    assert_eq!(members[2].svc.read_meta().await.term, t + 1);
    for _ in 0..2 {
        members[2].add(1).await;
    }
    members
}

/// B's log in memory, its WAL replay, and its state after a restart must all
/// agree with the leader's.
async fn assert_follower_durable(mut members: Vec<Member>) {
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 6).await, vec![6, 6, 6]);
    }
    let in_memory = members[1].log().await;
    assert_eq!(in_memory, members[2].log().await, "B's log must equal C's");
    let snapshot_index = members[1].svc.read_meta().await.last_snapshot_index;
    assert_eq!(
        durable_log(&members[1].dir, snapshot_index),
        in_memory,
        "B's WAL replays to a different log than the one it acknowledged"
    );

    let b = members.remove(1);
    let (dir, addr) = b.stop().await;
    async_wait_secs().await;
    let b = Member::launch(&dir, &addr, NEVER).await;
    assert_eq!(
        b.value().await,
        6,
        "B's replay applied a suffix it had discarded"
    );
    b.stop().await;
    for member in members {
        member.stop().await;
    }
}

/// The truncation must be as durable as the append that follows it: a
/// restart replays the WAL, and a stale suffix still on disk comes back under
/// a commit cursor that now covers it. Conflict found entry by entry.
#[tokio::test(flavor = "multi_thread")]
async fn a_truncated_suffix_does_not_resurrect_after_restart() {
    let _ = env_logger::try_init();
    let (members, _n, _t) = diverged_follower("resurrect").await;
    assert_follower_durable(members).await;
}

/// Same, with the conflict found by the consistency check at prev.
#[tokio::test(flavor = "multi_thread")]
async fn a_suffix_discarded_at_prev_does_not_resurrect_after_restart() {
    let _ = env_logger::try_init();
    let members = diverged_at_prev("resurrect_prev").await;
    assert_follower_durable(members).await;
}

/// A member that joins after the leader compacted needs the snapshot, then
/// the entries after it -- each applied exactly once, and the snapshot must
/// actually reach its registered state machines.
#[tokio::test(flavor = "multi_thread")]
async fn install_snapshot_then_append_applies_each_entry_once() {
    let _ = env_logger::try_init();
    let addrs = addresses(2);
    let a = Member::launch(&scratch_dir("install_a"), &addrs[0], 1).await;
    a.svc.bootstrap().await;
    for _ in 0..10 {
        a.add(1).await;
    }
    let snapshot_index = snapshot_and_compact(&a.svc).await;
    // Applied past the snapshot, below any further snapshot.
    for _ in 0..3 {
        a.add(1).await;
    }
    assert_eq!(a.value().await, 13);

    let b = Member::launch(&scratch_dir("install_b"), &addrs[1], 1).await;
    b.svc
        .join(&vec![addrs[0].clone()])
        .await
        .expect("join after compaction");
    let values = wait_for_values(&[&a, &b], 13).await;
    assert_eq!(
        values,
        vec![13, 13],
        "the joining member must hold the snapshot's state plus each later entry once"
    );
    {
        let meta = b.svc.read_meta().await;
        assert_eq!(meta.last_snapshot_index, snapshot_index);
    }
    // And more entries after the install.
    a.add(1).await;
    assert_eq!(wait_for_values(&[&a, &b], 14).await, vec![14, 14]);

    // The installed snapshot and the entries after it are durable on B.
    let (dir, addr) = b.stop().await;
    async_wait_secs().await;
    let b = Member::launch(&dir, &addr, 1).await;
    assert_eq!(b.value().await, 14, "B's restart lost the installed snapshot");
    b.stop().await;
    a.stop().await;
}

/// InstallSnapshot from a stale leader (lower term), or one older than what
/// the member has already applied, must not roll the member's state back.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_or_older_snapshot_does_not_roll_a_member_back() {
    let _ = env_logger::try_init();
    let members = cluster("stale_install", 2, NEVER).await;
    // A bootstrapped plane sits at term 0, which has no stale term below it.
    assert!(Service::reelect(members[0].svc.as_ref(), PlaneId::type1()).await);
    for _ in 0..4 {
        members[0].add(1).await;
    }
    let all: Vec<&Member> = members.iter().collect();
    assert_eq!(wait_for_values(&all, 4).await, vec![4, 4]);
    let follower = &members[1];
    let (term, applied) = {
        let meta = follower.svc.read_meta().await;
        (meta.term, meta.last_applied)
    };
    assert!(term > 0, "the follower must have followed the re-election");
    // An image of an EMPTY ledger: installing it would erase four commands.
    let empty_image = {
        let master = MasterStateMachine::new_on_plane(DEFAULT_SERVICE_ID, PlaneId::type1());
        crate::raft::state_machine::StateMachineCtl::snapshot(&master)
    };
    let leader_id = members[0].svc.id;
    // Older than what the follower applied, from the current leader.
    Service::install_snapshot(
        follower.svc.as_ref(),
        PlaneId::type1(),
        term,
        leader_id,
        1,
        term,
        empty_image.clone(),
    )
    .await;
    assert_eq!(follower.value().await, 4, "an older snapshot rolled B back");
    assert_eq!(follower.svc.read_meta().await.last_applied, applied);
    // From a deposed leader (lower term).
    Service::install_snapshot(
        follower.svc.as_ref(),
        PlaneId::type1(),
        term.saturating_sub(1),
        leader_id,
        applied + 100,
        term.saturating_sub(1),
        empty_image,
    )
    .await;
    assert_eq!(follower.value().await, 4, "a stale leader's snapshot was installed");
    {
        let meta = follower.svc.read_meta().await;
        assert_eq!(meta.last_applied, applied, "a stale leader moved B's apply cursor");
        assert!(meta.last_snapshot_index < applied + 100);
    }
    for member in members {
        member.stop().await;
    }
}

/// A follower whose log ends exactly at the leader's snapshot boundary: the
/// leader's boundary entry is compacted, so its consistency check comes from
/// the snapshot's (index, term), and replication carries on.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_at_the_snapshot_boundary_keeps_replicating() {
    let _ = env_logger::try_init();
    let members = cluster("boundary_multi", 3, 1).await;
    for _ in 0..5 {
        members[0].add(1).await;
    }
    let all: Vec<&Member> = members.iter().collect();
    assert_eq!(wait_for_values(&all, 5).await, vec![5, 5, 5]);
    let snapshot_index = snapshot_and_compact(&members[0].svc).await;
    assert_eq!(members[1].position().await.0, snapshot_index);
    let (_, id) = members[0].add(1).await;
    assert_eq!(id, snapshot_index + 1);
    assert_eq!(wait_for_values(&all, 6).await, vec![6, 6, 6]);
    assert_eq!(members[1].log().await, members[2].log().await);
    for member in members {
        member.stop().await;
    }
}

/// Compact the leader to an empty log map, lose it, elect another member:
/// ids continue above every id handed out, a long-lived client keeps reading
/// its own writes, and the compacted member restarts and catches up.
///
/// Five members: the leader's commit rule (`is_majority` over followers only)
/// needs every follower of a three-member plane, so one lost member there
/// stops all commits -- a separate availability issue this test steps around.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_change_after_compaction_continues_the_log() {
    let _ = env_logger::try_init();
    let mut members = cluster("leader_change", 5, 1).await;
    let addrs: Vec<String> = members.iter().map(|m| m.addr.clone()).collect();
    let client = RaftClient::new(&addrs, DEFAULT_SERVICE_ID).await.unwrap();
    for _ in 0..5 {
        client
            .execute_command_with_index_on_plane(PlaneId::type1(), LEDGER_SM_ID, add::new(&1))
            .await
            .unwrap();
    }
    let snapshot_index = snapshot_and_compact(&members[0].svc).await;
    let mut last_id = 0;
    for _ in 0..2 {
        last_id = client
            .execute_command_with_index_on_plane(PlaneId::type1(), LEDGER_SM_ID, add::new(&1))
            .await
            .unwrap()
            .1;
    }
    assert!(last_id > snapshot_index);
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 7).await, vec![7; 5]);
    }

    let a = members.remove(0);
    let (a_dir, a_addr) = a.stop().await;
    assert!(
        Service::reelect(members[0].svc.as_ref(), PlaneId::type1()).await,
        "B must win the election without A"
    );
    // Let B's first heartbeats name it to C, D and E. Right after the
    // election they still answer NotLeader(A), and the client's six leader
    // attempts (no backoff) can all land on A or on that stale answer -- a
    // client failover limit, not what this test is about.
    async_wait_secs().await;
    let (value, id) = client
        .execute_command_with_index_on_plane(PlaneId::type1(), LEDGER_SM_ID, add::new(&1))
        .await
        .expect("command after the leader change");
    assert_eq!(value, 8);
    assert!(id > last_id, "log id {} did not advance past {}", id, last_id);
    let read: u64 = client
        .execute_on_plane(PlaneId::type1(), LEDGER_SM_ID, get::new())
        .await
        .expect("the client reads its own write after the leader change");
    assert_eq!(read, 8);

    // The compacted ex-leader restarts from its snapshot + WAL and catches up.
    // Its persisted term is the one it stopped at, below the new leader's: it
    // follows B, and nobody is deposed (it used to restart at its last log
    // id, far above the plane's term, and force an election).
    let leader_term = members[0].svc.read_meta().await.term;
    let a = Member::launch(&a_dir, &a_addr, 1).await;
    assert!(
        a.svc.read_meta().await.term <= leader_term,
        "the restarted member came back above the plane's term"
    );
    async_wait_secs().await;
    assert!(members[0].is_leader().await, "the restart deposed B");
    assert_eq!(members[0].svc.read_meta().await.term, leader_term);
    members[0].add(1).await;
    let mut all: Vec<&Member> = members.iter().collect();
    all.push(&a);
    assert_eq!(wait_for_values(&all, 9).await, vec![9; 5]);
    a.stop().await;
    for member in members {
        member.stop().await;
    }
}

/// After compaction the voter's own last position is the snapshot boundary:
/// a candidate behind it is refused, one at it is granted.
#[tokio::test(flavor = "multi_thread")]
async fn a_vote_after_compaction_compares_against_the_snapshot() {
    let _ = env_logger::try_init();
    let members = cluster("vote", 3, 1).await;
    for _ in 0..5 {
        members[0].add(1).await;
    }
    let snapshot_index = snapshot_and_compact(&members[0].svc).await;
    let (term, snapshot_term) = {
        let meta = members[0].svc.read_meta().await;
        (meta.term, meta.last_snapshot_term)
    };
    assert_eq!(members[0].position().await, (snapshot_index, snapshot_term));
    let candidate = members[2].svc.id;
    let behind = Service::request_vote(
        members[0].svc.as_ref(),
        PlaneId::type1(),
        term + 1,
        candidate,
        snapshot_index - 1,
        snapshot_term,
    )
    .await;
    assert!(!behind.1, "a candidate behind the snapshot boundary won the vote");
    let level = Service::request_vote(
        members[0].svc.as_ref(),
        PlaneId::type1(),
        term + 1,
        candidate,
        snapshot_index,
        snapshot_term,
    )
    .await;
    assert!(level.1, "a candidate at the snapshot boundary must be granted");
    for member in members {
        member.stop().await;
    }
}

/// A command answered NotCommitted stays in the leader's log; the next
/// command that commits commits it too (Log Matching). The leader must apply
/// both, in order, as every follower does -- not jump its apply cursor past
/// the earlier one.
#[tokio::test(flavor = "multi_thread")]
async fn the_leader_applies_an_earlier_uncommitted_entry_in_order() {
    let _ = env_logger::try_init();
    let members = cluster("apply_order", 3, NEVER).await;
    members[0].add(1).await;
    let all: Vec<&Member> = members.iter().collect();
    assert_eq!(wait_for_values(&all, 1).await, vec![1, 1, 1]);
    {
        // The state a NotCommitted answer leaves behind: appended, not
        // replicated to a majority, not applied.
        let meta = members[0].svc.write_meta().await;
        let mut entry = add_entry(0, 0, 10);
        members[0]
            .svc
            .leader_append_log(&meta, &mut entry)
            .await
            .expect("append to the leader's log");
    }
    let (value, _) = members[0].add(1).await;
    assert_eq!(value, 12, "the leader skipped the earlier committed entry");
    assert_eq!(wait_for_values(&all, 12).await, vec![12, 12, 12]);
    for member in members {
        member.stop().await;
    }
}

/// The persisted term is checked against the log on restart: a member never
/// holds an entry from a term beyond its own, so a hard state below the log's
/// entry terms is raised (with an error log) rather than trusted. Running
/// below the log, the next leader term would rank older entries above newer
/// ones (section 5.4.1), and a candidate holding only the older entry could
/// overwrite a newer committed one.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_never_lowers_the_term_below_the_log() {
    let _ = env_logger::try_init();
    const ENTRY_TERM: u64 = 7;
    let dir = scratch_dir("term_restore");
    let addr = addresses(1).remove(0);
    {
        // The first entry adds the member as a voter (the log is the only
        // source of the configuration). Three more committed entries written
        // in term 7 (seven elections, three entries -- routine on a
        // multi-member plane).
        let mut wal = std::fs::File::create(dir.join("log.dat")).unwrap();
        for id in 1..=4 {
            let log = if id == 1 {
                let (fn_id, _, data) = super::state_machine::configs::commands::new_member_::new(&addr).encode();
                LogEntry {
                    id,
                    term: ENTRY_TERM,
                    sm_id: super::state_machine::configs::CONFIG_SM_ID,
                    fn_id,
                    data,
                }
            } else {
                add_entry(id, ENTRY_TERM, 1)
            };
            let record = DiskLogEntry {
                term: id, // the header field holds the log id, as append_logs writes it
                commit_index: id,
                last_applied: id,
                log,
            }
            .encode();
            wal.write_all(&(4u64 + record.len() as u64).to_le_bytes())
                .unwrap();
            wal.write_all(&crc32fast::hash(&record).to_le_bytes())
                .unwrap();
            wal.write_all(&record).unwrap();
        }
        let mut commit = std::fs::File::create(dir.join("commit.idx")).unwrap();
        commit.write_all(&4u64.to_le_bytes()).unwrap();
        commit.write_all(&4u64.to_le_bytes()).unwrap();
        // A hard state below the log's terms -- what an inconsistent store
        // would hold.
        super::disk::write_hard_state(
            &dir,
            &super::disk::HardState {
                term: 3,
                voted_for: None,
            },
        )
        .unwrap();
    }
    let node = Member::launch(&dir, &addr, NEVER).await;
    assert_eq!(node.value().await, 3);
    let restored_term = node.svc.read_meta().await.term;
    assert!(
        restored_term >= ENTRY_TERM,
        "restarted at term {} below its own last entry's term {}",
        restored_term,
        ENTRY_TERM
    );
    let (value, id) = node.add(1).await;
    assert_eq!((value, id), (4, 5));
    let entry_term = node.svc.read_meta().await.logs.read().await[&5].term;
    assert!(
        entry_term >= ENTRY_TERM,
        "entry 4 has term {} below entry 3's term {}",
        entry_term,
        ENTRY_TERM
    );
    node.stop().await;
}

/// A state machine that is not recoverable: the WAL keeps a no-op in place
/// of each of its commands.
mod volatile {
    use super::*;
    use futures::FutureExt;

    pub(super) const VOLATILE_SM_ID: u64 = 4343;

    pub(super) struct Volatile {
        pub(super) count: u64,
    }

    raft_state_machine! {
        def cmd bump() -> u64;
    }

    impl StateMachineCmds for Volatile {
        fn bump(&mut self) -> BoxFuture<u64> {
            self.count += 1;
            futures::future::ready(self.count).boxed()
        }
    }

    impl StateMachineCtl for Volatile {
        raft_sm_complete!();
        fn id(&self) -> u64 {
            VOLATILE_SM_ID
        }
        fn snapshot(&self) -> Vec<u8> {
            crate::utils::serde::serialize(&self.count)
        }
        fn recover(&mut self, _: Vec<u8>) -> BoxFuture<()> {
            futures::future::ready(()).boxed()
        }
        fn recoverable(&self) -> bool {
            false
        }
    }

    pub(super) fn bump_entry() -> LogEntry {
        let (fn_id, _, data) = commands::bump::new().encode();
        LogEntry {
            id: 0,
            term: 0,
            sm_id: VOLATILE_SM_ID,
            fn_id,
            data,
        }
    }
}

/// The WAL keeps a no-op (same id and term) in place of each command of a
/// state machine that is not recoverable, so a restarted leader's log has no
/// hole. A lagging follower whose `next_index` walks back over that id finds
/// the entry at prev and catches up. With a hole the leader panicked in the
/// heartbeat task at each round ("Cannot find old logs for follower").
#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_follower_catches_up_past_a_dropped_command_in_the_leaders_log() {
    use volatile::{bump_entry, Volatile};
    let _ = env_logger::try_init();
    let mut members = cluster("leader_hole", 3, NEVER).await;
    for member in &members {
        member
            .svc
            .register_state_machine(Box::new(Volatile { count: 0 }))
            .await;
    }
    for _ in 0..2 {
        members[0].add(1).await;
    }
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 2).await, vec![2, 2, 2]);
    }

    // C is down while three entries commit through A and B. The middle one
    // is a command of the state machine that is not recoverable.
    let (c_dir, c_addr) = members.remove(2).stop().await;
    members[0].add(1).await;
    let dropped_id =
        match Service::c_command(members[0].svc.as_ref(), PlaneId::type1(), bump_entry()).await {
            ClientCmdResponse::Success { last_log_id, .. } => last_log_id,
            other => panic!("bump on A failed: {:?}", other),
        };
    members[0].add(1).await;

    // A restarts: its log is what the WAL replay gives.
    let (a_dir, a_addr) = members.remove(0).stop().await;
    async_wait_secs().await;
    let a = Member::launch(&a_dir, &a_addr, NEVER).await;
    a.svc
        .register_state_machine(Box::new(Volatile { count: 0 }))
        .await;
    members.insert(0, a);
    {
        let meta = members[0].svc.read_meta().await;
        let logs = meta.logs.read().await;
        let placeholder = logs.get(&dropped_id).expect("the replayed log has a hole");
        assert_eq!(placeholder.sm_id, NOOP_SM_ID);
        assert!(placeholder.data.is_empty());
        assert!(
            disk::check_log_contiguous(PlaneId::type1(), meta.last_snapshot_index, &logs).is_ok()
        );
    }
    // A new term resets A's follower table: C's next_index starts after A's
    // last entry. C comes back after it, so its next_index walks back over
    // the id of the dropped command.
    assert!(Service::reelect(members[0].svc.as_ref(), PlaneId::type1()).await);
    members[0].add(1).await;
    members.push(Member::launch(&c_dir, &c_addr, NEVER).await);

    let target = members[0].position().await.0;
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut reached = members[2].position().await.0;
    while reached < target && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        reached = members[2].position().await.0;
    }
    assert!(
        reached >= target,
        "C stopped at log id {}, the leader is at {}",
        reached,
        target
    );
    {
        let all: Vec<&Member> = members.iter().collect();
        assert_eq!(wait_for_values(&all, 5).await, vec![5, 5, 5]);
    }
    // C got the entry from the restarted A, which holds the no-op.
    let c_entry = members[2].svc.read_meta().await.logs.read().await[&dropped_id].clone();
    assert_eq!(c_entry.sm_id, NOOP_SM_ID);
    for member in members {
        member.stop().await;
    }
}
