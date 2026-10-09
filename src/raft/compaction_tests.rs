//! Log position across snapshot + compaction.
//!
//! Compaction deletes every entry at or below the snapshot index, and a
//! snapshot is taken right after an apply, so the log map is routinely EMPTY
//! afterwards. The last log position must then fall back to the snapshot's
//! last included (index, term); deriving it from the map alone restarts log
//! ids at 1, strands every client cursor above the server's applied index and
//! leaves the renumbered entries below the WAL's append cursor, unpersisted.

use self::commands::{add, get};
use super::*;
use crate::raft::client::RaftClient;
use crate::raft::disk::{self, DiskLogEntry};
use crate::raft::state_machine::master::ExecError;
use crate::raft::state_machine::StateMachineCtl;
use crate::rpc::Server;
use crate::utils::time::async_wait_secs;
use futures::FutureExt;
use std::convert::TryInto;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) const LEDGER_SM_ID: u64 = 4242;

pub(super) struct Ledger {
    pub(super) value: u64,
}

raft_state_machine! {
    def cmd add(value: u64) -> u64;
    def qry get() -> u64;
}

impl StateMachineCmds for Ledger {
    fn add(&mut self, value: u64) -> BoxFuture<u64> {
        self.value += value;
        futures::future::ready(self.value).boxed()
    }
    fn get(&self) -> BoxFuture<u64> {
        futures::future::ready(self.value).boxed()
    }
}

impl StateMachineCtl for Ledger {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        LEDGER_SM_ID
    }
    fn snapshot(&self) -> Vec<u8> {
        crate::utils::serde::serialize(&self.value)
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        if let Some(value) = crate::utils::serde::deserialize(&data) {
            self.value = value;
        }
        futures::future::ready(()).boxed()
    }
    fn recoverable(&self) -> bool {
        true
    }
}

struct Node {
    svc: Arc<RaftService>,
    server: Arc<Server>,
}

pub(super) fn scratch_dir(tag: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("raft_compaction_{}_{}", tag, rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

pub(super) fn disk_options(path: &Path, snapshot_threshold: u64, compaction_threshold: u64) -> Options {
    Options {
        storage: Storage::DISK(disk::DiskOptions {
            path: path.to_str().unwrap().to_string(),
            take_snapshots: true,
            append_logs: true,
            trim_logs: true,
            snapshot_log_threshold: snapshot_threshold,
            log_compaction_threshold: compaction_threshold,
        }),
        address: String::new(),
        service_id: DEFAULT_SERVICE_ID,
    }
}

/// Start (or restart) the node over `path`. The state machine is registered
/// after `start` so a snapshot on disk is applied on registration, then the
/// committed WAL tail is replayed, as in a real restart.
async fn start_node(
    path: &Path,
    addr: &str,
    snapshot_threshold: u64,
    compaction_threshold: u64,
    initial: u64,
) -> Node {
    let mut opts = disk_options(path, snapshot_threshold, compaction_threshold);
    opts.address = addr.to_string();
    let svc = RaftService::new(opts);
    let server = Server::new(&addr.to_string());
    server.register_service(&svc).await;
    Server::listen_and_resume(&server).await;
    assert!(RaftService::start(&svc, false).await);
    svc.register_state_machine(Box::new(Ledger { value: initial }))
        .await;
    svc.bootstrap().await;
    svc.recover_after_register().await;
    async_wait_secs().await;
    Node { svc, server }
}

async fn add_one(client: &Arc<RaftClient>) -> Result<(u64, u64), ExecError> {
    client
        .execute_command_with_index_on_plane(PlaneId::type1(), LEDGER_SM_ID, add::new(&1))
        .await
}

async fn get_value(client: &Arc<RaftClient>) -> Result<u64, ExecError> {
    client
        .execute_on_plane(PlaneId::type1(), LEDGER_SM_ID, get::new())
        .await
}

/// Take a snapshot at the current apply position and compact down to an
/// empty log map; returns the snapshot index.
pub(super) async fn snapshot_and_compact(svc: &Arc<RaftService>) -> u64 {
    let mut meta = svc.write_meta().await;
    svc.take_snapshot(&mut meta).await;
    let snapshot_index = meta.last_snapshot_index;
    assert!(snapshot_index > 0, "a snapshot must have been taken");
    assert_eq!(snapshot_index, meta.last_applied);
    assert!(
        meta.logs.read().await.is_empty(),
        "compaction at the apply position must leave the log map empty"
    );
    snapshot_index
}

/// Ids of the entries persisted in `log.dat`, in file order.
pub(super) fn wal_entries(path: &Path) -> Vec<DiskLogEntry> {
    let mut bytes = Vec::new();
    std::fs::File::open(path.join("log.dat"))
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos + 12 <= bytes.len() {
        let record_len = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap()) as usize;
        let payload = &bytes[pos + 12..pos + 8 + record_len];
        entries.push(DiskLogEntry::decode(payload).unwrap());
        pos += 8 + record_len;
    }
    entries
}

#[tokio::test(flavor = "multi_thread")]
async fn last_log_position_falls_back_to_the_snapshot() {
    let dir = scratch_dir("position");
    let addr = format!("127.0.0.1:{}", 5100 + (rand::random::<u16>() % 400));
    let node = start_node(&dir, &addr, 1_000_000, 1, 0).await;
    let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
        .await
        .unwrap();

    let mut last_index = 0;
    for _ in 0..6 {
        last_index = add_one(&client).await.unwrap().1;
    }
    let snapshot_index = snapshot_and_compact(&node.svc).await;
    assert_eq!(snapshot_index, last_index);

    // Every derivation of the last position must now agree with the snapshot.
    let info = node.svc.cluster_info().await;
    assert_eq!(info.last_log_id, snapshot_index);
    {
        let meta = node.svc.read_meta().await;
        assert_eq!(info.last_log_term, meta.last_snapshot_term);
    }

    // The next entry must come AFTER the snapshot, not restart at 1.
    let (value, new_index) = add_one(&client).await.unwrap();
    assert_eq!(value, 7);
    assert_eq!(new_index, snapshot_index + 1);
    // And the long-lived client must be able to read its own write.
    assert_eq!(get_value(&client).await.unwrap(), 7);
    drop(node.server);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Past the snapshot threshold, snapshot + compact, restart, append, query:
/// a client whose cursor sits above the restarted server's old numbering
/// must neither see LeftBehind stalls nor lose state.
#[tokio::test(flavor = "multi_thread")]
async fn restart_after_compaction_resumes_above_the_snapshot() {
    let dir = scratch_dir("restart");
    let addr = format!("127.0.0.1:{}", 5500 + (rand::random::<u16>() % 400));
    const BEFORE: u64 = 40;

    let client;
    let snapshot_index;
    {
        let node = start_node(&dir, &addr, 1_000_000, 1, 0).await;
        client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        for _ in 0..BEFORE {
            add_one(&client).await.unwrap();
        }
        assert_eq!(get_value(&client).await.unwrap(), BEFORE);
        snapshot_index = snapshot_and_compact(&node.svc).await;
        // abrupt stop: no graceful flush beyond what the commit path did
        drop(node);
    }
    async_wait_secs().await;

    let node = start_node(&dir, &addr, 1_000_000, 1, 999).await;
    {
        let meta = node.svc.read_meta().await;
        assert_eq!(meta.last_snapshot_index, snapshot_index);
    }
    let appended = add_one(&client).await;
    let queried = get_value(&client).await;
    let (value, index) = appended.expect("append after restart");
    assert!(
        index > snapshot_index,
        "log id {} restarted at or below the snapshot index {}",
        index,
        snapshot_index
    );
    assert_eq!(value, BEFORE + 1, "state machine state must be intact");
    // No LeftBehind stall, no TooManyRetry / ApplyStalled.
    assert_eq!(queried.expect("query after restart"), BEFORE + 1);
    drop(node);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Entries appended after compaction must reach `log.dat` under their real
/// ids and survive a kill + restart.
#[tokio::test(flavor = "multi_thread")]
async fn entries_after_compaction_are_persisted_and_survive_restart() {
    let dir = scratch_dir("persist");
    let addr = format!("127.0.0.1:{}", 5900 + (rand::random::<u16>() % 400));
    const BEFORE: u64 = 25;
    const AFTER: u64 = 7;

    let snapshot_index;
    {
        let node = start_node(&dir, &addr, 1_000_000, 1, 0).await;
        let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        for _ in 0..BEFORE {
            add_one(&client).await.unwrap();
        }
        snapshot_index = snapshot_and_compact(&node.svc).await;
        let mut indexes = Vec::new();
        for _ in 0..AFTER {
            indexes.push(add_one(&client).await.unwrap().1);
        }
        let expected: Vec<u64> = (snapshot_index + 1..=snapshot_index + AFTER).collect();
        assert_eq!(indexes, expected, "ids continue above the snapshot");

        let persisted: Vec<u64> = wal_entries(&dir)
            .into_iter()
            .filter(|e| e.log.sm_id == LEDGER_SM_ID && e.log.id > snapshot_index)
            .map(|e| e.log.id)
            .collect();
        assert_eq!(
            persisted, expected,
            "post-compaction entries must be in log.dat"
        );
        // kill: drop without any shutdown path
        drop(node);
    }
    async_wait_secs().await;

    let node = start_node(&dir, &addr, 1_000_000, 1, 999).await;
    let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    assert_eq!(
        get_value(&client).await.unwrap(),
        BEFORE + AFTER,
        "acknowledged post-compaction commands must survive the restart"
    );
    drop(node);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A follower exactly on the snapshot boundary must still accept the leader's
/// next entries: the boundary entry is compacted, so the consistency check
/// has to be answered from the snapshot's (index, term).
#[tokio::test(flavor = "multi_thread")]
async fn append_entries_check_uses_the_snapshot_boundary() {
    let dir = scratch_dir("boundary");
    let addr = format!("127.0.0.1:{}", 6300 + (rand::random::<u16>() % 400));
    let node = start_node(&dir, &addr, 1_000_000, 1, 0).await;
    let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    for _ in 0..5 {
        add_one(&client).await.unwrap();
    }
    let snapshot_index = snapshot_and_compact(&node.svc).await;
    let (term, snapshot_term) = {
        let meta = node.svc.read_meta().await;
        (meta.term, meta.last_snapshot_term)
    };
    let entry = LogEntry {
        id: snapshot_index + 1,
        term,
        sm_id: LEDGER_SM_ID,
        fn_id: add::new(&1).encode().0,
        data: add::new(&1).encode().2,
    };
    let leader_id = node.svc.id;
    let ok = Service::append_entries(
        node.svc.as_ref(),
        PlaneId::type1(),
        term,
        leader_id,
        snapshot_index,
        snapshot_term,
        &Some(vec![entry.clone()]),
        0,
    )
    .await;
    assert!(
        matches!(ok.1, AppendEntriesResult::Ok),
        "boundary prev entry must match the snapshot, got {:?}",
        ok.1
    );
    let wrong = Service::append_entries(
        node.svc.as_ref(),
        PlaneId::type1(),
        term,
        leader_id,
        snapshot_index,
        snapshot_term + 1,
        &None,
        0,
    )
    .await;
    assert!(matches!(wrong.1, AppendEntriesResult::LogMismatch));
    // A vote request from a candidate that only has the snapshot position
    // must compare against the snapshot, not against (0, 0).
    let stale = Service::request_vote(
        node.svc.as_ref(),
        PlaneId::type1(),
        term + 1,
        leader_id,
        0,
        0,
    )
    .await;
    assert!(
        !stale.1,
        "a candidate behind the snapshot must not win the vote"
    );
    drop(node);
    let _ = std::fs::remove_dir_all(&dir);
}

// BEGIN-FIX-ONLY
#[tokio::test(flavor = "multi_thread")]
async fn a_query_the_member_cannot_serve_reports_a_typed_stall() {
    // A client cursor beyond the member's whole log is never served: the
    // query ends in the typed ApplyStalled (with the member's position), not
    // a bare TooManyRetry.
    let dir = scratch_dir("stalled");
    let addr = format!("127.0.0.1:{}", 6700 + (rand::random::<u16>() % 400));
    let node = start_node(&dir, &addr, 1_000_000, 1_000_000, 0).await;
    let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    for _ in 0..3 {
        add_one(&client).await.unwrap();
    }
    client
        .force_cursor_for_test(PlaneId::type1(), 100_000)
        .await;
    match get_value(&client).await {
        Err(ExecError::ApplyStalled {
            client_cursor,
            server_applied,
            server_last_log,
            ..
        }) => {
            assert_eq!(client_cursor, 100_000);
            assert!(server_applied < client_cursor);
            assert!(server_last_log < client_cursor);
        }
        other => panic!("expected ApplyStalled, got {:?}", other),
    }
    drop(node);
    let _ = std::fs::remove_dir_all(&dir);
}
