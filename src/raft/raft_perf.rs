//! Raft performance probes (ignored by default; see `PERF_PROTOCOL.md`).
//!
//! Run a release test binary with `--ignored --exact --nocapture` and read the
//! `PERF key=value` lines. Only APIs that exist both before and after the
//! compaction fix are used, so the same file measures BASE and FIX.

use self::commands::add;
use super::*;
use crate::raft::client::RaftClient;
use crate::raft::disk;
use crate::raft::state_machine::StateMachineCtl;
use crate::rpc::Server;
use crate::utils::time::async_wait_secs;
use futures::FutureExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

const PERF_SM_ID: u64 = 5151;

struct PerfLedger {
    value: u64,
}

raft_state_machine! {
    def cmd add(value: u64) -> u64;
    def qry get() -> u64;
}

impl StateMachineCmds for PerfLedger {
    fn add(&mut self, value: u64) -> BoxFuture<u64> {
        self.value += value;
        futures::future::ready(self.value).boxed()
    }
    fn get(&self) -> BoxFuture<u64> {
        futures::future::ready(self.value).boxed()
    }
}

impl StateMachineCtl for PerfLedger {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        PERF_SM_ID
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

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn perf_dir(tag: &str) -> PathBuf {
    let base = std::env::var("RAFT_PERF_DIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().into());
    let dir = PathBuf::from(base).join(format!("raft_perf_{}_{}", tag, rand::random::<u64>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn start_node(
    dir: &PathBuf,
    addr: &str,
    snapshot_threshold: u64,
    compaction_threshold: u64,
) -> (Arc<RaftService>, Arc<Server>) {
    let svc = RaftService::new(Options {
        storage: Storage::DISK(disk::DiskOptions {
            path: dir.to_str().unwrap().to_string(),
            take_snapshots: true,
            append_logs: true,
            trim_logs: true,
            snapshot_log_threshold: snapshot_threshold,
            log_compaction_threshold: compaction_threshold,
        }),
        address: addr.to_string(),
        service_id: DEFAULT_SERVICE_ID,
    });
    let server = Server::new(&addr.to_string());
    server.register_service(&svc).await;
    Server::listen_and_resume(&server).await;
    assert!(RaftService::start(&svc, false).await);
    svc.register_state_machine(Box::new(PerfLedger { value: 0 }))
        .await;
    svc.bootstrap().await;
    svc.recover_after_register().await;
    (svc, server)
}

fn perf_addr() -> String {
    format!("127.0.0.1:{}", 20000 + (rand::random::<u16>() % 20000))
}

async fn run_commands(client: &Arc<RaftClient>, n: u64) {
    for _ in 0..n {
        client
            .execute_on_plane(PlaneId::type1(), PERF_SM_ID, add::new(&1))
            .await
            .unwrap();
    }
}

/// Append + commit throughput with the production durability settings (WAL
/// fsync per command), snapshots out of the way.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn perf_cmd_throughput() {
    let n = env_u64("RAFT_PERF_CMDS", 3000);
    let dir = perf_dir("cmd");
    let addr = perf_addr();
    let (svc, _server) = start_node(&dir, &addr, u64::MAX / 2, u64::MAX / 2).await;
    async_wait_secs().await;
    let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    run_commands(&client, 100).await; // warm-up
    let started = Instant::now();
    run_commands(&client, n).await;
    let secs = started.elapsed().as_secs_f64();
    println!(
        "PERF cmds_per_sec={:.2} cmds={} secs={:.4}",
        n as f64 / secs,
        n,
        secs
    );
    drop(svc);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Cost of one snapshot (+ compaction, when the map exceeds the threshold)
/// after `RAFT_PERF_CMDS` applied entries.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn perf_snapshot_compaction() {
    let n = env_u64("RAFT_PERF_CMDS", 2500);
    let dir = perf_dir("snap");
    let addr = perf_addr();
    let (svc, _server) = start_node(&dir, &addr, u64::MAX / 2, 1000).await;
    async_wait_secs().await;
    let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
        .await
        .unwrap();
    run_commands(&client, n).await;
    let started = Instant::now();
    {
        let mut meta = svc.write_meta().await;
        svc.take_snapshot(&mut meta).await;
    }
    let ms = started.elapsed().as_secs_f64() * 1e3;
    println!("PERF snapshot_compaction_ms={:.3} entries={}", ms, n);
    drop(svc);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Time to bring a node back from a data dir holding a snapshot and a WAL
/// tail: `RaftService::new` through `recover_after_register`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn perf_restart() {
    let n = env_u64("RAFT_PERF_CMDS", 2500);
    let tail = env_u64("RAFT_PERF_TAIL", 500);
    let dir = perf_dir("restart");
    let addr = perf_addr();
    {
        let (svc, server) = start_node(&dir, &addr, u64::MAX / 2, 1000).await;
        async_wait_secs().await;
        let client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        run_commands(&client, n).await;
        {
            let mut meta = svc.write_meta().await;
            svc.take_snapshot(&mut meta).await;
        }
        run_commands(&client, tail).await;
        drop(client);
        drop(svc);
        drop(server);
    }
    async_wait_secs().await;
    let addr2 = perf_addr();
    let started = Instant::now();
    let (svc, _server) = start_node(&dir, &addr2, u64::MAX / 2, 1000).await;
    let ms = started.elapsed().as_secs_f64() * 1e3;
    println!(
        "PERF restart_ms={:.3} snapshot_entries={} tail={}",
        ms, n, tail
    );
    drop(svc);
    let _ = std::fs::remove_dir_all(&dir);
}
