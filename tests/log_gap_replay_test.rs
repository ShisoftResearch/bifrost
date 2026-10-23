use bifrost::raft::disk::DiskOptions;
/// The raft WAL has a record at every log id: `append_logs` writes a no-op in
/// place of each command of a state machine that is not recoverable. So a
/// hole in the replayed log is damage, and the start stops with a typed error
/// (`LogHole`). It must not apply past the hole (the state machines would run
/// on a state the other members do not have), and it must not wedge silently.
///
/// The test makes the hole directly: run a node, commit a few configuration
/// commands, stop it, remove the first record AFTER the bootstrap voter entry
/// from log.dat, keep commit.idx, and restart.
use bifrost::raft::state_machine::callback::SubKey;
use bifrost::raft::{
    client::RaftClient, Options, PlaneId, RaftService, Storage, DEFAULT_SERVICE_ID,
};
use bifrost::rpc::Server;
use std::convert::TryFrom;
use std::time::Duration;
use tokio::time::sleep;

/// Strip the record at `skip_records` (0-based) out of log.dat.
fn strip_log_record(log_path: &std::path::Path, skip_records: usize) {
    let data = std::fs::read(log_path).expect("read log.dat");
    // Framing: [8B record_len = 4 + payload][4B crc][payload]
    assert!(data.len() > 12, "log.dat too short to hold one record");
    let mut pos = 0usize;
    for _ in 0..skip_records {
        let record_len =
            u64::from_le_bytes(<[u8; 8]>::try_from(&data[pos..pos + 8]).unwrap()) as usize;
        pos += 8 + record_len;
    }
    let record_len =
        u64::from_le_bytes(<[u8; 8]>::try_from(&data[pos..pos + 8]).unwrap()) as usize;
    let record_total = 8 + record_len;
    assert!(
        data.len() > pos + record_total,
        "log.dat holds only {} records; the test needs later entries to stay behind",
        skip_records + 1
    );
    let mut stripped = Vec::with_capacity(data.len() - record_total);
    stripped.extend_from_slice(&data[..pos]);
    stripped.extend_from_slice(&data[pos + record_total..]);
    std::fs::write(log_path, &stripped).expect("rewrite log.dat");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_hole_in_the_wal_stops_the_start() {
    let _ = env_logger::try_init();

    // Under TMPDIR, so a run can keep its scratch data off /tmp.
    let data_path = std::env::temp_dir()
        .join("bifrost_test_log_gap_replay_18020")
        .to_string_lossy()
        .to_string();
    let _ = std::fs::remove_dir_all(&data_path);
    std::fs::create_dir_all(&data_path).unwrap();
    let address = "127.0.0.1:18020".to_string();

    let disk = |path: &str| {
        Storage::DISK(DiskOptions {
            path: path.to_string(),
            take_snapshots: false,
            append_logs: true,
            trim_logs: false,
            snapshot_log_threshold: 1_000_000,
            log_compaction_threshold: 1_000_000,
        })
    };

    // Phase 1: run a node and put several committed entries on disk.
    {
        let raft_service = RaftService::new(Options {
            storage: disk(&data_path),
            address: address.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&address);
        Server::listen_and_resume(&server).await;
        server.register_service(&raft_service).await;
        assert!(RaftService::start(&raft_service, true).await);
        raft_service.bootstrap().await;
        sleep(Duration::from_millis(300)).await;
        assert!(raft_service.is_leader());

        let client = RaftClient::new(&vec![address.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        // `subscribe` is a configuration-state-machine command that does not
        // change the voters (`config_change` ignores it). A dummy member that
        // never joins must not be added as a voter: it raises the quorum to 2
        // and the entry cannot commit (rules C6 and C8).
        use bifrost::raft::state_machine::configs::commands;
        for i in 0..4u32 {
            let key = SubKey::new(
                DEFAULT_SERVICE_ID,
                PlaneId::type1(),
                bifrost::raft::state_machine::configs::CONFIG_SM_ID,
                i as u64,
                0,
            );
            client
                .execute(
                    bifrost::raft::state_machine::configs::CONFIG_SM_ID,
                    commands::subscribe::new(&key, &format!("dummy{}:9999", i), &(i as u64)),
                )
                .await
                .expect("phase 1 command");
        }
        sleep(Duration::from_millis(300)).await;
        // Persist commit progress; a kill preserves commit.idx from the last
        // flush, and replay is driven by it.
        raft_service
            .flush_persistence()
            .await
            .expect("flush the WAL and commit progress");
        raft_service.shutdown().await;
        server.shutdown().await;
    }
    // Give the tokio runtime a beat to drop file handles and free the port.
    sleep(Duration::from_millis(1000)).await;

    // Make the hole: remove the first command record (id 2). The hole is
    // after the bootstrap voter entry (id 1) and before later durable entries.
    strip_log_record(
        std::path::Path::new(&data_path).join("log.dat").as_path(),
        1,
    );

    // Phase 2: restart on the changed log, at the same address (the address
    // gives the identity).
    {
        let raft_service = RaftService::new(Options {
            storage: disk(&data_path),
            address: address.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&address);
        Server::listen_and_resume(&server).await;
        server.register_service(&raft_service).await;
        assert!(
            !RaftService::start(&raft_service, true).await,
            "a node whose log has a hole must not start"
        );
        assert!(!raft_service.is_leader());
        assert_eq!(raft_service.last_applied_index().await, 0);
        raft_service.shutdown().await;
        server.shutdown().await;
    }

    let _ = std::fs::remove_dir_all(&data_path);
}
