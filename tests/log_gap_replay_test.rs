use bifrost::raft::disk::DiskOptions;
/// A kill can leave the on-disk raft log with holes below the commit index:
/// `append_logs` deliberately skips entries whose state machine is not
/// recoverable, so the ids on disk are not dense. Replay used to treat the
/// first hole as an entry that might still arrive and deferred forever --
/// on a single node nothing ever re-sends it, so every state machine on the
/// plane wedged behind log id 1 and the ranged index lost its placements.
///
/// This test manufactures the hole directly: run a node, commit a handful of
/// config commands, stop it, strip the FIRST record out of log.dat while
/// leaving commit.idx alone, and restart. The plane must come back live --
/// leader elected, a fresh command applied -- instead of wedging.
use bifrost::raft::{client::RaftClient, Options, RaftService, Storage, DEFAULT_SERVICE_ID};
use bifrost::rpc::Server;
use std::convert::TryFrom;
use std::time::Duration;
use tokio::time::sleep;

fn strip_first_log_record(log_path: &std::path::Path) {
    let data = std::fs::read(log_path).expect("read log.dat");
    // Framing: [8B record_len = 4 + payload][4B crc][payload]
    assert!(data.len() > 12, "log.dat too short to hold one record");
    let record_len =
        u64::from_le_bytes(<[u8; 8]>::try_from(&data[0..8]).unwrap()) as usize;
    let first_record_total = 8 + record_len;
    assert!(
        data.len() > first_record_total,
        "log.dat holds only one record; the test needs later entries to stay behind"
    );
    std::fs::write(log_path, &data[first_record_total..]).expect("rewrite log.dat");
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_skips_holes_below_durable_entries() {
    let _ = env_logger::try_init();

    let data_path = "/tmp/bifrost_test_log_gap_replay_18020".to_string();
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
        use bifrost::raft::state_machine::configs::commands;
        for i in 0..4u32 {
            client
                .execute(
                    bifrost::raft::state_machine::configs::CONFIG_SM_ID,
                    commands::new_member_::new(&format!("dummy{}:9999", i)),
                )
                .await
                .expect("phase 1 command");
        }
        sleep(Duration::from_millis(300)).await;
        // Persist commit progress; a kill preserves commit.idx from the last
        // flush, and replay is driven by it.
        raft_service.flush_persistence().await;
    }
    // Give the tokio runtime a beat to drop file handles.
    sleep(Duration::from_millis(300)).await;

    // Manufacture the hole a skipped unrecoverable entry leaves behind.
    strip_first_log_record(std::path::Path::new(&data_path).join("log.dat").as_path());

    // Phase 2: restart on the doctored log. The plane must be live.
    {
        let address2 = "127.0.0.1:18021".to_string();
        let raft_service = RaftService::new(Options {
            storage: disk(&data_path),
            address: address2.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&address2);
        Server::listen_and_resume(&server).await;
        server.register_service(&raft_service).await;
        assert!(RaftService::start(&raft_service, true).await);
        raft_service.bootstrap().await;
        sleep(Duration::from_millis(500)).await;
        assert!(
            raft_service.is_leader(),
            "restarted node must elect itself despite the log hole"
        );

        // The application-side replay entry point: this is what Nebuchadnezzar
        // calls at startup, and what wedged behind the hole.
        raft_service.recover_after_register().await;
        let applied = raft_service.last_applied_index().await;
        // Phase 1 committed ids 1..=4 and the hole is id 1: a healthy replay
        // lands on the last durable id. Wedged replay reports 0.
        assert!(
            applied >= 3,
            "replay must walk past the hole to the durable entries (last_applied={}, expected >= 3)",
            applied
        );
    }

    let _ = std::fs::remove_dir_all(&data_path);
}
