/// Test for single-node Raft cluster recovery from disk
/// 
/// This test verifies the fix for the bug where single-node clusters
/// failed to elect themselves as leader after recovering from persistent storage.

use bifrost::raft::{RaftService, Options, Storage, DEFAULT_SERVICE_ID, client::RaftClient};
use bifrost::raft::disk::DiskOptions;
use bifrost::rpc::Server;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn test_single_node_cluster_recovery_becomes_leader() {
    let _ = env_logger::try_init();
    
    // Use test-specific directory
    let data_path = "/tmp/bifrost_test_single_node_recovery_18000".to_string();
    // Clean up any existing data from previous runs
    let _ = std::fs::remove_dir_all(&data_path);
    std::fs::create_dir_all(&data_path).unwrap();
    
    let address = "127.0.0.1:18000".to_string();
    
    println!("=== Phase 1: Create initial single-node cluster ===");
    
    let initial_leader_id;
    
    // Phase 1: Create and run a single-node cluster with disk storage
    {
        let raft_service = RaftService::new(Options {
            storage: Storage::DISK(DiskOptions {
                path: data_path.clone(),
                take_snapshots: true,
                append_logs: true,
                trim_logs: false,
                snapshot_log_threshold: 5,
                log_compaction_threshold: 10,
            }),
            address: address.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        
        let server = Server::new(&address);
        Server::listen_and_resume(&server).await;
        server.register_service(&raft_service).await;
        
        // Start and bootstrap
        let started = RaftService::start(&raft_service, true).await;
        assert!(started, "Phase 1: Should start successfully");
        
        raft_service.bootstrap().await;
        sleep(Duration::from_millis(500)).await;
        
        // Verify it's a leader
        assert!(raft_service.is_leader(), "Phase 1: Should be leader");
        initial_leader_id = raft_service.leader_id().await;
        assert!(initial_leader_id != 0, "Phase 1: Should have valid leader ID");
        
        println!("Phase 1: Leader ID = {}, Server ID = {}", initial_leader_id, raft_service.id);
        
        // Perform some operations to generate logs
        let client = RaftClient::new(&vec![address.clone()], DEFAULT_SERVICE_ID).await.unwrap();
        println!("Phase 1: Created Raft client");
        
        // Generate some logs by performing state machine operations
        // Use the config state machine to add/remove a dummy member
        use bifrost::raft::state_machine::configs::commands;
        
        // Add a dummy member (will generate a log entry)
        let result1 = client.execute(
            bifrost::raft::state_machine::configs::CONFIG_SM_ID,
            commands::new_member_::new(&"dummy:9999".to_string())
        ).await;
        println!("Phase 1: Added dummy member: {:?}", result1.is_ok());
        
        // Remove the dummy member (another log entry)
        let result2 = client.execute(
            bifrost::raft::state_machine::configs::CONFIG_SM_ID,
            commands::del_member_::new(&"dummy:9999".to_string())
        ).await;
        println!("Phase 1: Removed dummy member: {:?}", result2.is_ok());
        
        // Wait for persistence
        sleep(Duration::from_millis(1000)).await;
        
        // Check that logs exist
        let num_logs = raft_service.num_logs().await;
        println!("Phase 1: Number of logs: {}", num_logs);
        assert!(num_logs > 0, "Phase 1: Should have generated some logs");
        
        // Shutdown gracefully
        println!("Phase 1: Shutting down...");
        drop(client);  // Drop client first
        raft_service.shutdown().await;
        server.shutdown().await;
        sleep(Duration::from_secs(2)).await;
        
        // Prevent runtime drop panic in test context
        std::mem::forget(raft_service);
        std::mem::forget(server);
    }
    
    // Give OS time to release resources
    sleep(Duration::from_millis(500)).await;
    
    println!("\n=== Phase 2: Restart and recover from disk ===");
    
    // Phase 2: Restart the server with same storage - THIS IS THE BUG FIX TEST
    {
        let raft_service2 = RaftService::new(Options {
            storage: Storage::DISK(DiskOptions {
                path: data_path.clone(),  // Same path - will recover state
                take_snapshots: true,
                append_logs: true,
                trim_logs: false,
                snapshot_log_threshold: 5,
                log_compaction_threshold: 10,
            }),
            address: address.clone(),  // Same address
            service_id: DEFAULT_SERVICE_ID,
        });
        
        let server2 = Server::new(&address);
        Server::listen_and_resume(&server2).await;
        server2.register_service(&raft_service2).await;
        
        // Start - should recover and immediately become leader
        let started2 = RaftService::start(&raft_service2, true).await;
        assert!(started2, "Phase 2: Should start successfully");
        
        // Give it a moment to stabilize
        sleep(Duration::from_millis(1000)).await;
        
        // THE KEY TEST: Should be leader immediately after recovery
        let is_leader = raft_service2.is_leader();
        println!("Phase 2: Is leader? {}", is_leader);
        
        let leader_id2 = raft_service2.leader_id().await;
        println!("Phase 2: Leader ID = {}, Server ID = {}", leader_id2, raft_service2.id);
        
        assert!(is_leader, "Phase 2: CRITICAL - Should be leader after recovery (single-node cluster)");
        assert!(leader_id2 != 0, "Phase 2: Should have valid leader ID after recovery");
        assert_eq!(leader_id2, raft_service2.id, "Phase 2: Should be its own leader");
        
        // Verify client can connect successfully
        let client2_result = RaftClient::new(&vec![address.clone()], DEFAULT_SERVICE_ID).await;
        assert!(
            client2_result.is_ok(),
            "Phase 2: Should be able to create RaftClient (leader should be elected)"
        );
        
        println!("Phase 2: ✅ Single-node cluster successfully recovered and became leader!");
        
        // Cleanup
        raft_service2.shutdown().await;
        server2.shutdown().await;
        sleep(Duration::from_millis(500)).await;
        
        // Prevent runtime drop panic in test context
        std::mem::forget(raft_service2);
        std::mem::forget(server2);
    }
    
    // Cleanup test directory
    let _ = std::fs::remove_dir_all(&data_path);
    
    println!("\n✅ TEST PASSED: Single-node cluster recovery works correctly!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_single_node_multiple_restart_cycles() {
    let _ = env_logger::try_init();
    
    let data_path = "/tmp/bifrost_test_single_node_cycles_18001".to_string();
    // Clean up any existing data from previous runs
    let _ = std::fs::remove_dir_all(&data_path);
    std::fs::create_dir_all(&data_path).unwrap();
    
    let address = "127.0.0.1:18001".to_string();
    
    // Perform 3 restart cycles
    for cycle in 1..=3 {
        println!("\n=== Cycle {} ===", cycle);
        
        let raft_service = RaftService::new(Options {
            storage: Storage::DISK(DiskOptions {
                path: data_path.clone(),
                take_snapshots: true,
                append_logs: true,
                trim_logs: false,
                snapshot_log_threshold: 5,
                log_compaction_threshold: 10,
            }),
            address: address.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        
        let server = Server::new(&address);
        Server::listen_and_resume(&server).await;
        server.register_service(&raft_service).await;
        
        let started = RaftService::start(&raft_service, true).await;
        assert!(started, "Cycle {}: Should start", cycle);
        
        if cycle == 1 {
            // First cycle: bootstrap
            raft_service.bootstrap().await;
        }
        
        sleep(Duration::from_millis(1000)).await;
        
        // Should be leader in all cycles
        assert!(raft_service.is_leader(), "Cycle {}: Should be leader", cycle);
        let leader_id = raft_service.leader_id().await;
        assert!(leader_id != 0, "Cycle {}: Should have valid leader ID", cycle);
        
        // Verify client works and generate some logs
        let client = RaftClient::new(&vec![address.clone()], DEFAULT_SERVICE_ID).await;
        assert!(client.is_ok(), "Cycle {}: RaftClient should connect", cycle);
        
        // Generate logs to ensure persistence
        if let Ok(ref client) = client {
            use bifrost::raft::state_machine::configs::commands;
            let dummy_addr = format!("dummy{}:9999", cycle);
            let _ = client.execute(
                bifrost::raft::state_machine::configs::CONFIG_SM_ID,
                commands::new_member_::new(&dummy_addr)
            ).await;
            let _ = client.execute(
                bifrost::raft::state_machine::configs::CONFIG_SM_ID,
                commands::del_member_::new(&dummy_addr)
            ).await;
            sleep(Duration::from_millis(500)).await; // Wait for persistence
        }
        
        let num_logs = raft_service.num_logs().await;
        println!("Cycle {}: ✅ Leader elected successfully, {} logs", cycle, num_logs);
        
        // Shutdown
        raft_service.shutdown().await;
        server.shutdown().await;
        sleep(Duration::from_secs(2)).await;
        
        // Prevent runtime drop panic in test context
        std::mem::forget(raft_service);
        std::mem::forget(server);
    }
    
    // Cleanup test directory
    let _ = std::fs::remove_dir_all(&data_path);
    
    println!("\n✅ All 3 restart cycles passed!");
}

