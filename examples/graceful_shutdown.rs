/// Example demonstrating graceful shutdown of Bifrost services
///
/// This example shows how to:
/// 1. Start a Raft service with an RPC server
/// 2. Handle shutdown signals (Ctrl+C)
/// 3. Gracefully shutdown all services
///
/// Run with: cargo run --example graceful_shutdown
use bifrost::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
use bifrost::rpc::Server;
use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() {
    env_logger::init();

    let address = "127.0.0.1:9000".to_string();

    println!("Starting Bifrost services on {}...", address);

    // Create Raft service
    let raft_service = RaftService::new(Options {
        storage: Storage::MEMORY,
        address: address.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });

    // Create and start RPC server
    let server = Server::new(&address);
    Server::listen_and_resume(&server).await;
    server.register_service(&raft_service).await;

    // Start Raft service
    if RaftService::start(&raft_service, false).await {
        println!("Raft service started successfully");
        raft_service.bootstrap().await;
        println!("Raft cluster bootstrapped");
    } else {
        eprintln!("Failed to start Raft service");
        return;
    }

    println!("\nServices running. Press Ctrl+C to trigger graceful shutdown...\n");

    // Wait for Ctrl+C signal
    match signal::ctrl_c().await {
        Ok(()) => {
            println!("\n\nReceived Ctrl+C, initiating graceful shutdown...\n");
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            return;
        }
    }

    // Gracefully shutdown all services
    println!("1. Shutting down Raft service...");
    raft_service.shutdown().await;
    println!("   ✓ Raft service shut down");

    println!("2. Shutting down RPC server...");
    server.shutdown().await;
    println!("   ✓ RPC server shut down");

    println!("\n✓ All services shut down gracefully\n");

    // Give a moment for any final log messages
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}
