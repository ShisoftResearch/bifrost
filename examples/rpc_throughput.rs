//! Transport-only RPC throughput. The logical fixture has eight 1 GiB chunks.
//! IDs visit all chunks in round-robin order. No storage engine is measured.
#[macro_use]
extern crate bifrost;
#[macro_use]
extern crate lazy_static;

use bifrost::rpc::{RPCClient, RPCService, Server};
use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

mod fixture {
    use super::*;
    service! { rpc lookup(chunk: u64, offset: u64) -> u64; }
    pub struct Chunk(pub u64);
    impl Service for Chunk {
        fn lookup(&self, chunk: u64, offset: u64) -> futures::future::BoxFuture<'_, u64> {
            assert_eq!(chunk, self.0);
            futures::future::ready(chunk ^ offset).boxed()
        }
    }
    dispatch_rpc_service_functions!(Chunk);
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).expect("local or tcp").clone();
    assert!(mode == "local" || mode == "tcp");
    let threads: usize = args.get(2).expect("threads").parse().unwrap();
    let calls: u64 = args.get(3).expect("calls per thread").parse().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async move {
        let chunks: Arc<Vec<_>> = Arc::new((0..8).map(|id| Arc::new(fixture::Chunk(id))).collect());
        let mut server = None;
        let mut network_task = None;
        let address = if mode == "local" {
            let address = bifrost::tcp::STANDALONE_ADDRESS.to_string();
            let local = Server::new(&address);
            for (id, service) in chunks.iter().enumerate() { local.register_service_with_id(id as u64, service).await; }
            Server::listen_and_resume(&local).await;
            while !bifrost::tcp::shortcut::is_local(local.server_id).await { tokio::task::yield_now().await; }
            server = Some(local);
            address
        } else {
            // No shortcut is registered for this listener, so every call uses TCP.
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let chunks = chunks.clone();
            network_task = Some(tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
                while let Some(Ok(mut frame)) = frames.next().await {
                    let message = frame.get_u64_le();
                    let chunk = frame.get_u64_le();
                    let result = chunks[chunk as usize].dispatch(frame).await.unwrap();
                    let mut response = BytesMut::with_capacity(9 + result.len());
                    response.put_u64_le(message);
                    response.put_u8(0);
                    response.extend_from_slice(&result);
                    frames.send(response.freeze()).await.unwrap();
                }
            }));
            address
        };
        let client = RPCClient::new_async(&address).await.unwrap();
        // Warm the connection and all fixture keys before the timed interval.
        for chunk in 0..8 { assert_eq!(fixture::ImmeServiceClient::lookup(chunk, &client, chunk, 64).await.unwrap(), chunk ^ 64); }
        let barrier = Arc::new(tokio::sync::Barrier::new(threads + 1));
        let mut tasks = Vec::new();
        for worker in 0..threads {
            let client = client.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                for sequence in 0..calls {
                    let id = sequence * threads as u64 + worker as u64;
                    let chunk = id % 8;
                    let offset = ((id / 8) * 64) % (1024 * 1024 * 1024);
                    assert_eq!(fixture::ImmeServiceClient::lookup(chunk, &client, chunk, offset).await.unwrap(), chunk ^ offset);
                }
            }));
        }
        let start = Instant::now();
        barrier.wait().await;
        for task in tasks { task.await.unwrap(); }
        let seconds = start.elapsed().as_secs_f64();
        let total = calls * threads as u64;
        println!("{{\"mode\":\"{mode}\",\"threads\":{threads},\"calls\":{total},\"seconds\":{seconds},\"calls_per_second\":{},\"logical_chunk_count\":8,\"logical_chunk_bytes\":1073741824}}", total as f64 / seconds);
        drop(client);
        if let Some(server) = server { server.shutdown().await; }
        if let Some(task) = network_task { task.abort(); let _ = task.await; }
    });
}
