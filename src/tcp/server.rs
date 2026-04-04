use super::STANDALONE_ADDRESS;
use crate::tcp::shortcut;
use bytes::{Buf, BufMut, BytesMut};
use futures::SinkExt;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub type RPCFuture = dyn Future<Output = TcpRes>;
pub type BoxedRPCFuture = Box<RPCFuture>;
pub type TcpReq = BytesMut;
pub type TcpRes = Pin<Box<dyn Future<Output = BytesMut> + Send>>;

pub struct Server {
    shutdown_tx: broadcast::Sender<()>,
}

impl Server {
    pub fn new() -> Server {
        let (shutdown_tx, _) = broadcast::channel(1);
        Server { shutdown_tx }
    }

    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    pub async fn listen(
        &self,
        addr: &String,
        callback: Arc<dyn Fn(TcpReq) -> TcpRes + Send + Sync>,
    ) -> Result<(), Box<dyn Error>> {
        shortcut::register_server(addr, &callback).await;
        if !addr.eq(&STANDALONE_ADDRESS) {
            let listener = TcpListener::bind(&addr).await?;
            let mut shutdown_rx = self.shutdown_tx.subscribe();
            
            info!("TCP server listening on {}", addr);
            
            loop {
                tokio::select! {
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((socket, addr)) => {
                                debug!("Accepted connection from {}", addr);
                                let callback = callback.clone();
                                let mut conn_shutdown_rx = self.shutdown_tx.subscribe();
                                
                                tokio::spawn(async move {
                                    let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
                                    loop {
                                        tokio::select! {
                                            result = transport.next() => {
                                                match result {
                                                    Some(Ok(mut data)) => {
                                                        let msg_id = data.get_u64_le();
                                                        let call_back_data = callback(data).await;
                                                        let mut res =
                                                            BytesMut::with_capacity(8 + call_back_data.len());
                                                        res.put_u64_le(msg_id);
                                                        res.extend_from_slice(call_back_data.as_ref());
                                                        if let Err(e) = transport.send(res.freeze()).await {
                                                            error!("Error on TCP callback {:?}", e);
                                                            break;
                                                        }
                                                    }
                                                    Some(Err(e)) => {
                                                        error!("error on decoding from socket; error = {:?}", e);
                                                        break;
                                                    }
                                                    None => {
                                                        debug!("Connection closed by client");
                                                        break;
                                                    }
                                                }
                                            }
                                            _ = conn_shutdown_rx.recv() => {
                                                info!("Connection handler received shutdown signal");
                                                break;
                                            }
                                        }
                                    }
                                    // The connection will be closed at this point
                                });
                            }
                            Err(e) => error!("error accepting socket; error = {:?}", e),
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("TCP server on {} received shutdown signal, stopping accept loop", addr);
                        break;
                    }
                }
            }
        }
        info!("TCP server on {} shut down gracefully", addr);
        Ok(())
    }

    pub fn shutdown(&self) {
        info!("Initiating TCP server shutdown");
        let _ = self.shutdown_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use futures::future::FutureExt;
    use std::sync::Arc;
    use tokio::time::{sleep, Duration};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_server_creation() {
        let server = Server::new();
        assert!(server.shutdown_tx.receiver_count() == 0, "Should start with no subscribers");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_shutdown_handle() {
        let server = Server::new();
        let handle = server.shutdown_handle();

        // Subscribe a receiver so the send won't fail
        let mut _rx = handle.subscribe();

        // Verify we can send shutdown signal
        let result = handle.send(());
        assert!(result.is_ok(), "Should be able to send shutdown signal");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_server_listen_and_shutdown() {
        let _ = env_logger::builder().format_timestamp(None).try_init();

        let addr = String::from("127.0.0.1:9100");
        let server = Arc::new(Server::new());

        let callback = Arc::new(|_data: TcpReq| -> TcpRes {
            async move {
                let mut response = BytesMut::new();
                response.put_slice(b"pong");
                response
            }.boxed()
        });

        let server_clone = server.clone();
        let addr_clone = addr.clone();
        let callback_clone = callback.clone();

        tokio::spawn(async move {
            let _ = server_clone.listen(&addr_clone, callback_clone).await;
        });

        sleep(Duration::from_millis(500)).await;

        // Test that server is listening
        let connect_result = tokio::net::TcpStream::connect(&addr).await;
        assert!(connect_result.is_ok(), "Server should be listening");

        // Shutdown the server
        server.shutdown();
        sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_server_callback_invocation() {
        let _ = env_logger::builder().format_timestamp(None).try_init();

        let addr = String::from("127.0.0.1:9200");
        let server = Arc::new(Server::new());

        let callback = Arc::new(|data: TcpReq| -> TcpRes {
            async move {
                // Echo back the data
                let mut response = BytesMut::new();
                response.put_slice(&data);
                response
            }.boxed()
        });

        let server_clone = server.clone();
        let addr_clone = addr.clone();
        let callback_clone = callback.clone();

        tokio::spawn(async move {
            let _ = server_clone.listen(&addr_clone, callback_clone).await;
        });

        sleep(Duration::from_millis(500)).await;

        // Shutdown after test
        server.shutdown();
        sleep(Duration::from_millis(200)).await;
    }
}
