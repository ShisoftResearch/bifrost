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
