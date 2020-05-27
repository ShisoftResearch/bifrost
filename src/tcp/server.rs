use super::STANDALONE_ADDRESS;
use crate::tcp::shortcut;
use crate::tcp::shortcut::call;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::SinkExt;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub type RPCFuture = dyn Future<Output = TcpRes>;
pub type BoxedRPCFuture = Box<RPCFuture>;
pub type TcpReq = BytesMut;
pub type TcpRes = Pin<Box<dyn Future<Output = BytesMut> + Send>>;

pub struct Server;

impl Server {
    pub async fn new(
        addr: &String,
        callback: Arc<dyn Fn(TcpReq) -> TcpRes + Send + Sync>,
    ) -> Result<(), Box<dyn Error>> {
        shortcut::register_server(addr, &callback).await;
        if !addr.eq(&STANDALONE_ADDRESS) {
            let mut listener = TcpListener::bind(&addr).await?;
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        // Like with other small servers, we'll `spawn` this client to ensure it
                        // runs concurrently with all other clients. The `move` keyword is used
                        // here to move ownership of our db handle into the async closure.
                        let callback = callback.clone();
                        tokio::spawn(async move {
                            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
                            while let Some(result) = transport.next().await {
                                match result {
                                    Ok(mut data) => {
                                        let msg_id = data.get_u64_le();
                                        let call_back_data = callback(data).await;
                                        let mut res =
                                            BytesMut::with_capacity(8 + call_back_data.len());
                                        // debug!("Received TCP message {}", msg_id);
                                        res.put_u64_le(msg_id);
                                        res.extend_from_slice(call_back_data.as_ref());
                                        if let Err(e) = transport.send(res.freeze()).await {
                                            error!("Error on TCP callback {:?}", e);
                                        }
                                    }
                                    Err(e) => {
                                        error!("error on decoding from socket; error = {:?}", e);
                                    }
                                }
                            }
                            // The connection will be closed at this point as `lines.next()` has returned `None`.
                        });
                    }
                    Err(e) => error!("error accepting socket; error = {:?}", e),
                }
            }
        }
        Ok(())
    }
}
