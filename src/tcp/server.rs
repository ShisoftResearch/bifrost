use super::STANDALONE_ADDRESS;
use crate::tcp::shortcut;
use bytes::BytesMut;
use futures::future::BoxFuture;
use futures::SinkExt;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
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
        callback: Box<dyn Fn(TcpReq) -> TcpRes + Send + Sync>,
    ) -> Result<(), Box<dyn Error>> {
        shortcut::register_server(addr, callback).await;
        if !addr.eq(&STANDALONE_ADDRESS) {
            let mut listener = TcpListener::bind(&addr).await?;
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        // Like with other small servers, we'll `spawn` this client to ensure it
                        // runs concurrently with all other clients. The `move` keyword is used
                        // here to move ownership of our db handle into the async closure.
                        tokio::spawn(async {
                            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
                            while let Some(result) = transport.next().await {
                                match result {
                                    Ok(data) => {
                                        let res = callback(data).await;
                                        if let Err(e) = transport.send(res.freeze()).await {
                                            println!("Error on TCP callback {:?}", e);
                                        }
                                    }
                                    Err(e) => {
                                        println!("error on decoding from socket; error = {:?}", e);
                                    }
                                }
                            }
                            // The connection will be closed at this point as `lines.next()` has returned `None`.
                        });
                    }
                    Err(e) => println!("error accepting socket; error = {:?}", e),
                }
            }
        }
        Ok(())
    }
}
