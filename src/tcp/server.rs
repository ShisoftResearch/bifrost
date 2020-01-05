use super::STANDALONE_ADDRESS;
use crate::tcp::shortcut;
use std::future::Future;
use std::error::Error;
use crate::tcp::framed::BytesCodec;
use tokio_util::codec::Framed;
use tokio::stream::StreamExt;
use futures::SinkExt;
use tokio::net::TcpListener;
use bytes::BytesMut;

pub type RPCFuture = dyn Future<Output = TcpRes>;
pub type BoxedRPCFuture = Box<RPCFuture>;
pub type TcpReq = BytesMut;
pub type TcpRes = BytesMut;

pub struct Server<F: Future<Output = TcpRes>, C: Fn(TcpReq) -> F>;

impl <F: Future<Output = TcpRes>, C: Fn(BytesMut) -> F> Server<F, C> {
    pub async fn new(addr: &String, callback: C) -> Result<(), Box<dyn Error>> {
        shortcut::register_server::<F, C>(addr, callback).await;
        if !addr.eq(&STANDALONE_ADDRESS) { 
            let mut listener = TcpListener::bind(&addr).await?;
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        // Like with other small servers, we'll `spawn` this client to ensure it
                        // runs concurrently with all other clients. The `move` keyword is used
                        // here to move ownership of our db handle into the async closure.
                        tokio::spawn(async move {
                            let mut transport = Framed::new(socket, BytesCodec);
                            while let Some(result) = transport.next().await {
                                match result {
                                    Ok(data) => {
                                        let res = callback(data).await;
                                        if let Err(e) = transport.send(res).await {
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
