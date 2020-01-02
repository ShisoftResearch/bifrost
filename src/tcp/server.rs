use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

use super::STANDALONE_ADDRESS;
use crate::tcp::shortcut;
use std::future::Future;
use crate::tcp::framed::BytesCodec;
use tokio_util::codec::Framed;
use tokio::stream::StreamExt;
use bytes::BytesMut;

pub type RPCFuture = dyn Future<RPCRes>;
pub type BoxedRPCFuture = Box<RPCFuture>;
pub type RPCReq = BytesMut;
pub type RPCRes = BytesMut;

pub struct Server<C: Fn(RPCReq) -> BoxedRPCFuture>;

impl <C: Fn(BytesMut) -> BoxedRPCFuture> Server<C> {
    pub async fn new(addr: &String, callback: C) {
        shortcut::register_server::<C>(addr, callback).await;
        if !addr.eq(&STANDALONE_ADDRESS) {
            let mut listener = TcpListener::bind(&addr).await?;
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        // Like with other small servers, we'll `spawn` this client to ensure it
                        // runs concurrently with all other clients. The `move` keyword is used
                        // here to move ownership of our db handle into the async closure.
                        tokio::spawn(async move {
                            let mut data = Framed::new(socket, BytesCodec);
                            while let Some(result) = data.next().await {
                                match result {
                                    Ok(data) => {
                                        callback(data)
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
    }
}
