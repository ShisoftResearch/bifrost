use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

use super::STANDALONE_ADDRESS;
use crate::tcp::shortcut;
use std::future::Future;
use crate::tcp::framed::BytesCodec;
use tokio_util::codec::Framed;
use tokio::stream::StreamExt;

unsafe impl Send for ServerCallback {}
unsafe impl Sync for ServerCallback {}

pub type BoxedRPCFuture = Box<RPCFuture>;
pub type RPCReq = Vec<u8>;

pub struct Server<C: Fn(RPCReq) -> BoxedRPCFuture>;

impl <C: Fn(BytesMut) -> BoxedRPCFuture> Service for Server<C> {
    fn call(req: RPCReq) -> BoxedRPCFuture {
        let call = C;
        call(req)
    }
}

impl <C: Fn(Vec<u8>) -> BoxedRPCFuture> Server<C> {
    pub async fn new(addr: &String) {
        shortcut::register_server::<C>(addr).await;
        if !addr.eq(&STANDALONE_ADDRESS) {
            let mut listener = TcpListener::bind(&addr).await?;
            loop {
                match listener.accept().await {
                    Ok((socket, _)) => {
                        // Like with other small servers, we'll `spawn` this client to ensure it
                        // runs concurrently with all other clients. The `move` keyword is used
                        // here to move ownership of our db handle into the async closure.
                        tokio::spawn(async move {
                            while let Some(result) = data.next().await {
                                match result {
                                    Ok((mid, data)) => {
                                        Self::ca
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
