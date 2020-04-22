use std::sync::Arc;
use std::time::Duration;

use crate::tcp::{shortcut, STANDALONE_ADDRESS};
use crate::DISABLE_SHORTCUT;
use bifrost_hasher::hash_str;

use crate::tcp::server::{TcpReq, TcpRes};
use bytes::{BytesMut, BufMut, Bytes, Buf};
use futures::SinkExt;
use std::future::Future;
use std::pin::Pin;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::time;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::sync::mpsc::*;
use futures::prelude::*;
use futures::stream::SplitSink;
use std::collections::HashMap;
use tokio::sync::mpsc;

pub struct Client {
    //client: Option<SplitSink<Bytes>>,
    client: Option<Framed<TcpStream, LengthDelimitedCodec>>,
    msg_counter: u64,
    token_rx: mpsc::Receiver<()>,
    token_tx: mpsc::Sender<()>,
    pub server_id: u64,
}

impl Client {
    pub async fn connect_with_timeout(address: &String, timeout: Duration) -> io::Result<Self> {
        let server_id = hash_str(address);
        let client = {
            if !DISABLE_SHORTCUT && shortcut::is_local(server_id).await {
                None
            } else {
                if address.eq(&STANDALONE_ADDRESS) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "STANDALONE server is not found",
                    ));
                }
                let socket = time::timeout(timeout, TcpStream::connect(address)).await??;
                Some(Framed::new(socket, LengthDelimitedCodec::new()))
            }
        };
        let (mut token_tx, token_rx) = mpsc::channel(1);
        token_tx.send(()).await;
        Ok(Client {
            client,
            server_id,
            token_tx,
            token_rx,
            msg_counter: 0
        })
    }
    pub async fn connect(address: &String) -> io::Result<Self> {
        Client::connect_with_timeout(address, Duration::from_secs(5)).await
    }
    pub async fn send_msg(&mut self, msg: TcpReq) -> io::Result<BytesMut> {
        if let Some(ref mut transport) = self.client {
            self.token_rx.recv().await;
            let mut frame = BytesMut::with_capacity(8 + msg.len());
            let msg_id = self.msg_counter;
            frame.put_u64_le(msg_id);
            frame.extend_from_slice(msg.as_ref());
            self.msg_counter += 1;
            transport.send(frame.freeze()).await?;
            while let Some(res) = transport.next().await {
                self.token_tx.send(()).await;
                match res {
                    Err(e) => return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Cannot decode data: {:?}", e),
                    )),
                    Ok(mut data) => {
                        let res_msg_id = data.get_u64_le();
                        assert_eq!(res_msg_id, msg_id);
                        return Ok(data)
                    }
                }
            }
            self.token_tx.send(()).await;
            Err(io::Error::new(io::ErrorKind::NotConnected, ""))
        } else {
            Ok(shortcut::call(self.server_id, msg).await?)
        }
    }
}

unsafe impl Send for Client {}
