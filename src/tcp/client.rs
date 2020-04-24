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
use parking_lot::Mutex as SyncMutex;
use tokio::sync::oneshot;

pub struct Client {
    //client: Option<SplitSink<Bytes>>,
    client: Option<SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>>,
    msg_counter: u64,
    senders: Arc<SyncMutex<HashMap<u64, oneshot::Sender<BytesMut>>>>,
    timeout: Duration,
    pub server_id: u64,
}

impl Client {
    pub async fn connect_with_timeout(address: &String, timeout: Duration) -> io::Result<Self> {
        let server_id = hash_str(address);
        let senders = Arc::new(SyncMutex::new(HashMap::new()));
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
                let transport = Framed::new(socket, LengthDelimitedCodec::new());
                let (writer, mut reader) = transport.split();
                let cloned_senders = senders.clone();
                tokio::spawn(async move {
                    while let Some(res) = reader.next().await {
                        if let Ok(mut data) = res {
                            let res_msg_id = data.get_u64_le();
                            let sender: oneshot::Sender<BytesMut> = cloned_senders.lock().remove(&res_msg_id).unwrap();
                            sender.send(data).unwrap();
                        }
                    }
                });
                Some(writer)
            }
        };
        Ok(Client {
            client,
            server_id,
            senders,
            timeout,
            msg_counter: 0
        })
    }
    pub async fn connect(address: &String) -> io::Result<Self> {
        Client::connect_with_timeout(address, Duration::from_secs(2)).await
    }
    pub async fn send_msg(&mut self, msg: TcpReq) -> io::Result<BytesMut> {
        if let Some(ref mut transport) = self.client {
            let mut frame = BytesMut::with_capacity(8 + msg.len());
            let rx = {
                let mut senders = self.senders.lock();
                let msg_id = self.msg_counter;
                frame.put_u64_le(msg_id);
                frame.extend_from_slice(msg.as_ref());
                self.msg_counter += 1;
                let (tx, rx) = oneshot::channel();
                senders.insert(msg_id, tx);
                rx
            };
            time::timeout(self.timeout, transport.send(frame.freeze())).await??;
            Ok(time::timeout(self.timeout, rx).await?.unwrap())
        } else {
            Ok(shortcut::call(self.server_id, msg).await?)
        }
    }
}

unsafe impl Send for Client {}
