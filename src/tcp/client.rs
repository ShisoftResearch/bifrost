use std::sync::Arc;
use std::time::Duration;

use crate::tcp::{shortcut, STANDALONE_ADDRESS};
use crate::DISABLE_SHORTCUT;
use bifrost_hasher::hash_str;

use crate::tcp::server::TcpReq;
use async_std::sync::Mutex;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::prelude::*;
use futures::stream::SplitSink;
use futures::SinkExt;
use parking_lot::Mutex as SyncMutex;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct Client {
    //client: Option<SplitSink<Bytes>>,
    client: Option<Mutex<SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>>>,
    msg_counter: AtomicU64,
    senders: Arc<SyncMutex<HashMap<u64, oneshot::Sender<BytesMut>>>>,
    timeout: Duration,
    pub server_id: u64,
}

impl Client {
    pub async fn connect_with_timeout(address: &String, timeout: Duration) -> io::Result<Self> {
        let server_id = hash_str(address);
        let senders = Arc::new(SyncMutex::new(HashMap::new()));
        debug!(
            "TCP connect to {}, server id {}, timeout {}ms",
            address,
            server_id,
            timeout.as_millis()
        );
        let client = {
            if !DISABLE_SHORTCUT && shortcut::is_local(server_id).await {
                debug!("Local connection, using shortcut");
                None
            } else {
                if address.eq(&STANDALONE_ADDRESS) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "STANDALONE server is not found",
                    ));
                }
                debug!("Create socket on {}", address);
                let socket = time::timeout(timeout, TcpStream::connect(address)).await??;
                let transport = Framed::new(socket, LengthDelimitedCodec::new());
                let (writer, mut reader) = transport.split();
                let cloned_senders = senders.clone();
                debug!("Streaming messages for {}", address);
                let address = address.clone();
                tokio::spawn(async move {
                    while let Some(res) = reader.next().await {
                        if let Ok(mut data) = res {
                            let res_msg_id = data.get_u64_le();
                            trace!("Received msg for {}, size {}", res_msg_id, data.len());
                            let sender: oneshot::Sender<BytesMut> =
                                cloned_senders.lock().remove(&res_msg_id).unwrap();
                            sender.send(data).unwrap();
                        }
                    }
                    debug!("Stream from TCP server {} broken", address);
                });
                Some(Mutex::new(writer))
            }
        };
        Ok(Client {
            client,
            server_id,
            senders,
            timeout,
            msg_counter: AtomicU64::new(0),
        })
    }
    pub async fn connect(address: &String) -> io::Result<Self> {
        Client::connect_with_timeout(address, Duration::from_secs(2)).await
    }
    pub async fn send_msg(&self, msg: TcpReq) -> io::Result<BytesMut> {
        if let Some(ref transport) = self.client {
            let msg_id = self.msg_counter.fetch_add(1, Relaxed);
            let mut frame = BytesMut::with_capacity(8 + msg.len());
            let rx = {
                frame.put_u64_le(msg_id);
                frame.extend_from_slice(msg.as_ref());
                let (tx, rx) = oneshot::channel();
                let mut senders = self.senders.lock();
                senders.insert(msg_id, tx);
                rx
            };
            trace!("Sending msg {}, size {}", msg_id, frame.len());
            time::timeout(self.timeout, transport.lock().await.send(frame.freeze())).await??;
            trace!("Sent msg {}", msg_id);
            Ok(time::timeout(self.timeout, rx).await?.unwrap())
        } else {
            Ok(shortcut::call(self.server_id, msg).await?)
        }
    }
}

unsafe impl Send for Client {}
