use std::sync::Arc;
use std::time::Duration;

use crate::tcp::{STANDALONE_ADDRESS, shortcut};
use bifrost_hasher::hash_str;
use crate::DISABLE_SHORTCUT;

use tokio::io;
use tokio::net::TcpStream;
use tokio::time;
use tokio::stream::StreamExt;
use futures::SinkExt;
use tokio_util::codec::Framed;
use crate::tcp::framed::BytesCodec;
use std::future::Future;
use crate::tcp::server::{TcpReq, TcpRes};

pub struct Client {
    client: Option<Framed<TcpStream, BytesCodec>>,
    pub server_id: u64,
}

impl Client {
    pub async fn connect_with_timeout(address: &String, timeout: Duration) -> io::Result<Self> {
        let server_id = hash_str(address);
        let client = {
            if !DISABLE_SHORTCUT && shortcut::is_local(server_id) {
                None
            } else {
                if address.eq(&STANDALONE_ADDRESS) {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "STANDALONE server is not found",
                    ));
                }
                Some(time::timeout(timeout, TcpStream::connect(address)).await??)
            }
        };;
        Ok(Client {
            client: client.map(|socket| {
                Framed::new(socket, BytesCodec)
            }),
            server_id
        })
    }
    pub fn connect(address: &String) -> impl Future<Output = io::Result<Self>>  {
        Client::connect_with_timeout(address, Duration::from_secs(5))
    }
    pub async fn send(&mut self, msg: TcpReq) -> impl Future<Output = Result<TcpRes, io::Error>> {
        if let Some(ref mut transport) = self.client {
            transport.send(msg).await?;
            time::timeout(timeout, transport.next()).await??
        } else {
            shortcut::call(self.server_id, msg)
        }
    }
}

unsafe impl Send for Client {}
