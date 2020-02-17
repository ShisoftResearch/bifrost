use std::sync::Arc;
use std::time::Duration;

use crate::tcp::{shortcut, STANDALONE_ADDRESS};
use crate::DISABLE_SHORTCUT;
use bifrost_hasher::hash_str;

use crate::tcp::server::{TcpReq, TcpRes};
use bytes::BytesMut;
use futures::SinkExt;
use std::future::Future;
use std::pin::Pin;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::stream::{Stream, StreamExt};
use tokio::time;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub struct Client {
    client: Option<Framed<TcpStream, LengthDelimitedCodec>>,
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
                Some(time::timeout(timeout, TcpStream::connect(address)).await??)
            }
        };
        Ok(Client {
            client: client.map(|socket| Framed::new(socket, LengthDelimitedCodec::new())),
            server_id,
        })
    }
    pub async fn connect(address: &String) -> io::Result<Self> {
        Client::connect_with_timeout(address, Duration::from_secs(5)).await
    }
    pub async fn send_msg(mut self: Pin<&mut Self>, msg: TcpReq) -> io::Result<BytesMut> {
        if let Some(ref mut transport) = self.client {
            transport.send(msg.freeze()).await?;
            while let Some(res) = transport.next().await {
                return res.map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Cannot decode data: {:?}", e),
                    )
                });
            }
            Err(io::Error::new(io::ErrorKind::NotConnected, ""))
        } else {
            Ok(shortcut::call(self.server_id, msg).await?)
        }
    }
}

unsafe impl Send for Client {}
