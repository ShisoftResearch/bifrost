use std::io;
use std::time::Duration;
use std::sync::Arc;

use futures::{Future, future};

use tokio_service::Service;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;
use tokio_proto::TcpClient;
use tokio_proto::multiplex::{ClientService};
use tokio_middleware::Timeout;
use tokio_timer::Timer;

use tcp::proto::BytesClientProto;
use tcp::shortcut;
use bifrost_hasher::hash_str;
use super::STANDALONE_ADDRESS;
use DISABLE_SHORTCUT;

use utils::future_parking_lot::Mutex;

pub struct ClientCore {
    inner: ClientService<TcpStream, BytesClientProto>,
}

pub struct Client {
    client: Option<Arc<Mutex<Timeout<ClientCore>>>>,
    pub server_id: u64,
}

impl Service for ClientCore {

    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    // Again for simplicity, we are just going to box a future
    type Future = Box<Future<Item = Self::Response, Error = io::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        Box::new(self.inner.call(req))
    }
}

impl Client {

    pub fn connect_with_timeout_async (address: String, timeout: Duration)
        -> Box<Future<Item = Client, Error = io::Error>>
    {
        let server_id = hash_str(&address);
        if !DISABLE_SHORTCUT && shortcut::is_local(server_id) {
            return box future::ok(Client {
                client: None,
                server_id
            })
        } else {
            if address.eq(&STANDALONE_ADDRESS) {
                return box future::err(io::Error::new(io::ErrorKind::Other, "STANDALONE server is not found"))
            }
            let mut core = match Core::new() {
                Ok(c) => c,
                Err(e) => return box future::err(e)
            };
            let socket_address = address.parse().unwrap();
            return box TcpClient::new(BytesClientProto)
                .connect(&socket_address, &core.handle())
                .map(|c| {
                    Some(Arc::new(Mutex::new(Timeout::new(
                        ClientCore {
                            inner: c,
                        },
                        Timer::default(),
                        timeout
                    ))))
                })
                .map(|client| {
                    Client {
                        client,
                        server_id,
                    }
                })
        }
    }
    pub fn connect_async (address: String)
        -> impl Future<Item = Client, Error = io::Error>
    {
        Client::connect_with_timeout_async(address, Duration::from_secs(5))
    }
    pub fn send(&self, msg: Vec<u8>) -> io::Result<Vec<u8>> {
        self.send_async(msg).wait()
    }
    pub fn send_async(&self, msg: Vec<u8>)
        -> impl Future<Item = Vec<u8>, Error = io::Error>
    {
        if let Some(ref client) = self.client {
            box client
                .clone()
                .lock_async()
                .map_err(|_| io::Error::from(io::ErrorKind::Other))
                .and_then(|c|
                    c.call(msg))
                .map_err(|_|
                    io::Error::from(io::ErrorKind::TimedOut))
        } else {
            shortcut::call(self.server_id, msg)
        }
    }
}

unsafe impl Send for Client {}