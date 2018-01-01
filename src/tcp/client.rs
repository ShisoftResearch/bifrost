use std::io;
use std::time::Duration;

use futures::{Future};

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

pub type ResFuture = Future<Item = Vec<u8>, Error = io::Error>;

pub struct ClientCore {
    inner: ClientService<TcpStream, BytesClientProto>,
}

pub struct Client {
    client: Option<(Timeout<ClientCore>, Core)>,
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
    pub fn connect_with_timeout (address: &String, timeout: Duration) -> io::Result<Client> {
        let server_id = hash_str(address);
        let client = {
            if !DISABLE_SHORTCUT && shortcut::is_local(server_id) {
                None
            } else {
                if address.eq(&STANDALONE_ADDRESS) {
                    return Err(io::Error::new(io::ErrorKind::Other, "STANDALONE server is not found"))
                }
                let mut core = Core::new()?;
                let socket_address = address.parse().unwrap();
                let future = Box::new(TcpClient::new(BytesClientProto)
                    .connect(&socket_address, &core.handle())
                    .map(|c| Timeout::new(
                        ClientCore {
                            inner: c,
                        },
                        Timer::default(),
                        timeout)));
                Some((core.run(future)?, core))
            }
        };
        Ok(Client {
            client,
            server_id,
        })
    }
    pub fn connect (address: &String) -> io::Result<Client> {
        Client::connect_with_timeout(address, Duration::from_secs(5))
    }
    pub fn send(&mut self, msg: Vec<u8>) -> io::Result<Vec<u8>> {
        if let Some((ref client, ref mut core)) = self.client {
            let future = client.call(msg);
            core.run(future)
        } else {
            shortcut::call(self.server_id, msg).wait()
        }
    }
    pub fn send_async(&mut self, msg: Vec<u8>) -> Box<ResFuture> {
        if let Some((ref client, _)) = self.client {
            Box::new(client.call(msg))
        } else {
            shortcut::call(self.server_id, msg)
        }
    }
}

unsafe impl Send for Client {}