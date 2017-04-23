use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{self, BoxFuture, Future};

use tokio_service::Service;
use tokio_core::io::Io;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;
use tokio_proto::TcpClient;
use tokio_proto::multiplex::{ClientProto, ClientService};
use tokio_middleware::Timeout;
use tokio_timer::Timer;

use tcp::proto::BytesClientProto;
use bifrost_hasher::hash_str;
use shortcut::{tcp as shortcut};

pub type ResFuture = Future<Item = Vec<u8>, Error = io::Error>;

pub struct ClientCore {
    inner: ClientService<TcpStream, BytesClientProto>,
}

pub struct Client {
    client: Option<Timeout<ClientCore>>,
    server_id: u64,
    core: Box<Core>,
}

impl Service for ClientCore {

    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    // Again for simplicity, we are just going to box a future
    type Future = Box<Future<Item = Self::Response, Error = io::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        self.inner.call(req).boxed()
    }
}

impl Client {
    pub fn connect_with_timeout (address: &String, timeout: Duration) -> io::Result<Client> {
        let mut core = Core::new()?;
        let server_id = hash_str(address);
        let client = {
            if shortcut::is_local(server_id) {
                None
            } else {
                let socket_address = address.parse().unwrap();
                let future = Box::new(TcpClient::new(BytesClientProto)
                    .connect(&socket_address, &core.handle())
                    .map(|c| Timeout::new(
                        ClientCore {
                            inner: c,
                        },
                        Timer::default(),
                        timeout)));
                Some(core.run(future)?)
            }
        };
        Ok(Client {
            client: client,
            core: Box::new(core),
            server_id: server_id,
        })
    }
    pub fn connect (address: &String) -> io::Result<Client> {
        Client::connect_with_timeout(address, Duration::from_secs(5))
    }
    pub fn send(&mut self, msg: Vec<u8>) -> io::Result<Vec<u8>> {
        let future = self.send_async(msg);
        self.core.run(future)
    }
    pub fn send_async(&mut self, msg: Vec<u8>) -> Box<ResFuture> {
        if let Some(ref client) = self.client {
            Box::new(client.call(msg))
        } else {
            shortcut::call(self.server_id, msg)
        }
    }
}

unsafe impl Send for Client {}