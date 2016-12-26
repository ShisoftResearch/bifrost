use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use futures::{self, Future};

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

struct ClientCore {
    inner: ClientService<TcpStream, BytesClientProto>,
}

pub struct Client {
    client: Timeout<ClientCore>,
    core: Box<Core>,
}

impl Service for ClientCore {

    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    // Again for simplicity, we are just going to box a future
    type Future = Box<Future<Item = Self::Response, Error = io::Error>>;

    fn call(&mut self, req: Self::Request) -> Self::Future {
        self.inner.call(req).boxed()
    }
}

impl Client {
    pub fn connect_with_timeout (address: &String, timeout: Duration) -> Client {
        let mut core = Core::new().unwrap();
        let address = address.parse().unwrap();
        let future = Box::new(TcpClient::new(BytesClientProto)
                                  .connect(&address, &core.handle())
                                  .map(|c| Timeout::new(
                                      ClientCore {
                                          inner: c,
                                      },
                                      Timer::default(),
                                      timeout
                                  )));
        let client = core.run(future).unwrap();
        Client {
            client: client,
            core: Box::new(core),
        }
    }
    pub fn connect (address: &String) -> Client {
        Client::connect_with_timeout(address, Duration::from_millis(500))
    }
    pub fn send(&mut self, msg: Vec<u8>) -> io::Result<Vec<u8>> {
        let resq = self.client.call(msg);
        self.core.run(resq)
    }
}

unsafe impl Send for Client {}