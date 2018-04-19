use std::io;
use std::time::Duration;
use std::sync::Arc;

use futures::{Future, future};
use futures_cpupool::CpuPool;
use futures::prelude::*;
use num_cpus;

use tokio;
use tokio::prelude::*;
use tokio::reactor::Reactor;
use tokio::net::TcpStream;
use tokio::timer::Deadline;
use bytes::BytesMut;

// Use length delimited frames
use tokio_io::codec::length_delimited::{FramedRead, FramedWrite};

use tcp::shortcut;
use bifrost_hasher::hash_str;
use super::STANDALONE_ADDRESS;
use DISABLE_SHORTCUT;
use tokio_io::io::{WriteHalf, ReadHalf};
use utils::async_locks::Mutex;

pub type ResFuture = Future<Item = BytesMut, Error = io::Error>;

lazy_static! {
    pub static ref CONNECTING_POOL: CpuPool = CpuPool::new(10 * num_cpus::get());
}


struct ClientCore {
    bw: FramedWrite<WriteHalf<TcpStream>>,
    br: Arc<Mutex<FramedRead<ReadHalf<TcpStream>>>>
}

impl ClientCore {
    fn new(socket: TcpStream) -> ClientCore {
        let (read, write) = socket.split();
        let reader = FramedRead::new(read);
        let writer = FramedWrite::new(write);
        ClientCore {
            bw: writer, br: Arc::new(Mutex::new(reader))
        }
    }
    fn send(&self, data: BytesMut) -> impl Future<Item = BytesMut, Error = io::Error> {
        let reader_lock = self.br.clone();
        self.bw
            .send(data)
            .and_then(move |_|
                reader_lock.lock_async()
                    .map_err(|_| io::Error::new(io::ErrorKind::Other, ""))
                    .then(|dr| dr))
    }
}

pub struct Client {
    client: Option<ClientCore>,
    pub server_id: u64,
}

impl Client {
    pub fn connect_with_timeout (address: &String, timeout: Duration)
        -> impl Future<Item = Client, Error = io::Error>
    {
        let server_id = hash_str(address);
        let client = {
            if !DISABLE_SHORTCUT && shortcut::is_local(server_id) {
                None
            } else {
                if address.eq(&STANDALONE_ADDRESS) {
                    return Err(io::Error::new(io::ErrorKind::Other, "STANDALONE server is not found"))
                }
                let socket_address = address.parse().unwrap();
                let socket = TcpStream::connect(socket_address);
                let future = socket
                    .map(|socket| {
                        ClientCore::new(socket)
                    });
                Some(tokio::spawn(future).unwrap()) // this is required, or client won't receive response
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
    pub fn connect_with_timeout_async (address: String, timeout: Duration)
        -> impl Future<Item = Client, Error = io::Error>
    {
        CONNECTING_POOL.spawn_fn(move ||{
            Client::connect_with_timeout(&address, timeout)
        })
    }
    pub fn connect_async (address: String)
        -> impl Future<Item = Client, Error = io::Error>
    {
        CONNECTING_POOL.spawn_fn(move || {
            Client::connect_with_timeout(&address, Duration::from_secs(5))
        })
    }
    pub fn send_async(&self, msg: BytesMut) -> Box<ResFuture> {
        if let Some(ref client) = self.client {
            box client.send(msg)
        } else {
            shortcut::call(self.server_id, msg)
        }
    }
}

unsafe impl Send for Client {}