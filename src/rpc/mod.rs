pub mod server;
pub mod connection;
#[macro_use]
pub mod proto;

use std::net::SocketAddr;

use mio::*;
use mio::tcp::*;
use env_logger;
use std::sync::RwLock;
use std::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use byteorder::{ByteOrder, LittleEndian};
use std::io::prelude::*;
use self::server::ServerCallback;
use std::time::Duration;
use utils::time::{get_time, duration_to_ms};

pub struct Server {
    addr: SocketAddr,
    poll: Poll,
    server: server::Server
}

impl Server {
    pub fn new(addr: &String, callback: Box<ServerCallback>) -> Server {
        // Before doing anything, let us register a logger. The mio library has really good logging
        // at the _trace_ and _debug_ levels. Having a logger setup is invaluable when trying to
        // figure out why something is not working correctly.
        let addr = addr.parse::<SocketAddr>()
            .ok().expect("Failed to parse host:port string");
        let sock = TcpListener::bind(&addr).ok().expect("Failed to bind address");

        // Create a polling object that will be used by the server to receive events
        let poll = Poll::new().expect("Failed to create Poll");

        // Create our Server object and start polling for events. I am hiding away
        // the details of how registering works inside of the `Server` object. One reason I
        // really like this is to get around having to have `const SERVER = Token(0)` at the top of my
        // file. It also keeps our polling options inside `Server`.
        let actual_server = server::Server::new(sock, callback);
        Server {
            addr: addr,
            poll: poll,
            server: actual_server
        }
    }

    pub fn start(&mut self) {
        self.server.run(&mut self.poll).expect("Failed to run server");
    }
}

pub struct Client {
    stream: TcpStream,
    timeout: u64,
}

impl Client {
    pub fn with_timeout(addr: &String, timeout: Duration) -> Client {
        let socket_addr = addr.parse::<SocketAddr>()
            .ok().expect("Failed to parse host:port string");
        Client {
            stream: TcpStream::connect(&socket_addr).unwrap(),
            timeout: duration_to_ms(timeout),
        }
    }
    pub fn new(addr: &String) -> Client {
        Client::with_timeout(addr, Duration::new(5, 0))
    }
    pub fn send_message(&mut self, data: Vec<u8>) -> Option<Vec<u8>> { //TODO: package segment
        let time_checker = TimeoutChecker::new(self.timeout);
        let mut buf = [0u8; 8];
        LittleEndian::write_u64(&mut buf, data.len() as u64);
        self.stream.write_all(buf.as_ref()).unwrap();
        self.stream.write_all(data.as_ref()).unwrap();

        let mut buf = [0u8; 8];
        while self.stream.read(&mut buf).is_err() {
            if !time_checker.check() {return None;}
        }
        let msg_len = LittleEndian::read_u64(&mut buf);
        debug!("CLIENT: Msg LEN {}", msg_len);
        let mut r = vec![0u8; msg_len as usize];
        let s_ref = <TcpStream as Read>::by_ref(&mut self.stream);
        while s_ref.take(msg_len).read(&mut r).is_err() {
            if !time_checker.check() {return None;}
        }
        Some(r)
    }
}

type ClientCallback = FnMut(&Option<Vec<u8>>) + Send;

pub struct Clients {
    clients: Arc<RwLock<HashMap<String, Arc<Mutex<Client>>>>>,
    callback: Box<ClientCallback>,
}

impl Clients {
    pub fn new(callback: Box<ClientCallback>) -> Clients {
        Clients {
            clients: Arc::new(RwLock::new(HashMap::<String, Arc<Mutex<Client>>>::default())),
            callback: callback,
        }
    }
    fn client_for(&mut self, addr: &String) -> Arc<Mutex<Client>> {
        {
            let map = self.clients.read().unwrap();
            let client = (*map).get(addr);
            match client {
                Some(c) => {return c.clone()}
                None => {}
            }
        }
        {
            let mut map = self.clients.write().unwrap();
            (*map).entry(addr.clone()).or_insert_with(move || {
                Arc::new(Mutex::new(
                    Client::new(addr)
                ))
            });
            map[addr].clone()
        }
    }
    pub fn send_message (&mut self, server_addr: &String, data: Vec<u8>) {
        let client_lock = self.client_for(server_addr).clone();
        let mut client = client_lock.lock().unwrap();
        let feedback = client.send_message(data);
        (self.callback)(&feedback)
    }
}

pub fn new (server_port: u32, server_callback: Box<ServerCallback>, client_callback: Box<ClientCallback>)
    -> (Arc<Mutex<Server>>, Arc<Mutex<Clients>>) {
    let server_addr = format!("0.0.0.0:{}", server_port);
    (
        Arc::new(Mutex::new(Server::new(&server_addr, server_callback))),
        Arc::new(Mutex::new(Clients::new(client_callback)))
    )
}

struct TimeoutChecker {
    end: u64,
}

impl TimeoutChecker {
    pub fn new(duration: u64) -> TimeoutChecker {
        let start = get_time();
        TimeoutChecker {
            end: start + duration,
        }
    }
    pub fn check(&self) -> bool{
        !(get_time() > self.end)
    }
}