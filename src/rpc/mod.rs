pub mod server;
pub mod connection;
pub mod proto;

use std::net::SocketAddr;

use mio::*;
use mio::tcp::*;
use env_logger;
use std::sync::RwLock;
use std::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use byteorder::{ByteOrder, BigEndian};
use std::io::prelude::*;
use self::server::ServerCallback;

pub struct Server {
    addr: SocketAddr,
    poll: Poll,
    server: server::Server
}

impl Server {
    pub fn new(addr: String, callback: Box<ServerCallback>) -> Server {
        // Before doing anything, let us register a logger. The mio library has really good logging
        // at the _trace_ and _debug_ levels. Having a logger setup is invaluable when trying to
        // figure out why something is not working correctly.
        env_logger::init().ok().expect("Failed to init logger");

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
    stream: TcpStream
}

impl Client {
    pub fn send_message(&mut self, data: Vec<u8>) -> Vec<u8> { //TODO: package segment
        let mut buf = [0u8; 8];
        BigEndian::write_u64(&mut buf, data.len() as u64);
        self.stream.write_all(buf.as_ref()).unwrap();
        self.stream.write_all(data.as_ref()).unwrap();

        let mut buf = [0u8; 8];
        while self.stream.read(&mut buf).is_err() {}
        let msg_len = BigEndian::read_u64(&mut buf);
        debug!("CLIENT: Msg LEN {}", msg_len);
        let mut r = vec![0u8; msg_len as usize];
        let s_ref = <TcpStream as Read>::by_ref(&mut self.stream);
        while s_ref.take(msg_len).read(&mut r).is_err() {}
        r
    }
}

type ClientCallback = FnMut(&Vec<u8>) + Send;

pub struct Clients {
    clients: Arc<RwLock<HashMap<String, Arc<Mutex<Client>>>>>,
    callback: Box<ClientCallback>
}

impl Clients {
    pub fn new(callback: Box<ClientCallback>) -> Clients {
        Clients {
            clients: Arc::new(RwLock::new(HashMap::<String, Arc<Mutex<Client>>>::default())),
            callback: callback
        }
    }
    fn chk_client_for(&mut self, addr: &String) {
        {
            let map = self.clients.read().unwrap();
            let client = (*map).get(addr);
            match client {
                Some(c) => {return}
                None => {}
            }
        }
        {
            let mut map = self.clients.write().unwrap();
            let socket_addr = addr.parse::<SocketAddr>()
                .ok().expect("Failed to parse host:port string");
            (*map).entry(addr.clone()).or_insert_with(move || {
                Arc::new(Mutex::new(
                    Client {
                        stream: TcpStream::connect(&socket_addr).unwrap()
                    }
                ))
            });
        }
    }
    pub fn client_for (&mut self, addr: String) -> Arc<Mutex<Client>> {
        self.chk_client_for(&addr);
        let map = self.clients.clone();
        let c = map.read().unwrap();
        c[&addr].clone()
    }
    pub fn send_message (&mut self, server_addr: String, data: Vec<u8>) {
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
        Arc::new(Mutex::new(Server::new(server_addr, server_callback))),
        Arc::new(Mutex::new(Clients::new(client_callback)))
    )
}