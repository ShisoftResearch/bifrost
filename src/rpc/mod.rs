mod server;
mod client;
mod connection;

use std::net::SocketAddr;

use mio::*;
use mio::tcp::*;
use env_logger;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
use std::mem;
use std::rc::Rc;
use byteorder::{ByteOrder, BigEndian};
use std::io::prelude::*;

struct Server<CF> {
    addr: SocketAddr,
    poll: Poll,
    server: server::Server<CF>
}

impl <CF> Server <CF> where CF: FnMut(&Vec<u8>, &mut connection::Connection) {
    pub fn new(addr: String, callback: CF) -> Server<CF> {
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
        let actual_server = server::Server::<CF>::new(sock, callback);
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

struct Client {
    stream: Mutex<TcpStream>
}

impl Client {
    pub fn send_message(&mut self, data: Vec<u8>) -> Vec<u8> { //TODO: package segment
        let mut stream = self.stream.get_mut().unwrap();
        let mut buf = [0u8; 8];
        BigEndian::write_u64(&mut buf, data.len() as u64);
        stream.write_all(buf.as_ref()).unwrap();
        stream.write_all(data.as_ref()).unwrap();

        let mut buf = [0u8; 8];
        stream.read(&mut buf).unwrap();
        let msg_len = BigEndian::read_u64(&mut buf);
        let mut r = Vec::<u8>::with_capacity(msg_len as usize);

        loop {
            let s_ref = <TcpStream as Read>::by_ref(&mut stream);
            match s_ref.take(msg_len).read(&mut r) {
                Ok(0) => {
                    break;
                },
                Ok(n) => {
                    break;
                },
                Err(e) => {
                    panic!("{}", e);
                }
            }
        }
        r
    }
}

struct Clients {
    clients: Arc<RwLock<HashMap<String, Arc<Client>>>>
}

impl Clients {
    fn new() -> Clients {
        Clients {
            clients: Arc::new(RwLock::new(HashMap::<String, Arc<Client>>::default()))
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
                Arc::new(Client {
                    stream: Mutex::new(TcpStream::connect(&socket_addr).unwrap())
                })
            });
        }
    }
    pub fn client_for (&mut self, addr: String) -> Arc<Client> {
        self.chk_client_for(&addr);
        let map = self.clients.clone();
        let c = map.read().unwrap();
        c[&addr].clone()
    }
}