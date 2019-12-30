use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use super::STANDALONE_ADDRESS;

pub struct ServerCallback {
    closure: Box<Fn(Vec<u8>) -> Box<Future<Item = Vec<u8>, Error = io::Error>>>,
}

impl ServerCallback {
    pub fn new<F: 'static>(f: F) -> ServerCallback
    where
        F: Fn(Vec<u8>) -> Box<Future<Item = Vec<u8>, Error = io::Error>>,
    {
        ServerCallback {
            closure: Box::new(f),
        }
    }
    pub fn call(&self, data: Vec<u8>) -> Box<Future<Item = Vec<u8>, Error = io::Error>> {
        (self.closure)(data)
    }
}

unsafe impl Send for ServerCallback {}
unsafe impl Sync for ServerCallback {}

pub struct Server {
    callback: Arc<ServerCallback>,
}

pub struct NewServer {
    callback: Arc<ServerCallback>,
}

impl Service for Server {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    type Future = Box<Future<Item = Vec<u8>, Error = io::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        self.callback.call(req)
    }
}

impl NewService for NewServer {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    type Instance = Server;

    fn new_service(&self) -> io::Result<Self::Instance> {
        Ok(Server {
            callback: self.callback.clone(),
        })
    }
}

impl Server {
    pub fn new(addr: &String, callback: ServerCallback) {
        let callback_ref = Arc::new(callback);
        shortcut::register_server(addr, &callback_ref);
        let new_server = NewServer {
            callback: callback_ref,
        };
        if !addr.eq(&STANDALONE_ADDRESS) {
            let socket_addr: SocketAddr = addr.parse().unwrap();
            TcpServer::new(BytesServerProto, socket_addr).serve(new_server);
        }
    }
}
