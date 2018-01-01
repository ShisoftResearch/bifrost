use std::io::{self};
use std::sync::Arc;
use std::net::SocketAddr;

use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};
use futures::{future, Future, BoxFuture};

use tcp::proto::BytesServerProto;
use tcp::shortcut;
use super::STANDALONE_ADDRESS;

// TODO: USE FUTURE IN THIS FUNCTION TO ENSURE FULL ASYNC CHAIN TO SERVICES
// WARN: current implementation cannot employ performance gain from tokio
pub type ServerCallback = Box<Fn(Vec<u8>) -> Vec<u8> + Send + Sync>;

pub struct Server {
    callback: Arc<ServerCallback>
}

pub struct NewServer {
    callback: Arc<ServerCallback>
}

impl Service for Server {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    type Future = BoxFuture<Vec<u8>, io::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        future::finished((self.callback) (req)).boxed()
    }
}

impl NewService for NewServer {

    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    type Instance = Server;

    fn new_service(&self) -> io::Result<Self::Instance> {
        Ok(Server{
          callback: self.callback.clone()
        })
    }
}

impl Server {
    pub fn new(addr: &String, callback: ServerCallback) {
        let callback_ref = Arc::new(callback);
        shortcut::register_server(addr, &callback_ref);
        let new_server = NewServer {
            callback: callback_ref
        };
        if !addr.eq(&STANDALONE_ADDRESS) {
            let socket_addr: SocketAddr = addr.parse().unwrap();
            TcpServer::new(BytesServerProto, socket_addr).serve(new_server);
        }
    }
}