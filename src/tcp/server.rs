use std::io::{self, ErrorKind, Write};
use std::sync::Arc;

use tokio_proto::TcpServer;
use tokio_service::{Service, NewService};
use futures::{future, Future, BoxFuture};

use tcp::framed::BytesCodec;
use tcp::proto::BytesServerProto;

pub type ServerCallback = Fn(Vec<u8>) -> Vec<u8> + Send + Sync;

pub struct Server {
    callback: Arc<Box<ServerCallback>>
}

pub struct NewServer {
    callback: Arc<Box<ServerCallback>>
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
    pub fn new(addr: &String, callback: Box<ServerCallback>) {
        let addr = addr.parse().unwrap();
        let new_server = NewServer {
            callback: Arc::new(callback)
        };
        TcpServer::new(BytesServerProto, addr).serve(new_server);
    }
}