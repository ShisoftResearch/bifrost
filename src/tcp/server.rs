use std::io::{self};
use std::sync::Arc;
use std::net::SocketAddr;

use tokio;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio_io::codec::length_delimited::{FramedRead, FramedWrite};
use futures::{Future};
use bytes::BytesMut;

use tcp::shortcut;
use super::STANDALONE_ADDRESS;
use tokio::net::TcpStream;
use byteorder::LittleEndian;

pub struct Server {}
pub struct ServerCallback {
    closure: Box<Fn(BytesMut) -> Box<Future<Item = BytesMut, Error = io::Error>>>
}

impl ServerCallback {
    pub fn new<F: 'static>(f: F) -> ServerCallback
        where F: Fn(BytesMut) -> Box<Future<Item = BytesMut, Error = io::Error>>
    {
        ServerCallback {
            closure: Box::new(f)
        }
    }
    pub fn call(&self, data: BytesMut) -> Box<Future<Item = BytesMut, Error = io::Error>> {
        (self.closure)(data)
    }
}

unsafe impl Send for ServerCallback {}
unsafe impl Sync for ServerCallback {}


struct Receiving {
    inner: Box<Future<Item = (), Error = ()>>
}

impl Future for Receiving {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        self.inner.poll()
    }
}

unsafe impl Sync for Receiving {}
unsafe impl Send for Receiving {}

fn receive(tcp: TcpStream, callback: Arc<ServerCallback>)
    -> Receiving
{
    let (reader, writer) = tcp.split();
    let bytes_reader = FramedRead::new(reader);
    let bytes_writer = FramedWrite::new(writer);
    Receiving {
        inner: box bytes_reader
            .for_each(|mut bytes| {
                let msg_header = bytes.split_to(4);
                callback.call(bytes)
                    .and_then(move |result_bytes| {
                        let mut msg_bytes = msg_header;
                        msg_bytes.unsplit(result_bytes);
                        bytes_writer.send(msg_bytes)
                    })
                    .map(|_| ())
            })
            .map_err(|e| error!("{:?}", e))
    }
}

struct Service {
    inner: Box<Future<Item = (), Error = ()>>
}

impl Service {
    pub fn new(listener: TcpListener, callback_ref: Arc<ServerCallback>) -> Service {
        Service {
            inner: box listener
                .incoming()
                .map_err(|e| error!("{:?}", e))
                .for_each(|tcp| {
                    tokio::spawn(receive(tcp, callback_ref.to_owned()))
                })
        }
    }
}

impl Future for Service {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Result<Async<<Self as Future>::Item>, <Self as Future>::Error> {
        self.inner.poll()
    }
}

unsafe impl Sync for Service {}
unsafe impl Send for Service {}

impl Server {
    pub fn new(
        addr: &String,
        callback: ServerCallback)
    {
        let callback_ref = Arc::new(callback);
        shortcut::register_server(addr, &callback_ref);
        if !addr.eq(&STANDALONE_ADDRESS) {
            let socket_addr: SocketAddr = addr.parse().unwrap();
            let tcp = TcpListener::bind(&socket_addr).unwrap();
            tokio::run(Service::new(tcp, callback_ref));
        }
    }
}