use std::io::{self};
use std::sync::Arc;
use std::net::SocketAddr;

use tokio;
use tokio::net::TcpListener;
use tokio::prelude::*;
use tokio_io::codec::length_delimited::{FramedRead, FramedWrite};
use futures::{future, Future};
use bytes::BytesMut;

use tcp::shortcut;
use super::STANDALONE_ADDRESS;
use tokio::net::TcpStream;

pub struct Server {}
pub type CallBack = Box<Fn(BytesMut) -> Box<Future<Item = BytesMut, Error = io::Error>>>;

struct DataStreamer<W: AsyncWrite, R: AsyncRead> {
    bw: FramedWrite<W>,
    br: FramedRead<R>
}

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

fn receive(tcp: TcpStream, callback: Arc<CallBack>)
    -> Receiving
{
    let (reader, writer) = tcp.split();
    let bytes_reader = FramedRead::new(reader);
    let bytes_writer = FramedWrite::new(writer);
    Receiving {
        inner: box bytes_reader
            .for_each(|bytes| {
                callback(bytes)
                    .and_then(|result_bytes| {
                        bytes_writer.send(result_bytes)
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
    pub fn new(listener: TcpListener) -> Service {
        Service {
            inner: box tcp
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
        callback: CallBack)
    {
        let callback_ref = Arc::new(callback);
        shortcut::register_server(addr, &callback_ref);
        if !addr.eq(&STANDALONE_ADDRESS) {
            let socket_addr: SocketAddr = addr.parse().unwrap();
            let tcp = TcpListener::bind(&socket_addr).unwrap();
            tokio::run(Service::new(tcp));
        }
    }
}