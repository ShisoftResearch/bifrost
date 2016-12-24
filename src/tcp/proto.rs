use std::str;
use std::io::{self, ErrorKind, Write};

use tokio_proto::multiplex::ServerProto;
use tokio_core::io::{Io, Framed};

use tcp::framed::BytesCodec;

pub struct BytesProto;

impl<T: Io + 'static> ServerProto<T> for BytesProto {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Error = io::Error;
    type Transport = Framed<T, BytesCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(BytesCodec))
    }
}
