use std::io::{self, ErrorKind, Write};
use std::str;

pub struct BytesServerProto;
pub struct BytesClientProto;

impl<T: Io + 'static> ServerProto<T> for BytesServerProto {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Transport = Framed<T, BytesCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(BytesCodec))
    }
}

impl<T: Io + 'static> ClientProto<T> for BytesClientProto {
    type Request = Vec<u8>;
    type Response = Vec<u8>;
    type Transport = Framed<T, BytesCodec>;
    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(BytesCodec))
    }
}
