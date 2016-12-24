use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use std::{io, str};
use byteorder::{ByteOrder, LittleEndian};

pub struct BytesCodec;

impl Codec for BytesCodec {

    type In = Vec<u8>;
    type Out = Vec<u8>;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        let buf_len = buf.as_ref().len();
        if buf_len >= 8 {
            let len = LittleEndian::read_u64(buf.as_ref());
            if buf_len as u64 >= 8 + len {
                buf.drain_to(8);
                let mut data = Vec::with_capacity(len as usize);
                data.extend_from_slice(buf.drain_to(len as usize).as_slice());
                return Ok(Some(data))
            }
        }
        return Ok(None);
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let len = msg.len();
        let mut len_bytes = [0u8; 8];
        LittleEndian::write_u64(&mut len_bytes, len as u64);
        buf.reserve_exact(len + 8);
        buf.extend_from_slice(&len_bytes);
        buf.extend_from_slice(msg.as_slice());
        Ok(())
    }
}

pub type FramedBytesTransport<T> = Framed<T, BytesCodec>;

pub fn new_bytes_transport<T>(inner: T) -> FramedBytesTransport<T>
    where T: Io,
{
    inner.framed(BytesCodec)
}