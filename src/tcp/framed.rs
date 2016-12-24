use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use std::{io, str};
use byteorder::{ByteOrder, LittleEndian};

pub struct BytesCodec;

impl Codec for BytesCodec {

    type In = (u64, Vec<u8>);
    type Out = (u64, Vec<u8>);

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, io::Error> {
        let buf_len = buf.as_ref().len();
        if buf_len >= 8 * 2 {
            let mid = LittleEndian::read_u64(buf.as_ref());
            let len = LittleEndian::read_u64(&buf.as_ref()[8..16]);
            if buf_len as u64 >= 8 * 2 + len {
                buf.drain_to(16);
                let mut data = Vec::with_capacity(len as usize);
                data.extend_from_slice(buf.drain_to(len as usize).as_slice());
                return Ok(Some((mid, data)))
            }
        }
        return Ok(None);
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let (mid, msg) = msg;
        let len = msg.len();
        let mut mid_bytes = [0u8; 8];
        let mut len_bytes = [0u8; 8];
        LittleEndian::write_u64(&mut mid_bytes, mid as u64);
        LittleEndian::write_u64(&mut len_bytes, len as u64);
        buf.reserve_exact(len + 8 * 2);
        buf.extend_from_slice(&mid_bytes);
        buf.extend_from_slice(&len_bytes);
        buf.extend_from_slice(msg.as_slice());
        Ok(())
    }
}