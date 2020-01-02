use byteorder::{ByteOrder, LittleEndian};
use std::{io, str};
use tokio_util::codec::{Decoder, Encoder};
use bytes::{BytesMut, BufMut};

pub struct BytesCodec;

impl Decoder for BytesCodec {
    type Item = (u64, Vec<u8>);
    type Error = ();

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::In>, Self::Error> {
        let buf_len = src.len();
        if buf_len >= 8 * 2 {
            let src_data = src.as_ref();
            let mid = LittleEndian::read_u64(&src_data[0..8]);
            let len = LittleEndian::read_u64(&src_data[8..16]);
            if buf_len as u64 >= 8 * 2 + len {
                buf.drain_to(16);
                let data = Vec::from(&src_data[16..]);
                return Ok(Some((mid, data)));
            }
        }
        return Ok(None);
    }
}

impl Encoder for BytesCodec {
    type Item = (u64, Vec<u8>);
    type Error = ();

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let (mid, msg) = msg;
        let len = msg.len();
        dst.reserve(8 * 2 + len);
        LittleEndian::write_u64(&mut dst.as_mut()[..8], mid as u64);
        LittleEndian::write_u64(&mut dst.as_mut()[8..16], len as u64);
        let mut data_buf = dst.split_off(16);
        data_buf.put(msg);
        dst.unsplit(data_buf);
        return Ok(())
    }
}