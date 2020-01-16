use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, BytesMut};
use std::{io, str};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BytesCodec;
const BYTE_LEN: usize = 9;

impl Decoder for BytesCodec {
    type Item = BytesMut;
    type Error = ();

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let buf_len = src.len();
        if buf_len >= BYTE_LEN {
            let len = LittleEndian::read_u64(&src[0..BYTE_LEN]) as usize;
            src.advance(BYTE_LEN);
            if buf_len >= len {
                let data_bytes = src.split_to(len);
                return Ok(Some(data_bytes));
            }
        }
        return Ok(None);
    }
}

impl Encoder for BytesCodec {
    type Item = BytesMut;
    type Error = ();

    fn encode(&mut self, msg: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let len = msg.len();
        dst.reserve(8 + len);
        LittleEndian::write_u64(&mut dst[0..8], len as u64);
        let mut data_buf = dst.split_off(16);
        data_buf.put(msg);
        dst.unsplit(data_buf);
        return Ok(());
    }
}

impl Default for BytesCodec {
    fn default() -> Self {
        Self
    }
}

