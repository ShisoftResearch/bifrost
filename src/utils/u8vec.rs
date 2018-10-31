use byteorder::{ByteOrder, LittleEndian};

pub fn prepend_u64(num: u64, vec: Vec<u8>) -> Vec<u8> {
    let mut s_id_vec = [0u8; 8].to_vec();
    LittleEndian::write_u64(&mut s_id_vec, num);
    let data_iter = s_id_vec.into_iter().chain(vec.into_iter());
    data_iter.collect()
}

pub fn extract_u64_head(vec: Vec<u8>) -> (u64, Vec<u8>) {
    let num = LittleEndian::read_u64(&vec);
    let vec: Vec<u8> = vec.into_iter().skip(8).collect();
    (num, vec)
}
