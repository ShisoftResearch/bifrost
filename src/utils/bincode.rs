use bincode;
use serde;

pub fn serialize<T>(obj: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    match bincode::serialize(obj) {
        Ok(data) => data,
        Err(e) => panic!("Cannot serialize: {:?}", e),
    }
}

pub fn deserialize<'a, T>(data: &'a [u8]) -> T
where
    T: serde::Deserialize<'a>,
{
    match bincode::deserialize(data) {
        Ok(data) => data,
        Err(e) => panic!("Cannot deserialize: {:?}, data len: {}", e, data.len()),
    }
}
