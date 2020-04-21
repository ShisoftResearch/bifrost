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

pub fn deserialize<'a, T>(data: &'a [u8]) -> Option<T>
where
    T: serde::Deserialize<'a>,
{
    bincode::deserialize(data).ok()
}
