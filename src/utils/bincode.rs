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
    match bincode::deserialize(data) {
        Ok(obj) => Some(obj),
        Err(e) => {
            warn!("Error on decoding data for type '{}'", unsafe { std::intrinsics::type_name::<T>() });
            None
        }
    }
}
