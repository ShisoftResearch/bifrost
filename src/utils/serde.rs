use serde;
use serde_cbor::{from_slice, to_vec};
use std::backtrace;

#[cfg(not(debug_assertions))]
pub fn serialize<T>(obj: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    match to_vec(obj) {
        Ok(data) => data,
        Err(e) => panic!("Cannot serialize: {:?}", e),
    }
}

#[cfg(not(debug_assertions))]
pub fn deserialize<'a, T>(data: &'a [u8]) -> Option<T>
where
    T: serde::Deserialize<'a>,
{
    match from_slice(data) {
        Ok(obj) => Some(obj),
        Err(e) => {
            warn!(
                "Error on decoding data for type '{}', {}",
                unsafe { std::intrinsics::type_name::<T>() },
                e
            );
            None
        }
    }
}

#[cfg(debug_assertions)]
pub fn serialize<T>(obj: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    match serde_json::to_vec(obj) {
        Ok(data) => data,
        Err(e) => panic!("Cannot serialize: {:?}", e),
    }
}

#[cfg(debug_assertions)]
pub fn deserialize<'a, T>(data: &'a [u8]) -> Option<T>
where
    T: serde::Deserialize<'a>,
{
    let type_name = unsafe { std::intrinsics::type_name::<T>() };
    match serde_json::from_slice(data) {
        Ok(obj) => Some(obj),
        Err(e) => {
            warn!(
                "Error on decoding data for type '{}', {}, json: {}",
                type_name,
                e,
                String::from_utf8_lossy(data)
            );
            None
        }
    }
}
