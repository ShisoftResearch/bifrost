use bifrost_hasher::hash_bytes;
use serde;

/// The single wire and storage codec of the stack: CBOR in every build profile.
///
/// Panics when the value cannot be encoded. Known unencodable values are 128-bit
/// integers (`u128` / `i128`) outside the supported CBOR integer range.
pub fn serialize<T>(obj: &T) -> Vec<u8>
where
    T: serde::Serialize,
{
    match serde_cbor::to_vec(obj) {
        Ok(data) => data,
        Err(e) => panic!("Cannot serialize: {:?}", e),
    }
}

pub fn deserialize<'a, T>(data: &'a [u8]) -> Option<T>
where
    T: serde::Deserialize<'a>,
{
    match serde_cbor::from_slice(data) {
        Ok(obj) => Some(obj),
        Err(e) => {
            warn!(
                "Error on decoding data for type '{}' ({} bytes), {}",
                std::any::type_name::<T>(),
                data.len(),
                e
            );
            None
        }
    }
}

/// Hashes the serialized bytes, so the result is stable across build profiles.
pub fn hash<T>(obj: &T) -> u64
where
    T: serde::Serialize,
{
    let data = serialize(obj);
    hash_bytes(data.as_slice())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::{BTreeMap, HashMap};

    fn round_trip<T>(value: &T) -> T
    where
        T: Serialize + for<'a> Deserialize<'a>,
    {
        let bytes = serialize(value);
        deserialize::<T>(&bytes).expect("value must decode")
    }

    #[test]
    fn non_finite_floats_round_trip() {
        let v: Vec<(u64, f64)> = vec![(1, f64::INFINITY), (2, f64::NEG_INFINITY), (3, 1.5)];
        assert_eq!(round_trip(&v), v);
        let nan = round_trip(&f64::NAN);
        assert!(nan.is_nan());
        let v32: Vec<f32> = vec![f32::INFINITY, f32::NEG_INFINITY, -0.25];
        assert_eq!(round_trip(&v32), v32);
        assert!(round_trip(&f32::NAN).is_nan());
    }

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
    struct StructKey {
        a: u32,
        b: String,
    }

    #[test]
    fn struct_and_tuple_map_keys_round_trip() {
        let mut by_struct = BTreeMap::new();
        by_struct.insert(
            StructKey {
                a: 1,
                b: "x".into(),
            },
            10u64,
        );
        by_struct.insert(
            StructKey {
                a: 2,
                b: "y".into(),
            },
            20u64,
        );
        assert_eq!(round_trip(&by_struct), by_struct);

        let mut by_tuple = HashMap::new();
        by_tuple.insert((1u64, 2u32), "a".to_string());
        by_tuple.insert((3u64, 4u32), "b".to_string());
        assert_eq!(round_trip(&by_tuple), by_tuple);
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind")]
    enum Tagged {
        Unit,
        Named { value: u64, label: String },
    }

    #[test]
    fn internally_tagged_enum_round_trips() {
        for v in [
            Tagged::Unit,
            Tagged::Named {
                value: u64::MAX,
                label: "l".into(),
            },
        ] {
            assert_eq!(round_trip(&v), v);
        }
    }

    #[test]
    fn json_value_round_trips() {
        let v = serde_json::json!({
            "a": [1, -2, 3.5, null, true],
            "big": u64::MAX,
            "nested": { "k": "v" },
        });
        assert_eq!(round_trip(&v), v);
    }

    #[test]
    fn hash_is_deterministic_for_the_cbor_bytes() {
        let obj = (7u64, "key".to_string());
        assert_eq!(hash(&obj), hash_bytes(&serde_cbor::to_vec(&obj).unwrap()));
    }

    #[test]
    fn undecodable_bytes_yield_none() {
        assert!(deserialize::<u64>(&[0xff, 0xff]).is_none());
        // JSON text is not CBOR: the old debug format must be refused, not guessed.
        assert!(deserialize::<Vec<u64>>(b"[1,2,3]").is_none());
    }

    /// Documents the codec limit: 128-bit integers beyond the 64-bit range cannot
    /// be encoded as CBOR (values that fit in 64 bits are written as plain
    /// integers), so no persisted or wire type may carry a full-range `u128` /
    /// `i128`. Such a value makes `serialize` panic rather than corrupt data.
    #[test]
    fn wide_u128_is_refused_by_the_codec() {
        let wide_u = u64::MAX as u128 + 1;
        let wide_i = -(u64::MAX as i128) - 2;
        assert!(serde_cbor::to_vec(&wide_u).is_err());
        assert!(serde_cbor::to_vec(&wide_i).is_err());
        assert!(std::panic::catch_unwind(|| serialize(&wide_u)).is_err());
        assert!(std::panic::catch_unwind(|| serialize(&wide_i)).is_err());
        // Values inside the 64-bit range still round-trip.
        assert_eq!(round_trip(&7u128), 7u128);
    }
}
