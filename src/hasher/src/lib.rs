use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

extern crate twox_hash;

pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = twox_hash::XxHash::default();
    hasher.write(bytes);
    hasher.finish()
}

pub fn hash_str (text: &String) -> u64 { // the same as the one in utils hash
    let text_bytes = text.as_bytes();
    hash_bytes(text_bytes)
}

pub fn hash_bytes_secondary(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::default();
    hasher.write(bytes);
    hasher.finish()
}