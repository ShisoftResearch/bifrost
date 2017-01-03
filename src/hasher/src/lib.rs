use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

pub fn hash_str (text: String) -> u64 { // the same as the one in utils hash
    let mut hasher = DefaultHasher::default();
    let text_bytes = text.into_bytes();
    let text_bytes = text_bytes.as_slice();
    hasher.write(&text_bytes);
    hasher.finish()
}