use std::boxed::FnBox;
use std::collections::HashMap;
use std::sync::{RwLock};

lazy_static! {
    pub static ref SUBSCRIPTIONS: RwLock<HashMap<(u64, u64, u64), Vec<Box<FnBox(Vec<u8>) + Send + Sync>>>> = RwLock::new(HashMap::new());
}
