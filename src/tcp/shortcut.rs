use bifrost_hasher::hash_str;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use crate::tcp::server::BoxedRPCFuture;
use futures_locks::RwLock;

lazy_static! {
    pub static ref TCP_CALLBACKS: RwLock<BTreeMap<u64, Box<Fn(Vec<u8>) -> BoxedRPCFuture>>> =
        RwLock::new(BTreeMap::new());
}

pub async fn register_server<C: Fn(Vec<u8>) -> BoxedRPCFuture>(server_address: &String) {
    let callback = C;
    let server_id = hash_str(server_address);
    let mut servers_cbs = TCP_CALLBACKS.write().await;
    servers_cbs.insert(server_id, box callback);
}

pub fn call(server_id: u64, data: Vec<u8>) -> BoxedRPCFuture {
    box async move {
        let server_cbs = TCP_CALLBACKS.read().await;
        match server_cbs.get(&server_id) {
            Some(c) => c(data).await,
            _ => Error::new(
                ErrorKind::Other,
                "Cannot found callback for shortcut",
            ),
        }
    }
}

pub fn is_local(server_id: u64) -> bool {
    let cbs = TCP_CALLBACKS.read();
    cbs.contains_key(&server_id)
}
