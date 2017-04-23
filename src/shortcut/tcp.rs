use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use std::io::{Error, ErrorKind};
use bifrost_hasher::hash_str;
use parking_lot::RwLock;
use tcp::server::ServerCallback;
use tokio_timer::Timer;
use futures::{future, BoxFuture};

lazy_static! {
    pub static ref TCP_CALLBACKS: RwLock<BTreeMap<u64, Arc<ServerCallback>>> = RwLock::new(BTreeMap::new());
}

pub fn register_server(server_address: &String, callback: &Arc<ServerCallback>) {
    let mut servers_cbs = TCP_CALLBACKS.write();
    let server_id = hash_str(server_address);
    servers_cbs.insert(server_id, callback.clone());
}

pub fn call(server_id: u64, data: Vec<u8>) -> BoxFuture<Vec<u8>, Error> {
    let server_cbs = TCP_CALLBACKS.read();
    Box::new(match server_cbs.get(&server_id) {
        Some(callback) => {
            future::finished(callback(data))
        },
        _ => {
            future::err(Error::new(ErrorKind::Other, "cannot found callback"))
        }
    })
}

pub fn is_local(server_id: u64) -> bool {
    let cbs = TCP_CALLBACKS.read();
    cbs.contains_key(&server_id)
}