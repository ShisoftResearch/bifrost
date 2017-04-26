use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use std::io::{Error, ErrorKind, Result};
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

pub fn call_async(server_id: u64, data: Vec<u8>) -> BoxFuture<Vec<u8>, Error> {
    Box::new(match call(server_id, data) {
        Ok(data) => future::finished(data),
        Err(e) => future::err(e)
    })
}

pub fn call(server_id: u64, data: Vec<u8>) -> Result<Vec<u8>> {
    let callback = {
        let server_cbs = TCP_CALLBACKS.read();
        match server_cbs.get(&server_id) {
            Some(c) => Some(c.clone()),
            _ => None
        }
    };
    match callback {
        Some(callback) => Ok(callback(data)),
        None => Err(Error::new(ErrorKind::Other, "Cannot found callback for shortcut"))
    }
}

pub fn is_local(server_id: u64) -> bool {
    let cbs = TCP_CALLBACKS.read();
    cbs.contains_key(&server_id)
}