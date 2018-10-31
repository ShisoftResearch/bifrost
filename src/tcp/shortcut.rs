use bifrost_hasher::hash_str;
use futures::{future, Future};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use tcp::server::ServerCallback;

lazy_static! {
    pub static ref TCP_CALLBACKS: RwLock<BTreeMap<u64, Arc<ServerCallback>>> =
        RwLock::new(BTreeMap::new());
}

pub fn register_server(server_address: &String, callback: &Arc<ServerCallback>) {
    let mut servers_cbs = TCP_CALLBACKS.write();
    let server_id = hash_str(server_address);
    servers_cbs.insert(server_id, callback.clone());
}

pub fn call(server_id: u64, data: Vec<u8>) -> Box<Future<Item = Vec<u8>, Error = Error>> {
    let callback = {
        let server_cbs = TCP_CALLBACKS.read();
        match server_cbs.get(&server_id) {
            Some(c) => Some(c.clone()),
            _ => None,
        }
    };
    match callback {
        Some(callback) => return callback.call(data),
        None => {
            return Box::new(future::failed(Error::new(
                ErrorKind::Other,
                "Cannot found callback for shortcut",
            )))
        }
    }
}

pub fn is_local(server_id: u64) -> bool {
    let cbs = TCP_CALLBACKS.read();
    cbs.contains_key(&server_id)
}
