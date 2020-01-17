use crate::tcp::server::{TcpReq, TcpRes};
use crate::utils::rwlock::*;
use bifrost_hasher::hash_str;
use bytes::BytesMut;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};

trait TcpCallbackFunc = Fn(TcpReq) -> TcpRes;
trait TcpCallbackFuncShareable = TcpCallbackFunc + Send + Sync;

lazy_static! {
    pub static ref TCP_CALLBACKS: RwLock<BTreeMap<u64, Box<dyn TcpCallbackFuncShareable>>> =
        RwLock::new(BTreeMap::new());
}

pub async fn register_server(server_address: &String, callback: Box<dyn TcpCallbackFuncShareable>) {
    let server_id = hash_str(server_address);
    let mut servers_cbs = TCP_CALLBACKS.write().await;
    servers_cbs.insert(server_id, callback);
}

pub async fn call(server_id: u64, data: TcpReq) -> Result<BytesMut> {
    let server_cbs = TCP_CALLBACKS.read().await;
    match server_cbs.get(&server_id) {
        Some(c) => Ok(c(data).await),
        _ => Err(Error::new(
            ErrorKind::Other,
            "Cannot found callback for shortcut",
        )),
    }
}

pub async fn is_local(server_id: u64) -> bool {
    let cbs = TCP_CALLBACKS.read().await;
    cbs.contains_key(&server_id)
}
