use bifrost_hasher::hash_str;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use crate::tcp::server::{TcpReq, TcpRes};
use futures_locks::RwLock;
use std::future::Future;

type FnRT = Box<dyn Future<Output = TcpRes>>;

lazy_static! {
    pub static ref TCP_CALLBACKS: RwLock<BTreeMap<u64, Box<Fn(TcpReq) -> FnRT>>> = RwLock::new(BTreeMap::new());
}

pub async fn register_server<
    F: Future<Output = TcpRes>,
    C: Fn(TcpReq) -> F
>(server_address: &String, callback: C)
{
    let server_id = hash_str(server_address);
    let callbacks_fut = TCP_CALLBACKS.write().await;
    let mut servers_cbs = callbacks_fut.await;
    servers_cbs.insert(server_id, box callback);
}

pub fn call(server_id: u64, data: TcpReq) -> Box<dyn Future<Output = Result<TcpRes>>> {
    box async move {
        let callbacks_fut = TCP_CALLBACKS.read().await;
        let server_cbs = callbacks_fut.await;
        match server_cbs.get(&server_id) {
            Some(c) => Ok(c(data).await),
            _ => Err(Error::new(
                ErrorKind::Other,
                "Cannot found callback for shortcut",
            )),
        }
    }
}

pub fn is_local(server_id: u64) -> bool {
    let cbs = TCP_CALLBACKS.read();
    cbs.contains_key(&server_id)
}
