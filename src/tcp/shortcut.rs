use bifrost_hasher::hash_str;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use crate::tcp::server::{TcpReq, TcpRes};
use crate::utils::rwlock::*;
use std::future::Future;
use futures::FutureExt;
use std::pin::Pin;

type FnRT = Pin<Box<dyn Future<Output = TcpRes>>>;

lazy_static! {
    pub static ref TCP_CALLBACKS: RwLock<BTreeMap<u64, Box<dyn Fn(TcpReq) -> FnRT>>> = RwLock::new(BTreeMap::new());
}

pub async fn register_server<
    F: Future<Output = TcpRes> + Send + 'static,
    C: Fn(TcpReq) -> F + Send + 'static
>(server_address: &String, callback: C)
{
    let server_id = hash_str(server_address);
    let mut servers_cbs = TCP_CALLBACKS.write().await;
    servers_cbs.insert(server_id, Box::new(move |t| callback(t).boxed()));
}

pub async fn call(server_id: u64, data: TcpReq) -> Result<TcpRes> {
    let server_cbs = TCP_CALLBACKS.read().await;
        match server_cbs.get(&server_id) { 
            Some(c) => {
                Ok(c(data).await)
            },
            _ => Err(Error::new(
                ErrorKind::Other,
                "Cannot found callback for shortcut",
            )),
        }
}

pub fn is_local(server_id: u64) -> bool {
    let cbs = TCP_CALLBACKS.read();
    cbs.contains_key(&server_id)
}
