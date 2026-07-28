use crate::tcp::server::{TcpReq, TcpRes};
use bifrost_hasher::hash_str;
use bytes::BytesMut;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

trait TcpCallbackFunc = Fn(TcpReq) -> TcpRes;
trait TcpCallbackFuncShareable = TcpCallbackFunc + Send + Sync;

struct CallbackRegistration {
    token: ShortcutToken,
    callback: Arc<dyn TcpCallbackFuncShareable>,
}

lazy_static! {
    static ref TCP_CALLBACKS: RwLock<BTreeMap<u64, CallbackRegistration>> =
        RwLock::new(BTreeMap::new());
}

#[derive(Clone)]
pub struct ShortcutToken(Arc<()>);

impl ShortcutToken {
    fn new() -> Self {
        Self(Arc::new(()))
    }

    pub(crate) fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub struct ShortcutRegistration {
    server_id: u64,
    token: ShortcutToken,
}

impl ShortcutRegistration {
    pub fn token(&self) -> ShortcutToken {
        self.token.clone()
    }
}

impl Drop for ShortcutRegistration {
    fn drop(&mut self) {
        let removed = {
            let mut callbacks = TCP_CALLBACKS.write();
            if callbacks
                .get(&self.server_id)
                .map(|registration| registration.token.is_same(&self.token))
                == Some(true)
            {
                callbacks.remove(&self.server_id)
            } else {
                None
            }
        };
        drop(removed);
    }
}

pub async fn register_server(
    server_address: &String,
    callback: &Arc<dyn TcpCallbackFuncShareable>,
) -> ShortcutRegistration {
    let server_id = hash_str(server_address);
    let token = ShortcutToken::new();
    let replaced = {
        let mut callbacks = TCP_CALLBACKS.write();
        callbacks.insert(
            server_id,
            CallbackRegistration {
                token: token.clone(),
                callback: callback.clone(),
            },
        )
    };
    drop(replaced);
    ShortcutRegistration { server_id, token }
}

pub async fn call(server_id: u64, data: TcpReq) -> Result<BytesMut> {
    let callback = TCP_CALLBACKS
        .read()
        .get(&server_id)
        .map(|registration| registration.callback.clone());
    match callback {
        Some(callback) => Ok(callback(data).await),
        _ => Err(Error::new(
            ErrorKind::Other,
            "Cannot found callback for shortcut",
        )),
    }
}

pub async fn is_local(server_id: u64) -> bool {
    TCP_CALLBACKS.read().contains_key(&server_id)
}

pub(crate) async fn registration_token(server_id: u64) -> Option<ShortcutToken> {
    TCP_CALLBACKS
        .read()
        .get(&server_id)
        .map(|registration| registration.token.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test]
    async fn stale_registration_drop_does_not_remove_replacement() {
        let address = "shortcut-generation-test".to_string();
        let callback: Arc<dyn TcpCallbackFuncShareable> =
            Arc::new(|_request: TcpReq| async { BytesMut::new() }.boxed());

        let older = register_server(&address, &callback).await;
        let newer = register_server(&address, &callback).await;

        drop(older);
        assert!(is_local(hash_str(&address)).await);

        drop(newer);
        assert!(!is_local(hash_str(&address)).await);
    }
}
