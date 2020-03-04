#[macro_use]
pub mod proto;

use crate::tcp::client::Client;
use crate::utils::mutex::Mutex;
use crate::utils::rwlock::RwLock;
use crate::{tcp, DISABLE_SHORTCUT};
use bifrost_hasher::hash_str;
use byteorder::{ByteOrder, LittleEndian};
use bytes::buf::BufExt;
use bytes::{Buf, BufMut, BytesMut};
use futures::future::{err, BoxFuture};
use futures::prelude::*;
use futures::{future, Future};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::thread;

lazy_static! {
    pub static ref DEFAULT_CLIENT_POOL: ClientPool = ClientPool::new();
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RPCRequestError {
    FunctionIdNotFound,
    ServiceIdNotFound,
    Other,
}

#[derive(Debug)]
pub enum RPCError {
    IOError(io::Error),
    RequestError(RPCRequestError),
}

pub trait RPCService: Sync + Send {
    fn dispatch(&self, data: BytesMut) -> BoxFuture<Result<BytesMut, RPCRequestError>>;
    fn register_shortcut_service(&self, service_ptr: usize, server_id: u64, service_id: u64)
        -> ::std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;
}

pub struct Server {
    services: RwLock<HashMap<u64, Arc<dyn RPCService>>>,
    pub address: String,
    pub server_id: u64,
}

unsafe impl Sync for Server {}

pub struct ClientPool {
    clients: Arc<Mutex<HashMap<u64, Arc<RPCClient>>>>,
}

fn encode_res(res: Result<BytesMut, RPCRequestError>) -> BytesMut {
    match res {
        Ok(buffer) => [0u8; 1].iter().cloned().chain(buffer.into_iter()).collect(),
        Err(e) => {
            let err_id = match e {
                RPCRequestError::FunctionIdNotFound => 1u8,
                RPCRequestError::ServiceIdNotFound => 2u8,
                _ => 255u8,
            };
            BytesMut::from(&[err_id][..])
        }
    }
}

fn decode_res(res: io::Result<BytesMut>) -> Result<BytesMut, RPCError> {
    match res {
        Ok(mut res) => {
            if res[0] == 0u8 {
                res.advance(1);
                Ok(res)
            } else {
                match res[0] {
                    1u8 => Err(RPCError::RequestError(RPCRequestError::FunctionIdNotFound)),
                    2u8 => Err(RPCError::RequestError(RPCRequestError::ServiceIdNotFound)),
                    _ => Err(RPCError::RequestError(RPCRequestError::Other)),
                }
            }
        }
        Err(e) => Err(RPCError::IOError(e)),
    }
}

pub fn read_u64_head(mut data: BytesMut) -> (u64, BytesMut) {
    let num = LittleEndian::read_u64(data.as_ref());
    data.advance_mut(8);
    (num, data)
}

impl Server {
    pub fn new(address: &String) -> Arc<Server> {
        Arc::new(Server {
            services: RwLock::new(HashMap::new()),
            address: address.clone(),
            server_id: hash_str(address),
        })
    }
    pub fn listen(server: &Arc<Server>) {
        let address = &server.address;
        let server = server.clone();
        tcp::server::Server::new(address, box move |mut data| {
            async move {
                let (svr_id, data) = read_u64_head(data);
                let svr_map = server.services.read().await;
                let service = svr_map.get(&svr_id);
                match service {
                    Some(ref service) => {
                        let svr_res = service.dispatch(data).await;
                        encode_res(svr_res)
                    }
                    None => encode_res(Err(RPCRequestError::ServiceIdNotFound)),
                }
            }
            .boxed()
        });
    }
    pub fn listen_and_resume(server: &Arc<Server>) {
        let server = server.clone();
        thread::Builder::new()
            .name(format!("RPC Server - {}", server.address))
            .spawn(move || {
                let server = server;
                Server::listen(&server);
            });
    }
    pub async fn register_service<T>(&self, service_id: u64, service: &Arc<T>)
    where
        T: RPCService + Sized + 'static,
    {
        let service = service.clone();
        if !DISABLE_SHORTCUT {
            let service_ptr = Arc::into_raw(service.clone()) as usize;
            service.register_shortcut_service(service_ptr, self.server_id, service_id).await;
        } else {
            println!("SERVICE SHORTCUT DISABLED");
        }
        self.services.write().await.insert(service_id, service);
    }

    pub async fn remove_service(&self, service_id: u64) {
        self.services.write().await.remove(&service_id);
    }
    pub fn address(&self) -> &String {
        &self.address
    }
}

pub struct RPCClient {
    client: Mutex<tcp::client::Client>,
    pub server_id: u64,
    pub address: String,
}

pub fn prepend_u64(num: u64, data: BytesMut) -> BytesMut {
    let mut bytes = BytesMut::new();
    bytes.reserve(8);
    LittleEndian::write_u64(bytes.as_mut(), num);
    bytes.unsplit(data);
    bytes
}

impl RPCClient {
    pub async fn send_async(
        self: Pin<&Self>,
        svr_id: u64,
        data: BytesMut,
    ) -> Result<BytesMut, RPCError> {
        let mut client = self.client.lock().await;
        let bytes = prepend_u64(svr_id, data);
        decode_res(Client::send_msg(Pin::new(&mut *client), bytes).await)
    }
    pub async fn new_async(addr: &String) -> io::Result<RPCClient> {
        let client = tcp::client::Client::connect(addr).await?;
        Ok(RPCClient {
            server_id: client.server_id,
            client: Mutex::new(client),
            address: addr.clone(),
        })
    }
}

impl ClientPool {
    pub fn new() -> ClientPool {
        ClientPool {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get(&self, addr: &String) -> io::Result<Arc<RPCClient>> {
        let addr_clone = addr.clone();
        let server_id = hash_str(addr);
        self.get_by_id(server_id, move |_| addr_clone).await
    }

    pub async fn get_by_id<F>(&self, server_id: u64, addr_fn: F) -> io::Result<Arc<RPCClient>>
    where
        F: FnOnce(u64) -> String,
    {
        let mut clients = self.clients.lock().await;
        if clients.contains_key(&server_id) {
            let client = clients.get(&server_id).unwrap().clone();
            Ok(client)
        } else {
            let mut client = Arc::new(RPCClient::new_async(&addr_fn(server_id)).await?);
            clients.insert(server_id, client.clone());
            Ok(client)
        }
    }
}
