#[macro_use]
pub mod proto;

use bifrost_hasher::hash_str;
use futures::prelude::*;
use futures::{future, Future};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use crate::utils::rwlock::RwLock;
use crate::utils::mutex::Mutex;
use crate::{tcp, DISABLE_SHORTCUT};
use byteorder::{LittleEndian, ByteOrder};
use bytes::{BufMut, Buf, BytesMut};
use futures::future::{err, BoxFuture};
use std::pin::Pin;

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
    fn register_shortcut_service(&self, service_ptr: usize, server_id: u64, service_id: u64);
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
            BytesMut::from([err_id])
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
        tcp::server::Server::new(
            address,
            move |mut data| {
                async {
                    let svr_id = LittleEndian::read_u64(data.as_ref());
                    data.advance(8);
                    let svr_map = server.services.read().await;
                    let service = svr_map.get(&svr_id);
                    match service {
                        Some(ref service) => {
                            let svr_Res = service.dispatch(data).await;
                            encode_res(svr_Res)
                        }
                        None => encode_res(Err(
                            RPCRequestError::ServiceIdNotFound, 
                        )),
                    }
                }
            },
        );
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
    pub fn register_service<T>(&self, service_id: u64, service: &Arc<T>)
    where
        T: RPCService + Sized + 'static,
    {
        let service = service.clone();
        if !DISABLE_SHORTCUT {
            let service_ptr = Arc::into_raw(service.clone()) as usize;
            service.register_shortcut_service(service_ptr, self.server_id, service_id);
        } else {
            println!("SERVICE SHORTCUT DISABLED");
        }
        self.services.write().insert(service_id, service);
    }

    pub fn remove_service(&self, service_id: u64) {
        self.services.write().remove(&service_id);
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

impl RPCClient {
    pub async fn send_async(
        self: Pin<&Self>,
        svr_id: u64,
        data: BytesMut,
    ) -> Result<BytesMut, RPCError> {
        let mut client = self.client.lock().await;
        let res = client.send(prepend_u64(svr_id, data)).await;
        decode_res(res)
    }
    pub fn new_async(addr: String) -> impl Future<Item = Arc<RPCClient>, Error = io::Error> {
        tcp::client::Client::connect_async(addr.clone()).map(|client| {
            Arc::new(RPCClient {
                server_id: client.server_id,
                client: Mutex::new(client),
                address: addr,
            })
        })
    }

    pub fn with_timeout(
        addr: String,
        timeout: Duration,
    ) -> impl Future<Item = Arc<RPCClient>, Error = io::Error> {
        tcp::client::Client::connect_with_timeout_async(addr.clone(), timeout).map(|client| {
            Arc::new(RPCClient {
                server_id: client.server_id,
                client: Mutex::new(client),
                address: addr,
            })
        })
    }
}

impl ClientPool {
    pub fn new() -> ClientPool {
        ClientPool {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_async(
        &self,
        addr: &String,
    ) -> impl Future<Item = Arc<RPCClient>, Error = io::Error> {
        let addr_clone = addr.clone();
        let server_id = hash_str(addr);
        self.get_by_id_async(server_id, move |_| addr_clone)
    }

    pub fn get(&self, addr: &String) -> Result<Arc<RPCClient>, io::Error> {
        let addr_clone = addr.clone();
        let server_id = hash_str(addr);
        self.get_by_id(server_id, move |_| addr_clone)
    }

    pub fn get_by_id<F>(&self, server_id: u64, addr_fn: F) -> Result<Arc<RPCClient>, io::Error>
    where
        F: FnOnce(u64) -> String,
    {
        let mut clients = self.clients.lock();
        if clients.contains_key(&server_id) {
            let client = clients.get(&server_id).unwrap().clone();
            Ok(client)
        } else {
            let client = RPCClient::new_async(addr_fn(server_id)).wait()?;
            clients.insert(server_id, client.clone());
            return Ok(client.clone());
        }
    }

    pub fn get_by_id_async<F>(
        &self,
        server_id: u64,
        addr_fn: F,
    ) -> impl Future<Item = Arc<RPCClient>, Error = io::Error>
    where
        F: FnOnce(u64) -> String,
    {
        self.clients
            .lock_async()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))
            .and_then(
                move |mut clients| -> Box<Future<Item = Arc<RPCClient>, Error = io::Error>> {
                    if clients.contains_key(&server_id) {
                        let client = clients.get(&server_id).unwrap().clone();
                        box future::ok(client)
                    } else {
                        box RPCClient::new_async(addr_fn(server_id)).map(move |client| {
                            clients.insert(server_id, client.clone());
                            return client.clone();
                        })
                    }
                },
            )
    }
}
