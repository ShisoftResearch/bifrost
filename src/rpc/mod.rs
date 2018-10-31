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
use tcp;
use utils::async_locks::{Mutex, RwLock};
use utils::time;
use utils::u8vec::*;
use DISABLE_SHORTCUT;

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
    fn dispatch(&self, data: Vec<u8>) -> Box<Future<Item = Vec<u8>, Error = RPCRequestError>>;
    fn register_shortcut_service(&self, service_ptr: usize, server_id: u64, service_id: u64);
}

pub struct Server {
    services: RwLock<HashMap<u64, Arc<RPCService>>>,
    pub address: String,
    pub server_id: u64,
}

pub struct ClientPool {
    clients: Arc<Mutex<HashMap<u64, Arc<RPCClient>>>>,
}

fn encode_res(res: Result<Vec<u8>, RPCRequestError>) -> Vec<u8> {
    match res {
        Ok(vec) => [0u8; 1].iter().cloned().chain(vec.into_iter()).collect(),
        Err(e) => {
            let err_id = match e {
                RPCRequestError::FunctionIdNotFound => 1u8,
                RPCRequestError::ServiceIdNotFound => 2u8,
                _ => 255u8,
            };
            vec![err_id]
        }
    }
}

fn decode_res(res: io::Result<Vec<u8>>) -> Result<Vec<u8>, RPCError> {
    match res {
        Ok(res) => {
            if res[0] == 0u8 {
                Ok(res.into_iter().skip(1).collect())
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
            tcp::server::ServerCallback::new(move |data| {
                let (svr_id, data) = extract_u64_head(data);
                let svr_map = server.services.read();
                let service = svr_map.get(&svr_id);
                match service {
                    Some(ref service) => {
                        let service = service.clone();
                        Box::new(service.dispatch(data).then(|r| Ok(encode_res(r))))
                    }
                    None => Box::new(future::finished(encode_res(Err(
                        RPCRequestError::ServiceIdNotFound,
                    )))),
                }
            }),
        );
    }
    pub fn listen_and_resume(server: &Arc<Server>) {
        let server = server.clone();
        thread::spawn(move || {
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
    pub fn send_async(
        &self,
        svr_id: u64,
        data: Vec<u8>,
    ) -> impl Future<Item = Vec<u8>, Error = RPCError> {
        self.client
            .lock_async()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))
            .and_then(move |client| client.send_async(prepend_u64(svr_id, data)))
            .then(move |res| decode_res(res))
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
        let server_id = hash_str(&addr);
        let mut clients = self.clients.lock();
        if clients.contains_key(&server_id) {
            let client = clients.get(&server_id).unwrap().clone();
            Ok(client)
        } else {
            let client = RPCClient::new_async(addr.clone()).wait()?;
            clients.insert(server_id, client.clone());
            return Ok(client.clone());
        }
    }

    fn get_by_id_async<F>(
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
