#[macro_use]
pub mod proto;

use std::collections::HashMap;
use std::sync::Arc;
use std::io;
use std::time::Duration;
use utils::future_parking_lot::{Mutex};
use utils::future_parking_lot::RwLock;
use std::thread;
use tcp;
use utils::time;
use utils::u8vec::*;
use futures::prelude::*;
use futures::future;
use bifrost_hasher::hash_str;
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
    services: Arc<RwLock<HashMap<u64, Arc<RPCService>>>>,
    pub address: String,
    pub server_id: u64
}

pub struct ClientPool {
    clients: Arc<Mutex<HashMap<String, Arc<RPCClient>>>>
}

fn encode_res(res: Result<Vec<u8>, RPCRequestError>) -> Vec<u8> {
    match res {
        Ok(vec) => {
            [0u8; 1].iter().cloned().chain(vec.into_iter()).collect()
        },
        Err(e) => {
            let err_id = match e {
                RPCRequestError::FunctionIdNotFound => 1u8,
                RPCRequestError::ServiceIdNotFound => 2u8,
                _ => 255u8
            };
            vec!(err_id)
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
        },
        Err(e) => Err(RPCError::IOError(e))
    }
}

impl Server {
    pub fn new(address: &String) -> Arc<Server> {
        Arc::new(Server {
            services: Arc::new(RwLock::new(HashMap::new())),
            address: address.clone(),
            server_id: hash_str(address)
        })
    }
    pub fn listen(server: &Arc<Server>) {
        let address = &server.address;
        let server = server.clone();
        tcp::server::Server::new(
            address,
            tcp::server::ServerCallback::new(
                move |data| {
                    let (svr_id, data) = extract_u64_head(data);
                    let svr_map = server.services.read();
                    let service = svr_map.get(&svr_id);
                    match service {
                        Some(ref service) => {
                            let service = service.clone();
                            Box::new(
                                service
                                    .dispatch(data)
                                    .then(|r|
                                        Ok(encode_res(r)))
                            )
                        },
                        None => Box::new(future::finished(encode_res(Err(RPCRequestError::ServiceIdNotFound))))
                    }
                }
            )
        );
    }

    pub fn listen_and_resume(server: &Arc<Server>) {
        let server = server.clone();
        thread::spawn(move|| {
            let server = server;
            Server::listen(&server);
        });
    }

    pub fn register_service_async<T>(&self, service_id: u64,  service: Arc<T>)
        -> impl Future<Item = (), Error = ()>
        where T: RPCService + Sized + 'static
    {
        let service = service.clone();
        if !DISABLE_SHORTCUT {
            let service_ptr = Arc::into_raw(service.clone()) as usize;
            service.register_shortcut_service(service_ptr, self.server_id, service_id);
        } else {
            println!("SERVICE SHORTCUT DISABLED");
        }
        self.services
            .clone()
            .write_async()
            .map(|mut svrs|
                svrs.insert(service_id, service))
            .map(|_| ())
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
    pub address: String
}

impl RPCClient {
    pub fn send(&self, svr_id: u64, data: Vec<u8>) -> Result<Vec<u8>, RPCError> {
        decode_res(self.client.lock().send(prepend_u64(svr_id, data)))
    }
    pub fn send_async(&self, svr_id: u64, data: Vec<u8>)
        -> impl Future<Item = Vec<u8>, Error = RPCError>
    {
        self.client
            .lock_async()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))
            .then(move |mut cr|
                cr.map(|c|
                    c.send_async(prepend_u64(svr_id, data))))
            .then(move |res|
                future::result(decode_res(res)))
    }
    #[async]
    pub fn new_async(addr: String) -> io::Result<Arc<RPCClient>> {
        let client = await!(tcp::client::Client::connect_async(addr.clone()))?;
        Ok(Arc::new(RPCClient {
            server_id: client.server_id,
            client: Mutex::new(client),
            address: addr
        }))
    }

    pub fn with_timeout_async(addr: String, timeout: Duration)
                              -> impl Future<Item = Arc<RPCClient>, Error = io::Error>
    {
        tcp::client::Client::connect_with_timeout_async(
            addr.clone(),
            timeout
        ).map(|client|
            Arc::new(RPCClient {
                server_id: client.server_id,
                client: Mutex::new(client),
                address: addr.clone()
            })
        )
    }
}

impl ClientPool {
    pub fn new() -> ClientPool {
        ClientPool {
            clients: Arc::new(Mutex::new(HashMap::new()))
        }
    }

    pub fn get(&self, addr: String) -> impl Future<Item = Arc<RPCClient>, Error = io::Error> {
        self.clients
            .clone()
            .lock_async()
            .map_err(|_| io::Error::from(io::ErrorKind::Other))
            .and_then(move |mut clients|
                future::result(clients.get(&addr)
                    .cloned()
                    .ok_or(Err(())))
                    .or_else(|_| RPCClient::new_async(addr)
                        .map(|client| {
                            clients.insert(addr, client.clone());
                            return client;
                        })
                    )
            )
    }
}