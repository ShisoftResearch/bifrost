#[macro_use]
pub mod proto;

use std::collections::HashMap;
use std::sync::Arc;
use std::io;
use std::time::Duration;
use parking_lot::{Mutex, RwLock};
use std::thread;
use bincode::{SizeLimit, serde as bincode};
use tcp;
use utils::time;
use utils::u8vec::*;

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
    fn dispatch(&self, data: Vec<u8>) -> Result<Vec<u8>, RPCRequestError>;
}

pub struct Server {
    services: RwLock<HashMap<u64, Arc<RPCService>>>,
    pub address: RwLock<Option<String>>,
}

pub struct ClientPool {
    clients: Mutex<HashMap<String, Arc<RPCSyncClient>>>
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
    pub fn new(services: Vec<(u64, Arc<RPCService>)>) -> Arc<Server> {
        let mut svr_map = HashMap::with_capacity(services.len());
        for (svr_id, svr) in services {
            svr_map.insert(svr_id, svr.clone());
        }
        Arc::new(Server {
            services: RwLock::new(svr_map),
            address: RwLock::new(None),
        })
    }
    fn reset_address(&self, addr: &String) {
        let mut server_address = self.address.write();
        *server_address = Some(addr.clone());
    }
    pub fn listen(server: Arc<Server>, addr: &String) {
        server.reset_address(addr);
        let server = server.clone();
        tcp::server::Server::new(addr, Box::new(move |data| {
            let (svr_id, data) = extract_u64_head(data);
            let t = time::get_time();
            let svr_map = server.services.read();
            let service = svr_map.get(&svr_id);
            let res = match service {
                Some(service) => {
                    encode_res(service.dispatch(data))
                },
                None => encode_res(Err(RPCRequestError::ServiceIdNotFound) as Result<Vec<u8>, RPCRequestError>)
            };
            //println!("SVR RPC: {} - {}ms", svr_id, time::get_time() - t);
            res
        }));
    }
    pub fn listen_and_resume(server: Arc<Server>, addr: &String) {
        server.reset_address(&addr);
        let addr = addr.clone();
        let server = server.clone();
        thread::spawn(move|| {
            Server::listen(server, &addr);
        });
    }
    pub fn append_service(&self, service_id: u64,  service: Arc<RPCService>) {
        self.services.write().insert(service_id, service);
    }
    pub fn remove_service(&self, service_id: u64) {
        self.services.write().remove(&service_id);
    }
    pub fn address(&self) -> Option<String> {
        self.address.read().clone()
    }
}

pub struct RPCSyncClient {
    client: Mutex<tcp::client::Client>,
    pub address: String
}

impl RPCSyncClient {
    pub fn send(&self, svr_id: u64, data: Vec<u8>) -> Result<Vec<u8>, RPCError> {
        decode_res(self.client.lock().send(prepend_u64(svr_id, data)))
    }

    pub fn new(addr: &String) -> io::Result<Arc<RPCSyncClient>> {
        Ok(Arc::new(RPCSyncClient {
            client: Mutex::new(tcp::client::Client::connect(addr)?),
            address: addr.clone()
        }))
    }
    pub fn with_timeout(addr: &String, timeout: Duration) -> io::Result<Arc<RPCSyncClient>> {
        Ok(Arc::new(RPCSyncClient {
            client: Mutex::new(tcp::client::Client::connect_with_timeout(addr, timeout)?),
            address: addr.clone()
        }))
    }
}

impl ClientPool {
    pub fn new() -> ClientPool {
        ClientPool {
            clients: Mutex::new(HashMap::new())
        }
    }

    pub fn get(&self, addr: &String) -> io::Result<Arc<RPCSyncClient>> {
        let mut clients = self.clients.lock();
        if clients.contains_key(addr) {
            Ok(clients.get(addr).unwrap().clone())
        } else {
            let client = RPCSyncClient::new(addr);
            if let Ok(client) = client {
                clients.insert(addr.clone(), client.clone());
                Ok(client)
            } else {
                Err(client.err().unwrap())
            }
        }
    }
}