#[macro_use]
pub mod proto;

use std::collections::HashMap;
use std::sync::Arc;
use std::io;
use std::time::Duration;
use bincode::{SizeLimit, serde as bincode};
use tcp;
use utils::u8vec::*;

#[derive(Serialize, Deserialize, Debug)]
pub enum RPCRequestError {
    FunctionIdNotFound,
    ServiceIdNotFound,
    Other,
}

pub enum RPCError {
    IOError(io::Error),
    RequestError(RPCRequestError),
}

pub trait RPCService: Sync + Send {
    fn id(&self) -> u64;
    fn dispatch(&self, data: Vec<u8>) -> Result<Vec<u8>, RPCRequestError>;
}

pub struct Server {
    services: HashMap<u64, Arc<RPCService>>
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
    pub fn new(services: Vec<Arc<RPCService>>) -> Arc<Server> {
        let mut svr_map = HashMap::with_capacity(services.len());
        for svr in services {
            svr_map.insert(svr.id(), svr.clone());
        }
        Arc::new(Server {
            services: svr_map
        })
    }
    pub fn listen(server: Arc<Server>, addr: &String) {
        let server = server.clone();
        tcp::server::Server::new(addr, Box::new(move |data| {
            let (svr_id, data) = extract_u64_head(data);
            let service = server.services.get(&svr_id);
            match service {
                Some(service) => {
                    encode_res(service.dispatch(data))
                },
                None => encode_res(Err(RPCRequestError::ServiceIdNotFound) as Result<Vec<u8>, RPCRequestError>)
            }
        }));
    }
    pub fn append_service(&mut self,  service: Arc<RPCService>) {
        self.services.insert(service.id(), service);
    }
    pub fn remove_service(&mut self, service_id: u64) {
        self.services.remove(&service_id);
    }
}

pub struct RPCSyncClient {
    client: tcp::client::Client,
    pub address: String
}

impl RPCSyncClient {
    pub fn send(&mut self, svr_id: u64, data: Vec<u8>) -> Result<Vec<u8>, RPCError> {
        decode_res(self.client.send(prepend_u64(svr_id, data)))
    }

    pub fn new(addr: &String) -> io::Result<Arc<RPCSyncClient>> {
        Ok(Arc::new(RPCSyncClient {
            client: tcp::client::Client::connect(addr)?,
            address: addr.clone()
        }))
    }
    pub fn with_timeout(addr: &String, timeout: Duration) -> io::Result<Arc<RPCSyncClient>> {
        Ok(Arc::new(RPCSyncClient {
            client: tcp::client::Client::connect_with_timeout(addr, timeout)?,
            address: addr.clone()
        }))
    }
}