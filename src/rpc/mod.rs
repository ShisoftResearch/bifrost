#[macro_use]
pub mod proto;

use std::collections::HashMap;
use std::sync::Arc;
use byteorder::{ByteOrder, LittleEndian};
use tcp;

pub trait RPCService: Sync + Send {
    fn id(&self) -> u64;
    fn dispatch(&self, data: Vec<u8>) -> Vec<u8>;
}

pub struct Server {
    services: HashMap<u64, Arc<RPCService>>
}

impl Server {
    fn new(services: Vec<Arc<RPCService>>) -> Arc<Server> {
        let mut svr_map = HashMap::with_capacity(services.len());
        for svr in services {
            svr_map.insert(svr.id(), svr.clone());
        }
        Arc::new(Server {
            services: svr_map
        })
    }
    fn listen(server: Arc<Server>, addr: &String) {
        let server = server.clone();
        tcp::server::Server::new(addr, Box::new(move |data| {
            let svr_id = LittleEndian::read_u64(&data);
            let data: Vec<u8> = data.iter().skip(8).cloned().collect();//TODO avoid clone
            let service = server.services.get(&svr_id).unwrap();
            service.dispatch(data)
        }));
    }
}