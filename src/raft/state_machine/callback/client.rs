use super::*;
use futures::future;
use futures::prelude::*;
use parking_lot::RwLock;
use rpc::Server;
use std::boxed::FnBox;
use std::collections::HashMap;
use std::sync::Arc;
use utils::time::get_time;

pub struct SubscriptionService {
    pub subs: RwLock<HashMap<SubKey, Vec<(Box<Fn(Vec<u8>) + Send + Sync>, u64)>>>,
    pub server_address: String,
    pub session_id: u64,
}

impl Service for SubscriptionService {
    fn notify(&self, key: SubKey, data: Vec<u8>) -> Box<Future<Item = (), Error = ()>> {
        let subs = self.subs.read();
        if let Some(subs) = subs.get(&key) {
            for &(ref fun, _) in subs {
                fun(data.clone());
            }
        }
        box future::finished(())
    }
}
dispatch_rpc_service_functions!(SubscriptionService);

impl SubscriptionService {
    pub fn initialize(server: &Arc<Server>) -> Arc<SubscriptionService> {
        let service = Arc::new(SubscriptionService {
            subs: RwLock::new(HashMap::new()),
            server_address: server.address().clone(),
            session_id: get_time() as u64,
        });
        server.register_service(DEFAULT_SERVICE_ID, &service);
        return service;
    }
}
