use std::boxed::FnBox;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use super::*;
use rpc::Server;
use utils::time::get_time;
use futures::prelude::*;

pub struct SubscriptionService {
    pub subs: RwLock<HashMap<SubKey, Vec<Box<Fn(Vec<u8>) + Send + Sync>>>>,
    pub server_address: String,
    pub session_id: u64
}

impl Service for SubscriptionService {
    fn notify(&self, key: SubKey, data: Vec<u8>) -> Box<Future<Item = (), Error = ()>> {
        let subs = self.subs.read();
        if let Some(sub_fns) = subs.get(&key) {
            for fun in sub_fns {
                fun(data.clone());
            }
        }
        box future::finished(())
    }
}
dispatch_rpc_service_functions!(SubscriptionService);

impl SubscriptionService {
    #[async]
    pub fn initialize(server: Arc<Server>) -> Result<Arc<SubscriptionService>, ()> {
        let service = Arc::new(SubscriptionService {
            subs: RwLock::new(HashMap::new()),
            server_address: server.address().clone(),
            session_id: get_time() as u64
        });
        await!(server.register_service_async(DEFAULT_SERVICE_ID, service.clone()))?;
        return Ok(service);
    }
}