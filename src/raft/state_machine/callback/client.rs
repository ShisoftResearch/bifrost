use std::boxed::FnBox;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use super::*;
use rpc::Server;
use utils::time::get_time;

pub struct SubscriptionService {
    pub subs: RwLock<HashMap<SubKey, Vec<Box<Fn(Vec<u8>) + Send + Sync>>>>,
    pub server_address: String,
    pub session_id: u64
}

impl Service for SubscriptionService {
    fn notify(&self, key: &SubKey, data: &Vec<u8>) -> Result<(), ()> {
        let (raft_sid, sm_id, fn_id, pattern_id) = *key;
        let subs = self.subs.read();
        if let Some(sub_fns) = subs.get(&key) {
            for fun in sub_fns {
                fun(data.clone());
            }
        }
        Ok(())
    }
}
dispatch_rpc_service_functions!(SubscriptionService);

impl SubscriptionService {
    pub fn initialize(server: &Arc<Server>) -> Arc<SubscriptionService> {
        let service = Arc::new(SubscriptionService {
            subs: RwLock::new(HashMap::new()),
            server_address: server.address().clone(),
            session_id: get_time() as u64
        });
        server.register_service(DEFAULT_SERVICE_ID, &service);
        return service;
    }
}