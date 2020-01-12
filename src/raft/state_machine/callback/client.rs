use super::*;
use crate::utils::rwlock::*;
use std::collections::HashMap;
use std::sync::Arc;
use crate::utils::time::get_time;
use async_trait::async_trait;

pub struct SubscriptionService {
    pub subs: RwLock<HashMap<SubKey, Vec<(Box<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = ()>>>>, u64)>>>,
    pub server_address: String,
    pub session_id: u64,
}

#[async_trait]
impl Service for SubscriptionService {
    async fn notify(&self, key: SubKey, data: Vec<u8>) {
        let subs = self.subs.read().await;
        if let Some(subs) = subs.get(&key) {
            for &(ref fun, _) in subs {
                fun(data.clone()).await;
            }
        }
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
