use super::*;
use crate::utils::time::get_time;
use async_std::sync::*;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::sync::Arc;

trait SubFunc = Fn(Vec<u8>) -> BoxFuture<'static, ()>;
trait BoxedSubFunc = SubFunc + Send + Sync;

pub struct SubscriptionService {
    pub subs: RwLock<HashMap<SubKey, Vec<(Box<dyn BoxedSubFunc>, u64)>>>,
    pub server_address: String,
    pub session_id: u64,
}

impl Service for SubscriptionService {
    fn notify(&self, key: SubKey, data: Vec<u8>) -> BoxFuture<()> {
        debug!("Received notification for key {:?}", key);
        async move {
            let subs = self.subs.read().await;
            if let Some(subs) = subs.get(&key) {
                let subs = Pin::new(subs);
                let futs: FuturesUnordered<_> = subs
                    .iter()
                    .map(|(fun, _)| {
                        let fun_pinned = Pin::new(fun);
                        fun_pinned(data.clone())
                    })
                    .collect();
                // Spawn async task DETACHED with the function to avoid deadlocks inside raft state machine
                tokio::spawn(async move {
                    let _: Vec<_> = futs.collect().await;
                });
            }
        }
        .boxed()
    }
}
dispatch_rpc_service_functions!(SubscriptionService);

impl SubscriptionService {
    pub async fn initialize(server: &Arc<Server>) -> Arc<SubscriptionService> {
        let service = Arc::new(SubscriptionService {
            subs: RwLock::new(HashMap::new()),
            server_address: server.address().clone(),
            session_id: get_time() as u64,
        });
        server.register_service(DEFAULT_SERVICE_ID, &service).await;
        service
    }
}
