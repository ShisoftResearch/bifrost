use std::boxed::FnBox;
use std::collections::HashMap;
use std::sync::{RwLock, Arc};
use super::*;
use rpc::Server;
use utils::time::get_time;

lazy_static! {
    pub static ref CLIENT_SUBSCRIPTIONS: RwLock<HashMap<SubKey, Vec<Box<FnBox(Vec<u8>) + Send + Sync>>>> = RwLock::new(HashMap::new());
    pub static ref SUBSCRIPTIONS_SERVICE: RwLock<Option<Arc<CallbackService>>> = RwLock::new(None);
    pub static ref SESSION_ID: u64 = get_time() as u64;
}

pub fn init_subscription(server: Arc<Server>) {
    let mut service_ref = SUBSCRIPTIONS_SERVICE.write().unwrap();
    let empty_ref = service_ref.is_none();
    if empty_ref {
        let service = Arc::new(CallbackService);
        server.append_service(DEFAULT_SERVICE_ID, service.clone());
        *service_ref = Some(service);
    }
}

pub fn is_ready() -> bool {
    SUBSCRIPTIONS_SERVICE.read().unwrap().is_some()
}