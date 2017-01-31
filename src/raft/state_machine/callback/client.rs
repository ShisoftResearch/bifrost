use std::boxed::FnBox;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use super::*;
use rpc::Server;
use utils::time::get_time;

lazy_static! {
    pub static ref CLIENT_SUBSCRIPTIONS: RwLock<HashMap<SubKey, Vec<Box<Fn(Vec<u8>) + Send + Sync>>>> = RwLock::new(HashMap::new());
    pub static ref SUBSCRIPTIONS_SERVICE: RwLock<Option<Arc<CallbackService>>> = RwLock::new(None);
    pub static ref SERVER_ADDRESS: RwLock<Option<String>> = RwLock::new(None);
    pub static ref SESSION_ID: Option<u64> = Some(get_time() as u64);
}

pub fn init_subscription(server: Arc<Server>) {
    let mut service_ref = SUBSCRIPTIONS_SERVICE.write();
    let empty_ref = service_ref.is_none();
    if empty_ref {
        let service = Arc::new(CallbackService);
        let mut address_ref = SERVER_ADDRESS.write();
        server.append_service(DEFAULT_SERVICE_ID, service.clone());
        *service_ref = Some(service.clone());
        *address_ref = Some(server.clone().address().clone().unwrap());
    }
}

pub fn is_ready() -> bool {
    SUBSCRIPTIONS_SERVICE.read().is_some()
}

pub fn server_address() -> String {
    SERVER_ADDRESS.read().clone().unwrap()
}

pub fn session_id() -> u64 {SESSION_ID.unwrap()}