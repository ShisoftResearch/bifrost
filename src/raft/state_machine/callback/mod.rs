pub mod client;
pub mod server;

use raft::state_machine::callback::{client as c};

pub type SubKey = (u64, u64, u64, u64);

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_SM_CALLBACK_DEFAULT_SERVICE) as u64;

service! {
    rpc notify(key: SubKey, data: Vec<u8>);
}

pub struct CallbackService;

impl Service for CallbackService {
    fn notify(&self, key: SubKey, data: Vec<u8>) -> Result<(), ()> {
        let (raft_sid, sm_id, fn_id, pattern_id) = key;
        let subs = c::CLIENT_SUBSCRIPTIONS.read();
        if let Some(sub_fns) = subs.get(&key) {
            for fun in sub_fns {
                fun(data.clone());
            }
        }
        Ok(())
    }
}
dispatch_rpc_service_functions!(CallbackService);