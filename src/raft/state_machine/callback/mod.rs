pub mod client;
pub mod server;
//                (server_id, raft_sid, sm_id, fn_id, pattern_id)
pub type SubKey = (u64, u64, u64, u64);

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_SM_CALLBACK_DEFAULT_SERVICE) as u64;

service! {
    rpc notify(key: SubKey, data: Vec<u8>);
}