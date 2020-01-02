use bifrost_hasher::hash_str;

pub mod client;
pub mod framed;
pub mod server;
pub mod shortcut;

pub static STANDALONE_ADDRESS: &'static str = "STANDALONE";

lazy_static! {
    pub static ref STANDALONE_ADDRESS_STRING: String = String::from(STANDALONE_ADDRESS);
    pub static ref STANDALONE_SERVER_ID: u64 = hash_str(&STANDALONE_ADDRESS_STRING);
}
