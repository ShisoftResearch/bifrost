pub mod framed;
pub mod server;
pub mod proto;
pub mod client;
pub mod shortcut;

pub static STANDALONE_ADDRESS: &'static str = "STANDALONE";

lazy_static! {
    pub static ref STANDALONE_ADDRESS_STRING: String = String::from(STANDALONE_ADDRESS);
}