pub enum Storage {
    MEMORY,
    DISK(String),
}

trait StateMachineCtl: Sync + Send {
    fn id(&self) -> u64;
    fn snapshot(&self) -> Option<Vec<u8>>;
    fn fn_dispatch(&mut self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>>;
}

#[macro_use]
pub mod macros;
pub mod master;
pub mod configs;
