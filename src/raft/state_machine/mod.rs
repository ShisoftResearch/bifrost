#[macro_use]
pub mod macros;
pub mod master;
pub mod configs;

pub enum Storage {
    MEMORY,
    DISK(String),
}

trait StateMachineInterface {
    fn id(&self) -> u64;
    fn snapshot(&self) -> Option<Vec<u8>>;
}
