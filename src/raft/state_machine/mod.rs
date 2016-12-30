use std::any::Any;

pub enum Storage {
    MEMORY,
    DISK(String),
}

pub enum OpType {
    COMMAND,
    QUERY
}

trait StateMachineCtl: Sync + Send + Any {
    fn id(&self) -> u64;
    fn snapshot(&self) -> Option<Vec<u8>>;
    fn recover(&mut self, data: Vec<u8>);
    fn fn_dispatch_qry(&self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>>;
    fn fn_dispatch_cmd(&mut self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>>;
    fn op_type(&mut self, fn_id: u64) -> Option<OpType>;
}

trait OpTypes {
    fn op_type(&self, fn_id: u64) -> Option<OpType>;
}

#[macro_use]
pub mod macros;
pub mod master;
pub mod configs;
