use crate::raft::client::RaftPlaneClient;
use std::any::Any;
use std::sync::Arc;

pub enum Storage {
    MEMORY,
    DISK(String),
}

#[derive(Debug)]
pub enum OpType {
    COMMAND,
    QUERY,
    SUBSCRIBE,
}

pub trait StateMachineCtl: Sync + Send + Any {
    fn id(&self) -> u64;
    fn snapshot(&self) -> Vec<u8>;
    fn recover(&mut self, data: Vec<u8>) -> ::futures::future::BoxFuture<()>;
    fn recoverable(&self) -> bool;
    /// Whether entries buffered while this state machine was unregistered
    /// should be replayed into it at registration. State machines that
    /// recover their state through their own persistence must decline when
    /// that state already reflects the buffered commands.
    fn accept_buffered_replay(&self) -> bool {
        true
    }
    fn fn_dispatch_qry<'a>(
        &'a self,
        fn_id: u64,
        data: &'a Vec<u8>,
    ) -> ::futures::future::BoxFuture<'a, Option<Vec<u8>>>;
    fn fn_dispatch_cmd<'a>(
        &'a mut self,
        fn_id: u64,
        data: &'a Vec<u8>,
    ) -> ::futures::future::BoxFuture<'a, Option<Vec<u8>>>;
    fn op_type(&mut self, fn_id: u64) -> Option<OpType>;
}

pub trait OpTypes {
    fn op_type(&self, fn_id: u64) -> Option<OpType>;
}

pub trait StateMachineClient {
    fn new_instance(sm_id: u64, client: &Arc<RaftPlaneClient>) -> Self;
}

pub const MASTER_SM_ID: u64 = 0;
pub const CONFIG_SM_ID: u64 = 1;
pub const RESERVED_INTERNAL_SM_ID_END: u64 = 2;

pub const fn is_reserved_internal_sm_id(sm_id: u64) -> bool {
    sm_id <= RESERVED_INTERNAL_SM_ID_END
}

#[macro_use]
pub mod macros;
pub mod callback;
pub mod configs;
pub mod master;
