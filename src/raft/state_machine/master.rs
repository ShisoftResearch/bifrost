use super::super::*;
use super::*;

raft_state_machine! {
    def cmd append(sub_id: u64, entries: Option<LogEntries>) -> u8;
}

pub struct MasterStateMachine {

}

impl StateMachine for MasterStateMachine {
    fn append(&mut self, sub_id: u64, entries: Option<LogEntries>) -> Result<u8, ()> {
        Err(())
    }
}

impl StateMachineInterface for MasterStateMachine {
    fn snapshot(&self) -> Option<Vec<u8>> {
        None
    }
    fn id(&self) -> u64 {0}
}