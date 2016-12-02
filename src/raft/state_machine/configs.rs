use raft::SyncClient;
use std::collections::HashMap;
use super::*;
use bifrost_plugins::hash_str;

pub struct RaftMember {
    rpc: SyncClient,
    address: String,
    hash: u64,
    last_term: u64,
    last_log: u64,
    alive: bool,
}

pub struct Configures {
    members: HashMap<u64, RaftMember>,
}

raft_state_machine! {
    def cmd new_member(address: String);
    def cmd del_member(address: String);
}

impl StateMachineCmds for Configures {
    fn new_member(&mut self, address: String) -> Result<(),()> {
        let addr = address.clone();
        let hash = hash_str(addr);
        self.members.insert(hash, RaftMember {
            rpc: SyncClient::new(&address),
            address: address,
            hash: hash,
            last_log: 0,
            last_term: 0,
            alive: true,
        });
        Ok(())
    }
    fn del_member(&mut self, address: String) -> Result<(),()> {
        Ok(())
    }
}

impl StateMachineCtl for Configures {
    fn_dispatch!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        None
    }
    fn id(&self) -> u64 {1}
}

impl Configures {
    pub fn new() -> Configures {
        Configures {
            members: HashMap::new()
        }
    }
}