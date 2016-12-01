use raft::SyncClient;
use std::collections::HashMap;

pub struct RaftMember {
    rpc: SyncClient,
    address: String,
    last_term: u64,
    last_log: u64,
    alive: bool,
}

pub struct Configures {
    members: HashMap<String, RaftMember>,
}

raft_state_machine! {
    def cmd new_member(address: String);
    def cmd del_member(address: String);
}