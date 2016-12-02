use raft::SyncClient;
use std::collections::{HashMap, HashSet};
use super::*;
use bifrost_plugins::hash_str;

pub const CONFIG_SM_ID: u64 = 1;

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

pub type MemberConfigSnapshot = HashSet<String>;

#[derive(Serialize, Deserialize, Debug)]
pub struct ConfigSnapshot {
    members: MemberConfigSnapshot
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
        let hash = hash_str(address);
        self.members.remove(&hash);
        Ok(())
    }
}

impl StateMachineCtl for Configures {
    fn_dispatch!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        let mut snapshot = ConfigSnapshot{
            members: HashSet::with_capacity(self.members.len())
        };
        for (_, member) in self.members.iter() {
            snapshot.members.insert(member.address.clone());
        }
        Some(serialize!(&snapshot))
    }
    fn recover(&mut self, data: Vec<u8>) {
        let snapshot:ConfigSnapshot = deserialize!(&data);
        self.recover_members(&snapshot.members)
    }
    fn id(&self) -> u64 {CONFIG_SM_ID}
}

impl Configures {
    pub fn new() -> Configures {
        Configures {
            members: HashMap::new()
        }
    }
    fn recover_members (&mut self, snapshot: &MemberConfigSnapshot) {
        let mut curr_members: MemberConfigSnapshot = HashSet::with_capacity(self.members.len());
        for (_, member) in self.members.iter() {
            curr_members.insert(member.address.clone());
        }
        let to_del = curr_members.difference(snapshot);
        let to_add = snapshot.difference(&curr_members);
        for addr in to_del {
            self.del_member(addr.clone());
        }
        for addr in to_add {
            self.new_member(addr.clone());
        }
    }
}