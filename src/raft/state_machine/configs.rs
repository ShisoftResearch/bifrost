use raft::SyncServiceClient;
use rpc;
use super::*;
use super::callback::SubKey;
use super::callback::server::{Subscriptions, SUBSCRIPTIONS};
use bifrost_hasher::hash_str;
use std::sync::Arc;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::io;

pub const CONFIG_SM_ID: u64 = 1;

pub struct RaftMember {
    pub rpc: Arc<SyncServiceClient>,
    pub address: String,
    pub id: u64,
}

pub struct Configures {
    pub members: HashMap<u64, RaftMember>,
    service_id: u64,
}

pub type MemberConfigSnapshot = HashSet<String>;

#[derive(Serialize, Deserialize, Debug)]
pub struct ConfigSnapshot {
    members: MemberConfigSnapshot,
    //TODO: snapshot for subscriptions
}

raft_state_machine! {
    def cmd new_member_(address: String);
    def cmd del_member_(address: String);
    def qry member_address() -> Vec<String>;

    def cmd subscribe(key: SubKey, address: String, session_id: u64) -> u64;
}

impl StateMachineCmds for Configures {
    fn new_member_(&mut self, address: String) -> Result<(), ()> {
        let addr = address.clone();
        let id = hash_str(&addr);
        if !self.members.contains_key(&id) {
            match rpc::DEFAULT_CLIENT_POOL.get(&address) {
                Ok(client) => {
                    self.members.insert(id, RaftMember {
                        rpc: SyncServiceClient::new(self.service_id, client),
                        address: address,
                        id: id,
                    });
                    return Ok(());
                },
                Err(_) => {}
            }
        }
        Err(())
    }
    fn del_member_(&mut self, address: String) -> Result<(),()> {
        let hash = hash_str(&address);
        self.members.remove(&hash);
        Ok(())
    }
    fn member_address(&self) -> Result<Vec<String>,()> {
        let mut members = Vec::with_capacity(self.members.len());
        for (_, member) in self.members.iter() {
            members.push(member.address.clone());
        }
        Ok(members)
    }
    fn subscribe(&mut self, key: SubKey, address: String, session_id: u64) -> Result<u64, ()> {
        let mut subscriptions_map = SUBSCRIPTIONS.write();
        if let Some(ref mut subscriptions) = subscriptions_map.get_mut(&self.service_id) {
            subscriptions.subscribe(key, &address, session_id)
        } else {
            Err(())
        }
    }
}

impl StateMachineCtl for Configures {
    sm_complete!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        let mut snapshot = ConfigSnapshot{
            members: HashSet::with_capacity(self.members.len()),
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
    pub fn new(service_id: u64) -> Configures {
        let mut subscription_map = SUBSCRIPTIONS.write();
        subscription_map.insert(service_id, Subscriptions::new());
        Configures {
            members: HashMap::new(),
            service_id: service_id
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
    pub fn new_member(&mut self, address: String) -> Result<(),()> {
        self.new_member_(address)
    }
    pub fn del_member(&mut self, address: String) -> Result<(),()> {
        self.del_member_(address)
    }
    pub fn member_existed(&self, id: u64) -> bool {
        self.members.contains_key(&id)
    }
}