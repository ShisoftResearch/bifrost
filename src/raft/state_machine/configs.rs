use crate::raft::state_machine::callback::server::Subscriptions;
use crate::raft::state_machine::callback::SubKey;
use crate::raft::state_machine::StateMachineCtl;
use crate::raft::AsyncServiceClient;
use crate::rpc;
use async_std::sync::*;
use bifrost_hasher::hash_str;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub const CONFIG_SM_ID: u64 = 1;

pub struct RaftMember {
    pub rpc: Arc<AsyncServiceClient>,
    pub address: String,
    pub id: u64,
}

pub struct Configures {
    pub members: HashMap<u64, RaftMember>,
    // keep it in arc lock for reference in callback server.rs
    pub subscriptions: Arc<RwLock<Subscriptions>>,
    service_id: u64,
}

pub type MemberConfigSnapshot = HashSet<String>;

#[derive(Serialize, Deserialize, Debug)]
pub struct ConfigSnapshot {
    members: MemberConfigSnapshot,
    //TODO: snapshot for subscriptions
}

raft_state_machine! {
    def cmd new_member_(address: String) -> bool;
    def cmd del_member_(address: String);
    def qry member_address() -> Vec<String>;

    def cmd subscribe(key: SubKey, address: String, session_id: u64) -> Result<u64, ()>;
    def cmd unsubscribe(sub_id: u64);
}

impl StateMachineCmds for Configures {
    fn new_member_(&mut self, address: String) -> BoxFuture<bool> {
        async move {
            let addr = address.clone();
            let id = hash_str(&addr);
            if !self.members.contains_key(&id) {
                match rpc::DEFAULT_CLIENT_POOL.get(&address).await {
                    Ok(client) => {
                        self.members.insert(
                            id,
                            RaftMember {
                                rpc: AsyncServiceClient::new(self.service_id, &client),
                                address,
                                id,
                            },
                        );
                        return true;
                    }
                    Err(_) => {}
                }
            }
            false
        }
        .boxed()
    }
    fn del_member_(&mut self, address: String) -> BoxFuture<()> {
        let hash = hash_str(&address);
        self.members.remove(&hash);
        future::ready(()).boxed()
    }
    fn member_address(&self) -> BoxFuture<Vec<String>> {
        future::ready(self.members.values().map(|m| m.address.clone()).collect()).boxed()
    }
    fn subscribe(
        &mut self,
        key: SubKey,
        address: String,
        session_id: u64,
    ) -> BoxFuture<Result<u64, ()>> {
        async move {
            let mut subs = self.subscriptions.write().await;
            subs.subscribe(key, &address, session_id).await
        }
        .boxed()
    }
    fn unsubscribe(&mut self, sub_id: u64) -> BoxFuture<()> {
        async move {
            let mut subs = self.subscriptions.write().await;
            subs.remove_subscription(sub_id);
        }
        .boxed()
    }
}

impl StateMachineCtl for Configures {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        CONFIG_SM_ID
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        let mut snapshot = ConfigSnapshot {
            members: HashSet::with_capacity(self.members.len()),
        };
        for (_, member) in self.members.iter() {
            snapshot.members.insert(member.address.clone());
        }
        Some(bincode::serialize(&snapshot).unwrap())
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        let snapshot: ConfigSnapshot = bincode::deserialize(&data).unwrap();
        self.recover_members(snapshot.members).boxed()
    }
}

impl Configures {
    pub fn new(service_id: u64) -> Configures {
        Configures {
            members: HashMap::new(),
            service_id,
            subscriptions: Arc::new(RwLock::new(Subscriptions::new())),
        }
    }
    async fn recover_members(&mut self, snapshot: MemberConfigSnapshot) {
        let mut curr_members: MemberConfigSnapshot = HashSet::with_capacity(self.members.len());
        for (_, member) in self.members.iter() {
            curr_members.insert(member.address.clone());
        }
        let to_del = curr_members.difference(&snapshot);
        let to_add = snapshot.difference(&curr_members);
        for addr in to_del {
            self.del_member(addr.clone()).await;
        }
        for addr in to_add {
            self.new_member(addr.clone()).await;
        }
    }
    pub async fn new_member(&mut self, address: String) -> bool {
        self.new_member_(address).await
    }
    pub async fn del_member(&mut self, address: String) {
        self.del_member_(address).await
    }
    pub fn member_existed(&self, id: u64) -> bool {
        self.members.contains_key(&id)
    }
}
