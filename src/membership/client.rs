use std::sync::Arc;
use bifrost_hasher::hash_str;
use super::raft::client::SMClient;

#[derive(Serialize, Deserialize, Debug)]
pub struct Member {
    pub id: u64,
    pub address: String,
    pub online: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Group {
    pub id: u64,
    pub name: String,
    pub members: u64,
}

pub struct MemberClient {
    pub id: u64,
    pub sm_client: Arc<SMClient>
}

impl MemberClient {
    pub fn join_group(&self, group: String) -> Option<Result<(), ()>> {
        if let Ok(r) = self.sm_client.join_group(hash_str(group), self.id) {
            Some(r)
        } else {
            None
        }
    }
    pub fn leave_group(&self, group: String) -> Option<Result<(), ()>> {
        if let Ok(r) = self.sm_client.leave_group(hash_str(group), self.id) {
            Some(r)
        } else {
            None
        }
    }
}