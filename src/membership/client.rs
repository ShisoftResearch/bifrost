use std::sync::Arc;
use raft::client::{RaftClient, SubscriptionError};
use raft::state_machine::master::ExecError;
use bifrost_hasher::hash_str;
use super::raft::client::SMClient;
use super::DEFAULT_SERVICE_ID;

pub type WatchResult = Result<Result<u64, SubscriptionError>, ExecError>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Member {
    pub id: u64,
    pub address: String,
    pub online: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    pub fn join_group(&self, group: &String) -> Result<Result<(), ()>, ExecError> {
        self.sm_client.join_group(group, &self.id)
    }
    pub fn leave_group(&self, group: &String) -> Result<Result<(), ()>, ExecError> {
        self.sm_client.leave_group(&hash_str(group), &self.id)
    }
}

pub struct ObserverClient {
    pub sm_client: Arc<SMClient>
}

impl ObserverClient {
    pub fn new(raft_client: &Arc<RaftClient>) -> ObserverClient {
        ObserverClient {
            sm_client: Arc::new(SMClient::new(DEFAULT_SERVICE_ID, &raft_client))
        }
    }
    pub fn new_from_sm(sm_client: &Arc<SMClient>) -> ObserverClient {
        ObserverClient {
            sm_client: sm_client.clone()
        }
    }
    pub fn new_group(&self, name: &String) -> Result<Result<u64, u64>, ExecError> {
        self.sm_client.new_group(name)
    }
    pub fn del_group(&self, name: &String) -> Result<Result<(), ()>, ExecError> {
        self.sm_client.del_group(&hash_str(name))
    }
    pub fn group_leader(&self, group: &String) -> Result<Result<(Option<Member>, u64), ()>, ExecError> {
        self.sm_client.group_leader(&hash_str(group))
    }
    pub fn group_members(&self, group: &String, online_only: bool) -> Result<Result<(Vec<Member>, u64), ()>, ExecError> {
        self.sm_client.group_members(&hash_str(group), &online_only)
    }
    pub fn all_members(&self, online_only: bool) -> Result<Result<(Vec<Member>, u64), ()>, ExecError> {
        self.sm_client.all_members(&online_only)
    }
    pub fn on_group_member_offline<'a, F>(&self, f: F, group: &'a str) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_group_member_offline(f, &hash_str(group))
    }
    pub fn on_any_member_offline<F>(&self, f: F) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_any_member_offline(f)
    }
    pub fn on_group_member_online<'a, F>(&self, f: F, group: &'a str) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_group_member_online(f, &hash_str(group))
    }
    pub fn on_any_member_online<F>(&self, f: F) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_any_member_online(f)
    }
    pub fn on_group_member_joined<'a, F>(&self, f: F, group: &'a str) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_group_member_joined(f, &hash_str(group))
    }
    pub fn on_any_member_joined<F>(&self, f: F) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_any_member_joined(f)
    }
    pub fn on_group_member_left<'a, F>(&self, f: F, group: &'a str) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_group_member_left(f, &hash_str(group))
    }
    pub fn on_any_member_left<F>(&self, f: F) -> WatchResult
        where F: Fn(Result<(Member, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_any_member_left(f)
    }
    pub fn on_group_leader_changed<F>(&self, f: F, group: &String)
        -> WatchResult
        where F: Fn(Result<(Option<Member>, Option<Member>, u64), ()>)  + 'static + Send + Sync {
        self.sm_client.on_group_leader_changed(f, &hash_str(group))
    }
}