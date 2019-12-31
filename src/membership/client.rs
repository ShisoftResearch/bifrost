use super::raft::client::SMClient;
use bifrost_hasher::hash_str;
use std::sync::Arc;
use std::future::Future;
use crate::raft::state_machine::master::ExecError;
use crate::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use crate::membership::DEFAULT_SERVICE_ID;

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
    pub sm_client: Arc<SMClient>,
}

impl MemberClient {
    pub fn join_group(
        &self,
        group: &String,
    ) -> impl Future<Output = Result<(), ExecError>> {
        self.sm_client.join_group(group, &self.id)
    }
    pub fn leave_group(
        &self,
        group: &String,
    ) -> impl Future<Output = Result<(), ExecError>> {
        self.sm_client.leave_group(&hash_str(group), &self.id)
    }
}

pub struct ObserverClient {
    pub sm_client: Arc<SMClient>,
}

impl ObserverClient {
    pub fn new(raft_client: &Arc<RaftClient>) -> ObserverClient {
        ObserverClient {
            sm_client: Arc::new(SMClient::new(DEFAULT_SERVICE_ID, &raft_client)),
        }
    }
    pub fn new_from_sm(sm_client: &Arc<SMClient>) -> ObserverClient {
        ObserverClient {
            sm_client: sm_client.clone(),
        }
    }
    pub fn new_group(
        &self,
        name: &String,
    ) -> impl Future<Item = Result<u64, u64>, Error = ExecError> {
        self.sm_client.new_group(name)
    }
    pub fn del_group(
        &self,
        name: &String,
    ) -> impl Future<Item = Result<(), ()>, Error = ExecError> {
        self.sm_client.del_group(&hash_str(name))
    }
    pub fn group_leader(
        &self,
        group: &String,
    ) -> impl Future<Item = Result<(Option<Member>, u64), ()>, Error = ExecError> {
        self.sm_client.group_leader(&hash_str(group))
    }
    pub fn group_members(
        &self,
        group: &String,
        online_only: bool,
    ) -> impl Future<Item = Result<(Vec<Member>, u64), ()>, Error = ExecError> {
        self.sm_client.group_members(&hash_str(group), &online_only)
    }
    pub fn all_members(
        &self,
        online_only: bool,
    ) -> impl Future<Item = Result<(Vec<Member>, u64), ()>, Error = ExecError> {
        self.sm_client.all_members(&online_only)
    }
    pub fn on_group_member_offline<F>(
        &self,
        f: F,
        group: &str,
    ) -> impl Future<Output = Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_group_member_offline(f, &hash_str(group))
    }
    pub fn on_any_member_offline<F>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_offline(f)
    }
    pub fn on_group_member_online<F>(
        &self,
        f: F,
        group: &str,
    ) -> impl Future<Output = Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_group_member_online(f, &hash_str(group))
    }
    pub fn on_any_member_online<F>(
        &self,
        f: F,
    ) -> impl Future<Item = Result<SubscriptionReceipt, SubscriptionError>, Error = ExecError>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_online(f)
    }
    pub fn on_group_member_joined<F>(
        &self,
        f: F,
        group: &str,
    ) -> impl Future<Output = Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_group_member_joined(f, &hash_str(group))
    }
    pub fn on_any_member_joined<F>(
        &self,
        f: F,
    ) -> impl Future<Output = Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_joined(f)
    }
    pub fn on_group_member_left<F>(
        &self,
        f: F,
        group: &str,
    ) -> impl Future<Output = Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_group_member_left(f, &hash_str(group))
    }
    pub fn on_any_member_left<F>(
        &self,
        f: F,
    ) -> impl Future<Item = Result<SubscriptionReceipt, SubscriptionError>, Error = ExecError>
    where
        F: Fn(Result<(Member, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_left(f)
    }
    pub fn on_group_leader_changed<F>(
        &self,
        f: F,
        group: &String,
    ) -> impl Future<Item = Result<SubscriptionReceipt, SubscriptionError>, Error = ExecError>
    where
        F: Fn(Result<(Option<Member>, Option<Member>, u64), ()>) + 'static + Send + Sync,
    {
        self.sm_client.on_group_leader_changed(f, &hash_str(group))
    }
}
