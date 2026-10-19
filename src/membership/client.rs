use crate::membership::raft::client::SMClient;
use crate::membership::DEFAULT_SERVICE_ID;
use crate::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use crate::raft::state_machine::master::ExecError;
use bifrost_hasher::hash_str;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use super::server::MemberGroup;

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
    pub async fn join_group(&self, group: &String) -> Result<bool, ExecError> {
        self.sm_client.join_group(group, &self.id).await
    }
    pub async fn leave_group(&self, group: &String) -> Result<bool, ExecError> {
        self.sm_client.leave_group(&hash_str(group), &self.id).await
    }
}

pub struct ObserverClient {
    pub sm_client: Arc<SMClient>,
    #[cfg(feature = "c3doubt_trace")]
    test_roster: std::sync::OnceLock<(Vec<Member>, u64)>,
}

impl ObserverClient {
    pub fn new(raft_client: &Arc<RaftClient>) -> ObserverClient {
        ObserverClient {
            sm_client: Arc::new(SMClient::new(DEFAULT_SERVICE_ID, &raft_client)),
            #[cfg(feature = "c3doubt_trace")]
            test_roster: std::sync::OnceLock::new(),
        }
    }
    pub fn new_from_sm(sm_client: &Arc<SMClient>) -> ObserverClient {
        ObserverClient {
            sm_client: sm_client.clone(),
            #[cfg(feature = "c3doubt_trace")]
            test_roster: std::sync::OnceLock::new(),
        }
    }
    pub async fn new_group(&self, name: &String) -> Result<Result<u64, u64>, ExecError> {
        self.sm_client.new_group(name).await
    }
    pub async fn del_group(&self, name: &String) -> Result<bool, ExecError> {
        self.sm_client.del_group(&hash_str(name)).await
    }
    pub async fn group_leader(
        &self,
        group: &String,
    ) -> Result<Option<(Option<Member>, u64)>, ExecError> {
        self.sm_client.group_leader(&hash_str(group)).await
    }
    pub async fn group_members(
        &self,
        group: &String,
        online_only: bool,
    ) -> Result<Option<(Vec<Member>, u64)>, ExecError> {
        self.sm_client
            .group_members(&hash_str(group), &online_only)
            .await
    }
    pub async fn all_members(&self, online_only: bool) -> Result<(Vec<Member>, u64), ExecError> {
        #[cfg(feature = "c3doubt_trace")]
        if let Some((members, version)) = self.test_roster.get() {
            return Ok((
                members
                    .iter()
                    .filter(|member| !online_only || member.online)
                    .cloned()
                    .collect(),
                *version,
            ));
        }
        self.sm_client.all_members(&online_only).await
    }
    /// Supply a captured roster to model a lagging ordinary query reader.
    #[cfg(feature = "c3doubt_trace")]
    pub fn set_query_roster_for_test(&self, roster: (Vec<Member>, u64)) {
        self.test_roster
            .set(roster)
            .expect("the test roster is already set");
    }
    pub async fn on_group_member_offline<F>(
        &self,
        f: F,
        group: &str,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client
            .on_group_member_offline(f, &hash_str(group))
            .await
    }
    pub async fn on_any_member_offline<F>(
        &self,
        f: F,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_offline(f).await
    }
    pub async fn on_group_member_online<F>(
        &self,
        f: F,
        group: &str,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client
            .on_group_member_online(f, &hash_str(group))
            .await
    }
    pub async fn on_any_member_online<F>(
        &self,
        f: F,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_online(f).await
    }
    pub async fn on_group_member_joined<F>(
        &self,
        f: F,
        group: &str,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client
            .on_group_member_joined(f, &hash_str(group))
            .await
    }
    pub async fn on_any_member_joined<F>(
        &self,
        f: F,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_joined(f).await
    }
    pub async fn on_group_member_left<F>(
        &self,
        f: F,
        group: &str,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client
            .on_group_member_left(f, &hash_str(group))
            .await
    }
    pub async fn on_any_member_left<F>(
        &self,
        f: F,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Member, u64)) -> BoxFuture<'static, ()> + 'static + Send + Sync,
    {
        self.sm_client.on_any_member_left(f).await
    }
    pub async fn on_group_leader_changed<F>(
        &self,
        f: F,
        group: &String,
    ) -> Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>
    where
        F: Fn((Option<Member>, Option<Member>, u64)) -> BoxFuture<'static, ()>
            + 'static
            + Send
            + Sync,
    {
        self.sm_client
            .on_group_leader_changed(f, &hash_str(group))
            .await
    }
    pub async fn all_groups(&self) -> Result<BTreeMap<u64, MemberGroup>, ExecError> {
        self.sm_client.all_groups().await
    }
}
