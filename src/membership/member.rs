use super::client::{MemberClient, ObserverClient};
use super::heartbeat_rpc::*;
use super::raft::client::SMClient;
use bifrost_hasher::hash_str;
use futures::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time;

use crate::membership::DEFAULT_SERVICE_ID;
use crate::raft::client::RaftClient;
use crate::raft::state_machine::master::ExecError;
use crate::raft::RaftService;
use crate::utils::time::get_time;

static PING_INTERVAL: u64 = 500;

pub struct MemberService {
    member_client: MemberClient,
    sm_client: Arc<SMClient>,
    raft_client: Arc<RaftClient>,
    closed: AtomicBool,
    id: u64,
}

impl MemberService {
    pub async fn new(
        server_address: &String,
        raft_client: &Arc<RaftClient>,
        raft_service: &Arc<RaftService>,
    ) -> Arc<MemberService> {
        let server_id = hash_str(server_address);
        let sm_client = Arc::new(SMClient::new(DEFAULT_SERVICE_ID, &raft_client));
        let service = Arc::new(MemberService {
            sm_client: sm_client.clone(),
            member_client: MemberClient {
                id: server_id,
                sm_client: sm_client.clone(),
            },
            raft_client: raft_client.clone(),
            closed: AtomicBool::new(false),
            id: server_id,
        });
        let _join_res = sm_client.join(&server_address).await;
        let service_clone = service.clone();
        raft_service.rt.spawn(async move {
            while !service_clone.closed.load(Ordering::Relaxed) {
                let start_time = get_time();
                // Ping the leader. A follower answers `NotLeader` with a hint
                // and a dead leader answers nothing: in both cases the cache of
                // the leader is refreshed and the ping is sent again at once.
                let mut accepted = false;
                for _ in 0..3 {
                    let rpc_client = service_clone.raft_client.current_leader_rpc_client().await;
                    let reply = match rpc_client {
                        Ok(rpc_client) => {
                            ImmeServiceClient::ping(DEFAULT_SERVICE_ID, &rpc_client, service_clone.id)
                                .await
                                .ok()
                        }
                        Err(_) => {
                            error!("Cannot find RPC client for membership heartbeat to leader");
                            None
                        }
                    };
                    let hint = match reply {
                        Some(PingReply::Accepted) => {
                            accepted = true;
                            break;
                        }
                        Some(PingReply::NotLeader { leader_hint }) => leader_hint,
                        None => 0,
                    };
                    if !service_clone.raft_client.refresh_leader(hint).await {
                        break;
                    }
                }
                if !accepted {
                    debug!("Membership heartbeat of {} was not accepted by a leader", service_clone.id);
                }
                let time_now = get_time();
                let elapsed_time = time_now - start_time;
                trace!(
                    "Membership ping at time {}, elapsed {}ms",
                    time_now,
                    elapsed_time
                );
                if (elapsed_time as u64) < PING_INTERVAL {
                    let wait_time = PING_INTERVAL - elapsed_time as u64;
                    trace!("Waiting membership heartbeat for {}ms", wait_time);
                    time::sleep(time::Duration::from_millis(wait_time)).await;
                }
            }
            debug!("Member service closed");
        });
        return service;
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
    pub async fn leave(&self) -> Result<bool, ExecError> {
        self.close();
        self.sm_client.leave(&self.id).await
    }
    pub async fn join(&self, server_address: &String) -> Result<Option<u64>, ExecError> {
        self.sm_client.join(server_address).await
    }
    pub async fn join_group(&self, group: &String) -> Result<bool, ExecError> {
        self.member_client.join_group(group).await
    }
    pub async fn leave_group(&self, group: &String) -> Result<bool, ExecError> {
        self.member_client.leave_group(group).await
    }
    pub fn client(&self) -> ObserverClient {
        ObserverClient::new_from_sm(&self.sm_client)
    }
    pub fn get_server_id(&self) -> u64 {
        self.id
    }
}

impl Drop for MemberService {
    fn drop(&mut self) {
        let sm_client = self.sm_client.clone();
        let self_id = self.id;
        tokio::spawn(async move { sm_client.leave(&self_id).await }.boxed());
    }
}
