use super::client::{MemberClient, ObserverClient};
use super::heartbeat_rpc::*;
use super::raft::client::SMClient;
use bifrost_hasher::hash_str;
use futures::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::{runtime, time};

use crate::membership::DEFAULT_SERVICE_ID;
use crate::raft::client::RaftClient;
use crate::raft::state_machine::master::ExecError;
use crate::utils::time::get_time;

static PING_INTERVAL: u64 = 500;

pub struct MemberService {
    member_client: MemberClient,
    sm_client: Arc<SMClient>,
    raft_client: Arc<RaftClient>,
    closed: AtomicBool,
    rt: runtime::Runtime,
    id: u64,
}

impl MemberService {
    pub async fn new(server_address: &String, raft_client: &Arc<RaftClient>) -> Arc<MemberService> {
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
            rt: runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("membership")
                .worker_threads(1)
                .max_blocking_threads(1)
                .event_interval(5)
                .build()
                .unwrap(),
        });
        let _join_res = sm_client.join(&server_address).await;
        let service_clone = service.clone();
        service.rt.spawn(async move {
            while !service_clone.closed.load(Ordering::Relaxed) {
                let start_time = get_time();
                let rpc_client = service_clone.raft_client.current_leader_rpc_client().await;
                if let Ok(rpc_client) = rpc_client {
                    let _ping_res =
                        ImmeServiceClient::ping(DEFAULT_SERVICE_ID, &rpc_client, service_clone.id)
                            .await;
                } else {
                    error!("Cannot find RPC client for membership heartbeat to leader");
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
