use raft::client::RaftClient;
use raft::state_machine::master::ExecError;
use super::DEFAULT_SERVICE_ID;
use super::heartbeat_rpc::*;
use super::raft::client::SMClient;
use super::client::{MemberClient, ObserverClient as ObserverClient};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{thread, time};
use bifrost_hasher::hash_str;

static PING_INTERVAL: u64 = 100;

pub struct MemberService {
    member_client: MemberClient,
    sm_client: Arc<SMClient>,
    raft_client: Arc<RaftClient>,
    address: String,
    closed: AtomicBool,
    id: u64,
}

impl MemberService {
    pub fn new(server_address: &String, raft_client: &Arc<RaftClient>) -> Arc<MemberService> {
        let server_id = hash_str(server_address);
        let sm_client = Arc::new(SMClient::new(DEFAULT_SERVICE_ID, &raft_client));
        let service = Arc::new(MemberService {
            sm_client: sm_client.clone(),
            member_client: MemberClient {
                id: server_id,
                sm_client: sm_client.clone()
            },
            raft_client: raft_client.clone(),
            address: server_address.clone(),
            closed: AtomicBool::new(false),
            id: server_id,
        });
        sm_client.join(server_address.clone());
        let service_clone = service.clone();
        thread::spawn(move || {
            while !service_clone.closed.load(Ordering::Relaxed) {
                let rpc_client = service_clone.raft_client.current_leader_rpc_client();
                if let Some(rpc_client) = rpc_client {
                    let heartbeat_client = SyncServiceClient::new(DEFAULT_SERVICE_ID, rpc_client);
                    heartbeat_client.ping(service_clone.id);
                }
                thread::sleep(time::Duration::from_millis(PING_INTERVAL))
            }
        });
        return service;
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
    }
    pub fn leave(&self) -> Result<Result<(), ()>, ExecError>{
        self.close();
        self.sm_client.leave(self.id)
    }
    pub fn join_group(&self, group: &String) -> Result<Result<(), ()>, ExecError> {
        self.member_client.join_group(group)
    }
    pub fn leave_group(&self, group: &String) -> Result<Result<(), ()>, ExecError> {
        self.member_client.leave_group(group)
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
        self.leave();
    }
}

