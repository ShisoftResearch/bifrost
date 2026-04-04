use crate::raft::state_machine::callback::server::Subscriptions;
use crate::raft::state_machine::callback::SubKey;
use crate::raft::state_machine::StateMachineCtl;
use crate::raft::AsyncServiceClient;
use crate::rpc::{self, ServiceClient};
use async_std::sync::*;
use bifrost_hasher::hash_str;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub const CONFIG_SM_ID: u64 = 1;

#[derive(Clone)]
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
                                rpc: AsyncServiceClient::new_with_service_id(
                                    self.service_id,
                                    &client,
                                ),
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
        async move {
            let hash = hash_str(&address);
            self.members.remove(&hash);
        }
        .boxed()
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
    fn snapshot(&self) -> Vec<u8> {
        let mut snapshot = ConfigSnapshot {
            members: HashSet::with_capacity(self.members.len()),
        };
        for (_, member) in self.members.iter() {
            snapshot.members.insert(member.address.clone());
        }
        crate::utils::serde::serialize(&snapshot)
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        match crate::utils::serde::deserialize::<ConfigSnapshot>(&data) {
            Some(snapshot) => self.recover_members(snapshot.members).boxed(),
            None => {
                error!("Failed to deserialize config state machine snapshot. Config recovery failed.");
                // Return empty future - state machine will start with empty config
                future::ready(()).boxed()
            }
        }
    }
    fn recoverable(&self) -> bool {
        true
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::StateMachineCtl;

    #[test]
    fn test_configures_new() {
        let service_id = 12345u64;
        let config = Configures::new(service_id);

        assert_eq!(config.service_id, service_id);
        assert!(config.members.is_empty());
        assert_eq!(config.id(), CONFIG_SM_ID);
    }

    #[test]
    fn test_configures_id() {
        let config = Configures::new(1);
        assert_eq!(config.id(), CONFIG_SM_ID);
        assert_eq!(CONFIG_SM_ID, 1);
    }

    #[test]
    fn test_configures_recoverable() {
        let config = Configures::new(1);
        assert!(config.recoverable());
    }

    #[test]
    fn test_member_existed() {
        let config = Configures::new(1);
        let member_id = hash_str(&String::from("test_member"));

        assert!(!config.member_existed(member_id));
        assert!(!config.member_existed(123456));
        assert!(!config.member_existed(0));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_member_address() {
        let config = Configures::new(1);
        let addresses = config.member_address().await;
        assert!(addresses.is_empty());
    }

    #[test]
    fn test_snapshot_empty() {
        let config = Configures::new(1);
        let snapshot = config.snapshot();

        assert!(!snapshot.is_empty());

        let deserialized: Option<ConfigSnapshot> =
            crate::utils::serde::deserialize(&snapshot);
        assert!(deserialized.is_some());

        let snapshot_data = deserialized.unwrap();
        assert!(snapshot_data.members.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_empty_snapshot() {
        let mut config = Configures::new(1);
        let snapshot = config.snapshot();

        config.recover(snapshot).await;

        assert!(config.members.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_invalid_data() {
        let mut config = Configures::new(1);

        // Try to recover from invalid data
        let invalid_data = vec![0xFF, 0xFF, 0xFF];
        config.recover(invalid_data).await;

        // Should not crash, just log error and continue with empty config
        assert!(config.members.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_del_member() {
        let mut config = Configures::new(1);

        // Delete non-existent member should not crash
        config.del_member(String::from("non_existent")).await;

        assert!(config.members.is_empty());
    }

    #[test]
    fn test_config_snapshot_serialization() {
        let mut snapshot = ConfigSnapshot {
            members: HashSet::new(),
        };
        snapshot.members.insert(String::from("member1"));
        snapshot.members.insert(String::from("member2"));

        let serialized = crate::utils::serde::serialize(&snapshot);
        let deserialized: Option<ConfigSnapshot> = crate::utils::serde::deserialize(&serialized);

        assert!(deserialized.is_some());
        let recovered = deserialized.unwrap();
        assert_eq!(recovered.members.len(), 2);
        assert!(recovered.members.contains("member1"));
        assert!(recovered.members.contains("member2"));
    }

    #[test]
    fn test_configures_has_subscriptions() {
        let config = Configures::new(1);

        // Verify subscriptions Arc is initialized
        assert_eq!(Arc::strong_count(&config.subscriptions), 1);
    }

    // ============================================================================
    // REAL-WORLD MEMBERSHIP CHANGE SCENARIOS
    // ============================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_server_join() {
        use crate::rpc::Server;

        let mut config = Configures::new(1);

        // Initially no members
        assert!(config.members.is_empty());
        assert_eq!(config.member_address().await.len(), 0);

        // Start a test server for the member to join
        let member_addr = String::from("127.0.0.1:4100");
        let server = Server::new(&member_addr);
        Server::listen_and_resume(&server).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Attempt to add new member
        let result = config.new_member(member_addr.clone()).await;
        assert!(result, "Member should join successfully");

        // Verify member was added
        let member_id = hash_str(&member_addr);
        assert!(config.member_existed(member_id));
        assert_eq!(config.members.len(), 1);

        let addresses = config.member_address().await;
        assert_eq!(addresses.len(), 1);
        assert!(addresses.contains(&member_addr));

        // Cleanup
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_server_leave() {
        use crate::rpc::Server;

        let mut config = Configures::new(1);

        // Add a member first
        let member_addr = String::from("127.0.0.1:4200");
        let server = Server::new(&member_addr);
        Server::listen_and_resume(&server).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        config.new_member(member_addr.clone()).await;
        let member_id = hash_str(&member_addr);
        assert!(config.member_existed(member_id));

        // Now remove the member
        config.del_member(member_addr.clone()).await;

        // Verify member was removed
        assert!(!config.member_existed(member_id));
        assert_eq!(config.members.len(), 0);

        let addresses = config.member_address().await;
        assert_eq!(addresses.len(), 0);

        // Cleanup
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_server_rejoin() {
        use crate::rpc::Server;

        let mut config = Configures::new(1);

        let member_addr = String::from("127.0.0.1:4300");
        let server = Server::new(&member_addr);
        Server::listen_and_resume(&server).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Initial join
        let result1 = config.new_member(member_addr.clone()).await;
        assert!(result1, "First join should succeed");

        let member_id = hash_str(&member_addr);
        assert!(config.member_existed(member_id));

        // Leave
        config.del_member(member_addr.clone()).await;
        assert!(!config.member_existed(member_id));

        // Rejoin - should succeed
        let result2 = config.new_member(member_addr.clone()).await;
        assert!(result2, "Rejoin should succeed");
        assert!(config.member_existed(member_id));

        // Cleanup
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_multiple_servers_join() {
        use crate::rpc::Server;

        let mut config = Configures::new(1);

        // Add multiple servers
        let addrs = vec![
            String::from("127.0.0.1:4400"),
            String::from("127.0.0.1:4401"),
            String::from("127.0.0.1:4402"),
        ];

        let mut servers = vec![];
        for addr in &addrs {
            let server = Server::new(addr);
            Server::listen_and_resume(&server).await;
            servers.push(server);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Join all servers
        for addr in &addrs {
            let result = config.new_member(addr.clone()).await;
            assert!(result, "Server {} should join", addr);
        }

        // Verify all are members
        assert_eq!(config.members.len(), 3);
        let member_addrs = config.member_address().await;
        assert_eq!(member_addrs.len(), 3);

        for addr in &addrs {
            assert!(member_addrs.contains(addr), "Should contain {}", addr);
            let member_id = hash_str(addr);
            assert!(config.member_existed(member_id));
        }

        // Cleanup
        for server in servers {
            server.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_partial_leave() {
        use crate::rpc::Server;

        let mut config = Configures::new(1);

        let addrs = vec![
            String::from("127.0.0.1:4500"),
            String::from("127.0.0.1:4501"),
            String::from("127.0.0.1:4502"),
        ];

        let mut servers = vec![];
        for addr in &addrs {
            let server = Server::new(addr);
            Server::listen_and_resume(&server).await;
            servers.push(server);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // All join
        for addr in &addrs {
            config.new_member(addr.clone()).await;
        }

        assert_eq!(config.members.len(), 3);

        // Remove middle server
        config.del_member(addrs[1].clone()).await;

        // Verify only 2 remain
        assert_eq!(config.members.len(), 2);

        let member_addrs = config.member_address().await;
        assert!(member_addrs.contains(&addrs[0]));
        assert!(!member_addrs.contains(&addrs[1])); // Removed
        assert!(member_addrs.contains(&addrs[2]));

        // Cleanup
        for server in servers {
            server.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_duplicate_join_attempt() {
        use crate::rpc::Server;

        let mut config = Configures::new(1);

        let member_addr = String::from("127.0.0.1:4600");
        let server = Server::new(&member_addr);
        Server::listen_and_resume(&server).await;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // First join
        let result1 = config.new_member(member_addr.clone()).await;
        assert!(result1);

        // Attempt duplicate join - should fail gracefully
        let result2 = config.new_member(member_addr.clone()).await;
        assert!(!result2, "Duplicate join should fail");

        // Still only one member
        assert_eq!(config.members.len(), 1);

        // Cleanup
        server.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_snapshot_with_members() {
        use crate::rpc::Server;

        let mut config = Configures::new(1);

        let addrs = vec![
            String::from("127.0.0.1:4700"),
            String::from("127.0.0.1:4701"),
        ];

        let mut servers = vec![];
        for addr in &addrs {
            let server = Server::new(addr);
            Server::listen_and_resume(&server).await;
            servers.push(server);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Add members
        for addr in &addrs {
            config.new_member(addr.clone()).await;
        }

        // Take snapshot
        let snapshot = config.snapshot();
        assert!(!snapshot.is_empty());

        // Verify snapshot contains members
        let snapshot_data: Option<ConfigSnapshot> = crate::utils::serde::deserialize(&snapshot);
        assert!(snapshot_data.is_some());

        let snapshot_data = snapshot_data.unwrap();
        assert_eq!(snapshot_data.members.len(), 2);
        for addr in &addrs {
            assert!(snapshot_data.members.contains(addr));
        }

        // Cleanup
        for server in servers {
            server.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_recovery_with_changes() {
        use crate::rpc::Server;

        let mut config1 = Configures::new(1);

        let addrs = vec![
            String::from("127.0.0.1:4800"),
            String::from("127.0.0.1:4801"),
        ];

        let mut servers = vec![];
        for addr in &addrs {
            let server = Server::new(addr);
            Server::listen_and_resume(&server).await;
            servers.push(server);
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Add members to first config
        for addr in &addrs {
            config1.new_member(addr.clone()).await;
        }

        // Take snapshot
        let snapshot = config1.snapshot();

        // Create new config and recover
        let mut config2 = Configures::new(1);
        config2.recover(snapshot).await;

        // Add the third server
        let new_addr = String::from("127.0.0.1:4802");
        let new_server = Server::new(&new_addr);
        Server::listen_and_resume(&new_server).await;
        servers.push(new_server);

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // The recovered config should be able to add new members
        let result = config2.new_member(new_addr.clone()).await;
        assert!(result);

        // Cleanup
        for server in servers {
            server.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_leave_nonexistent() {
        let mut config = Configures::new(1);

        // Try to remove a member that doesn't exist - should not crash
        config.del_member(String::from("127.0.0.1:9999")).await;

        assert_eq!(config.members.len(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_membership_join_unreachable_server() {
        let mut config = Configures::new(1);

        // Try to add a member that's not actually running
        let unreachable_addr = String::from("127.0.0.1:9998");
        let result = config.new_member(unreachable_addr.clone()).await;

        // Should fail because server is not reachable
        assert!(!result, "Join should fail for unreachable server");
        assert_eq!(config.members.len(), 0);
    }
}
