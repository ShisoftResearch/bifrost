// Group membership manager regardless actual raft members

pub mod client;
pub mod member;
pub mod server;

use crate::membership::client::Member as ClientMember;
use bifrost_plugins::hash_ident;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_MEMBERSHIP_SERVICE) as u64;

pub mod raft {
    use super::*;
    raft_state_machine! {
        def cmd hb_online_changed(online: Vec<u64>, offline: Vec<u64>);
        def cmd join(address: String) -> Option<u64>;
        def cmd leave(id: u64) -> bool;
        def cmd join_group(group_name: String, id: u64) -> bool;
        def cmd leave_group(group: u64, id: u64) -> bool;
        def cmd new_group(name: String) -> Result<u64, u64>;
        def cmd del_group(id: u64) -> bool;
        def qry group_leader(group: u64) -> Option<(Option<ClientMember>, u64)>;
        def qry group_members (group: u64, online_only: bool) -> Option<(Vec<ClientMember>, u64)>;
        def qry all_members (online_only: bool) -> (Vec<ClientMember>, u64);
        def sub on_group_member_offline(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_offline() -> (ClientMember, u64); //
        def sub on_group_member_online(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_online() -> (ClientMember, u64); //
        def sub on_group_member_joined(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_joined() -> (ClientMember, u64); //
        def sub on_group_member_left(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_left() -> (ClientMember, u64); //
        def sub on_group_leader_changed(group: u64) -> (Option<ClientMember>, Option<ClientMember>, u64);
    }
}

// The service only responsible for receiving heartbeat and
// Updating last updated time
// Expired update time will trigger timeout in the raft state machine
mod heartbeat_rpc {
    service! {
        rpc ping(id: u64);
    }
}

#[cfg(test)]
mod test {
    use crate::membership::client::ObserverClient;
    use crate::membership::member::MemberService;
    use crate::membership::server::Membership;
    use crate::raft::client::RaftClient;
    use crate::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use futures::prelude::*;
    use std::sync::atomic::*;
    use std::sync::Arc;

    #[tokio::test(flavor = "multi_thread")]
    async fn primary() {
        let _ = env_logger::builder().format_timestamp(None).try_init();
        let addr = String::from("127.0.0.1:2100");
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        info!("Creating server");
        let server = Server::new(&addr);
        info!("Register service");
        server
            .register_service(DEFAULT_SERVICE_ID, &raft_service)
            .await;
        info!("Server listen and resume");
        Server::listen_and_resume(&server).await;
        info!("Start raft service");
        RaftService::start(&raft_service).await;
        info!("Bootstrap raft service");
        raft_service.bootstrap().await;
        info!("Creating membership service");
        Membership::new(&server, &raft_service).await;

        let group_1 = String::from("test_group_1");
        let group_2 = String::from("test_group_2");
        let group_3 = String::from("test_group_3");

        info!("Creating raft client");
        let wild_raft_client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();

        info!("Create observer");
        let client = ObserverClient::new(&wild_raft_client);

        info!("Prepare subscription");
        RaftClient::prepare_subscription(&server).await;

        info!("Creating new group: {}", group_1);
        client.new_group(&group_1).await.unwrap().unwrap();
        info!("Creating new group {}", group_2);
        client.new_group(&group_2).await.unwrap().unwrap();
        info!("Creating new group {}", group_3);
        client.new_group(&group_3).await.unwrap().unwrap();

        let any_member_joined_count = Arc::new(AtomicUsize::new(0));
        let any_member_left_count = Arc::new(AtomicUsize::new(0));
        let any_member_offline_count = Arc::new(AtomicUsize::new(0));
        let any_member_online_count = Arc::new(AtomicUsize::new(0));
        let group_leader_changed_count = Arc::new(AtomicUsize::new(0));
        let group_member_joined_count = Arc::new(AtomicUsize::new(0));
        let group_member_left_count = Arc::new(AtomicUsize::new(0));
        let group_member_online_count = Arc::new(AtomicUsize::new(0));
        let group_member_offline_count = Arc::new(AtomicUsize::new(0));

        let any_member_joined_count_clone = any_member_joined_count.clone();
        let any_member_left_count_clone = any_member_left_count.clone();
        let any_member_offline_count_clone = any_member_offline_count.clone();
        let any_member_online_count_clone = any_member_online_count.clone();
        let group_leader_changed_count_clone = group_leader_changed_count.clone();
        let group_member_joined_count_clone = group_member_joined_count.clone();
        let group_member_left_count_clone = group_member_left_count.clone();
        let group_member_online_count_clone = group_member_online_count.clone();
        let group_member_offline_count_clone = group_member_offline_count.clone();

        info!("Subscribe on_any_member_joined");
        client
            .on_any_member_joined(move |_| {
                any_member_joined_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            })
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_any_member_left");
        client
            .on_any_member_left(move |_| {
                any_member_left_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            })
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_any_member_offline");
        client
            .on_any_member_offline(move |_| {
                any_member_offline_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            })
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_any_member_online");
        client
            .on_any_member_online(move |_| {
                any_member_online_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            })
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_group_leader_changed");
        client
            .on_group_leader_changed(
                move |_| {
                    group_leader_changed_count_clone.fetch_add(1, Ordering::Relaxed);
                    future::ready(()).boxed()
                },
                &group_1,
            )
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_group_member_joined");
        client
            .on_group_member_joined(
                move |_| {
                    group_member_joined_count_clone.fetch_add(1, Ordering::Relaxed);
                    future::ready(()).boxed()
                },
                &group_1,
            )
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_group_member_left");
        client
            .on_group_member_left(
                move |_| {
                    group_member_left_count_clone.fetch_add(1, Ordering::Relaxed);
                    future::ready(()).boxed()
                },
                &group_1,
            )
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_group_member_online");
        client
            .on_group_member_online(
                move |_| {
                    group_member_online_count_clone.fetch_add(1, Ordering::Relaxed);
                    future::ready(()).boxed()
                },
                &group_1,
            )
            .await
            .unwrap()
            .unwrap();

        info!("Subscribe on_group_member_offline");
        client
            .on_group_member_offline(
                move |_| {
                    group_member_offline_count_clone.fetch_add(1, Ordering::Relaxed);
                    future::ready(()).boxed()
                },
                &group_1,
            )
            .await
            .unwrap()
            .unwrap();

        info!("New member1_raft_client");
        let member1_raft_client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        let member1_addr = String::from("server1");
        info!("New member service {}", member1_addr);
        let member1_svr = MemberService::new(&member1_addr, &member1_raft_client, &raft_service).await;

        info!("New member2_raft_client");
        let member2_raft_client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        let member2_addr = String::from("server2");
        info!("New member service {}", member2_addr);
        let member2_svr = MemberService::new(&member2_addr, &member2_raft_client, &raft_service).await;

        info!("New member3_raft_client");
        let member3_raft_client = RaftClient::new(&vec![addr.clone()], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        let member3_addr = String::from("server3");
        info!("New member service {}", member3_addr);
        let member3_svr = MemberService::new(&member3_addr, &member3_raft_client, &raft_service).await;

        info!("Member 1 join group 1");
        member1_svr.join_group(&group_1).await.unwrap();
        info!("Member 2 join group 1");
        member2_svr.join_group(&group_1).await.unwrap();
        info!("Member 3 join group 1");
        member3_svr.join_group(&group_1).await.unwrap();

        info!("Member 1 join group 2");
        member1_svr.join_group(&group_2).await.unwrap();
        info!("Member 2 join group 2");
        member2_svr.join_group(&group_2).await.unwrap();

        info!("Member 1 join group 3");
        member1_svr.join_group(&group_3).await.unwrap();

        info!("Checking group members after join");
        assert_eq!(
            member1_svr
                .client()
                .all_members(false)
                .await
                .unwrap()
                .0
                .len(),
            3
        );
        assert_eq!(
            member1_svr
                .client()
                .all_members(true)
                .await
                .unwrap()
                .0
                .len(),
            3
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_1, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            3
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_1, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            3
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_2, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            2
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_2, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            2
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_3, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            1
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_3, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            1
        );

        member1_svr.close(); // close only end the heartbeat thread

        info!("############### Waiting for membership changes ###############");
        for i in 0..10 {
            async_wait_secs().await;
        }
        info!("*************** Checking members ***************");

        assert_eq!(
            member1_svr
                .client()
                .all_members(false)
                .await
                .unwrap()
                .0
                .len(),
            3
        );
        assert_eq!(
            member1_svr
                .client()
                .all_members(true)
                .await
                .unwrap()
                .0
                .len(),
            2
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_1, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            3
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_1, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            2
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_2, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            2
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_2, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            1
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_3, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            1
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_3, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            0
        );

        member2_svr.leave().await.unwrap(); // leave will report to the raft servers to remove it from the list
        assert_eq!(
            member1_svr
                .client()
                .all_members(false)
                .await
                .unwrap()
                .0
                .len(),
            2
        );
        assert_eq!(
            member1_svr
                .client()
                .all_members(true)
                .await
                .unwrap()
                .0
                .len(),
            1
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_1, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            2
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_1, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            1
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_2, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            1
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_2, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            0
        );

        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_3, false)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            1
        );
        assert_eq!(
            member1_svr
                .client()
                .group_members(&group_3, true)
                .await
                .unwrap()
                .unwrap()
                .0
                .len(),
            0
        );

        async_wait_secs().await;

        info!("=========== Checking event trigger ===========");
        assert_eq!(any_member_joined_count.load(Ordering::Relaxed), 3);
        assert_eq!(any_member_left_count.load(Ordering::Relaxed), 1);
        assert_eq!(any_member_offline_count.load(Ordering::Relaxed), 1);
        assert_eq!(any_member_online_count.load(Ordering::Relaxed), 0); // no server online from offline
        assert!(group_leader_changed_count.load(Ordering::Relaxed) > 0); // Number depends on hashing
        assert_eq!(group_member_joined_count.load(Ordering::Relaxed), 3);
        // assert_eq!(group_member_left_count.load(Ordering::Relaxed), 2); // this test case is unstable
        assert_eq!(group_member_online_count.load(Ordering::Relaxed), 0);
        assert_eq!(group_member_offline_count.load(Ordering::Relaxed), 1);
    }
}
