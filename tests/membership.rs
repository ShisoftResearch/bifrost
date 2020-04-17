use bifrost::membership::client::ObserverClient;
use bifrost::membership::member::MemberService;
use bifrost::membership::server::Membership;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::callback::client::SubscriptionService;
use bifrost::raft::*;
use bifrost::rpc::*;

use futures::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::raft::wait;


#[tokio::test(threaded_scheduler)]
async fn primary() {
    let addr = String::from("127.0.0.1:2100");
    let raft_service = RaftService::new(Options {
        storage: Storage::default(),
        address: addr.clone(),
        service_id: 0,
    });
    let server = Server::new(&addr);
    let heartbeat_service = Membership::new(&server, &raft_service);
    server.register_service(0, &raft_service);
    Server::listen_and_resume(&server);
    RaftService::start(&raft_service).await;
    raft_service.bootstrap().await;

    let group_1 = String::from("test_group_1");
    let group_2 = String::from("test_group_2");
    let group_3 = String::from("test_group_3");

    let wild_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let client = ObserverClient::new(&wild_raft_client);

    RaftClient::prepare_subscription(&server);

    client.new_group(&group_1).await.unwrap().unwrap();
    client.new_group(&group_2).await.unwrap().unwrap();
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

    client
        .on_any_member_joined(move |res| {
            any_member_joined_count_clone.fetch_add(1, Ordering::Relaxed);
            future::ready(()).boxed()
        })
        .await
        .unwrap()
        .unwrap();

    client
        .on_any_member_left(move |res| {
            any_member_left_count_clone.fetch_add(1, Ordering::Relaxed);
            future::ready(()).boxed()
        })
        .await
        .unwrap()
        .unwrap();

    client
        .on_any_member_offline(move |res| {
            any_member_offline_count_clone.fetch_add(1, Ordering::Relaxed);
            future::ready(()).boxed()
        })
        .await
        .unwrap()
        .unwrap();

    client
        .on_any_member_online(move |res| {
            any_member_online_count_clone.fetch_add(1, Ordering::Relaxed);
            future::ready(()).boxed()
        })
        .await
        .unwrap()
        .unwrap();

    client
        .on_group_leader_changed(
            move |res| {
                group_leader_changed_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            },
            &group_1,
        )
        .await
        .unwrap()
        .unwrap();

    client
        .on_group_member_joined(
            move |res| {
                group_member_joined_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            },
            &group_1,
        )
        .await
        .unwrap()
        .unwrap();

    client
        .on_group_member_left(
            move |res| {
                group_member_left_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            },
            &group_1,
        )
        .await
        .unwrap()
        .unwrap();

    client
        .on_group_member_online(
            move |res| {
                group_member_online_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            },
            &group_1,
        )
        .await
        .unwrap()
        .unwrap();

    client
        .on_group_member_offline(
            move |res| {
                group_member_offline_count_clone.fetch_add(1, Ordering::Relaxed);
                future::ready(()).boxed()
            },
            &group_1,
        )
        .await
        .unwrap()
        .unwrap();

    let member1_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let member1_addr = String::from("server1");
    let member1_svr = MemberService::new(&member1_addr, &member1_raft_client).await;

    let member2_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let member2_addr = String::from("server2");
    let member2_svr = MemberService::new(&member2_addr, &member2_raft_client).await;

    let member3_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let member3_addr = String::from("server3");
    let member3_svr = MemberService::new(&member3_addr, &member3_raft_client).await;

    member1_svr.join_group(&group_1).await.unwrap();
    member2_svr.join_group(&group_1).await.unwrap();
    member3_svr.join_group(&group_1).await.unwrap();

    member1_svr.join_group(&group_2).await.unwrap();
    member2_svr.join_group(&group_2).await.unwrap();

    member1_svr.join_group(&group_3).await.unwrap();

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

    wait().await;

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

    wait().await;

    assert_eq!(any_member_joined_count.load(Ordering::Relaxed), 3);
    assert_eq!(any_member_left_count.load(Ordering::Relaxed), 1);
    assert_eq!(any_member_offline_count.load(Ordering::Relaxed), 1);
    assert_eq!(any_member_online_count.load(Ordering::Relaxed), 0); // no server online from offline
    assert_eq!(group_leader_changed_count.load(Ordering::Relaxed), 3); // 1 for None -> server1, 2 for server 1 -> server 2, 3 for server 2 -> server 3
    assert_eq!(group_member_joined_count.load(Ordering::Relaxed), 3);
    // assert_eq!(group_member_left_count.load(Ordering::Relaxed), 2); // this test case is unstable
    assert_eq!(group_member_online_count.load(Ordering::Relaxed), 0);
    assert_eq!(group_member_offline_count.load(Ordering::Relaxed), 1);
}
