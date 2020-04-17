use bifrost::conshash::weights::Weights;
use bifrost::conshash::{CHError, ConsistentHashing};
use bifrost::membership::client::ObserverClient;
use bifrost::membership::member::MemberService;
use bifrost::membership::server::Membership;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::callback::client::SubscriptionService;
use bifrost::raft::*;
use bifrost::rpc::*;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use crate::raft::wait;

#[tokio::test(threaded_scheduler)]
async fn primary() {
    let addr = String::from("127.0.0.1:2200");
    let raft_service = RaftService::new(Options {
        storage: Storage::default(),
        address: addr.clone(),
        service_id: 0,
    });
    let server = Server::new(&addr);
    let heartbeat_service = Membership::new(&server, &raft_service);
    server.register_service(0, &raft_service);
    Server::listen_and_resume(&server);
    RaftService::start(&raft_service);
    raft_service.bootstrap();

    let group_1 = String::from("test_group_1");
    let group_2 = String::from("test_group_2");
    let group_3 = String::from("test_group_3");

    let server_1 = String::from("server1");
    let server_2 = String::from("server2");
    let server_3 = String::from("server3");

    let wild_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let client = ObserverClient::new(&wild_raft_client);

    RaftClient::prepare_subscription(&server);

    client.new_group(&group_1).await.unwrap().unwrap();
    client.new_group(&group_2).await.unwrap().unwrap();
    client.new_group(&group_3).await.unwrap().unwrap();

    let member1_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let member1_svr = MemberService::new(&server_1, &member1_raft_client).await;

    let member2_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let member2_svr = MemberService::new(&server_2, &member2_raft_client).await;

    let member3_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
    let member3_svr = MemberService::new(&server_3, &member3_raft_client).await;

    member1_svr.join_group(&group_1).await.unwrap();
    member2_svr.join_group(&group_1).await.unwrap();
    member3_svr.join_group(&group_1).await.unwrap();

    member1_svr.join_group(&group_2).await.unwrap();
    member2_svr.join_group(&group_2).await.unwrap();

    member1_svr.join_group(&group_3).await.unwrap();

    let weight_service = Weights::new(&raft_service);

    let ch1 = ConsistentHashing::new(&group_1, &wild_raft_client).await.unwrap();
    let ch2 = ConsistentHashing::new(&group_2, &wild_raft_client).await.unwrap();
    let ch3 = ConsistentHashing::new(&group_3, &wild_raft_client).await.unwrap();

    ch1.set_weight(&server_1, 1).await.unwrap();
    ch1.set_weight(&server_2, 2).await.unwrap();
    ch1.set_weight(&server_3, 3).await.unwrap();

    ch2.set_weight(&server_1, 1).await.unwrap();
    ch2.set_weight(&server_2, 1).await.unwrap();

    ch3.set_weight(&server_1, 2).await.unwrap();

    ch1.init_table().await.unwrap();
    ch2.init_table().await.unwrap();
    ch3.init_table().await.unwrap();

    let ch1_server_node_changes_count = Arc::new(AtomicUsize::new(0));
    let ch1_server_node_changes_count_clone = Arc::new(AtomicUsize::new(0)).clone();
    ch1.watch_server_nodes_range_changed(&server_2, move |r| {
        ch1_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
    });

    let ch2_server_node_changes_count = Arc::new(AtomicUsize::new(0));
    let ch2_server_node_changes_count_clone = Arc::new(AtomicUsize::new(0)).clone();
    ch2.watch_server_nodes_range_changed(&server_2, move |r| {
        ch2_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
    });

    let ch3_server_node_changes_count = Arc::new(AtomicUsize::new(0));
    let ch3_server_node_changes_count_clone = Arc::new(AtomicUsize::new(0)).clone();
    ch3.watch_server_nodes_range_changed(&server_2, move |r| {
        ch3_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
    });

    assert_eq!(ch1.nodes_count(), 6);
    assert_eq!(ch2.nodes_count(), 2);
    assert_eq!(ch3.nodes_count(), 1);

    let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
    for i in 0..30000usize {
        let k = format!("k - {}", i);
        let server = ch1.get_server_by_string(&k).await.unwrap();
        *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
    }
    assert_eq!(ch_1_mapping.get(&server_1).unwrap(), &4936);
    assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &9923);
    assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &15141); // hard coded due to constant

    let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
    for i in 0..30000usize {
        let k = format!("k - {}", i);
        let server = ch2.get_server_by_string(&k).await.unwrap();
        *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
    }
    assert_eq!(ch_2_mapping.get(&server_1).unwrap(), &14967);
    assert_eq!(ch_2_mapping.get(&server_2).unwrap(), &15033);

    let mut ch_3_mapping: HashMap<String, u64> = HashMap::new();
    for i in 0..30000usize {
        let k = format!("k - {}", i);
        let server = ch3.get_server_by_string(&k).await.unwrap();
        *ch_3_mapping.entry(server.clone()).or_insert(0) += 1;
    }
    assert_eq!(ch_3_mapping.get(&server_1).unwrap(), &30000);

    member1_svr.leave().await.unwrap();

    wait().await;

    let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
    for i in 0..30000usize {
        let k = format!("k - {}", i);
        let server = ch1.get_server_by_string(&k).await.unwrap();
        *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
    }
    assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &11932);
    assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &18068);

    let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
    for i in 0..30000usize {
        let k = format!("k - {}", i);
        let server = ch2.get_server_by_string(&k).await.unwrap();
        *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
    }
    assert_eq!(ch_2_mapping.get(&server_2).unwrap(), &30000);

    for i in 0..30000usize {
        let k = format!("k - {}", i);
        assert!(ch3.get_server_by_string(&k).await.is_none()); // no member
    }

    //    wait();
    //    wait();
    //    assert_eq!(ch1_server_node_changes_count.load(Ordering::Relaxed), 1);
    //    assert_eq!(ch2_server_node_changes_count.load(Ordering::Relaxed), 1);
    //    assert_eq!(ch3_server_node_changes_count.load(Ordering::Relaxed), 0);
}
