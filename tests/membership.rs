use bifrost::rpc::*;
use bifrost::raft::*;
use bifrost::membership::server::Membership;
use bifrost::membership::member::MemberService;
use bifrost::membership::client::Client;
use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::callback::client::SubscriptionService;

use std::mem::forget;

use raft::wait;

#[test]
fn primary() {
    let addr = String::from("127.0.0.1:2100");
    let raft_service = RaftService::new(Options {
        storage: Storage::Default(),
        address: addr.clone(),
        service_id: 0,
    });
    let server = Server::new(vec!((0, raft_service.clone())));
    let heartbeat_service = Membership::new(&server, &raft_service);
    Server::listen_and_resume(&server, &addr);
    RaftService::start(&raft_service);
    raft_service.bootstrap();

    let group_1 = String::from("test_group_1");
    let group_2 = String::from("test_group_2");
    let group_3 = String::from("test_group_3");

    let wild_raft_client = RaftClient::new(vec!(addr.clone()), 0).unwrap();
    let client = Client::new(&wild_raft_client);

    let subs_service = SubscriptionService::initialize(&server);
    wild_raft_client.set_subscription(&subs_service);

    client.new_group(&group_1).unwrap().unwrap();
    client.new_group(&group_2).unwrap().unwrap();
    client.new_group(&group_3).unwrap().unwrap();

    client.on_any_member_joined(|res| {
        println!(">>>>>>>>>>>>>>>>>NEW MEMBER: {:?}", res.unwrap().address);
    }).unwrap().unwrap();

    client.on_any_member_left(|res| {
        println!(">>>>>>>>>>>>>>>>>MEMBER LEFT: {:?}", res.unwrap().address);
    }).unwrap().unwrap();

    client.on_any_member_offline(|res| {
        println!(">>>>>>>>>>>>>>>>>MEMBER OFFLINE: {:?}", res.unwrap().address);
    }).unwrap().unwrap();

    client.on_any_member_online(|res| {
        println!(">>>>>>>>>>>>>>>>>MEMBER ONLINE: {:?}", res.unwrap().address);
    }).unwrap().unwrap();

    client.on_group_leader_changed(|res| {
        println!(">>>>>>>>>>>>>>>>>GROUP LEADER CHANGED: {:?}", res.unwrap());
    }, &group_1).unwrap().unwrap();

    client.on_group_member_joined(|res| {
        println!(">>>>>>>>>>>>>>>>>GROUP MEMBER JOINED: {:?}", res.unwrap());
    }, &group_1).unwrap().unwrap();

    client.on_group_member_left(|res| {
        println!(">>>>>>>>>>>>>>>>>GROUP MEMBER LEFT: {:?}", res.unwrap());
    }, &group_1).unwrap().unwrap();

    client.on_group_member_online(|res| {
        println!(">>>>>>>>>>>>>>>>>GROUP MEMBER ONLINE: {:?}", res.unwrap());
    }, &group_1).unwrap().unwrap();

    client.on_group_member_offline(|res| {
        println!(">>>>>>>>>>>>>>>>>GROUP MEMBER OFFLINE: {:?}", res.unwrap());
    }, &group_1).unwrap().unwrap();

    let member1_raft_client = RaftClient::new(vec!(addr.clone()), 0).unwrap();
    let member1_addr = String::from("server1");
    let member1_svr = MemberService::new(&member1_addr, &member1_raft_client);

    let member2_raft_client = RaftClient::new(vec!(addr.clone()), 0).unwrap();
    let member2_addr = String::from("server2");
    let member2_svr = MemberService::new(&member2_addr, &member2_raft_client);

    let member3_raft_client = RaftClient::new(vec!(addr.clone()), 0).unwrap();
    let member3_addr = String::from("server3");
    let member3_svr = MemberService::new(&member3_addr, &member3_raft_client);

    member1_svr.join_group(&group_1).unwrap().unwrap();
    member2_svr.join_group(&group_1).unwrap().unwrap();
    member3_svr.join_group(&group_1).unwrap().unwrap();

    member1_svr.join_group(&group_2).unwrap().unwrap();
    member2_svr.join_group(&group_2).unwrap().unwrap();

    member1_svr.join_group(&group_3).unwrap().unwrap();

    assert_eq!(member1_svr.client().all_members(false).unwrap().unwrap().len(), 3);
    assert_eq!(member1_svr.client().all_members(true).unwrap().unwrap().len(), 3);

    assert_eq!(member1_svr.client().group_members(&group_1, false).unwrap().unwrap().len(), 3);
    assert_eq!(member1_svr.client().group_members(&group_1, true).unwrap().unwrap().len(), 3);

    assert_eq!(member1_svr.client().group_members(&group_2, false).unwrap().unwrap().len(), 2);
    assert_eq!(member1_svr.client().group_members(&group_2, true).unwrap().unwrap().len(), 2);

    assert_eq!(member1_svr.client().group_members(&group_3, false).unwrap().unwrap().len(), 1);
    assert_eq!(member1_svr.client().group_members(&group_3, true).unwrap().unwrap().len(), 1);

    member1_svr.close(); // close only end the heartbeat thread
    wait();
    wait();
    assert_eq!(member1_svr.client().all_members(false).unwrap().unwrap().len(), 3);
    assert_eq!(member1_svr.client().all_members(true).unwrap().unwrap().len(), 2);

    assert_eq!(member1_svr.client().group_members(&group_1, false).unwrap().unwrap().len(), 3);
    assert_eq!(member1_svr.client().group_members(&group_1, true).unwrap().unwrap().len(), 2);

    assert_eq!(member1_svr.client().group_members(&group_2, false).unwrap().unwrap().len(), 2);
    assert_eq!(member1_svr.client().group_members(&group_2, true).unwrap().unwrap().len(), 1);

    assert_eq!(member1_svr.client().group_members(&group_3, false).unwrap().unwrap().len(), 1);
    assert_eq!(member1_svr.client().group_members(&group_3, true).unwrap().unwrap().len(), 0);

    member2_svr.leave().unwrap().unwrap();// leave will report to the raft servers to remove it from the list
    assert_eq!(member1_svr.client().all_members(false).unwrap().unwrap().len(), 2);
    assert_eq!(member1_svr.client().all_members(true).unwrap().unwrap().len(), 1);

    assert_eq!(member1_svr.client().group_members(&group_1, false).unwrap().unwrap().len(), 2);
    assert_eq!(member1_svr.client().group_members(&group_1, true).unwrap().unwrap().len(), 1);

    assert_eq!(member1_svr.client().group_members(&group_2, false).unwrap().unwrap().len(), 1);
    assert_eq!(member1_svr.client().group_members(&group_2, true).unwrap().unwrap().len(), 0);

    assert_eq!(member1_svr.client().group_members(&group_3, false).unwrap().unwrap().len(), 1);
    assert_eq!(member1_svr.client().group_members(&group_3, true).unwrap().unwrap().len(), 0);

    wait();
}