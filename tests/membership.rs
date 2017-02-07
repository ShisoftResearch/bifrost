use bifrost::rpc::*;
use bifrost::raft::*;
use bifrost::membership::server::Membership;
use bifrost::membership::member::MemberService;
use bifrost::raft::client::RaftClient;

use std::mem::forget;

use raft::wait;

#[test]
fn primary() {
    let addr = String::from("127.0.0.1:2100");
    let raft_service = RaftService::new(Options {
        storage: Storage::Default(),
        address: addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    let server = Server::new(vec!((0, raft_service.clone())));
    let heartbeat_service = Membership::new(&server, &raft_service);
    Server::listen_and_resume(server.clone(), &addr);
    RaftService::start(&raft_service);
    raft_service.bootstrap();

    let member1_raft_client = RaftClient::new(vec!(addr.clone()), 0).unwrap();
    let member1_addr = String::from("server1");
    let member1_svr = MemberService::new(member1_addr, &member1_raft_client);

    let member2_raft_client = RaftClient::new(vec!(addr.clone()), 0).unwrap();
    let member2_addr = String::from("server2");
    let member2_svr = MemberService::new(member2_addr, &member2_raft_client);

    let member3_raft_client = RaftClient::new(vec!(addr.clone()), 0).unwrap();
    let member3_addr = String::from("server3");
    let member3_svr = MemberService::new(member3_addr, &member3_raft_client);

    assert_eq!(member1_svr.client().all_members(false).unwrap().unwrap().len(), 3);

    member1_svr.close();
    wait();
    wait();
    assert_eq!(member1_svr.client().all_members(false).unwrap().unwrap().len(), 3);
    assert_eq!(member1_svr.client().all_members(true).unwrap().unwrap().len(), 2);

    member2_svr.leave();
    assert_eq!(member1_svr.client().all_members(true).unwrap().unwrap().len(), 1);
}