use bifrost::raft::*;
use bifrost::raft::state_machine::master::ExecError;
use bifrost::rpc::Server;
use std::fs::File;
use super::wait;

#[test]
fn startup(){
    let server = RaftService::new(Options {
        storage: Storage::Default(),
        address: String::from("127.0.0.1:2000"),
        service_id: DEFAULT_SERVICE_ID,
    });
    assert!(server.is_some());
}

#[test]
fn server_membership(){
    let s1_addr = String::from("127.0.0.1:2001");
    let s2_addr = String::from("127.0.0.1:2002");
    let s3_addr = String::from("127.0.0.1:2003");
    let service1 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s1_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    assert!(service1.is_some());
    let service1 = service1.unwrap();
    let server1 = Server::new(vec!((DEFAULT_SERVICE_ID, service1.clone())));
    Server::listen_and_resume(server1, &s1_addr);
    RaftService::start(&service1);
    service1.bootstrap();
    assert_eq!(service1.num_members(), 1);
    let service2 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s2_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    assert!(service2.is_some());
    let service2 = service2.unwrap();
    let server2 = Server::new(vec!((DEFAULT_SERVICE_ID, service2.clone())));
    Server::listen_and_resume(server2, &s2_addr);
    RaftService::start(&service2);
    let join_result = service2.join(vec!(s1_addr.clone()));
    match join_result {
        Err(ExecError::ServersUnreachable) => panic!("Server unreachable"),
        Err(ExecError::CannotConstructClient) => panic!("Cannot Construct Client"),
        Err(e) => panic!(e),
        Ok(_) => {}
    }
    assert!(join_result.is_ok());
    assert_eq!(service1.num_members(), 2);
    assert_eq!(service2.num_members(), 2);
    let service3 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s3_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    assert!(service3.is_some());
    let service3 = service3.unwrap();
    let server3 = Server::new(vec!((DEFAULT_SERVICE_ID, service3.clone())));
    Server::listen_and_resume(server3, &s3_addr);
    RaftService::start(&service3);
    let join_result = service3.join(vec!(
        s1_addr.clone(),
        s2_addr.clone(),
    ));
    join_result.unwrap();
    assert_eq!(service1.num_members(), 3);
    assert_eq!(service3.num_members(), 3);

    // check in service2. Although it is a log replication problem but membership changes should take effect immediately
    assert_eq!(service2.num_members(), 3);

    // test remove member
    assert!(service2.leave());
    assert_eq!(service1.num_members(), 2);
    assert_eq!(service3.num_members(), 2);

    //test remove leader
    assert_eq!(service1.leader_id(), service1.id);
    assert!(service1.leave());
    wait(); // there will be some unavailability in leader transaction
    assert_eq!(service3.leader_id(), service3.id);
    assert_eq!(service3.num_members(), 1);
}

#[test]
fn log_replication(){
    let s1_addr = String::from("127.0.0.1:2004");
    let s2_addr = String::from("127.0.0.1:2005");
    let s3_addr = String::from("127.0.0.1:2006");
    let s4_addr = String::from("127.0.0.1:2007");
    let s5_addr = String::from("127.0.0.1:2008");
    let service1 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s1_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    }).unwrap();
    let service2 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s2_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    }).unwrap();
    let service3 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s3_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    }).unwrap();
    let service4 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s4_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    }).unwrap();
    let service5 = RaftService::new(Options {
        storage: Storage::Default(),
        address: s5_addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    }).unwrap();


    let server1 = Server::new(vec!((DEFAULT_SERVICE_ID, service1.clone())));
    Server::listen_and_resume(server1, &s1_addr);
    RaftService::start(&service1);
    service1.bootstrap();


    let server2 = Server::new(vec!((DEFAULT_SERVICE_ID, service2.clone())));
    Server::listen_and_resume(server2, &s2_addr);
    RaftService::start(&service2);
    let join_result = service2.join(vec!(
        s1_addr.clone(),
        s2_addr.clone(),
    ));
    join_result.unwrap();

    let server3 = Server::new(vec!((DEFAULT_SERVICE_ID, service3.clone())));
    Server::listen_and_resume(server3, &s3_addr);
    RaftService::start(&service3);
    let join_result = service3.join(vec!(
        s1_addr.clone(),
        s2_addr.clone(),
    ));
    join_result.unwrap();

    let server4 = Server::new(vec!((DEFAULT_SERVICE_ID, service4.clone())));
    Server::listen_and_resume(server4, &s4_addr);
    RaftService::start(&service4);
    let join_result = service4.join(vec!(
        s1_addr.clone(),
        s2_addr.clone(),
        s3_addr.clone(),
    ));
    join_result.unwrap();

    let server5 = Server::new(vec!((DEFAULT_SERVICE_ID, service5.clone())));
    Server::listen_and_resume(server5, &s5_addr);
    RaftService::start(&service5);
    let join_result = service5.join(vec!(
        s1_addr.clone(),
        s2_addr.clone(),
        s3_addr.clone(),
        s4_addr.clone(),
    ));
    join_result.unwrap();

    wait(); // wait for membership replication to take effect

    assert_eq!(service1.num_logs(), service2.num_logs());
    assert_eq!(service2.num_logs(), service3.num_logs());
    assert_eq!(service3.num_logs(), service4.num_logs());
    assert_eq!(service4.num_logs(), service5.num_logs());
    assert_eq!(service5.num_logs(), 4); // check all logs replicated

    wait();

    assert_eq!(service1.leader_id(), service1.id);
    assert_eq!(service2.leader_id(), service1.id);
    assert_eq!(service3.leader_id(), service1.id);
    assert_eq!(service4.leader_id(), service1.id);
    assert_eq!(service5.leader_id(), service1.id);
}
