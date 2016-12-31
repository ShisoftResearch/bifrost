use bifrost::raft::*;
use bifrost::raft::state_machine::master::ExecError;

#[test]
fn startup(){
    let server = RaftServer::new(Options {
        storage: Storage::Default(),
        address: String::from("127.0.0.1:2000"),
    });
    assert!(server.is_some());
}

#[test]
fn primary_server_membership(){
    let s1_addr = String::from("127.0.0.1:2001");
    let s2_addr = String::from("127.0.0.1:2002");
    let s3_addr = String::from("127.0.0.1:2003");
    let server1 = RaftServer::new(Options {
        storage: Storage::Default(),
        address: s1_addr.clone(),
    });
    assert!(server1.is_some());
    let server1 = server1.unwrap();
    server1.bootstrap();
    assert_eq!(server1.num_members(), 1);
    let server2 = RaftServer::new(Options {
        storage: Storage::Default(),
        address: s2_addr.clone(),
    });
    assert!(server2.is_some());
    let server2 = server2.unwrap();
    let join_result = server2.join(vec!(s1_addr.clone()));
    match join_result {
        Err(ExecError::ServerUnreachable) => panic!("Server unreachable"),
        Err(ExecError::CannotConstructClient) => panic!("Cannot Construct Client"),
        Err(e) => panic!(e),
        Ok(_) => {}
    }
    assert!(join_result.is_ok());
    assert_eq!(server1.num_members(), 2);
    assert_eq!(server2.num_members(), 2);
    let server3 = RaftServer::new(Options {
        storage: Storage::Default(),
        address: s3_addr.clone(),
    });
    assert!(server3.is_some());
    let server3 = server3.unwrap();
    let join_result = server3.join(vec!(
        s1_addr.clone(),
        s2_addr.clone(),
    ));
    assert!(join_result.is_ok());
    assert_eq!(server1.num_members(), 3);
    // will not check in server2 because it is a log replication problem
    assert_eq!(server3.num_members(), 3);
}
