use bifrost::raft::*;
use bifrost::raft::client::RaftClient;
use bifrost::store::value::string;
use bifrost::store::value::string::client::SMClient;
use bifrost::rpc::Server;

#[test]
fn string(){
    let addr = String::from("127.0.0.1:2010");
    let original_string = String::from("The stored text");
    let altered_string = String::from("The altered text");
    let string_sm = string::Value::new_by_name(
        String::from("test"),
        original_string.clone()
    );
    let service = RaftService::new(Options{
        storage: Storage::Default(),
        address: addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    let sm_id = string_sm.id;
    let server = Server::new(vec!((DEFAULT_SERVICE_ID, service.clone())));
    Server::listen_and_resume(server, &addr);
    assert!(RaftService::start(&service));
    service.register_state_machine(Box::new(string_sm));
    service.bootstrap();

    let client = RaftClient::new(vec!(addr), DEFAULT_SERVICE_ID).unwrap();
    let sm_client = SMClient::new(sm_id, &client);
    assert_eq!(
        sm_client.get().unwrap().unwrap(),
        original_string.clone()
    );
    sm_client.set(altered_string.clone()).unwrap().unwrap();
    assert_eq!(
        sm_client.get().unwrap().unwrap(),
        altered_string.clone()
    );
}