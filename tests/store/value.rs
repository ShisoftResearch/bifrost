use bifrost::raft::*;
use bifrost::raft::client::RaftClient;
use bifrost::store::value::string;
use bifrost::store::value::string::client::SMClient;
use bifrost::rpc::Server;
use bifrost::raft::state_machine::callback::client::SubscriptionService;

#[test]
fn string(){
    let addr = String::from("127.0.0.1:2010");
    let original_string = String::from("The stored text");
    let altered_string = String::from("The altered text");
    let mut string_sm = string::Value::new_by_name(
        &String::from("test"),
        original_string.clone()
    );
    let service = RaftService::new(Options{
        storage: Storage::Default(),
        address: addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    let sm_id = string_sm.id;
    let server = Server::new(vec!((DEFAULT_SERVICE_ID, service.clone())));
    string_sm.init_callback(&service);
    Server::listen_and_resume(&server, &addr);
    assert!(RaftService::start(&service));
    service.register_state_machine(Box::new(string_sm));
    service.bootstrap();

    let client = RaftClient::new(vec!(addr), DEFAULT_SERVICE_ID).unwrap();
    let sub_service = SubscriptionService::initialize(&server);
    let sm_client = SMClient::new(sm_id, &client);
    let unchanged_str = original_string.clone();
    let changed_str = altered_string.clone();
    client.set_subscription(&sub_service);
//    sm_client.on_changed(move |res| {
//        if let Ok((old, new)) = res {
//            println!("GOT VAL CALLBACK {:?} -> {:?}", old, new);
//            assert_eq!(old, unchanged_str);
//            assert_eq!(new, changed_str);
//        }
//    });
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