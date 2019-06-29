use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::callback::client::SubscriptionService;
use bifrost::raft::*;
use bifrost::rpc::Server;
use bifrost::store::value::string;
use bifrost::store::value::string::client::SMClient;
use futures::prelude::*;

#[test]
fn string() {
    let addr = String::from("127.0.0.1:2010");
    let original_string = String::from("The stored text");
    let altered_string = String::from("The altered text");
    let mut string_sm = string::Value::new_by_name(&String::from("test"), original_string.clone());
    let service = RaftService::new(Options {
        storage: Storage::default(),
        address: addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    let sm_id = string_sm.id;
    let server = Server::new(&addr);
    string_sm.init_callback(&service);
    server.register_service(DEFAULT_SERVICE_ID, &service);
    Server::listen_and_resume(&server);
    assert!(RaftService::start(&service));
    service.register_state_machine(Box::new(string_sm));
    service.bootstrap();

    let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID).unwrap();
    let sm_client = SMClient::new(sm_id, &client);
    let unchanged_str = original_string.clone();
    let changed_str = altered_string.clone();
    RaftClient::prepare_subscription(&server);
    //    sm_client.on_changed(move |res| {
    //        if let Ok((old, new)) = res {
    //            println!("GOT VAL CALLBACK {:?} -> {:?}", old, new);
    //            assert_eq!(old, unchanged_str);
    //            assert_eq!(new, changed_str);
    //        }
    //    }).unwrap().unwrap();
    assert_eq!(&sm_client.get().wait().unwrap().unwrap(), &original_string);
    sm_client.set(&altered_string).wait().unwrap().unwrap();
    assert_eq!(&sm_client.get().wait().unwrap().unwrap(), &altered_string);
}
