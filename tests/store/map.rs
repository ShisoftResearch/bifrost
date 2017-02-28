use bifrost::raft::*;
use bifrost::raft::state_machine::callback::client::SubscriptionService;
use bifrost::raft::client::RaftClient;
use bifrost::store::map::string_string_hashmap;
use bifrost::store::map::string_string_hashmap::client::SMClient;
use bifrost::rpc::*;

use std::collections::{HashSet, HashMap};
use std::iter::FromIterator;

use raft::wait;

#[test]
fn hash_map(){
    let addr = String::from("127.0.0.1:2013");
    let mut map_sm = string_string_hashmap::Map::new_by_name(&String::from("test"));
    let raft_service = RaftService::new(Options{
        storage: Storage::Default(),
        address: addr.clone(),
        service_id: DEFAULT_SERVICE_ID,
    });
    let server = Server::new(vec!((DEFAULT_SERVICE_ID, raft_service.clone())));
    Server::listen_and_resume(&server, &addr);
    let sm_id = map_sm.id;
    map_sm.init_callback(&raft_service);
    assert!(RaftService::start(&raft_service));
    raft_service.register_state_machine(Box::new(map_sm));
    raft_service.bootstrap();

    let raft_client = RaftClient::new(&vec!(addr), DEFAULT_SERVICE_ID).unwrap();
    let sm_client = SMClient::new(sm_id, &raft_client);
    let subs_service = SubscriptionService::initialize(&server);
    raft_client.set_subscription(&subs_service);

    let sk1 = String::from("k1");
    let sk2 = String::from("k2");
    let sk3 = String::from("k3");
    let sk4 = String::from("k4");

    let sv1 = String::from("v1");
    let sv2 = String::from("v2");
    let sv3 = String::from("v3");
    let sv4 = String::from("v4");

    let mut inserted_stash = HashMap::new();
    inserted_stash.insert(sk1.clone(), sv1.clone());
    inserted_stash.insert(sk2.clone(), sv2.clone());
    inserted_stash.insert(sk3.clone(), sv3.clone());
    inserted_stash.insert(sk4.clone(), sv4.clone());

    let mut removed_stash = HashMap::new();
    removed_stash.insert(sk2.clone(), sv2.clone());

    sm_client.on_inserted(move |res| {
        if let Ok((key, value)) = res {
            println!("GOT INSERT CALLBACK {:?} -> {:?}", key, value);
            assert_eq!(inserted_stash.get(&key).unwrap(), &value);
        }
    });
    sm_client.on_removed(move |res| {
        if let Ok((key, value)) = res {
            println!("GOT REMOVED CALLBACK {:?} -> {:?}", key, value);
            assert_eq!(removed_stash.get(&key).unwrap(), &value);
        }
    });
    sm_client.on_key_inserted(|res| {
        if let Ok(value) = res {
            println!("GOT K1 CALLBACK {:?}", value);
            assert_eq!(&String::from("v1"), &value);
        }
    }, sk1.clone());
    sm_client.on_key_removed(|res| {
        if let Ok(value) = res {
            println!("GOT K2 CALLBACK {:?}", value);
            assert_eq!(&String::from("v2"), &value);
        }
    }, sk2.clone());
    assert!(sm_client.is_empty().unwrap().unwrap());
    sm_client.insert(sk1.clone(), sv1.clone()).unwrap().unwrap();
    sm_client.insert(sk2.clone(), sv2.clone()).unwrap().unwrap();
    assert!(!sm_client.is_empty().unwrap().unwrap());

    assert_eq!(sm_client.len().unwrap().unwrap(), 2);
    assert_eq!(sm_client.get(sk1.clone()).unwrap().unwrap().unwrap(), sv1.clone());
    assert_eq!(sm_client.get(sk2.clone()).unwrap().unwrap().unwrap(), sv2.clone());

    sm_client.insert_if_absent(sk2.clone(), String::from("kv2")).unwrap().unwrap();
    assert_eq!(sm_client.len().unwrap().unwrap(), 2);
    assert_eq!(sm_client.get(sk2.clone()).unwrap().unwrap().unwrap(), sv2.clone());

    assert_eq!(sm_client.remove(sk2.clone()).unwrap().unwrap().unwrap(), sv2.clone());
    assert_eq!(sm_client.len().unwrap().unwrap(), 1);
    assert!(sm_client.get(sk2.clone()).unwrap().unwrap().is_none());

    sm_client.clear().unwrap().unwrap();
    assert_eq!(sm_client.len().unwrap().unwrap(), 0);

    sm_client.insert(sk1.clone(), sv1.clone()).unwrap().unwrap();
    sm_client.insert(sk2.clone(), sv2.clone()).unwrap().unwrap();
    sm_client.insert(sk3.clone(), sv3.clone()).unwrap().unwrap();
    sm_client.insert(sk4.clone(), sv4.clone()).unwrap().unwrap();
    assert_eq!(sm_client.len().unwrap().unwrap(),  4);

    let remote_keys = sm_client.keys().unwrap().unwrap();
    let remote_keys_set = HashSet::<String>::from_iter(remote_keys.iter().cloned());
    assert_eq!(remote_keys_set.len(), 4);

    let remote_values = sm_client.values().unwrap().unwrap();
    let remote_values_set = HashSet::<String>::from_iter(remote_values.iter().cloned());
    assert_eq!(remote_values_set.len(), 4);

    let remote_entries = sm_client.entries().unwrap().unwrap();
    let remote_entries_set = HashSet::<(String, String)>::from_iter(remote_entries.iter().cloned());
    assert_eq!(remote_entries_set.len(), 4);

    let expected_keys: HashSet<_> = [
        sk1.clone(), sk2.clone(), sk3.clone(), sk4.clone()
    ].iter().cloned().collect();
    let expected_values: HashSet<_> = [
        sv1.clone(), sv2.clone(), sv3.clone(), sv4.clone()
    ].iter().cloned().collect();
    let expected_entries: HashSet<_> = [
        (sk1.clone(), sv1.clone()), (sk2.clone(), sv2.clone()),
        (sk3.clone(), sv3.clone()), (sk4.clone(), sv4.clone())
    ].iter().cloned().collect();

    assert_eq!(remote_keys_set.intersection(&expected_keys).count(), 4);
    assert_eq!(remote_values_set.intersection(&expected_values).count(), 4);
    assert_eq!(remote_entries_set.intersection(&expected_entries).count(), 4);

    let mut expected_hashmap = HashMap::new();
    expected_hashmap.insert(sk1.clone(), sv1.clone());
    expected_hashmap.insert(sk2.clone(), sv2.clone());
    expected_hashmap.insert(sk3.clone(), sv3.clone());
    expected_hashmap.insert(sk4.clone(), sv4.clone());
    assert_eq!(expected_hashmap, sm_client.clone().unwrap().unwrap());

    assert!(sm_client.contains_key(sk1.clone()).unwrap().unwrap());
    assert!(sm_client.contains_key(sk2.clone()).unwrap().unwrap());
    assert!(sm_client.contains_key(sk3.clone()).unwrap().unwrap());
    assert!(sm_client.contains_key(sk4.clone()).unwrap().unwrap());

    wait();
}