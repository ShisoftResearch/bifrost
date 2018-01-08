mod u32 {
    use bifrost::raft::*;
    use bifrost::raft::client::RaftClient;
    use bifrost::store::number::U32;
    use bifrost::store::number::U32::commands::{
        set, get,
        get_and_add, add_and_get,
        get_and_minus, minus_and_get,
        get_and_incr, incr_and_get,
        get_and_decr, decr_and_get,
        get_and_multiply, multiply_and_get,
        get_and_divide, divide_and_get,
        compare_and_swap, swap
    };
    use bifrost::store::number::U32::client::SMClient;
    use bifrost::rpc::Server;
    use bifrost::raft::state_machine::callback::client::SubscriptionService;

    #[test]
    fn test(){
        let addr = String::from("127.0.0.1:2011");
        let mut num_sm = U32::Number::new_by_name(
            &String::from("test"),
            0
        );
        let service = RaftService::new(Options{
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let sm_id = num_sm.id;
        let server = Server::new(&addr);
        server.register_service(DEFAULT_SERVICE_ID, &service);
        Server::listen_and_resume(&server);
        num_sm.init_callback(&service);
        assert!(RaftService::start(&service));
        service.register_state_machine(Box::new(num_sm));
        service.bootstrap();

        let client = RaftClient::new(&vec!(addr), DEFAULT_SERVICE_ID).unwrap();
        let sm_client = SMClient::new(sm_id, &client);
        RaftClient::prepare_subscription(&server);

        sm_client.on_changed(|res| {
           if let Ok((old, new)) = res {
               println!("GOT NUM CHANGED: {} -> {}", old, new);
           }
        });

        assert_eq!(sm_client.get().unwrap().unwrap(), 0);
        sm_client.set(&1).unwrap().unwrap();
        assert_eq!(sm_client.get().unwrap().unwrap(), 1);
        assert_eq!(sm_client.get_and_add(&2).unwrap().unwrap(), 1);
        assert_eq!(sm_client.add_and_get(&3).unwrap().unwrap(), 6);
        assert_eq!(sm_client.get_and_minus(&4).unwrap().unwrap(), 6);
        assert_eq!(sm_client.minus_and_get(&2).unwrap().unwrap(), 0);
        assert_eq!(sm_client.get_and_incr().unwrap().unwrap(), 0);
        assert_eq!(sm_client.incr_and_get().unwrap().unwrap(), 2);
        assert_eq!(sm_client.get_and_multiply(&2).unwrap().unwrap(), 2);
        assert_eq!(sm_client.multiply_and_get(&2).unwrap().unwrap(), 8);
        assert_eq!(sm_client.get_and_divide(&2).unwrap().unwrap(), 8);
        assert_eq!(sm_client.divide_and_get(&4).unwrap().unwrap(), 1);
        assert_eq!(sm_client.swap(&5).unwrap().unwrap(), 1);
        assert_eq!(sm_client.get().unwrap().unwrap(), 5);
        assert_eq!(sm_client.compare_and_swap(&1, &10).unwrap().unwrap(), 5);
        assert_eq!(sm_client.get().unwrap().unwrap(), 5);
        assert_eq!(sm_client.compare_and_swap(&5 ,&11).unwrap().unwrap(), 5);
        assert_eq!(sm_client.get().unwrap().unwrap(), 11);
    }
}

mod f64 {
    use bifrost::raft::*;
    use bifrost::raft::client::RaftClient;
    use bifrost::store::number::F64;
    use bifrost::store::number::F64::commands::{
        set, get,
        get_and_add, add_and_get,
        get_and_minus, minus_and_get,
        get_and_incr, incr_and_get,
        get_and_decr, decr_and_get,
        get_and_multiply, multiply_and_get,
        get_and_divide, divide_and_get,
        compare_and_swap, swap
    };
    use bifrost::rpc::Server;
    use bifrost::store::number::F64::client::SMClient;

    #[test]
    fn test(){
        let addr = String::from("127.0.0.1:2012");
        let num_sm = F64::Number::new_by_name(
            &String::from("test"),
            0.0
        );
        let service = RaftService::new(Options{
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let sm_id = num_sm.id;
        let server = Server::new(&addr);
        server.register_service(DEFAULT_SERVICE_ID, &service);
        Server::listen_and_resume(&server);
        assert!(RaftService::start(&service));
        service.register_state_machine(Box::new(num_sm));
        service.bootstrap();

        let client = RaftClient::new(&vec!(addr), DEFAULT_SERVICE_ID).unwrap();
        let sm_client = SMClient::new(sm_id, &client);

        assert_eq!(sm_client.get().unwrap().unwrap(), 0.0);
        sm_client.set(&1.0).unwrap().unwrap();
        assert_eq!(sm_client.get().unwrap().unwrap(), 1.0);
        assert_eq!(sm_client.get_and_add(&2.0).unwrap().unwrap(), 1.0);
        assert_eq!(sm_client.add_and_get(&3.0).unwrap().unwrap(), 6.0);
        assert_eq!(sm_client.get_and_minus(&4.0).unwrap().unwrap(), 6.0);
        assert_eq!(sm_client.minus_and_get(&2.0).unwrap().unwrap(), 0.0);
        assert_eq!(sm_client.get_and_incr().unwrap().unwrap(), 0.0);
        assert_eq!(sm_client.incr_and_get().unwrap().unwrap(), 2.0);
        assert_eq!(sm_client.get_and_multiply(&2.0).unwrap().unwrap(), 2.0);
        assert_eq!(sm_client.multiply_and_get(&2.0).unwrap().unwrap(), 8.0);
        assert_eq!(sm_client.get_and_divide(&2.0).unwrap().unwrap(), 8.0);
        assert_eq!(sm_client.divide_and_get(&4.0).unwrap().unwrap(), 1.0);
        assert_eq!(sm_client.swap(&5.0).unwrap().unwrap(), 1.0);
        assert_eq!(sm_client.get().unwrap().unwrap(), 5.0);
        assert_eq!(sm_client.compare_and_swap(&1.0, &10.0).unwrap().unwrap(), 5.0);
        assert_eq!(sm_client.get().unwrap().unwrap(), 5.0);
        assert_eq!(sm_client.compare_and_swap(&5.0 ,&11.0).unwrap().unwrap(), 5.0);
        assert_eq!(sm_client.get().unwrap().unwrap(), 11.0);
    }
}