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

    #[test]
    fn test(){
        let addr = String::from("127.0.0.1:2011");
        let num_sm = U32::Number::new_by_name(
            String::from("test"),
            0
        );
        let server = RaftServer::new(Options{
            storage: Storage::Default(),
            address: addr.clone()
        });
        let server = server.unwrap();
        let sm_id = num_sm.id;
        server.register_state_machine(Box::new(num_sm));
        server.bootstrap();

        let client = RaftClient::new(vec!(addr)).unwrap();
        let sm_client = SMClient::new(sm_id, &client);
        assert_eq!(sm_client.get().unwrap().unwrap(), 0);
        sm_client.set(1).unwrap().unwrap();
        assert_eq!(sm_client.get().unwrap().unwrap(), 1);
        assert_eq!(client.execute(sm_id, &get_and_add{n: 2}).unwrap().unwrap(), 1);
        assert_eq!(client.execute(sm_id, &add_and_get{n: 3}).unwrap().unwrap(), 6);
        assert_eq!(client.execute(sm_id, &get_and_minus{n: 4}).unwrap().unwrap(), 6);
        assert_eq!(client.execute(sm_id, &minus_and_get{n: 2}).unwrap().unwrap(), 0);
        assert_eq!(client.execute(sm_id, &get_and_incr{}).unwrap().unwrap(), 0);
        assert_eq!(client.execute(sm_id, &incr_and_get{}).unwrap().unwrap(), 2);
        assert_eq!(client.execute(sm_id, &get_and_multiply{n: 2}).unwrap().unwrap(), 2);
        assert_eq!(client.execute(sm_id, &multiply_and_get{n: 2}).unwrap().unwrap(), 8);
        assert_eq!(client.execute(sm_id, &get_and_divide{n: 2}).unwrap().unwrap(), 8);
        assert_eq!(client.execute(sm_id, &divide_and_get{n: 4}).unwrap().unwrap(), 1);
        assert_eq!(client.execute(sm_id, &swap{n: 5}).unwrap().unwrap(), 1);
        assert_eq!(client.execute(sm_id, &get{}).unwrap().unwrap(), 5);
        assert_eq!(client.execute(sm_id, &compare_and_swap{original: 1, n: 10}).unwrap().unwrap(), 5);
        assert_eq!(client.execute(sm_id, &get{}).unwrap().unwrap(), 5);
        assert_eq!(client.execute(sm_id, &compare_and_swap{original: 5, n: 11}).unwrap().unwrap(), 5);
        assert_eq!(client.execute(sm_id, &get{}).unwrap().unwrap(), 11);
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
        get_and_divide, divide_and_get
    };

    #[test]
    fn test(){
        let addr = String::from("127.0.0.1:2012");
        let num_sm = F64::Number::new_by_name(
            String::from("test"),
            0.0
        );
        let server = RaftServer::new(Options{
            storage: Storage::Default(),
            address: addr.clone()
        });
        let server = server.unwrap();
        let sm_id = num_sm.id;
        server.register_state_machine(Box::new(num_sm));
        server.bootstrap();

        let client = RaftClient::new(vec!(addr)).unwrap();
        assert_eq!(client.execute(sm_id, &get{}).unwrap().unwrap(), 0.0);
        client.execute(sm_id, &set{n: 1.0}).unwrap().unwrap();
        assert_eq!(client.execute(sm_id, &get{}).unwrap().unwrap(), 1.0);
        assert_eq!(client.execute(sm_id, &get_and_add{n: 2.0}).unwrap().unwrap(), 1.0);
        assert_eq!(client.execute(sm_id, &add_and_get{n: 3.0}).unwrap().unwrap(), 6.0);
        assert_eq!(client.execute(sm_id, &get_and_minus{n: 4.0}).unwrap().unwrap(), 6.0);
        assert_eq!(client.execute(sm_id, &minus_and_get{n: 2.0}).unwrap().unwrap(), 0.0);
        assert_eq!(client.execute(sm_id, &get_and_incr{}).unwrap().unwrap(), 0.0);
        assert_eq!(client.execute(sm_id, &incr_and_get{}).unwrap().unwrap(), 2.0);
        assert_eq!(client.execute(sm_id, &get_and_multiply{n: 2.0}).unwrap().unwrap(), 2.0);
        assert_eq!(client.execute(sm_id, &multiply_and_get{n: 2.0}).unwrap().unwrap(), 8.0);
        assert_eq!(client.execute(sm_id, &get_and_divide{n: 2.0}).unwrap().unwrap(), 8.0);
        assert_eq!(client.execute(sm_id, &divide_and_get{n: 4.0}).unwrap().unwrap(), 1.0);
    }
}