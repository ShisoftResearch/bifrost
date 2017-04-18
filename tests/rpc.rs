use bifrost::rpc::*;
use std::sync::Arc;
use std::sync::mpsc::channel;
use byteorder::{ByteOrder, LittleEndian};
use std::thread;
use std::sync::Mutex;
use std::time::Duration;

mod simple_service {

    use std::thread;

    service! {
        rpc hello(name: String) -> String;
        rpc error(message: String) | String;
    }

    struct HelloServer;

    impl Service for HelloServer {
        fn hello(&self, name: String) -> Result<String, ()> {
            Ok(format!("Hello, {}!", name))
        }
        fn error(&self, message: String) -> Result<(), String> {
            Err(message)
        }
    }
    dispatch_rpc_service_functions!(HelloServer);

    #[test]
    fn simple_rpc () {
        let addr = String::from("127.0.0.1:1300");
        {
            let addr = addr.clone();
            let server = Server::new(vec!((0, Arc::new(HelloServer)))); // 0 is service id
            Server::listen_and_resume(&server, &addr);;
        }
        thread::sleep(Duration::from_millis(1000));
        let client = RPCClient::new(&addr).unwrap();
        let service_client = SyncServiceClient::new(0, client);
        let response = service_client.hello(&String::from("Jack"));
        let greeting_str = response.unwrap().unwrap();
        println!("SERVER RESPONDED: {}", greeting_str);
        assert_eq!(greeting_str, String::from("Hello, Jack!"));
        let expected_err_msg = String::from("This error is a good one");
        let response = service_client.error(&expected_err_msg.clone());
        let error_msg = response.unwrap().err().unwrap();
        assert_eq!(error_msg, expected_err_msg);
    }
}

mod struct_service {
    use std::thread;

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Greeting {
        name: String,
        time: u32
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Respond {
        text: String,
        owner: u32
    }

    service! {
        rpc hello(gret: Greeting) -> Respond;
    }

    struct HelloServer;

    impl Service for HelloServer {
        fn hello(&self, gret: Greeting) -> Result<Respond, ()> {
            Ok(Respond {
                text: format!("Hello, {}. It is {} now!", gret.name, gret.time),
                owner: 42
            })
        }
    }
    dispatch_rpc_service_functions!(HelloServer);

    #[test]
    fn struct_rpc () {
        let addr = String::from("127.0.0.1:1400");
        {
            let addr = addr.clone();
            let server = Server::new(vec!((0, Arc::new(HelloServer)))); // 0 is service id
            thread::spawn(move|| {
                Server::listen(server, &addr);
            });
        }
        thread::sleep(Duration::from_millis(1000));
        let client = RPCClient::new(&addr).unwrap();
        let service_client = SyncServiceClient::new(0, client);
        let response = service_client.hello(&Greeting {
            name: String::from("Jack"),
            time: 12
        });
        let res = response.unwrap().unwrap();
        let greeting_str = res.text;
        println!("SERVER RESPONDED: {}", greeting_str);
        assert_eq!(greeting_str, String::from("Hello, Jack. It is 12 now!"));
        assert_eq!(42, res.owner);
    }
}

mod multi_server {
    use std::thread;

    service! {
        rpc query_server_id() -> u64;
    }

    struct IdServer {
        id: u64
    }
    impl Service for IdServer {
        fn query_server_id(&self) -> Result<u64, ()> {
            Ok(self.id)
        }
    }
    dispatch_rpc_service_functions!(IdServer);

    #[test]
    fn multi_server_rpc () {
        let addrs = vec!(
            String::from("127.0.0.1:1500"),
            String::from("127.0.0.1:1600"),
            String::from("127.0.0.1:1700"),
            String::from("127.0.0.1:1800"),
        );
        let mut id = 0;
        for addr in &addrs {
            {
                let addr = addr.clone();
                thread::spawn(move|| {
                    let server = Server::new(vec!((id, Arc::new(IdServer {id: id})))); // 0 is service id
                    Server::listen(server, &addr);
                });
                id += 1;
            }
        }
        id = 0;
        thread::sleep(Duration::from_millis(1000));
        for addr in &addrs {
            let client = RPCClient::new(&addr).unwrap();
            let service_client = SyncServiceClient::new(id, client);
            let id_res = service_client.query_server_id().unwrap();
            assert_eq!(id_res.unwrap(), id);
            id += 1;
        }
    }
}