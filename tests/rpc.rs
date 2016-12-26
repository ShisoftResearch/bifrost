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

    impl Server for HelloServer {
        fn hello(&self, name: String) -> Result<String, ()> {
            Ok(format!("Hello, {}!", name))
        }
        fn error(&self, message: String) -> Result<(), String> {
            Err(message)
        }
    }
    #[test]
    fn simple_rpc () {
        let addr = String::from("127.0.0.1:1300");
        {
            let addr = addr.clone();
            let server = HelloServer{};
            thread::spawn(move|| {
                listen(Arc::new(server), &addr);
            });
        }
        thread::sleep(Duration::from_millis(1000));
        let mut client = SyncClient::new(&addr).unwrap();
        let response = client.hello(String::from("Jack"));
        let greeting_str = response.unwrap().unwrap();
        println!("SERVER RESPONDED: {}", greeting_str);
        assert_eq!(greeting_str, String::from("Hello, Jack!"));
        let expected_err_msg = String::from("This error is a good one");
        let response = client.error(expected_err_msg.clone());
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

    impl Server for HelloServer {
        fn hello(&self, gret: Greeting) -> Result<Respond, ()> {
            Ok(Respond {
                text: format!("Hello, {}. It is {} now!", gret.name, gret.time),
                owner: 42
            })
        }
    }
    #[test]
    fn struct_rpc () {
        let addr = String::from("127.0.0.1:1400");
        {
            let addr = addr.clone();
            let server = HelloServer{};
            thread::spawn(move|| {
                listen(Arc::new(server), &addr);
            });
        }
        thread::sleep(Duration::from_millis(1000));
        let mut client = SyncClient::new(&addr).unwrap();
        let response = client.hello(Greeting {
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
        rpc query_server_id() -> u32;
    }

    struct IdServer {
        id: u32
    }
    impl Server for IdServer {
        fn query_server_id(&self) -> Result<u32, ()> {
            Ok(self.id)
        }
    }
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
                id += 1;
                thread::spawn(move|| {
                    let server = IdServer {id: id};
                    listen(Arc::new(server), &addr);
                });
            }
        }
        id = 0;
        thread::sleep(Duration::from_millis(1000));
        for addr in &addrs {
            id += 1;
            let mut client = SyncClient::new(&addr).unwrap();
            let id_res = client.query_server_id().unwrap();
            assert_eq!(id_res.unwrap(), id);
        }
    }
}
