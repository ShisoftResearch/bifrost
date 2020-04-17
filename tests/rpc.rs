#![feature(proc_macro_hygiene)]
#![feature(box_syntax)]
#![feature(async_closure)]

use bifrost::*;
use std::sync::Arc;
use std::time::Duration;
use serde::{Serialize, Deserialize};
use tokio::time::delay_for;
use futures::future::BoxFuture;

pub mod simple_service {

    use super::*;

    service! {
        rpc hello(name: String) -> String;
        rpc error(message: String) -> Result<(), String>;
    }

    struct HelloServer;

    impl Service for HelloServer {
        fn hello(&self, name: String) -> BoxFuture<String> {
            future::ready(format!("Hello, {}!", name)).boxed()
        }
        fn error(&self, message: String) -> BoxFuture<Result<(), String>> {
            future::ready(Err(message.clone())).boxed()
        }
    }
    dispatch_rpc_service_functions!(HelloServer);

    #[tokio::test(threaded_scheduler)]
    pub async fn simple_rpc() {
        let addr = String::from("127.0.0.1:1300");
        {
            let addr = addr.clone();
            let server = Server::new(&addr);
            server.register_service(0, &Arc::new(HelloServer));
            Server::listen_and_resume(&server);
        }
        delay_for(Duration::from_millis(1000)).await;
        let client = RPCClient::new_async(&addr).await.unwrap();
        let service_client = AsyncServiceClient::new(0, &client);
        let response = service_client.hello(String::from("Jack"));
        let greeting_str = response.await.unwrap();
        println!("SERVER RESPONDED: {}", greeting_str);
        assert_eq!(greeting_str, String::from("Hello, Jack!"));
        let expected_err_msg = String::from("This error is a good one");
        let response = service_client.error(expected_err_msg.clone());
        let error_msg = response.await.unwrap().err().unwrap();
        assert_eq!(error_msg, expected_err_msg);
    }
}

pub mod struct_service {
    use super::*;
    use serde::{Serialize, Deserialize};

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Greeting {
        pub name: String,
        pub time: u32,
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Respond {
        pub text: String,
        pub owner: u32,
    }

    service! {
        rpc hello(gret: Greeting) -> Respond;
    }

    pub struct HelloServer;

    impl Service for HelloServer {
        fn hello(&self, gret: Greeting) -> BoxFuture<Respond> {
            future::ready(Respond {
                text: format!("Hello, {}. It is {} now!", gret.name, gret.time),
                owner: 42,
            }).boxed()
        }
    }
    dispatch_rpc_service_functions!(HelloServer);

    #[tokio::test(threaded_scheduler)]
    pub async fn struct_rpc() {
        let addr = String::from("127.0.0.1:1400");
        {
            let addr = addr.clone();
            let server = Server::new(&addr); // 0 is service id
            server.register_service(0, &Arc::new(HelloServer));
            Server::listen_and_resume(&server);
        }
        delay_for(Duration::from_millis(1000)).await;
        let client = RPCClient::new_async(&addr).await.unwrap();
        let service_client = AsyncServiceClient::new(0, &client);
        let response = service_client.hello(Greeting {
            name: String::from("Jack"),
            time: 12,
        });
        let res = response.await.unwrap();
        let greeting_str = res.text;
        println!("SERVER RESPONDED: {}", greeting_str);
        assert_eq!(greeting_str, String::from("Hello, Jack. It is 12 now!"));
        assert_eq!(42, res.owner);
    }
}

mod multi_server {

    use super::*;

    service! {
        rpc query_server_id() -> u64;
    }

    struct IdServer {
        id: u64,
    }
    impl Service for IdServer {
        fn query_server_id(&self) -> BoxFuture<u64> {
            future::ready(self.id).boxed()
        }
    }
    dispatch_rpc_service_functions!(IdServer);

    #[tokio::test(threaded_scheduler)]
    async fn multi_server_rpc() {
        let addrs = vec![
            String::from("127.0.0.1:1500"),
            String::from("127.0.0.1:1600"),
            String::from("127.0.0.1:1700"),
            String::from("127.0.0.1:1800"),
        ];
        let mut id = 0;
        for addr in &addrs {
            {
                let addr = addr.clone();
                tokio::spawn(async move {
                    let server = Server::new(&addr); // 0 is service id
                    server.register_service(id, &Arc::new(IdServer { id: id })).await;
                    Server::listen(&server)
                });
                id += 1;
            }
        }
        id = 0;
        delay_for(Duration::from_millis(1000)).await;
        for addr in &addrs {
            let client = RPCClient::new_async(addr).await.unwrap();
            let service_client = AsyncServiceClient::new(id, &client);
            let id_res = service_client.query_server_id().await.unwrap();
            assert_eq!(id_res, id);
            id += 1;
        }
    }
}

mod parallel {
    use super::struct_service::*;
    use super::*;
    use bifrost_hasher::hash_str;

    #[tokio::test(threaded_scheduler)]
    pub async fn lots_of_reqs() {
        let addr = String::from("127.0.0.1:1411");
        {
            let addr = addr.clone();
            let server = Server::new(&addr); // 0 is service id
            server.register_service(0, &Arc::new(HelloServer));
            Server::listen_and_resume(&server);
        }
        delay_for(Duration::from_millis(1000)).await;
        let client = RPCClient::new_async(addr.clone()).wait().unwrap();
        let service_client = AsyncServiceClient::new(0, &client);

        println!("Testing parallel RPC reqs");

        (0..1000).collect::<Vec<_>>().into_par_iter().for_each(|i| {
            let response = service_client.hello(Greeting {
                name: String::from("John"),
                time: i,
            });
            let res = response.wait().unwrap().unwrap();
            let greeting_str = res.text;
            println!("SERVER RESPONDED: {}", greeting_str);
            assert_eq!(greeting_str, format!("Hello, John. It is {} now!", i));
            assert_eq!(42, res.owner);
        });

        // test pool
        let server_id = hash_str(&addr);
        (0..1000).collect::<Vec<_>>().into_par_iter().for_each(|i| {
            let addr = (&addr).clone();
            let client =
                wait(DEFAULT_CLIENT_POOL.get_by_id_async(server_id, move |_| addr)).unwrap();
            let service_client = AsyncServiceClient::new(0, &client);
            let response = service_client.hello(Greeting {
                name: String::from("John"),
                time: i,
            });
            let res = response.wait().unwrap().unwrap();
            let greeting_str = res.text;
            println!("SERVER RESPONDED: {}", greeting_str);
            assert_eq!(greeting_str, format!("Hello, John. It is {} now!", i));
            assert_eq!(42, res.owner);
        });
    }
}
