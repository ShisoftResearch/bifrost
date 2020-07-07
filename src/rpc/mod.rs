#[macro_use]
pub mod proto;

use crate::{tcp, DISABLE_SHORTCUT};
use async_std::sync::*;
use bifrost_hasher::hash_str;
use bytes::{Buf, BufMut, BytesMut};
use futures::future::BoxFuture;
use futures::prelude::*;
use futures::Future;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::delay_for;
use tokio::time::*;

lazy_static! {
    pub static ref DEFAULT_CLIENT_POOL: ClientPool = ClientPool::new();
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RPCRequestError {
    FunctionIdNotFound,
    ServiceIdNotFound,
    BadRequest,
    Other,
}

#[derive(Debug)]
pub enum RPCError {
    IOError(io::Error),
    RequestError(RPCRequestError),
    ClientCannotDecodeResponse,
}

pub trait RPCService: Sync + Send {
    fn dispatch(&self, data: BytesMut) -> BoxFuture<Result<BytesMut, RPCRequestError>>;
    fn register_shortcut_service(
        &self,
        service_ptr: usize,
        server_id: u64,
        service_id: u64,
    ) -> ::std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;
}

pub struct Server {
    services: RwLock<HashMap<u64, Arc<dyn RPCService>>>,
    pub address: String,
    pub server_id: u64,
}

unsafe impl Sync for Server {}

pub struct ClientPool {
    clients: Arc<Mutex<HashMap<u64, Arc<RPCClient>>>>,
}

fn encode_res(res: Result<BytesMut, RPCRequestError>) -> BytesMut {
    match res {
        Ok(buffer) => [0u8; 1].iter().cloned().chain(buffer.into_iter()).collect(),
        Err(e) => {
            let err_id = match e {
                RPCRequestError::FunctionIdNotFound => 1u8,
                RPCRequestError::ServiceIdNotFound => 2u8,
                _ => 255u8,
            };
            BytesMut::from(&[err_id][..])
        }
    }
}

fn decode_res(res: io::Result<BytesMut>) -> Result<BytesMut, RPCError> {
    match res {
        Ok(mut res) => {
            if res[0] == 0u8 {
                res.advance(1);
                Ok(res.split())
            } else {
                match res[0] {
                    1u8 => Err(RPCError::RequestError(RPCRequestError::FunctionIdNotFound)),
                    2u8 => Err(RPCError::RequestError(RPCRequestError::ServiceIdNotFound)),
                    _ => Err(RPCError::RequestError(RPCRequestError::Other)),
                }
            }
        }
        Err(e) => Err(RPCError::IOError(e)),
    }
}

pub fn read_u64_head(mut data: BytesMut) -> (u64, BytesMut) {
    let num = data.get_u64_le();
    (num, data)
}

impl Server {
    pub fn new(address: &String) -> Arc<Server> {
        Arc::new(Server {
            services: RwLock::new(HashMap::new()),
            address: address.clone(),
            server_id: hash_str(address),
        })
    }
    pub async fn listen(server: &Arc<Server>) -> Result<(), Box<dyn Error>> {
        let address = &server.address;
        let server = server.clone();
        tcp::server::Server::new(
            address,
            Arc::new(move |data| {
                let server = server.clone();
                async move {
                    let (svr_id, data) = read_u64_head(data);
                    let service = server.services.read().await.get(&svr_id).cloned();
                    trace!("Processing request for service {}", svr_id);
                    match service {
                        Some(service) => {
                            let svr_res = service.dispatch(data).await;
                            encode_res(svr_res)
                        }
                        None => encode_res(Err(RPCRequestError::ServiceIdNotFound)),
                    }
                }
                .boxed()
            }),
        )
        .await
    }

    pub async fn listen_and_resume(server: &Arc<Server>) {
        let server = server.clone();
        tokio::spawn(async move {
            Self::listen(&server).await.unwrap();
        });
        delay_for(Duration::from_secs(1)).await
    }

    pub async fn register_service<T>(&self, service_id: u64, service: &Arc<T>)
    where
        T: RPCService + Sized + 'static,
    {
        let service = service.clone();
        if !DISABLE_SHORTCUT {
            let service_ptr = Arc::into_raw(service.clone()) as usize;
            service
                .register_shortcut_service(service_ptr, self.server_id, service_id)
                .await;
        } else {
            debug!("SERVICE SHORTCUT DISABLED");
        }
        self.services.write().await.insert(service_id, service);
    }

    pub async fn remove_service(&self, service_id: u64) {
        self.services.write().await.remove(&service_id);
    }
    pub fn address(&self) -> &String {
        &self.address
    }
}

pub struct RPCClient {
    client: tcp::client::Client,
    pub server_id: u64,
    pub address: String,
}

pub fn prepend_u64(num: u64, data: BytesMut) -> BytesMut {
    let mut bytes = BytesMut::with_capacity(8 + data.len());
    bytes.put_u64_le(num);
    bytes.extend_from_slice(data.as_ref());
    bytes
}

impl RPCClient {
    pub async fn send_async(
        self: Pin<&Self>,
        svr_id: u64,
        data: BytesMut,
    ) -> Result<BytesMut, RPCError> {
        let client = &self.client;
        let payload = prepend_u64(svr_id, data);
        let res = client.send_msg(payload).await;
        decode_res(res)
    }
    pub async fn new_async(addr: &String) -> io::Result<Arc<RPCClient>> {
        let client = tcp::client::Client::connect(addr).await?;
        Ok(Arc::new(RPCClient {
            server_id: client.server_id,
            client,
            address: addr.clone(),
        }))
    }
}

impl ClientPool {
    pub fn new() -> ClientPool {
        ClientPool {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get(&self, addr: &String) -> io::Result<Arc<RPCClient>> {
        let addr_clone = addr.clone();
        let server_id = hash_str(addr);
        self.get_by_id(server_id, move |_| future::ready(addr_clone).boxed()).await
    }

    pub async fn get_by_id<'a, F>(&self, server_id: u64, addr_fn: F) -> io::Result<Arc<RPCClient>>
    where
        F: FnOnce(u64) -> BoxFuture<'a, String>,
    {
        let mut clients = self.clients.lock().await;
        if clients.contains_key(&server_id) {
            let client = clients.get(&server_id).unwrap().clone();
            Ok(client)
        } else {
            let client = timeout(
                Duration::from_secs(5),
                RPCClient::new_async(&addr_fn(server_id).await),
            )
            .await??;
            clients.insert(server_id, client.clone());
            Ok(client)
        }
    }
}

#[cfg(test)]
mod test {
    use futures::future::BoxFuture;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::delay_for;

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
            let _ = env_logger::try_init();
            let addr = String::from("127.0.0.1:1300");
            {
                let addr = addr.clone();
                let server = Server::new(&addr);
                server.register_service(0, &Arc::new(HelloServer)).await;
                Server::listen_and_resume(&server).await;
            }
            delay_for(Duration::from_millis(1000)).await;
            let client = RPCClient::new_async(&addr).await.unwrap();
            let service_client = AsyncServiceClient::new(0, &client);
            let response = service_client.hello(String::from("Jack")).await;
            let greeting_str = response.unwrap();
            info!("SERVER RESPONDED: {}", greeting_str);
            assert_eq!(greeting_str, String::from("Hello, Jack!"));
            let expected_err_msg = String::from("This error is a good one");
            let response = service_client.error(expected_err_msg.clone());
            let error_msg = response.await.unwrap().err().unwrap();
            assert_eq!(error_msg, expected_err_msg);
        }
    }

    pub mod struct_service {
        use super::*;
        use serde::{Deserialize, Serialize};

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
                })
                .boxed()
            }
        }
        dispatch_rpc_service_functions!(HelloServer);

        #[tokio::test(threaded_scheduler)]
        pub async fn struct_rpc() {
            let _ = env_logger::try_init();
            let addr = String::from("127.0.0.1:1400");
            {
                let addr = addr.clone();
                let server = Server::new(&addr); // 0 is service id
                server.register_service(0, &Arc::new(HelloServer)).await;
                Server::listen_and_resume(&server).await;
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
            info!("SERVER RESPONDED: {}", greeting_str);
            assert_eq!(greeting_str, String::from("Hello, Jack. It is 12 now!"));
            assert_eq!(42, res.owner);
        }
    }

    mod multi_server {

        use super::*;

        #[derive(Serialize, Deserialize, Clone)]
        pub struct ComplexAnswer {
            name: String,
            id: u64,
            req: Option<String>,
        }

        service! {
            rpc query_server_id() -> u64;
            rpc query_answer(req: Option<String>) -> ComplexAnswer;
            rpc large_query(req: Option<String>) -> Vec<ComplexAnswer>;
            rpc large_req(req: Vec<ComplexAnswer>, req2: Vec<ComplexAnswer>) -> Vec<ComplexAnswer>;
        }

        struct IdServer {
            id: u64,
        }
        impl Service for IdServer {
            fn query_server_id(&self) -> BoxFuture<u64> {
                future::ready(self.id).boxed()
            }

            fn query_answer(&self, req: Option<String>) -> BoxFuture<ComplexAnswer> {
                future::ready(ComplexAnswer {
                    name: format!("Server for {:?}", req),
                    id: self.id,
                    req,
                })
                .boxed()
            }

            fn large_query(&self, req: Option<String>) -> BoxFuture<Vec<ComplexAnswer>> {
                let mut res = vec![];
                for i in 0..1024 {
                    res.push(ComplexAnswer {
                        name: format!("Server for {:?}", &req),
                        id: i,
                        req: req.clone(),
                    })
                }
                future::ready(res).boxed()
            }

            fn large_req(
                &self,
                mut req: Vec<ComplexAnswer>,
                mut req2: Vec<ComplexAnswer>,
            ) -> BoxFuture<Vec<ComplexAnswer>> {
                req.append(&mut req2);
                future::ready(req).boxed()
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
                    let server = Server::new(&addr); // 0 is service id
                    server
                        .register_service(id, &Arc::new(IdServer { id: id }))
                        .await;
                    Server::listen_and_resume(&server).await;
                    id += 1;
                }
            }
            id = 0;
            delay_for(Duration::from_millis(1000)).await;
            for addr in &addrs {
                let client = RPCClient::new_async(addr).await.unwrap();
                let service_client = AsyncServiceClient::new(id, &client);
                let id_res = service_client.query_server_id().await;
                let id_un = id_res.unwrap();
                assert_eq!(id_un, id);
                let user_str = format!("User {}", id);
                let complex = service_client
                    .query_answer(Some(user_str.to_string()))
                    .await
                    .unwrap();
                let large = service_client
                    .large_query(Some(user_str.to_string()))
                    .await
                    .unwrap();
                assert_eq!(large.len(), 1024);
                assert_eq!(complex.req, Some(user_str));
                let large_req = service_client
                    .large_req(large.clone(), large)
                    .await
                    .unwrap();
                assert_eq!(large_req.len(), 1024 * 2);
                id += 1;
            }
        }
    }

    mod parallel {
        use super::struct_service::*;
        use super::*;
        use crate::rpc::{RPCClient, Server, DEFAULT_CLIENT_POOL};
        use bifrost_hasher::hash_str;
        use futures::prelude::stream::*;
        use futures::prelude::*;
        use futures::FutureExt;

        #[tokio::test(threaded_scheduler)]
        pub async fn lots_of_reqs() {
            let _ = env_logger::try_init();
            let addr = String::from("127.0.0.1:1411");
            {
                let addr = addr.clone();
                let server = Server::new(&addr); // 0 is service id
                server.register_service(0, &Arc::new(HelloServer)).await;
                Server::listen_and_resume(&server).await;
            }
            delay_for(Duration::from_millis(1000)).await;
            let client = RPCClient::new_async(&addr).await.unwrap();
            let service_client = AsyncServiceClient::new(0, &client);

            info!("Testing parallel RPC reqs");

            let mut futs = (0..100)
                .map(|i| {
                    let service_client = service_client.clone();
                    tokio::spawn(async move {
                        let response = service_client.hello(Greeting {
                            name: String::from("John"),
                            time: i,
                        });
                        let res = response.await.unwrap();
                        let greeting_str = res.text;
                        info!("SERVER RESPONDED: {}", greeting_str);
                        assert_eq!(greeting_str, format!("Hello, John. It is {} now!", i));
                        assert_eq!(42, res.owner);
                    })
                    .boxed()
                })
                .collect::<FuturesUnordered<_>>();
            while futs.next().await.is_some() {}

            // test pool
            let server_id = hash_str(&addr);
            let mut futs = (0..100)
                .map(|i| {
                    let addr = (&addr).clone();
                    tokio::spawn(async move {
                        let client = DEFAULT_CLIENT_POOL
                            .get_by_id(server_id, move |_| future::ready(addr).boxed())
                            .await
                            .unwrap();
                        let service_client = AsyncServiceClient::new(0, &client);
                        let response = service_client.hello(Greeting {
                            name: String::from("John"),
                            time: i,
                        });
                        let res = response.await.unwrap();
                        let greeting_str = res.text;
                        info!("SERVER RESPONDED: {}", greeting_str);
                        assert_eq!(greeting_str, format!("Hello, John. It is {} now!", i));
                        assert_eq!(42, res.owner);
                    })
                    .boxed()
                })
                .collect::<FuturesUnordered<_>>();
            while futs.next().await.is_some() {}
        }
    }
}
