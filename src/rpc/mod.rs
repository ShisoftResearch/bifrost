#[macro_use]
pub mod proto;
pub mod cluster;

use crate::{tcp, DISABLE_SHORTCUT};
use bifrost_hasher::hash_str;
use bytes::{Buf, BufMut, BytesMut};
use futures::future::BoxFuture;
use futures::prelude::*;
use lightning::map::*;
use serde::{Deserialize, Serialize};
use std::backtrace;
use std::collections::BTreeMap;
use std::error::Error;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::*;

lazy_static! {
    pub static ref DEFAULT_CLIENT_POOL: ClientPool = ClientPool::new();
}

#[cfg(test)]
lazy_static! {
    static ref CLIENT_POOL_EVICT_AFTER_READ: StdMutex<Option<Box<dyn FnOnce() + Send>>> =
        StdMutex::new(None);
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

#[doc(hidden)]
#[derive(Clone)]
pub struct ShortcutToken(Arc<()>);

impl ShortcutToken {
    #[doc(hidden)]
    pub fn new() -> Self {
        Self(Arc::new(()))
    }

    #[doc(hidden)]
    pub fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub trait RPCService: Sync + Send {
    fn dispatch(&self, data: BytesMut) -> BoxFuture<Result<BytesMut, RPCRequestError>>;
    fn register_shortcut_service(
        &self,
        service_ptr: usize,
        server_id: u64,
        service_id: u64,
    ) -> ShortcutToken;
    fn unregister_shortcut_service(&self, server_id: u64, service_id: u64, token: &ShortcutToken);
    fn service_symbol(&self) -> &'static str;
}

pub struct Server {
    services: PtrHashMap<u64, Arc<dyn RPCService>>,
    pub address: String,
    pub server_id: u64,
    tcp_server: StdMutex<Option<Arc<tcp::server::Server>>>,
    shutdown_handle: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    service_shortcut_tokens: StdMutex<BTreeMap<u64, ShortcutToken>>,
    service_lifecycle: StdMutex<()>,
    shutting_down: AtomicBool,
}

unsafe impl Sync for Server {}

pub struct ClientPool {
    clients: PtrHashMap<u64, Arc<RPCClient>>,
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

fn request_callback(
    server: &Arc<Server>,
) -> Arc<dyn Fn(tcp::server::TcpReq) -> tcp::server::TcpRes + Send + Sync> {
    let server = server.clone();
    Arc::new(move |data| {
        let server = server.clone();
        async move {
            let (svr_id, data) = read_u64_head(data);
            let service = server.services.get(&svr_id);
            trace!("Processing request for service {}", svr_id);
            match service {
                Some(service) => {
                    let svr_res = service.dispatch(data).await;
                    encode_res(svr_res)
                }
                None => {
                    let service_list = server
                        .services
                        .entries()
                        .into_iter()
                        .map(|(sid, service)| format!("{}:{}", sid, service.service_symbol()))
                        .collect::<Vec<_>>();
                    error!(
                        "Service {} not found, have {:?}, backtrace: {:?}",
                        svr_id,
                        service_list.join(", "),
                        backtrace::Backtrace::capture()
                    );
                    encode_res(Err(RPCRequestError::ServiceIdNotFound))
                }
            }
        }
        .boxed()
    })
}

pub fn read_u64_head(mut data: BytesMut) -> (u64, BytesMut) {
    let num = data.get_u64_le();
    (num, data)
}

impl Server {
    pub fn new(address: &String) -> Arc<Server> {
        Arc::new(Server {
            services: PtrHashMap::with_capacity(16),
            address: address.clone(),
            server_id: hash_str(address),
            tcp_server: StdMutex::new(None),
            shutdown_handle: StdMutex::new(None),
            service_shortcut_tokens: StdMutex::new(BTreeMap::new()),
            service_lifecycle: StdMutex::new(()),
            shutting_down: AtomicBool::new(false),
        })
    }

    pub async fn listen(server: &Arc<Server>) -> Result<(), Box<dyn Error>> {
        let address = &server.address;
        let tcp_server = Arc::new(tcp::server::Server::new());

        // Store tcp_server reference
        match server.tcp_server.lock() {
            Ok(mut guard) => *guard = Some(tcp_server.clone()),
            Err(e) => error!("Failed to store tcp_server reference: {}", e),
        }

        tcp_server.listen(address, request_callback(server)).await
    }

    pub async fn listen_and_resume(server: &Arc<Server>) {
        let address = server.address.clone();
        let tcp_server = Arc::new(tcp::server::Server::new());

        // Store tcp_server in the server struct
        match server.tcp_server.lock() {
            Ok(mut guard) => *guard = Some(tcp_server.clone()),
            Err(e) => error!("Failed to store tcp_server reference: {}", e),
        }

        let callback = request_callback(server);
        let handle = tokio::spawn(async move {
            let result = tcp_server.listen(&address, callback).await;

            if let Err(e) = result {
                error!("RPC server error: {:?}", e);
            }
        });

        // Store handle
        match server.shutdown_handle.lock() {
            Ok(mut guard) => *guard = Some(handle),
            Err(e) => error!("Failed to store shutdown handle: {}", e),
        }

        sleep(Duration::from_secs(1)).await
    }

    pub async fn shutdown(&self) {
        info!("Shutting down RPC server on {}", self.address);
        self.shutting_down.store(true, Ordering::Release);
        let tcp_server = self
            .tcp_server
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let tcp_registration = tcp_server
            .as_ref()
            .and_then(|tcp_server| tcp_server.shutdown_owned());
        let shutdown_handle = self
            .shutdown_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(shutdown_handle) = shutdown_handle {
            if let Err(e) = shutdown_handle.await {
                error!("RPC listener task failed during shutdown: {:?}", e);
            }
        }
        drop(tcp_server);
        if let Some(tcp_registration) = tcp_registration.as_ref() {
            DEFAULT_CLIENT_POOL.evict_owned(self.server_id, tcp_registration);
        }
        self.release_services();
    }

    pub async fn register_service_with_id<T>(&self, service_id: u64, service: &Arc<T>)
    where
        T: RPCService + Sized + 'static,
    {
        let _lifecycle = self
            .service_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.shutting_down.load(Ordering::Acquire) {
            error!(
                "Cannot register service {} on shutting down RPC server {}",
                service_id, self.address
            );
            return;
        }
        let service = service.clone();
        if !DISABLE_SHORTCUT {
            let service_ptr = Arc::into_raw(service.clone()) as usize;
            let token = service.register_shortcut_service(service_ptr, self.server_id, service_id);
            self.service_shortcut_tokens
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(service_id, token);
        } else {
            debug!("SERVICE SHORTCUT DISABLED");
        }
        info!(
            "Registering service {} with id {}",
            service.service_symbol(),
            service_id
        );
        self.services.insert(service_id, service);
    }

    pub async fn register_service<T>(&self, service: &Arc<T>)
    where
        T: RPCServiceWithId + Sized + 'static,
    {
        self.register_service_with_id(T::SERVICE_ID, service).await
    }

    pub async fn remove_service(&self, service_id: u64) {
        let _lifecycle = self
            .service_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let service = self.services.get(&service_id);
        let shortcut_token = self
            .service_shortcut_tokens
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&service_id);
        if let (Some(service), Some(token)) = (service.as_ref(), shortcut_token) {
            service.unregister_shortcut_service(self.server_id, service_id, &token);
        }
        self.services.remove(&service_id);
    }

    fn release_services(&self) {
        let _lifecycle = self
            .service_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut shortcut_tokens = self
            .service_shortcut_tokens
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (service_id, service) in self.services.entries() {
            if let Some(token) = shortcut_tokens.remove(&service_id) {
                service.unregister_shortcut_service(self.server_id, service_id, &token);
            }
            self.services.remove(&service_id);
        }
        shortcut_tokens.clear();
    }

    pub fn address(&self) -> &String {
        &self.address
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Release);
        if let Some(tcp_server) = self
            .tcp_server
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            if let Some(tcp_registration) = tcp_server.shutdown_owned() {
                DEFAULT_CLIENT_POOL.evict_owned(self.server_id, &tcp_registration);
            }
        }
        self.release_services();
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
            clients: PtrHashMap::with_capacity(16),
        }
    }

    pub async fn get(&self, addr: &String) -> io::Result<Arc<RPCClient>> {
        let addr_clone = addr.clone();
        let server_id = hash_str(addr);
        self.get_by_id(server_id, move |_| addr_clone).await
    }

    pub async fn get_by_id<F>(&self, server_id: u64, addr_fn: F) -> io::Result<Arc<RPCClient>>
    where
        F: FnOnce(u64) -> String,
    {
        let clients = &self.clients;
        if let Some(client) = clients.get(&server_id) {
            Ok(client.clone())
        } else {
            let client = timeout(
                Duration::from_secs(5),
                RPCClient::new_async(&addr_fn(server_id)),
            )
            .await??;
            clients.insert(server_id, client.clone());
            Ok(client)
        }
    }

    fn evict_owned(&self, server_id: u64, registration: &tcp::shortcut::ShortcutToken) {
        let should_evict = self
            .clients
            .get(&server_id)
            .map(|client| {
                client
                    .client
                    .shortcut_token()
                    .map(|cached_registration| cached_registration.is_same(registration))
                    .unwrap_or(true)
            })
            .unwrap_or(false);
        #[cfg(test)]
        if let Some(hook) = CLIENT_POOL_EVICT_AFTER_READ
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            hook();
        }
        if should_evict {
            if let Some(client) = self.clients.lock(&server_id) {
                let still_owned = client
                    .client
                    .shortcut_token()
                    .map(|cached_registration| cached_registration.is_same(registration))
                    .unwrap_or(true);
                if still_owned {
                    client.remove();
                }
            }
        }
    }
}

pub trait ServiceClient: Send + Sync {
    fn new_instance_with_service_id(server_id: u64, client: &Arc<RPCClient>) -> Self;
    fn server_id(&self) -> u64;
    fn new_with_service_id(server_id: u64, client: &Arc<RPCClient>) -> Arc<Self>
    where
        Self: Sized,
    {
        Arc::new(Self::new_instance_with_service_id(server_id, client))
    }
}

pub trait ServiceClientWithId: ServiceClient {
    const SERVICE_ID: u64;

    fn new(client: &Arc<RPCClient>) -> Arc<Self>
    where
        Self: Sized,
    {
        Self::new_with_service_id(Self::SERVICE_ID, client)
    }
}

pub trait RPCServiceWithId: RPCService {
    const SERVICE_ID: u64;
}

#[cfg(test)]
mod test {
    use futures::future::BoxFuture;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

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

        #[tokio::test(flavor = "multi_thread")]
        pub async fn simple_rpc() {
            let _ = env_logger::try_init();
            let addr = String::from("127.0.0.1:1300");
            let server = Server::new(&addr);
            server
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&server).await;
            sleep(Duration::from_millis(1000)).await;
            let client = RPCClient::new_async(&addr).await.unwrap();
            let service_client = AsyncServiceClient::new_with_service_id(0, &client);
            let response = service_client.hello(String::from("Jack")).await;
            let greeting_str = response.unwrap();
            info!("SERVER RESPONDED: {}", greeting_str);
            assert_eq!(greeting_str, String::from("Hello, Jack!"));
            let expected_err_msg = String::from("This error is a good one");
            let response = service_client.error(expected_err_msg.clone());
            let error_msg = response.await.unwrap().err().unwrap();
            assert_eq!(error_msg, expected_err_msg);
            server.shutdown().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        pub async fn one_arg_dispatch_decodes_wire_tuple_payload() {
            let req_data = (String::from("Jack"),);
            let req_data_bytes =
                crate::bytes::BytesMut::from(crate::utils::serde::serialize(&req_data).as_slice());
            let req_bytes =
                crate::rpc::prepend_u64(bifrost_plugins::hash_ident!(hello) as u64, req_data_bytes);

            let res_bytes = HelloServer.inner_dispatch(req_bytes).await.unwrap();
            let greeting: String = crate::utils::serde::deserialize(&res_bytes).unwrap();

            assert_eq!(greeting, String::from("Hello, Jack!"));
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shutdown_releases_server_service_and_shortcuts() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);

            let server = Server::new(&addr);
            let weak_server = Arc::downgrade(&server);
            let service = Arc::new(HelloServer);
            let weak_service = Arc::downgrade(&service);

            server.register_service_with_id(0, &service).await;
            Server::listen_and_resume(&server).await;

            assert!(crate::tcp::shortcut::is_local(server.server_id).await);
            assert!(get_local(server.server_id, 0).await.is_some());
            let pooled_client = DEFAULT_CLIENT_POOL.get(&addr).await.unwrap();
            assert!(DEFAULT_CLIENT_POOL.clients.get(&server.server_id).is_some());

            server.shutdown().await;
            assert!(DEFAULT_CLIENT_POOL.clients.get(&server.server_id).is_none());
            drop(pooled_client);
            drop(service);
            drop(server);

            assert!(!crate::tcp::shortcut::is_local(hash_str(&addr)).await);
            assert!(get_local(hash_str(&addr), 0).await.is_none());
            assert!(weak_service.upgrade().is_none());
            assert!(weak_server.upgrade().is_none());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn stale_server_shutdown_preserves_replacement_service_shortcut() {
            let addr = "rpc-service-generation-test".to_string();
            let old_server = Server::new(&addr);
            let old_service = Arc::new(HelloServer);
            old_server.register_service_with_id(0, &old_service).await;

            let new_server = Server::new(&addr);
            let new_service = Arc::new(HelloServer);
            let expected: Arc<dyn Service> = new_service.clone();
            new_server.register_service_with_id(0, &new_service).await;

            old_server.shutdown().await;

            let registered = get_local(hash_str(&addr), 0)
                .await
                .expect("replacement service shortcut must remain registered");
            assert!(Arc::ptr_eq(&registered, &expected));

            new_server.shutdown().await;
            assert!(get_local(hash_str(&addr), 0).await.is_none());
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shutdown_awaits_listener_and_allows_immediate_same_port_rebind() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);

            let old_server = Server::new(&addr);
            let old_service = Arc::new(HelloServer);
            old_server.register_service_with_id(0, &old_service).await;
            Server::listen_and_resume(&old_server).await;
            old_server.shutdown().await;

            let new_server = Server::new(&addr);
            let new_service = Arc::new(HelloServer);
            new_server.register_service_with_id(0, &new_service).await;
            Server::listen_and_resume(&new_server).await;

            let replacement_client = DEFAULT_CLIENT_POOL.get(&addr).await.unwrap();
            old_server.shutdown().await;
            assert!(crate::tcp::shortcut::is_local(hash_str(&addr)).await);
            assert!(get_local(hash_str(&addr), 0).await.is_some());
            let cached_client = DEFAULT_CLIENT_POOL.get(&addr).await.unwrap();
            assert!(Arc::ptr_eq(&replacement_client, &cached_client));

            new_server.shutdown().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn pool_eviction_rechecks_identity_before_removing() {
            let addr = crate::tcp::STANDALONE_ADDRESS.to_string();
            let server_id = hash_str(&addr);

            let old_server = Server::new(&addr);
            let old_service = Arc::new(HelloServer);
            old_server.register_service_with_id(0, &old_service).await;
            Server::listen_and_resume(&old_server).await;
            let old_client = DEFAULT_CLIENT_POOL.get(&addr).await.unwrap();
            let old_token = old_client.client.shortcut_token().unwrap().clone();

            DEFAULT_CLIENT_POOL.clients.remove(&server_id);
            let new_server = Server::new(&addr);
            let new_service = Arc::new(HelloServer);
            new_server.register_service_with_id(0, &new_service).await;
            Server::listen_and_resume(&new_server).await;
            let new_client = DEFAULT_CLIENT_POOL.get(&addr).await.unwrap();
            let new_token = new_client.client.shortcut_token().unwrap().clone();
            assert!(!old_token.is_same(&new_token));

            let old_client_for_hook = old_client.clone();
            *CLIENT_POOL_EVICT_AFTER_READ
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                DEFAULT_CLIENT_POOL.clients.remove(&server_id);
                DEFAULT_CLIENT_POOL
                    .clients
                    .insert(server_id, old_client_for_hook);
            }));

            DEFAULT_CLIENT_POOL.evict_owned(server_id, &new_token);

            let cached = DEFAULT_CLIENT_POOL.clients.get(&server_id).unwrap();
            assert!(Arc::ptr_eq(&cached, &old_client));

            new_server.shutdown().await;
            old_server.shutdown().await;
        }

        #[cfg(target_os = "linux")]
        #[tokio::test(flavor = "multi_thread")]
        async fn repeated_raft_service_lifecycle_keeps_fds_bounded() {
            const CHILD_ENV: &str = "BIFROST_RPC_FD_LIFECYCLE_CHILD";
            if std::env::var_os(CHILD_ENV).is_none() {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg(
                        "rpc::test::simple_service::repeated_raft_service_lifecycle_keeps_fds_bounded",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ENV, "1")
                    .status()
                    .unwrap();
                assert!(status.success(), "isolated lifecycle child failed");
                return;
            }

            fn open_fd_count() -> usize {
                std::fs::read_dir("/proc/self/fd").unwrap().count()
            }

            let baseline = open_fd_count();
            let mut peak = baseline;
            for iteration in 0..6 {
                let addr = format!("fd-lifecycle-{}", iteration);
                let server = Server::new(&addr);
                let service = crate::raft::RaftService::new(crate::raft::Options {
                    storage: crate::raft::Storage::MEMORY,
                    address: addr,
                    service_id: 7,
                });
                let weak_service = Arc::downgrade(&service);
                peak = peak.max(open_fd_count());

                server.register_service_with_id(7, &service).await;
                service.shutdown().await;
                server.shutdown().await;
                drop(server);
                drop(service);

                assert!(
                    weak_service.upgrade().is_none(),
                    "RaftService remained rooted after lifecycle {}",
                    iteration
                );
            }

            let final_count = open_fd_count();
            eprintln!(
                "isolated repeated lifecycle fd baseline={}, peak={}, final={}",
                baseline, peak, final_count
            );
            assert!(
                final_count <= baseline + 2,
                "file descriptors grew from {} to {}",
                baseline,
                final_count
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shutdown_allows_raft_service_drop_inside_async_context() {
            let addr = "async-raft-service-drop".to_string();
            let server = Server::new(&addr);
            let service = crate::raft::RaftService::new(crate::raft::Options {
                storage: crate::raft::Storage::MEMORY,
                address: addr,
                service_id: 7,
            });

            server.register_service_with_id(7, &service).await;
            service.shutdown().await;
            server.shutdown().await;
            drop(server);
            drop(service);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn raft_runtime_drop_fallback_is_async_context_safe() {
            let service = crate::raft::RaftService::new(crate::raft::Options {
                storage: crate::raft::Storage::MEMORY,
                address: "async-raft-service-drop-fallback".to_string(),
                service_id: 7,
            });
            drop(service);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn raft_shutdown_called_from_owned_runtime_does_not_deadlock() {
            let service = crate::raft::RaftService::new(crate::raft::Options {
                storage: crate::raft::Storage::MEMORY,
                address: "owned-runtime-shutdown".to_string(),
                service_id: 7,
            });
            let shutdown_target = service.clone();
            let shutdown = service
                .rt
                .spawn(async move { shutdown_target.shutdown().await });

            tokio::time::timeout(Duration::from_secs(2), shutdown)
                .await
                .expect("owned-runtime shutdown must not deadlock")
                .expect("owned-runtime shutdown task must complete");
            drop(service);
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

        #[tokio::test(flavor = "multi_thread")]
        pub async fn struct_rpc() {
            let _ = env_logger::try_init();
            let addr = String::from("127.0.0.1:1400");
            let server = Server::new(&addr); // 0 is service id
            server
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&server).await;
            sleep(Duration::from_millis(1000)).await;
            let client = RPCClient::new_async(&addr).await.unwrap();
            let service_client = AsyncServiceClient::new_with_service_id(0, &client);
            let response = service_client.hello(Greeting {
                name: String::from("Jack"),
                time: 12,
            });
            let res = response.await.unwrap();
            let greeting_str = res.text;
            info!("SERVER RESPONDED: {}", greeting_str);
            assert_eq!(greeting_str, String::from("Hello, Jack. It is 12 now!"));
            assert_eq!(42, res.owner);
            server.shutdown().await;
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

        #[tokio::test(flavor = "multi_thread")]
        async fn multi_server_rpc() {
            let addrs = vec![
                String::from("127.0.0.1:1500"),
                String::from("127.0.0.1:1600"),
                String::from("127.0.0.1:1700"),
                String::from("127.0.0.1:1800"),
            ];
            let mut servers = Vec::new();
            for (id, addr) in addrs.iter().enumerate() {
                let server = Server::new(addr);
                server
                    .register_service_with_id(id as u64, &Arc::new(IdServer { id: id as u64 }))
                    .await;
                Server::listen_and_resume(&server).await;
                servers.push(server);
            }
            sleep(Duration::from_millis(1000)).await;
            for (id, addr) in addrs.iter().enumerate() {
                let client = RPCClient::new_async(addr).await.unwrap();
                let service_client = AsyncServiceClient::new_with_service_id(id as u64, &client);
                let id_res = service_client.query_server_id().await;
                let id_un = id_res.unwrap();
                assert_eq!(id_un, id as u64);
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
            }
            for server in servers {
                server.shutdown().await;
            }
        }
    }

    mod parallel {
        use super::struct_service::*;
        use super::*;
        use crate::rpc::{RPCClient, Server, ServiceClient, DEFAULT_CLIENT_POOL};
        use bifrost_hasher::hash_str;
        use futures::prelude::stream::*;
        use futures::FutureExt;

        #[tokio::test(flavor = "multi_thread")]
        pub async fn lots_of_reqs() {
            let _ = env_logger::try_init();
            let addr = String::from("127.0.0.1:1411");
            let server = Server::new(&addr); // 0 is service id
            server
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&server).await;
            sleep(Duration::from_millis(1000)).await;
            let client = RPCClient::new_async(&addr).await.unwrap();
            let service_client = AsyncServiceClient::new_with_service_id(0, &client);

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
                            .get_by_id(server_id, move |_| addr)
                            .await
                            .unwrap();
                        let service_client = AsyncServiceClient::new_with_service_id(0, &client);
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
            server.shutdown().await;
        }
    }
}
