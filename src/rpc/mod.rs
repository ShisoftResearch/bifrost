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
use std::sync::Mutex as StdMutex;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::time::sleep;
use tokio::time::*;

lazy_static! {
    pub static ref DEFAULT_CLIENT_POOL: ClientPool = ClientPool::new();
}

#[cfg(test)]
lazy_static! {
    /// One-shot hooks scoped to the server identity they were installed for.
    ///
    /// The scoping is load-bearing, not decoration. These fire on paths that
    /// nearly every RPC test walks, and an unscoped `take()` hands the hook to
    /// whichever test reaches the site first — which is how a hook installed by
    /// one test came to be run by another, on the other side of a channel whose
    /// sender had already been dropped.
    static ref CLIENT_POOL_EVICT_AFTER_READ: StdMutex<Option<(u64, Box<dyn FnOnce() + Send>)>> =
        StdMutex::new(None);
    static ref LISTENER_AFTER_PUBLICATION: StdMutex<Option<Box<dyn FnOnce() + Send>>> =
        StdMutex::new(None);
    static ref CLIENT_POOL_BEFORE_INSERT: StdMutex<Option<(u64, Box<dyn FnOnce() + Send>)>> =
        StdMutex::new(None);
    static ref CLIENT_POOL_AFTER_RESERVATION: StdMutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    > = StdMutex::new(None);
    static ref CLIENT_POOL_WAITER_READY: StdMutex<Option<tokio::sync::oneshot::Sender<()>>> =
        StdMutex::new(None);
    static ref RPC_SHUTDOWN_AFTER_CLOSING: StdMutex<Option<Box<dyn FnOnce() + Send>>> =
        StdMutex::new(None);
    static ref LISTENER_AFTER_COMPLETION: StdMutex<Option<Box<dyn FnOnce() + Send>>> =
        StdMutex::new(None);
}

/// Claim a one-shot hook only if it was installed for this server.
///
/// Leaving a non-matching hook in place is the whole point: it belongs to a test
/// that is still waiting for it, and running it here would strand that test.
#[cfg(test)]
fn take_hook_for(
    slot: &StdMutex<Option<(u64, Box<dyn FnOnce() + Send>)>>,
    server_id: u64,
) -> Option<Box<dyn FnOnce() + Send>> {
    let mut slot = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    match slot.as_ref() {
        Some((target, _)) if *target == server_id => slot.take().map(|(_, hook)| hook),
        _ => None,
    }
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

#[derive(Clone)]
struct ListenerIdentity(Arc<()>);

impl ListenerIdentity {
    fn new() -> Self {
        Self(Arc::new(()))
    }

    fn is_same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
struct ActiveListener {
    identity: ListenerIdentity,
    tcp_server: Arc<tcp::server::Server>,
    completed: tokio::sync::watch::Receiver<bool>,
}

struct ListenerRun {
    identity: ListenerIdentity,
    tcp_server: Arc<tcp::server::Server>,
    completed: tokio::sync::watch::Sender<bool>,
}

#[derive(Default)]
struct ListenerLifecycle {
    shutting_down: bool,
    active: Option<ActiveListener>,
}

pub struct Server {
    services: Arc<PtrHashMap<u64, Weak<dyn RPCService>>>,
    service_owners: StdMutex<BTreeMap<u64, Arc<dyn RPCService>>>,
    pub address: String,
    pub server_id: u64,
    listener: StdMutex<ListenerLifecycle>,
    service_shortcut_tokens: StdMutex<BTreeMap<u64, ShortcutToken>>,
    service_lifecycle: StdMutex<()>,
    shutting_down: AtomicBool,
}

unsafe impl Sync for Server {}

pub struct ClientPool {
    clients: PtrHashMap<u64, Arc<RPCClient>>,
    constructions: StdMutex<BTreeMap<u64, Arc<ClientConstruction>>>,
}

struct ClientConstruction {
    expected_registration: Option<tcp::shortcut::ShortcutToken>,
    completed: tokio::sync::watch::Sender<bool>,
}

struct ClientConstructionOwner<'a> {
    pool: &'a ClientPool,
    server_id: u64,
    construction: Arc<ClientConstruction>,
    armed: bool,
}

impl<'a> ClientConstructionOwner<'a> {
    fn new(pool: &'a ClientPool, server_id: u64, construction: Arc<ClientConstruction>) -> Self {
        Self {
            pool,
            server_id,
            construction,
            armed: true,
        }
    }

    fn cancel(&mut self) -> bool {
        if !self.armed {
            return false;
        }
        self.armed = false;
        self.pool
            .cancel_construction(self.server_id, &self.construction)
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ClientConstructionOwner<'_> {
    fn drop(&mut self) {
        self.cancel();
    }
}

enum ConstructionAccess {
    Ready(Arc<RPCClient>),
    Owner(Arc<ClientConstruction>),
    Wait(Arc<ClientConstruction>),
}

fn same_tcp_registration(
    left: Option<&tcp::shortcut::ShortcutToken>,
    right: Option<&tcp::shortcut::ShortcutToken>,
) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.is_same(right),
        (None, None) => true,
        _ => false,
    }
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
    let services = server.services.clone();
    Arc::new(move |data| {
        let services = services.clone();
        async move {
            let (svr_id, data) = read_u64_head(data);
            let service = services.get(&svr_id);
            trace!("Processing request for service {}", svr_id);
            match service.and_then(|service| service.upgrade()) {
                Some(service) => {
                    let svr_res = service.dispatch(data).await;
                    encode_res(svr_res)
                }
                None => {
                    let service_list = services
                        .entries()
                        .into_iter()
                        .filter_map(|(sid, service)| {
                            service
                                .upgrade()
                                .map(|service| format!("{}:{}", sid, service.service_symbol()))
                        })
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

#[cfg(test)]
fn run_listener_after_publication_hook() {
    if let Some(hook) = LISTENER_AFTER_PUBLICATION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
    {
        hook();
    }
}

pub fn read_u64_head(mut data: BytesMut) -> (u64, BytesMut) {
    let num = data.get_u64_le();
    (num, data)
}

impl Server {
    pub fn new(address: &String) -> Arc<Server> {
        Arc::new(Server {
            services: Arc::new(PtrHashMap::with_capacity(16)),
            service_owners: StdMutex::new(BTreeMap::new()),
            address: address.clone(),
            server_id: hash_str(address),
            listener: StdMutex::new(ListenerLifecycle::default()),
            service_shortcut_tokens: StdMutex::new(BTreeMap::new()),
            service_lifecycle: StdMutex::new(()),
            shutting_down: AtomicBool::new(false),
        })
    }

    fn begin_listener(&self) -> io::Result<ListenerRun> {
        let mut lifecycle = self
            .listener
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if lifecycle.shutting_down || self.shutting_down.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "RPC server is shutting down",
            ));
        }
        if lifecycle.active.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "RPC server already has an active listener",
            ));
        }

        let identity = ListenerIdentity::new();
        let tcp_server = Arc::new(tcp::server::Server::new());
        let (completed, completed_rx) = tokio::sync::watch::channel(false);
        lifecycle.active = Some(ActiveListener {
            identity: identity.clone(),
            tcp_server: tcp_server.clone(),
            completed: completed_rx,
        });
        Ok(ListenerRun {
            identity,
            tcp_server,
            completed,
        })
    }

    fn complete_listener(&self, run: &ListenerRun, retain_active: bool) {
        let retired = {
            let mut lifecycle = self
                .listener
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let _ = run.completed.send(true);
            if !retain_active
                && lifecycle
                    .active
                    .as_ref()
                    .map(|active| active.identity.is_same(&run.identity))
                    == Some(true)
            {
                lifecycle.active.take()
            } else {
                None
            }
        };
        drop(retired);
        #[cfg(test)]
        {
            let hook = LISTENER_AFTER_COMPLETION
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
    }

    pub async fn listen(server: &Arc<Server>) -> Result<(), Box<dyn Error>> {
        let run = server.begin_listener()?;
        #[cfg(test)]
        run_listener_after_publication_hook();

        let result = run
            .tcp_server
            .listen(&server.address, request_callback(server))
            .await;
        let retain_active = result.is_ok() && server.address == tcp::STANDALONE_ADDRESS;
        server.complete_listener(&run, retain_active);
        result
    }

    pub async fn listen_and_resume(server: &Arc<Server>) {
        let run = match server.begin_listener() {
            Ok(run) => run,
            Err(error) => {
                error!(
                    "Cannot start RPC listener on {}: {:?}",
                    server.address, error
                );
                return;
            }
        };
        let address = server.address.clone();
        #[cfg(test)]
        run_listener_after_publication_hook();

        let callback = request_callback(server);
        let listener_server = Arc::downgrade(server);
        let handle = tokio::spawn(async move {
            let result = run.tcp_server.listen(&address, callback).await;
            if let Some(listener_server) = listener_server.upgrade() {
                let retain_active = result.is_ok() && address == tcp::STANDALONE_ADDRESS;
                listener_server.complete_listener(&run, retain_active);
            }

            if let Err(e) = result {
                error!("RPC server error: {:?}", e);
            }
        });
        drop(handle);

        sleep(Duration::from_secs(1)).await
    }

    pub async fn shutdown(&self) {
        info!("Shutting down RPC server on {}", self.address);
        self.shutting_down.store(true, Ordering::Release);
        #[cfg(test)]
        {
            let hook = RPC_SHUTDOWN_AFTER_CLOSING
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let active = {
            let mut lifecycle = self
                .listener
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lifecycle.shutting_down = true;
            lifecycle.active.clone()
        };
        let tcp_registration = active
            .as_ref()
            .and_then(|active| active.tcp_server.shutdown_owned());
        if let Some(active) = active.as_ref() {
            let mut completed = active.completed.clone();
            while !*completed.borrow() {
                if completed.changed().await.is_err() {
                    break;
                }
            }
        }
        let retired = {
            let mut lifecycle = self
                .listener
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if lifecycle
                .active
                .as_ref()
                .zip(active.as_ref())
                .map(|(current, stopped)| current.identity.is_same(&stopped.identity))
                == Some(true)
            {
                lifecycle.active.take()
            } else {
                None
            }
        };
        drop(retired);
        if let Some(tcp_registration) = tcp_registration.as_ref() {
            DEFAULT_CLIENT_POOL.evict_owned(self.server_id, tcp_registration);
        }
        self.release_services();
    }

    pub async fn register_service_with_id<T>(&self, service_id: u64, service: &Arc<T>)
    where
        T: RPCService + Sized + 'static,
    {
        self.register_service_inner(service_id, service, true).await
    }

    async fn register_service_inner<T>(&self, service_id: u64, service: &Arc<T>, owned: bool)
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
        let service: Arc<dyn RPCService> = service;
        let weak_service = Arc::downgrade(&service);
        self.services.insert(service_id, weak_service);
        let retired_service = if owned {
            self.service_owners
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(service_id, service)
        } else {
            // Weak registration: the caller keeps it alive. Drop any strong
            // reference a previous owned registration under this id left behind,
            // or the cycle survives the switch.
            let previous = self
                .service_owners
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&service_id);
            drop(service);
            previous
        };
        drop(_lifecycle);
        drop(retired_service);
    }

    /// Register a service the server does NOT keep alive.
    ///
    /// The server holds only the `Weak` it dispatches through; keeping the
    /// caller's service alive is the caller's job. That is the point: a service
    /// may legitimately own the server that hosts it, and when the server also
    /// owns the service the pair can never be dropped. `shutdown` breaks that
    /// cycle -- see `shutdown_releases_service_that_owns_its_server` -- but a
    /// host that is merely DROPPED cannot await `shutdown`, so its whole object
    /// graph is stranded. Nebuchadnezzar measured that as ~26 threads and an
    /// entire memory store per server.
    ///
    /// Registering weakly removes the cycle instead of unwinding it afterwards.
    /// The caller must hold its `Arc` somewhere that is not reachable FROM the
    /// service, or the service is dropped immediately and every dispatch to it
    /// fails to upgrade.
    pub async fn register_service_weak_with_id<T>(&self, service_id: u64, service: &Arc<T>)
    where
        T: RPCService + Sized + 'static,
    {
        self.register_service_inner(service_id, service, false).await
    }

    pub async fn register_service<T>(&self, service: &Arc<T>)
    where
        T: RPCServiceWithId + Sized + 'static,
    {
        self.register_service_with_id(T::SERVICE_ID, service).await
    }

    pub async fn remove_service(&self, service_id: u64) {
        self.remove_service_now(service_id)
    }

    /// `remove_service` without the `async`, so a `Drop` can call it.
    ///
    /// A server holds a **strong** `Arc` to every service it hosts, so a host
    /// that goes away without awaiting `shutdown` strands the whole object
    /// graph behind its services -- measured in Nebuchadnezzar as ~26 threads
    /// and one entire memory store retained per server. Nothing in the removal
    /// actually awaits, so the only thing standing between a `Drop` and letting
    /// go was the signature.
    ///
    /// Removing one service by id, rather than releasing them all, is the point:
    /// a process hosting several servers shares process-global machinery between
    /// them -- notably the Raft subscription callback, which lives on whichever
    /// server prepared it first -- and taking that down on behalf of one server
    /// breaks every other server still running.
    pub fn remove_service_now(&self, service_id: u64) {
        let retired_service = {
            let _lifecycle = self
                .service_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.services.remove(&service_id);
            let service = self
                .service_owners
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&service_id);
            let shortcut_token = self
                .service_shortcut_tokens
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&service_id);
            if let (Some(service), Some(token)) = (service.as_ref(), shortcut_token) {
                service.unregister_shortcut_service(self.server_id, service_id, &token);
            }
            service
        };
        drop(retired_service);
    }

    fn release_services(&self) {
        let retired_services = {
            let _lifecycle = self
                .service_lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let services = std::mem::take(
                &mut *self
                    .service_owners
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            );
            let mut shortcut_tokens = self
                .service_shortcut_tokens
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for (service_id, service) in services.iter() {
                self.services.remove(service_id);
                if let Some(token) = shortcut_tokens.remove(service_id) {
                    service.unregister_shortcut_service(self.server_id, *service_id, &token);
                }
            }
            shortcut_tokens.clear();
            services
        };
        drop(retired_services);
    }

    pub(crate) fn owns_registered_service<T>(&self, service_id: u64, service: &Arc<T>) -> bool
    where
        T: RPCService + Sized + 'static,
    {
        let _lifecycle = self
            .service_lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.shutting_down.load(Ordering::Acquire) {
            return false;
        }
        let service: Arc<dyn RPCService> = service.clone();
        self.service_owners
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&service_id)
            .map(|registered| Arc::ptr_eq(registered, &service))
            == Some(true)
    }

    pub fn address(&self) -> &String {
        &self.address
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutting_down.store(true, Ordering::Release);
        let active = {
            let mut lifecycle = self
                .listener
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lifecycle.shutting_down = true;
            lifecycle.active.take()
        };
        if let Some(active) = active {
            if let Some(tcp_registration) = active.tcp_server.shutdown_owned() {
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
            constructions: StdMutex::new(BTreeMap::new()),
        }
    }

    pub async fn get(&self, addr: &String) -> io::Result<Arc<RPCClient>> {
        let addr_clone = addr.clone();
        let server_id = hash_str(addr);
        self.get_by_id(server_id, move |_| Some(addr_clone)).await
    }

    /// Get or create a client for a member, resolving its address lazily.
    ///
    /// The resolver returns `Option` because an unresolvable member is a routine
    /// outcome rather than a bug -- a stored placement table can name a member that
    /// has left, and an operator can ask to reach one that was never there. It used
    /// to return `String`, which left callers no way to say "I do not know that
    /// one" except to panic.
    pub async fn get_by_id<F>(&self, server_id: u64, addr_fn: F) -> io::Result<Arc<RPCClient>>
    where
        F: FnOnce(u64) -> Option<String>,
    {
        if let Some(client) = self.clients.get(&server_id) {
            return Ok(client);
        }
        let address = addr_fn(server_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no address known for server id {server_id}"),
            )
        })?;

        loop {
            if let Some(client) = self.clients.get(&server_id) {
                return Ok(client);
            }
            let observed_registration = tcp::shortcut::registration_token(server_id).await;
            let access = {
                let mut constructions = self
                    .constructions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(client) = self.clients.get(&server_id) {
                    ConstructionAccess::Ready(client)
                } else if let Some(construction) = constructions.get(&server_id) {
                    ConstructionAccess::Wait(construction.clone())
                } else {
                    let construction = Arc::new(ClientConstruction {
                        expected_registration: observed_registration,
                        completed: tokio::sync::watch::channel(false).0,
                    });
                    constructions.insert(server_id, construction.clone());
                    ConstructionAccess::Owner(construction)
                }
            };

            let construction = match access {
                ConstructionAccess::Ready(client) => return Ok(client),
                ConstructionAccess::Wait(construction) => {
                    let mut completed = construction.completed.subscribe();
                    #[cfg(test)]
                    if let Some(ready) = CLIENT_POOL_WAITER_READY
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                    {
                        let _ = ready.send(());
                    }
                    let still_active = self
                        .constructions
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .get(&server_id)
                        .map(|current| Arc::ptr_eq(current, &construction))
                        .unwrap_or(false);
                    if still_active && !*completed.borrow() {
                        let _ = completed.changed().await;
                    }
                    continue;
                }
                ConstructionAccess::Owner(construction) => construction,
            };
            let mut owner = ClientConstructionOwner::new(self, server_id, construction.clone());
            #[cfg(test)]
            {
                let hook = CLIENT_POOL_AFTER_RESERVATION
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take();
                if let Some((reached, release)) = hook {
                    let _ = reached.send(());
                    let _ = release.await;
                }
            }

            let connect_result =
                timeout(Duration::from_secs(5), RPCClient::new_async(&address)).await;
            let client = match connect_result {
                Ok(Ok(client)) => client,
                Ok(Err(error)) => {
                    let owned = owner.cancel();
                    if owned {
                        return Err(error);
                    }
                    continue;
                }
                Err(error) => {
                    let owned = owner.cancel();
                    if owned {
                        return Err(error.into());
                    }
                    continue;
                }
            };
            #[cfg(test)]
            {
                let hook = take_hook_for(&CLIENT_POOL_BEFORE_INSERT, server_id);
                if let Some(hook) = hook {
                    hook();
                }
            }
            let current_registration = tcp::shortcut::registration_token(server_id).await;
            let (result, completed) = {
                let mut constructions = self
                    .constructions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let still_owner = constructions
                    .get(&server_id)
                    .map(|current| Arc::ptr_eq(current, &construction))
                    .unwrap_or(false);
                if !still_owner {
                    (None, false)
                } else {
                    let client_matches_expected = same_tcp_registration(
                        client.client.shortcut_token(),
                        construction.expected_registration.as_ref(),
                    );
                    let registration_is_current = same_tcp_registration(
                        current_registration.as_ref(),
                        construction.expected_registration.as_ref(),
                    );
                    constructions.remove(&server_id);
                    if client_matches_expected && registration_is_current {
                        let selected = if let Some(existing) = self.clients.get(&server_id) {
                            existing
                        } else {
                            self.clients.insert(server_id, client.clone());
                            client
                        };
                        (Some(selected), true)
                    } else {
                        (None, true)
                    }
                }
            };
            owner.disarm();
            if completed {
                construction.completed.send_replace(true);
            }
            if let Some(result) = result {
                return Ok(result);
            }
        }
    }

    fn cancel_construction(&self, server_id: u64, construction: &Arc<ClientConstruction>) -> bool {
        let owned = {
            let mut constructions = self
                .constructions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if constructions
                .get(&server_id)
                .map(|current| Arc::ptr_eq(current, construction))
                == Some(true)
            {
                constructions.remove(&server_id);
                true
            } else {
                false
            }
        };
        if owned {
            construction.completed.send_replace(true);
        }
        owned
    }

    fn evict_owned(&self, server_id: u64, registration: &tcp::shortcut::ShortcutToken) {
        #[cfg(test)]
        if let Some(hook) = take_hook_for(&CLIENT_POOL_EVICT_AFTER_READ, server_id) {
            hook();
        }
        let (cancelled, removed) = {
            let mut constructions = self
                .constructions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let cancelled = if constructions
                .get(&server_id)
                .and_then(|construction| construction.expected_registration.as_ref())
                .map(|expected| expected.is_same(registration))
                == Some(true)
            {
                constructions.remove(&server_id)
            } else {
                None
            };
            let removed = if let Some(client) = self.clients.lock(&server_id) {
                let still_owned = client
                    .client
                    .shortcut_token()
                    .map(|cached_registration| cached_registration.is_same(registration))
                    == Some(true);
                if still_owned {
                    Some(client.remove())
                } else {
                    None
                }
            } else {
                None
            };
            (cancelled, removed)
        };
        if let Some(cancelled) = cancelled {
            cancelled.completed.send_replace(true);
        }
        drop(removed);
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
        async fn dropping_running_server_releases_listener_cycle_and_services() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);

            let server = Server::new(&addr);
            let weak_server = Arc::downgrade(&server);
            let service = Arc::new(HelloServer);
            let weak_service = Arc::downgrade(&service);

            server.register_service_with_id(0, &service).await;
            Server::listen_and_resume(&server).await;
            drop(service);
            drop(server);

            for _ in 0..100 {
                if weak_server.upgrade().is_none() && weak_service.upgrade().is_none() {
                    break;
                }
                sleep(Duration::from_millis(5)).await;
            }

            assert!(
                weak_server.upgrade().is_none(),
                "running listener retained its RPC Server after the caller dropped it"
            );
            assert!(
                weak_service.upgrade().is_none(),
                "running listener retained the RPC Server's registered services"
            );
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
        async fn standalone_replacement_drops_old_callback_outside_shortcut_lock() {
            let _serial = crate::tcp::STANDALONE_TEST_LOCK.lock().await;
            const CHILD_ENV: &str = "BIFROST_SHORTCUT_REPLACEMENT_CHILD";
            if std::env::var_os(CHILD_ENV).is_none() {
                let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg(
                        "rpc::test::simple_service::standalone_replacement_drops_old_callback_outside_shortcut_lock",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ENV, "1")
                    .spawn()
                    .unwrap();
                let deadline = std::time::Instant::now() + Duration::from_secs(6);
                loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        assert!(status.success(), "isolated replacement child failed");
                        return;
                    }
                    if std::time::Instant::now() >= deadline {
                        child.kill().unwrap();
                        let _ = child.wait();
                        panic!("standalone replacement deadlocked");
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }

            let addr = crate::tcp::STANDALONE_ADDRESS.to_string();
            let old_server = Server::new(&addr);
            let weak_old_server = Arc::downgrade(&old_server);
            old_server
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&old_server).await;
            let old_registration = crate::tcp::shortcut::registration_token(hash_str(&addr))
                .await
                .expect("old shortcut must be registered");
            drop(old_server);

            let replacement = Server::new(&addr);
            replacement
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&replacement).await;
            let replacement_registration =
                crate::tcp::shortcut::registration_token(hash_str(&addr))
                    .await
                    .expect("replacement shortcut must be registered");

            assert!(!old_registration.is_same(&replacement_registration));
            assert!(
                weak_old_server.upgrade().is_none(),
                "replaced callback retained the old server"
            );
            replacement.shutdown().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shutdown_waits_for_concurrent_direct_listener_publication() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);
            let server = Server::new(&addr);
            let (published_tx, published_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            *LISTENER_AFTER_PUBLICATION
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                published_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }));

            let listen_server = server.clone();
            let listen = tokio::spawn(async move {
                Server::listen(&listen_server).await.unwrap();
            });
            published_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("direct listener was not published");

            let shutdown_server = server.clone();
            let mut shutdown = tokio::spawn(async move { shutdown_server.shutdown().await });
            let completed_before_listener_started =
                tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                    .await
                    .is_ok();

            release_tx.send(()).unwrap();
            listen.await.unwrap();
            if !completed_before_listener_started {
                shutdown.await.unwrap();
            }
            assert!(
                !completed_before_listener_started,
                "shutdown returned before the direct listener completed"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shutdown_waits_for_concurrent_resumed_listener_publication() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);
            let server = Server::new(&addr);
            let (published_tx, published_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            *LISTENER_AFTER_PUBLICATION
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                published_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }));

            let listen_server = server.clone();
            let listen =
                tokio::spawn(async move { Server::listen_and_resume(&listen_server).await });
            published_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("resumed listener was not published");

            let shutdown_server = server.clone();
            let mut shutdown = tokio::spawn(async move { shutdown_server.shutdown().await });
            let completed_before_listener_started =
                tokio::time::timeout(Duration::from_millis(100), &mut shutdown)
                    .await
                    .is_ok();

            release_tx.send(()).unwrap();
            listen.await.unwrap();
            if !completed_before_listener_started {
                shutdown.await.unwrap();
            }
            assert!(
                !completed_before_listener_started,
                "shutdown returned before the resumed listener completed"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn already_shutdown_server_rejects_direct_standalone_listen() {
            let _serial = crate::tcp::STANDALONE_TEST_LOCK.lock().await;
            let addr = crate::tcp::STANDALONE_ADDRESS.to_string();
            let server = Server::new(&addr);
            server.shutdown().await;

            let result = Server::listen(&server).await;

            assert!(result.is_err(), "listen must reject a stopped server");
            assert!(!crate::tcp::shortcut::is_local(hash_str(&addr)).await);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn already_shutdown_server_rejects_resumed_standalone_listen() {
            let _serial = crate::tcp::STANDALONE_TEST_LOCK.lock().await;
            let addr = crate::tcp::STANDALONE_ADDRESS.to_string();
            let server = Server::new(&addr);
            server.shutdown().await;

            Server::listen_and_resume(&server).await;

            assert!(!crate::tcp::shortcut::is_local(hash_str(&addr)).await);
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn standalone_shutdown_owns_retirement_before_returning() {
            let _serial = crate::tcp::STANDALONE_TEST_LOCK.lock().await;
            let addr = crate::tcp::STANDALONE_ADDRESS.to_string();
            let server_id = hash_str(&addr);
            let server = Server::new(&addr);
            let (published_tx, published_rx) = std::sync::mpsc::channel();
            let (publish_release_tx, publish_release_rx) = std::sync::mpsc::channel();
            *LISTENER_AFTER_PUBLICATION
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                published_tx.send(()).unwrap();
                publish_release_rx.recv().unwrap();
            }));
            let (closing_tx, closing_rx) = std::sync::mpsc::channel();
            let (closing_release_tx, closing_release_rx) = std::sync::mpsc::channel();
            *RPC_SHUTDOWN_AFTER_CLOSING
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                closing_tx.send(()).unwrap();
                closing_release_rx.recv().unwrap();
            }));
            let (completed_tx, completed_rx) = std::sync::mpsc::channel();
            let (completion_release_tx, completion_release_rx) = std::sync::mpsc::channel();
            *LISTENER_AFTER_COMPLETION
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                completed_tx.send(()).unwrap();
                completion_release_rx.recv().unwrap();
            }));

            let listen_server = server.clone();
            let listen = tokio::spawn(async move {
                Server::listen(&listen_server).await.unwrap();
            });
            published_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("standalone listener was not published");
            let shutdown_server = server.clone();
            let shutdown = tokio::spawn(async move { shutdown_server.shutdown().await });
            closing_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("shutdown did not mark the server closing");

            publish_release_tx.send(()).unwrap();
            completed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("standalone completion did not reach retirement");
            closing_release_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), shutdown)
                .await
                .expect("shutdown did not finish")
                .unwrap();
            let shortcut_retired_before_return = !crate::tcp::shortcut::is_local(server_id).await;

            completion_release_tx.send(()).unwrap();
            listen.await.unwrap();
            assert!(
                shortcut_retired_before_return,
                "shutdown returned while the finishing standalone run retained its registration"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shutdown_evicts_generation_published_after_tcp_precheck() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);
            let server_id = hash_str(&addr);
            DEFAULT_CLIENT_POOL.clients.remove(&server_id);
            let server = Server::new(&addr);

            let (before_tx, before_rx) = std::sync::mpsc::channel();
            let (before_release_tx, before_release_rx) = std::sync::mpsc::channel();
            *crate::tcp::server::TCP_BEFORE_REGISTRATION_PUBLICATION
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                before_tx.send(()).unwrap();
                before_release_rx.recv().unwrap();
            }));
            let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
            let (shutdown_release_tx, shutdown_release_rx) = std::sync::mpsc::channel();
            *crate::tcp::server::TCP_SHUTDOWN_AFTER_REGISTRATION_SNAPSHOT
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                shutdown_tx.send(()).unwrap();
                shutdown_release_rx.recv().unwrap();
            }));
            let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
            let (attempt_release_tx, attempt_release_rx) = std::sync::mpsc::channel();
            *crate::tcp::server::TCP_AFTER_REGISTRATION_ATTEMPT
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                attempt_tx.send(()).unwrap();
                attempt_release_rx.recv().unwrap();
            }));

            let listen_server = server.clone();
            let listen = tokio::spawn(async move {
                Server::listen(&listen_server).await.unwrap();
            });
            before_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("TCP listener did not reach pre-publication barrier");
            let shutdown_server = server.clone();
            let shutdown = tokio::spawn(async move { shutdown_server.shutdown().await });
            shutdown_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("TCP shutdown did not snapshot registration state");

            before_release_tx.send(()).unwrap();
            attempt_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("TCP listener did not finish its publication attempt");
            if crate::tcp::shortcut::registration_token(server_id)
                .await
                .is_some()
            {
                let transient = DEFAULT_CLIENT_POOL.get(&addr).await.unwrap();
                assert!(transient.client.shortcut_token().is_some());
            }
            attempt_release_tx.send(()).unwrap();
            shutdown_release_tx.send(()).unwrap();
            listen.await.unwrap();
            shutdown.await.unwrap();

            let cached_after_shutdown = DEFAULT_CLIENT_POOL.clients.get(&server_id);
            DEFAULT_CLIENT_POOL.clients.remove(&server_id);
            assert!(
                cached_after_shutdown.is_none(),
                "shutdown left a transient stopped TCP generation cached"
            );
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
            let _serial = crate::tcp::STANDALONE_TEST_LOCK.lock().await;
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
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((
                server_id,
                Box::new(move || {
                    DEFAULT_CLIENT_POOL.clients.remove(&server_id);
                    DEFAULT_CLIENT_POOL
                        .clients
                        .insert(server_id, old_client_for_hook);
                }),
            ));

            DEFAULT_CLIENT_POOL.evict_owned(server_id, &new_token);

            let cached = DEFAULT_CLIENT_POOL.clients.get(&server_id).unwrap();
            assert!(Arc::ptr_eq(&cached, &old_client));

            new_server.shutdown().await;
            old_server.shutdown().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn late_old_pool_insertion_does_not_overwrite_competing_replacement() {
            let _serial = crate::tcp::STANDALONE_TEST_LOCK.lock().await;
            let addr = crate::tcp::STANDALONE_ADDRESS.to_string();
            let server_id = hash_str(&addr);
            DEFAULT_CLIENT_POOL.clients.remove(&server_id);

            let old_server = Server::new(&addr);
            old_server
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&old_server).await;

            let (connected_tx, connected_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            *CLIENT_POOL_BEFORE_INSERT
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some((
                server_id,
                Box::new(move || {
                    connected_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }),
            ));
            let old_get =
                tokio::spawn(async move { DEFAULT_CLIENT_POOL.get(&addr).await.unwrap() });
            connected_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("old client did not reach pre-insert barrier");

            old_server.shutdown().await;
            let replacement_addr = crate::tcp::STANDALONE_ADDRESS.to_string();
            let replacement_server = Server::new(&replacement_addr);
            replacement_server
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&replacement_server).await;
            let replacement_client = DEFAULT_CLIENT_POOL.get(&replacement_addr).await.unwrap();

            release_tx.send(()).unwrap();
            let _old_client = old_get.await.unwrap();
            let cached = DEFAULT_CLIENT_POOL.clients.get(&server_id).unwrap();
            let replacement_preserved = Arc::ptr_eq(&cached, &replacement_client);

            replacement_server.shutdown().await;
            DEFAULT_CLIENT_POOL.clients.remove(&server_id);
            assert!(
                replacement_preserved,
                "late old construction overwrote the replacement client"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn old_local_eviction_preserves_tokenless_socket_replacement() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);
            let server_id = hash_str(&addr);
            DEFAULT_CLIENT_POOL.clients.remove(&server_id);

            let old_server = Server::new(&addr);
            old_server
                .register_service_with_id(0, &Arc::new(HelloServer))
                .await;
            Server::listen_and_resume(&old_server).await;
            let old_registration = crate::tcp::shortcut::registration_token(server_id)
                .await
                .expect("old local registration must exist");
            old_server.shutdown().await;

            let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let accept = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let _ = release_rx.await;
                drop(socket);
            });
            let replacement = RPCClient::new_async(&addr).await.unwrap();
            assert!(replacement.client.shortcut_token().is_none());
            DEFAULT_CLIENT_POOL
                .clients
                .insert(server_id, replacement.clone());

            DEFAULT_CLIENT_POOL.evict_owned(server_id, &old_registration);

            let cached = DEFAULT_CLIENT_POOL.clients.get(&server_id);
            let replacement_preserved = cached
                .as_ref()
                .map(|cached| Arc::ptr_eq(cached, &replacement))
                .unwrap_or(false);
            DEFAULT_CLIENT_POOL.clients.remove(&server_id);
            let _ = release_tx.send(());
            accept.await.unwrap();
            assert!(
                replacement_preserved,
                "old local generation evicted a tokenless socket replacement"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn aborted_pool_owner_releases_reservation_and_wakes_waiter() {
            let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = reserved.local_addr().unwrap().to_string();
            drop(reserved);
            let server_id = hash_str(&addr);
            DEFAULT_CLIENT_POOL.clients.remove(&server_id);

            let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
            let accept = tokio::spawn(async move { listener.accept().await.unwrap().0 });
            let (owner_reached_tx, owner_reached_rx) = tokio::sync::oneshot::channel();
            let (owner_release_tx, owner_release_rx) = tokio::sync::oneshot::channel();
            *CLIENT_POOL_AFTER_RESERVATION
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some((owner_reached_tx, owner_release_rx));

            let owner_addr = addr.clone();
            let owner =
                tokio::spawn(async move { DEFAULT_CLIENT_POOL.get(&owner_addr).await.unwrap() });
            owner_reached_rx
                .await
                .expect("construction owner did not reach reservation barrier");

            let (waiter_ready_tx, waiter_ready_rx) = tokio::sync::oneshot::channel();
            *CLIENT_POOL_WAITER_READY
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(waiter_ready_tx);
            let waiter_addr = addr.clone();
            let mut waiter =
                tokio::spawn(async move { DEFAULT_CLIENT_POOL.get(&waiter_addr).await.unwrap() });
            waiter_ready_rx
                .await
                .expect("second getter did not wait on owner reservation");

            owner.abort();
            match owner.await {
                Err(error) => assert!(error.is_cancelled()),
                Ok(_) => panic!("aborted construction owner completed"),
            }
            let _ = owner_release_tx.send(());

            let waiter_result = tokio::time::timeout(Duration::from_secs(1), &mut waiter).await;
            let waiter_completed = waiter_result.is_ok();
            if waiter_completed {
                waiter_result.unwrap().unwrap();
            } else {
                waiter.abort();
                let _ = waiter.await;
                if let Some(stranded) = DEFAULT_CLIENT_POOL
                    .constructions
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&server_id)
                {
                    let _ = stranded.completed.send(true);
                }
            }

            DEFAULT_CLIENT_POOL.clients.remove(&server_id);
            if waiter_completed {
                drop(accept.await.unwrap());
            } else {
                accept.abort();
                let _ = accept.await;
            }
            assert!(
                waiter_completed,
                "aborted owner stranded the construction reservation"
            );
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
            assert_eq!(
                final_count, baseline,
                "file descriptors grew from {} to {}",
                baseline, final_count
            );
        }

        #[cfg(target_os = "linux")]
        #[tokio::test(flavor = "multi_thread")]
        async fn external_raft_shutdown_releases_driver_fds_before_returning() {
            const CHILD_ENV: &str = "BIFROST_EXTERNAL_RAFT_FD_CHILD";
            if std::env::var_os(CHILD_ENV).is_none() {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg(
                        "rpc::test::simple_service::external_raft_shutdown_releases_driver_fds_before_returning",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ENV, "1")
                    .status()
                    .unwrap();
                assert!(status.success(), "isolated external-shutdown child failed");
                return;
            }

            let open_fd_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
            let baseline = open_fd_count();
            let service = crate::raft::RaftService::new(crate::raft::Options {
                storage: crate::raft::Storage::MEMORY,
                address: "external-raft-fd-release".to_string(),
                service_id: 7,
            });
            let peak = open_fd_count();

            service.shutdown().await;

            let after_shutdown = open_fd_count();
            eprintln!(
                "external shutdown fd baseline={}, peak={}, after={}",
                baseline, peak, after_shutdown
            );
            assert_eq!(
                after_shutdown, baseline,
                "external shutdown returned with driver FDs open: {} -> {}",
                baseline, after_shutdown
            );
            assert_eq!(Arc::strong_count(&service), 1);
            drop(service);
        }

        #[cfg(target_os = "linux")]
        #[tokio::test(flavor = "multi_thread")]
        async fn owned_runtime_shutdown_eventually_releases_owner_and_driver_fds() {
            const CHILD_ENV: &str = "BIFROST_SELF_RAFT_FD_CHILD";
            if std::env::var_os(CHILD_ENV).is_none() {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg(
                        "rpc::test::simple_service::owned_runtime_shutdown_eventually_releases_owner_and_driver_fds",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ENV, "1")
                    .status()
                    .unwrap();
                assert!(status.success(), "isolated self-shutdown child failed");
                return;
            }

            let open_fd_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
            let baseline = open_fd_count();
            let service = crate::raft::RaftService::new(crate::raft::Options {
                storage: crate::raft::Storage::MEMORY,
                address: "self-raft-fd-release".to_string(),
                service_id: 7,
            });
            let peak = open_fd_count();
            let weak_service = Arc::downgrade(&service);
            let shutdown_target = service.clone();
            let shutdown = service
                .rt
                .spawn(async move { shutdown_target.shutdown().await });

            tokio::time::timeout(Duration::from_secs(2), shutdown)
                .await
                .expect("owned-runtime shutdown caller must exit")
                .expect("owned-runtime shutdown task must complete");
            drop(service);

            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            loop {
                let owner_released = weak_service.upgrade().is_none();
                let fd_count = open_fd_count();
                if owner_released && fd_count == baseline {
                    eprintln!(
                        "self shutdown fd baseline={}, peak={}, final={}",
                        baseline, peak, fd_count
                    );
                    break;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "self shutdown did not release owner/FDs: owner={}, baseline={}, peak={}, current={}",
                    owner_released,
                    baseline,
                    peak,
                    fd_count
                );
                sleep(Duration::from_millis(10)).await;
            }
        }

        #[cfg(target_os = "linux")]
        #[tokio::test(flavor = "multi_thread")]
        async fn cancelled_external_teardown_is_awaited_by_next_external_shutdown() {
            const CHILD_ENV: &str = "BIFROST_CANCELLED_EXTERNAL_TEARDOWN_CHILD";
            if std::env::var_os(CHILD_ENV).is_none() {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg(
                        "rpc::test::simple_service::cancelled_external_teardown_is_awaited_by_next_external_shutdown",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ENV, "1")
                    .status()
                    .unwrap();
                assert!(
                    status.success(),
                    "isolated cancelled-external teardown child failed"
                );
                return;
            }

            let open_fd_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
            let baseline = open_fd_count();
            let service = crate::raft::RaftService::new(crate::raft::Options {
                storage: crate::raft::Storage::MEMORY,
                address: "cancelled-external-teardown".to_string(),
                service_id: 7,
            });
            let (teardown_tx, teardown_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            *crate::raft::RAFT_TEARDOWN_BEFORE_DROP
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(move || {
                teardown_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }));

            let first_target = service.clone();
            let first = tokio::spawn(async move { first_target.shutdown().await });
            teardown_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("first external teardown did not transfer runtime ownership");
            first.abort();
            assert!(first.await.unwrap_err().is_cancelled());

            let second_target = service.clone();
            let mut second = tokio::spawn(async move { second_target.shutdown().await });
            let returned_before_teardown =
                tokio::time::timeout(Duration::from_millis(100), &mut second)
                    .await
                    .is_ok();
            release_tx.send(()).unwrap();
            if !returned_before_teardown {
                second.await.unwrap();
            }

            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while open_fd_count() != baseline && tokio::time::Instant::now() < deadline {
                sleep(Duration::from_millis(10)).await;
            }
            let final_fds = open_fd_count();
            drop(service);
            assert!(
                !returned_before_teardown,
                "second external shutdown returned before prior teardown completed"
            );
            assert_eq!(
                final_fds, baseline,
                "external teardown did not restore the exact FD baseline"
            );
        }

        #[cfg(target_os = "linux")]
        #[tokio::test(flavor = "multi_thread")]
        async fn external_shutdown_waits_for_self_runtime_teardown() {
            const CHILD_ENV: &str = "BIFROST_SELF_EXTERNAL_TEARDOWN_CHILD";
            if std::env::var_os(CHILD_ENV).is_none() {
                let status = std::process::Command::new(std::env::current_exe().unwrap())
                    .arg("--exact")
                    .arg(
                        "rpc::test::simple_service::external_shutdown_waits_for_self_runtime_teardown",
                    )
                    .arg("--nocapture")
                    .env(CHILD_ENV, "1")
                    .status()
                    .unwrap();
                assert!(
                    status.success(),
                    "isolated self/external teardown child failed"
                );
                return;
            }

            let open_fd_count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
            let baseline = open_fd_count();
            let service = crate::raft::RaftService::new(crate::raft::Options {
                storage: crate::raft::Storage::MEMORY,
                address: "self-external-teardown".to_string(),
                service_id: 7,
            });
            let (self_returned_tx, self_returned_rx) = std::sync::mpsc::channel();
            let (self_release_tx, self_release_rx) = std::sync::mpsc::channel();
            let self_target = service.clone();
            let self_shutdown = service.rt.spawn(async move {
                self_target.shutdown().await;
                self_returned_tx.send(()).unwrap();
                self_release_rx.recv().unwrap();
            });
            self_returned_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("self-runtime shutdown did not return to its caller task");

            let external_target = service.clone();
            let mut external = tokio::spawn(async move { external_target.shutdown().await });
            let returned_before_self_task_exit =
                tokio::time::timeout(Duration::from_millis(100), &mut external)
                    .await
                    .is_ok();
            self_release_tx.send(()).unwrap();
            let _ = self_shutdown.await;
            if !returned_before_self_task_exit {
                external.await.unwrap();
            }

            let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
            while open_fd_count() != baseline && tokio::time::Instant::now() < deadline {
                sleep(Duration::from_millis(10)).await;
            }
            let final_fds = open_fd_count();
            drop(service);
            assert!(
                !returned_before_self_task_exit,
                "external shutdown returned before self-runtime teardown completed"
            );
            assert_eq!(
                final_fds, baseline,
                "self/external teardown did not restore the exact FD baseline"
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

    mod service_ownership {
        use crate::bytes::BytesMut;
        use crate::rpc::{RPCRequestError, RPCService, Server, ShortcutToken};
        use futures::future::BoxFuture;
        use futures::FutureExt;
        use std::sync::Arc;

        struct ServerOwningService {
            _server: Arc<Server>,
        }

        impl RPCService for ServerOwningService {
            fn dispatch(&self, _data: BytesMut) -> BoxFuture<Result<BytesMut, RPCRequestError>> {
                async { Err(RPCRequestError::FunctionIdNotFound) }.boxed()
            }

            fn register_shortcut_service(
                &self,
                service_ptr: usize,
                _server_id: u64,
                _service_id: u64,
            ) -> ShortcutToken {
                drop(unsafe { Arc::from_raw(service_ptr as *const ServerOwningService) });
                ShortcutToken::new()
            }

            fn unregister_shortcut_service(
                &self,
                _server_id: u64,
                _service_id: u64,
                _token: &ShortcutToken,
            ) {
            }

            fn service_symbol(&self) -> &'static str {
                "ServerOwningService"
            }
        }

        /// A weakly registered service must not keep its server alive, even
        /// when the service owns the server.
        ///
        /// The owned registration below needs `shutdown` to break that cycle. A
        /// host that is merely DROPPED cannot await `shutdown`, so its whole
        /// graph is stranded -- measured in Nebuchadnezzar as ~26 threads and an
        /// entire memory store per server. Registering weakly means the cycle
        /// never forms, so no unwinding is required.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_weakly_registered_service_never_forms_a_cycle() {
            let address = "rpc-weak-service-no-cycle".to_string();
            let server = Server::new(&address);
            let weak_server = Arc::downgrade(&server);
            let service = Arc::new(ServerOwningService {
                _server: server.clone(),
            });
            let weak_service = Arc::downgrade(&service);

            server.register_service_weak_with_id(9, &service).await;

            // The caller still holds it, so it is dispatchable.
            assert!(
                weak_service.upgrade().is_some(),
                "a weakly registered service stays alive while its owner holds it"
            );

            // No shutdown, just drops -- which is the whole point.
            drop(service);
            drop(server);

            assert!(
                weak_service.upgrade().is_none(),
                "the server kept a weakly registered service alive"
            );
            assert!(
                weak_server.upgrade().is_none(),
                "a weakly registered service still stranded its owning server"
            );
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn shutdown_releases_service_that_owns_its_server() {
            let address = "rpc-service-server-cycle".to_string();
            let server = Server::new(&address);
            let weak_server = Arc::downgrade(&server);
            let service = Arc::new(ServerOwningService {
                _server: server.clone(),
            });
            let weak_service = Arc::downgrade(&service);
            server.register_service_with_id(7, &service).await;
            drop(service);

            server.shutdown().await;
            drop(server);

            assert!(
                weak_service.upgrade().is_none(),
                "retired lock-free service node kept the service alive"
            );
            assert!(
                weak_server.upgrade().is_none(),
                "retired service kept its owning RPC server alive"
            );
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
                            .get_by_id(server_id, move |_| Some(addr))
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
