#[macro_export]
macro_rules! dispatch_rpc_service_functions {
    ($s:ty) => {
        use $crate::bytes::BytesMut;
        impl $crate::rpc::RPCService for $s {
            fn dispatch<'a>(
                &'a self,
                data: BytesMut,
            ) -> ::std::pin::Pin<
                Box<
                    dyn Future<
                            Output = Result<$crate::bytes::BytesMut, $crate::rpc::RPCRequestError>,
                        > + Send
                        + 'a,
                >,
            >
            where
                Self: Sized,
            {
                self.inner_dispatch(data)
            }
            fn register_shortcut_service(
                &self,
                service_ptr: usize,
                server_id: u64,
                service_id: u64,
            ) -> $crate::rpc::ShortcutToken {
                let token = $crate::rpc::ShortcutToken::new();
                let service = unsafe { Arc::from_raw(service_ptr as *const $s) };
                let service: Arc<dyn Service> = service;
                let service = Arc::downgrade(&service);
                RPC_SVRS.register((server_id, service_id), (token.clone(), service));
                token
            }
            fn unregister_shortcut_service(
                &self,
                server_id: u64,
                service_id: u64,
                token: &$crate::rpc::ShortcutToken,
            ) {
                RPC_SVRS.unregister_if(&(server_id, service_id), |(registered_token, _)| {
                    registered_token.is_same(token)
                });
            }
            fn service_symbol(&self) -> &'static str {
                stringify!($s)
            }
        }
    };
}

// this macro expansion design took credits from tarpc by Google Inc.
#[macro_export]
macro_rules! service {
    (
        $(
            $(#[$attr:meta])*
            rpc $fn_name:ident( $( $arg:ident : $in_:ty ),* ) $(-> $out:ty)*;
        )*
    ) => {
        service! {{
            $(
                $(#[$attr])*
                rpc $fn_name( $( $arg : $in_ ),* ) $(-> $out)*;
            )*
        }}
    };
    (
        {
            $(#[$attr:meta])*
            rpc $fn_name:ident( $( $arg:ident : $in_:ty ),* ); // No return, no error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        service! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            rpc $fn_name( $( $arg : $in_ ),* ) -> ();
        }
    };
    (
        {
            $(#[$attr:meta])*
            rpc $fn_name:ident( $( $arg:ident : $in_:ty ),* ) -> $out:ty;

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        service! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            rpc $fn_name( $( $arg : $in_ ),* ) -> $out;
        }
    };
    (
        {} // all expanded
        $(
            $(#[$attr:meta])*
            rpc $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty;
        )*
    ) => {

        use std::sync::Arc;
        use $crate::rpc::*;
        #[allow(unused_imports)]
        use futures::prelude::*;
        use std::pin::Pin;
        use bifrost_proc_macro::{deref_tuple_types, adjust_caller_identifiers, adjust_function_signature};

        lazy_static! {
            pub static ref RPC_SVRS: $crate::utils::shortcut_registry::ShortcutRegistry<
                (u64, u64),
                ($crate::rpc::ShortcutToken, ::std::sync::Weak<dyn Service>),
            > = $crate::utils::shortcut_registry::ShortcutRegistry::new();
        }

        pub trait Service : RPCService {
           $(
                $(#[$attr])*
                adjust_function_signature!{
                    fn $fn_name<'a>(&self, $($arg:$in_),*) -> ::futures::future::BoxFuture<'a, $out>;
                }
           )*
           fn inner_dispatch<'a>(&'a self, data: $crate::bytes::BytesMut) -> Pin<Box<dyn core::future::Future<Output = Result<$crate::bytes::BytesMut, RPCRequestError>> + Send + 'a>> {
               let (func_id, body) = read_u64_head(data);
               async move {
                match func_id as usize {
                    $(::bifrost_plugins::hash_ident!($fn_name) => {
                        if let Some(data) = $crate::utils::serde::deserialize(body.as_ref()) {
                            #[allow(unused_parens)]
                            let tuple : deref_tuple_types!(($($in_,)*)) = data;
                            let adjust_caller_identifiers!($($arg: $in_),*) = tuple;
                            let f_result = self.$fn_name($($arg,)*).await;
                            let res_data = $crate::bytes::BytesMut::from($crate::utils::serde::serialize(&f_result).as_slice());
                            Ok(res_data)
                        } else {
                            Err(RPCRequestError::BadRequest)
                        }
                    }),*
                    _ => {
                        Err(RPCRequestError::FunctionIdNotFound)
                    }
                }
               }.boxed()
           }
        }

        #[allow(dead_code)]
        pub async fn get_local(server_id: u64, service_id: u64) -> Option<Arc<dyn Service>> {
            let key = (server_id, service_id);
            // Owner removal or replacement retires a dead Weak. Client calls
            // only read, including when the service has already been dropped.
            RPC_SVRS.get(&key).and_then(|(_, service)| service.upgrade())
        }

        #[allow(dead_code)]
        pub struct AsyncServiceClient {
            pub service_id: u64,
            pub client: Arc<RPCClient>,
        }

        #[allow(dead_code)]
        impl AsyncServiceClient {
           $(
                #[allow(non_camel_case_types)]
                $(#[$attr])*
                pub async fn $fn_name(&self, $($arg:$in_),*) -> Result<$out, RPCError> {
                    ImmeServiceClient::$fn_name(self.service_id, &self.client, $($arg),*).await
                }
           )*
        }
        impl ServiceClient for AsyncServiceClient {
            fn new_instance_with_service_id(service_id: u64, client: &Arc<RPCClient>) -> Self {
                AsyncServiceClient{
                    service_id: service_id,
                    client: client.clone()
                }
            }
            fn server_id(&self) -> u64 {
                self.client.server_id
            }
        }
        pub struct ImmeServiceClient;
        impl ImmeServiceClient {
            $(
                $(#[$attr])*
                /// Judgement: Use data ownership transfer instead of borrowing.
                /// Some applications highly depend on RPC shortcut to achieve performance advantages.
                /// Cloning for shortcut will significantly increase overhead. Eg. Hivemind immutable queue
                pub async fn $fn_name(service_id: u64, client: &Arc<RPCClient>, $($arg:$in_),*) -> Result<$out, RPCError> {
                    if let Some(ref local) = get_local(client.server_id, service_id).await {
                        Ok(local.$fn_name($($arg),*).await)
                    } else {
                        let req_data = ($($arg,)*);
                        let req_data_bytes = $crate::bytes::BytesMut::from($crate::utils::serde::serialize(&req_data).as_slice());
                        let req_bytes = prepend_u64(::bifrost_plugins::hash_ident!($fn_name) as u64, req_data_bytes);
                        let res_bytes = RPCClient::send_async(Pin::new(&*client), service_id, req_bytes).await;
                        if let Ok(res_bytes) = res_bytes {
                            if let Some(data) = $crate::utils::serde::deserialize(&res_bytes) {
                                Ok(data)
                            } else {
                                Err(RPCError::ClientCannotDecodeResponse)
                            }
                        } else {
                            Err(res_bytes.err().unwrap())
                        }
                    }
                }
           )*
        }
    }
}

#[macro_export]
macro_rules! service_with_id {
    ($s:ty, $id:expr) => {
        impl $crate::rpc::RPCServiceWithId for $s {
            const SERVICE_ID: u64 = $id;
        }
        impl $crate::rpc::ServiceClientWithId for AsyncServiceClient {
            const SERVICE_ID: u64 = $id;
        }
    };
}

mod syntax_test {
    service! {
        rpc test(a: u32, b: u32) -> bool;
        rpc test2(a: u32);
        rpc test3(a: u32, b: u32, c: u32, d: u32);
        rpc test4(a: u32, b: Vec<u32>, c: &Vec<u32>, d: u32);
    }
}

#[cfg(test)]
mod struct_test {
    use serde::{Deserialize, Serialize};
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct A {
        b: u32,
        d: u64,
        e: String,
        f: f32,
    }

    service! {
        rpc test(a: A, b: u32) -> bool;
    }
}

#[cfg(test)]
mod registry_tests {
    service! { rpc value() -> u64; }

    struct TestService(u64);
    impl Service for TestService {
        fn value(&self) -> futures::future::BoxFuture<'_, u64> {
            futures::future::ready(self.0).boxed()
        }
    }
    dispatch_rpc_service_functions!(TestService);

    fn register(service: &Arc<TestService>, server: u64) -> ShortcutToken {
        service.register_shortcut_service(Arc::into_raw(service.clone()) as usize, server, 1)
    }

    #[tokio::test]
    async fn bflock_dead_weak_lookup_does_not_write_registry() {
        let service = Arc::new(TestService(1));
        let token = register(&service, 4701);
        let weak = Arc::downgrade(&service);
        drop(service);
        assert!(weak.upgrade().is_none());
        assert!(get_local(4701, 1).await.is_none());
        assert!(
            RPC_SVRS.get(&(4701, 1)).is_some(),
            "lookup must not remove a dead Weak"
        );
        // A replacement must retire the stale registration.
        let replacement = Arc::new(TestService(2));
        let new_token = register(&replacement, 4701);
        replacement.unregister_shortcut_service(4701, 1, &token);
        assert_eq!(get_local(4701, 1).await.unwrap().value().await, 2);
        replacement.unregister_shortcut_service(4701, 1, &new_token);
        assert!(get_local(4701, 1).await.is_none());
    }

    #[tokio::test]
    async fn bflock_dead_weak_uses_normal_tcp_dispatch() {
        use bytes::{Buf, BufMut};
        use tokio_util::codec::{Framed, LengthDelimitedCodec};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server_id = bifrost_hasher::hash_str(&address);
        let stale = Arc::new(TestService(1));
        let token = register(&stale, server_id);
        drop(stale);
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
            let mut frame = frames.next().await.unwrap().unwrap();
            let message = frame.get_u64_le();
            assert_eq!(frame.get_u64_le(), 1);
            let body = TestService(9).dispatch(frame).await.unwrap();
            let mut reply = bytes::BytesMut::new();
            reply.put_u64_le(message);
            reply.put_u8(0);
            reply.extend_from_slice(&body);
            frames.send(reply.freeze()).await.unwrap();
        });
        let client = RPCClient::new_async(&address).await.unwrap();
        assert_eq!(ImmeServiceClient::value(1, &client).await.unwrap(), 9);
        assert!(RPC_SVRS.get(&(server_id, 1)).is_some());
        TestService(0).unregister_shortcut_service(server_id, 1, &token);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn bflock_old_unregister_preserves_concurrent_replacement() {
        let old = Arc::new(TestService(1));
        let old_token = register(&old, 4702);
        let replacement = Arc::new(TestService(2));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let writer_barrier = barrier.clone();
        let writer = std::thread::spawn(move || {
            writer_barrier.wait();
            old.unregister_shortcut_service(4702, 1, &old_token);
        });
        let token = register(&replacement, 4702);
        barrier.wait();
        assert_eq!(get_local(4702, 1).await.unwrap().value().await, 2);
        writer.join().unwrap();
        assert_eq!(get_local(4702, 1).await.unwrap().value().await, 2);
        replacement.unregister_shortcut_service(4702, 1, &token);
    }

    #[test]
    fn bflock_register_unregister_during_calls() {
        let barrier = Arc::new(std::sync::Barrier::new(9));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..128 {
                        futures::executor::block_on(async {
                            if let Some(service) = get_local(4703, 1).await {
                                assert!(service.value().await < 128);
                            }
                        });
                    }
                });
            }
            barrier.wait();
            for value in 0..128 {
                let service = Arc::new(TestService(value));
                let token = register(&service, 4703);
                service.unregister_shortcut_service(4703, 1, &token);
            }
        });
        assert!(futures::executor::block_on(get_local(4703, 1)).is_none());
    }

    #[test]
    fn bflock_lightning_get_sees_live_value_under_key_guard() {
        use lightning::map::{Map, PtrHashMap};
        let map = PtrHashMap::with_capacity(16);
        // Do not use get_or_insert: it can publish a value-less mutex bit.
        let guard = match map.locked_with_upsert(1_u64, Arc::new(47_u64)) {
            Ok((guard, _)) | Err(guard) => guard,
        };
        assert_eq!(*map.get(&1).expect("new guarded value must be visible"), 47);
        drop(guard);
        let guard = map.lock(&1).unwrap();
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert_eq!(
                        *map.get(&1).expect("live guarded value must be visible"),
                        47
                    )
                })
                .join()
                .unwrap();
        });
        drop(guard);
        assert_eq!(*map.get(&1).unwrap(), 47);
    }
}
