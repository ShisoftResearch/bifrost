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
            ) -> ::std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
                async move {
                    let mut cbs = RPC_SVRS.write().await;
                    let service = unsafe { Arc::from_raw(service_ptr as *const $s) };
                    cbs.insert((server_id, service_id), service);
                }
                .boxed()
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
            pub static ref RPC_SVRS:
            async_std::sync::RwLock<::std::collections::BTreeMap<(u64, u64), Arc<dyn Service>>>
            = async_std::sync::RwLock::new(::std::collections::BTreeMap::new());
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
            let svrs = RPC_SVRS.read().await;
            match svrs.get(&(server_id, service_id)) {
                Some(s) => Some(s.clone()),
                _ => None
            }
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
           pub fn new(service_id: u64, client: &Arc<RPCClient>) -> Arc<AsyncServiceClient> {
                Arc::new(AsyncServiceClient{
                    service_id: service_id,
                    client: client.clone()
                })
           }
           pub fn server_id(&self) -> u64 {
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
