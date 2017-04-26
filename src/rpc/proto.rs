use bincode::{SizeLimit, serde as bincode};

#[macro_export]
macro_rules! serialize {
    ($e:expr) => {bincode::serialize($e, SizeLimit::Infinite).unwrap()};
}

#[macro_export]
macro_rules! deserialize {
    ($e:expr) => {bincode::deserialize($e).unwrap()};
}

#[macro_export]
macro_rules! dispatch_rpc_service_functions {
    ($s:ty) => {
        impl $crate::rpc::RPCService for $s {
            fn dispatch(&self, data: Vec<u8>) -> Result<Vec<u8>, $crate::rpc::RPCRequestError> {
                self.inner_dispatch(data)
            }
            fn register_shortcut_service(&self, service_ptr: usize, server_id: u64, service_id: u64) {
                let mut cbs = RPC_SVRS.write();
                let service = unsafe {Arc::from_raw(service_ptr as *const $s)};
                cbs.insert((server_id, service_id), service);
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
            rpc $fn_name:ident( $( $arg:ident : $in_:ty ),* ) $(-> $out:ty)* $(| $error:ty)*;
        )*
    ) => {
        service! {{
            $(
                $(#[$attr])*
                rpc $fn_name( $( $arg : $in_ ),* ) $(-> $out)* $(| $error)*;
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
            rpc $fn_name( $( $arg : $in_ ),* ) -> () | ();
        }
    };
    (
        {
            $(#[$attr:meta])*
            rpc $fn_name:ident( $( $arg:ident : $in_:ty ),* ) -> $out:ty; //return, no error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        service! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            rpc $fn_name( $( $arg : $in_ ),* ) -> $out | ();
        }
    };
    (
        {
            $(#[$attr:meta])*
            rpc $fn_name:ident( $( $arg:ident : $in_:ty ),* ) | $error:ty; //no return, error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        service! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            rpc $fn_name( $( $arg : $in_ ),* ) -> () | $error;
        }
    };
    (
        {
            $(#[$attr:meta])*
            rpc $fn_name:ident( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty; //return, error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        service! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            rpc $fn_name( $( $arg : $in_ ),* ) -> $out | $error;
        }
    };
    (
        {} // all expanded
        $(
            $(#[$attr:meta])*
            rpc $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty;
        )*
    ) => {
        use std;
        use std::time::Duration;
        use bincode::{SizeLimit, serde as bincode};
        use std::sync::Arc;
        use std::io;
        use $crate::rpc::*;
        use $crate::utils::u8vec::*;
        use futures::Future;

        lazy_static! {
            pub static ref RPC_SVRS:
            ::parking_lot::RwLock<::std::collections::BTreeMap<(u64, u64), Arc<Service>>>
            = ::parking_lot::RwLock::new(::std::collections::BTreeMap::new());
        }

        pub trait Service: RPCService {
           $(
                $(#[$attr])*
                fn $fn_name(&self, $($arg:$in_),*) -> std::result::Result<$out, $error>;
           )*
           fn inner_dispatch(&self, data: Vec<u8>) -> Result<Vec<u8>, RPCRequestError> {
               let (func_id, body) = extract_u64_head(data);
               match func_id as usize {
                   $(hash_ident!($fn_name) => {
                       let ($($arg,)*) : ($($in_,)*) = deserialize!(&body);
                       let f_result = self.$fn_name($($arg,)*);
                       Ok(serialize!(&f_result))
                   }),*
                   _ => {
                       Err(RPCRequestError::FunctionIdNotFound)
                   }
               }
           }
        }
        pub struct SyncServiceClient {
            pub service_id: u64,
            pub server_id: u64,
            pub client: Arc<RPCClient>,
        }
        impl SyncServiceClient {
           $(
                #[allow(non_camel_case_types)]
                $(#[$attr])*
                pub fn $fn_name(&self, $($arg:&$in_),*) -> Result<std::result::Result<$out, $error>, RPCError> {
                    let local_svr = {
                        let svrs = RPC_SVRS.read();
                        match svrs.get(&(self.server_id, self.service_id)) {
                            Some(s) => Some(s.clone()),
                            _ => None
                        }
                    };
                    if let Some(local) = local_svr {
                        Ok(local.$fn_name($($arg.clone()),*))
                    } else {
                        let req_data = ($($arg,)*);
                        let req_data_bytes = serialize!(&req_data);
                        let req_bytes = prepend_u64(hash_ident!($fn_name) as u64, req_data_bytes);
                        let res_bytes = self.client.send(self.service_id, req_bytes);
                        if let Ok(res_bytes) = res_bytes {
                            Ok(deserialize!(res_bytes.as_slice()))
                        } else {
                            Err(res_bytes.err().unwrap())
                        }
                    }
                }
           )*
           pub fn new(service_id: u64, client: &Arc<RPCClient>) -> Arc<SyncServiceClient> {
                Arc::new(SyncServiceClient{
                    service_id: service_id,
                    server_id:client.server_id,
                    client: client.clone()
                })
           }
        }
        pub struct AsyncServiceClient {
            pub service_id: u64,
            pub server_id: u64,
            pub client: Arc<RPCClient>,
        }
        impl AsyncServiceClient {
           $(
                #[allow(non_camel_case_types)]
                $(#[$attr])*
                pub fn $fn_name(&self, $($arg:&$in_),*) -> Box<Future<Item = std::result::Result<$out, $error>, Error = RPCError>> {
                    let req_data = ($($arg,)*);
                    let req_data_bytes = serialize!(&req_data);
                    let req_bytes = prepend_u64(hash_ident!($fn_name) as u64, req_data_bytes);
                    let res_bytes = self.client.send_async(self.service_id, req_bytes);
                    Box::new(res_bytes.then(|res_bytes| -> Result<std::result::Result<$out, $error>, RPCError> {
                        if let Ok(res_bytes) = res_bytes {
                            Ok(deserialize!(res_bytes.as_slice()))
                        } else {
                            Err(res_bytes.err().unwrap())
                        }
                    }))
                }
           )*
           pub fn new(service_id: u64, client: &Arc<RPCClient>) -> Arc<SyncServiceClient> {
                Arc::new(SyncServiceClient{
                    service_id: service_id,
                    server_id:client.server_id,
                    client: client.clone()
                })
           }
        }
    }
}

mod syntax_test {
    service! {
        rpc test(a: u32, b: u32) -> bool;
        rpc test2(a: u32);
        rpc test3(a: u32, b: u32, c: u32, d: u32);
        rpc test4(a: u32, b: u32, c: u32, d: u32) -> bool | String;
        rpc test5(a: u32, b: u32, c: u32, d: u32) | String;
    }
}

#[cfg(test)]
mod struct_test {

    #[derive(Serialize, Deserialize, Debug)]
    pub struct a {
        b: u32,
        d: u64,
        e: String,
        f: f32
    }

    service! {
        rpc test(a: a, b: u32) -> bool;
    }
}
