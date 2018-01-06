use bincode;

#[macro_export]
macro_rules! dispatch_rpc_service_functions {
    ($s:ty) => {
        impl $crate::rpc::RPCService for $s {
            fn dispatch(&self, data: Vec<u8>)
                -> Box<Future<Item = Vec<u8>, Error = $crate::rpc::RPCRequestError>>
                where Self: Sized
            {
                Self::inner_dispatch(this, data)
            }
            fn register_shortcut_service(&self, service_ptr: usize, server_id: u64, service_id: u64) {
                let mut cbs = RPC_SVRS.write();
                cbs.insert((server_id, service_id), this);
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
        use std::sync::Arc;
        use std::io;
        use $crate::rpc::*;
        use $crate::utils::u8vec::*;
        use futures::{Future, future};

        lazy_static! {
            pub static ref RPC_SVRS:
            ::parking_lot::RwLock<::std::collections::BTreeMap<(u64, u64), Arc<Service>>>
            = ::parking_lot::RwLock::new(::std::collections::BTreeMap::new());
        }

        pub trait Service: RPCService {
           $(
                $(#[$attr])*
                fn $fn_name(&self, $($arg:$in_),*) -> Box<Future<Item = $out, Error = $error>> where Self: Sized;
           )*
           fn inner_dispatch(&self, data: Vec<u8>) -> Box<Future<Item = Vec<u8>, Error = RPCRequestError>> where Self: Sized {
               let (func_id, body) = extract_u64_head(data);
               match func_id as usize {
                   $(hash_ident!($fn_name) => {
                       let ($($arg,)*) : ($($in_,)*) = $crate::utils::bincode::deserialize(&body);
                       Box::new(
                           self.$fn_name($($arg,)*)
                               .then(|f_result| future::ok($crate::utils::bincode::serialize(&f_result)))
                               .map_err(|_:$error| RPCRequestError::Other) // in this case error it is impossible
                       )
                   }),*
                   _ => {
                       Box::new(future::err(RPCRequestError::FunctionIdNotFound))
                   }
               }
           }
        }
        pub fn get_local(server_id: u64, service_id: u64) -> Option<Arc<Service>> {
            let svrs = RPC_SVRS.read();
            match svrs.get(&(server_id, service_id)) {
                Some(s) => Some(s.clone()),
                _ => None
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
                    if let Some(ref local) = get_local(self.server_id, self.service_id) {
                        let local = local.clone();
                        Box::new(future::finished(local.$fn_name($($arg.clone()),*).wait()))
                    } else {
                        let req_data = ($($arg,)*);
                        let req_data_bytes = $crate::utils::bincode::serialize(&req_data);
                        let req_bytes = prepend_u64(hash_ident!($fn_name) as u64, req_data_bytes);
                        let res_bytes = self.client.send_async(self.service_id, req_bytes);
                        Box::new(res_bytes.then(|res_bytes| -> Result<std::result::Result<$out, $error>, RPCError> {
                            if let Ok(res_bytes) = res_bytes {
                                Ok($crate::utils::bincode::deserialize(&res_bytes))
                            } else {
                                Err(res_bytes.err().unwrap())
                            }
                        }))
                    }
                }
           )*
           pub fn new(service_id: u64, client: &Arc<RPCClient>) -> Arc<AsyncServiceClient> {
                Arc::new(AsyncServiceClient{
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

    #[derive(Serialize, Deserialize, Debug, Clone)]
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
