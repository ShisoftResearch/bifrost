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

        mod rpc_args {
            #[allow(unused_variables)]
            #[allow(unused_imports)]

            use super::*;
            $(
                #[allow(non_camel_case_types)]
                #[derive(Serialize, Deserialize, Debug)]
                pub struct $fn_name {
                    $(pub $arg:$in_),*
                }
            )*
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
                       let decoded: rpc_args::$fn_name = deserialize!(&body);
                       let f_result = self.$fn_name($(decoded.$arg),*);
                       Ok(serialize!(&f_result))
                   }),*
                   _ => {
                       Err(RPCRequestError::FunctionIdNotFound)
                   }
               }
           }

        }
        mod encoders {
            use super::*;
            $(
                pub fn $fn_name($($arg:$in_),*) -> Vec<u8> {
                    let  obj = super::rpc_args::$fn_name {
                        $(
                            $arg: $arg
                        ),*
                    };
                    let mut data_vec = serialize!(&obj);
                    prepend_u64(hash_ident!($fn_name) as u64, data_vec)
                }
            )*
        }
        pub struct SyncServiceClient {
            pub id: u64,
            pub client: Arc<RPCSyncClient>,
        }
        impl SyncServiceClient {
           $(
                #[allow(non_camel_case_types)]
                $(#[$attr])*
                pub fn $fn_name(&self, $($arg:$in_),*) -> Result<std::result::Result<$out, $error>, RPCError> {
                    let req_bytes = encoders::$fn_name($($arg),*);
                    let res_bytes = self.client.send(self.id, req_bytes);
                    if let Ok(res_bytes) = res_bytes {
                        Ok(deserialize!(res_bytes.as_slice()))
                    } else {
                        Err(res_bytes.err().unwrap())
                    }
                }
           )*
           pub fn new(service_id: u64, client: Arc<RPCSyncClient>) -> Arc<SyncServiceClient> {
                Arc::new(SyncServiceClient{
                    id: service_id,
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
