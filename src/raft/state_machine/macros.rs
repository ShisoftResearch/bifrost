//TODO: Use higher order macro to merge with rpc service! macro when possible to do this in Rust.
//Current major problem is inner repeated macro will be recognized as outer macro which breaks expand

#[macro_export]
macro_rules! raft_trait_fn {
    (qry $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty) => {
        fn $fn_name<'a>(&'a self, $($arg:$in_),*) -> ::futures::future::BoxFuture<$out>;
    };
    (cmd $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty) => {
        fn $fn_name<'a>(&'a mut self, $($arg:$in_),*) -> ::futures::future::BoxFuture<$out>;
    };
    (sub $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty) => {}
}

#[macro_export]
macro_rules! raft_client_fn {
    (sub $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty) => {
        pub fn $fn_name<F>(&self, f: F, $($arg:$in_),* )
            -> BoxFuture<Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>>
        where F: Fn($out) -> BoxFuture<'static, ()> + 'static + Send + Sync
        {
            self.client.subscribe(
                self.sm_id,
                $fn_name::new($($arg,)*),
                f
            ).boxed()
        }
    };
    ($others:ident $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty) => {
        pub async fn $fn_name(&self, $($arg:$in_),*) -> Result<$out, ExecError> {
            self.client.execute(
                self.sm_id,
                $fn_name::new($($arg,)*)
            ).await
        }
    };
}

#[macro_export]
macro_rules! raft_fn_op_type {
    (qry) => {
        $crate::raft::state_machine::OpType::QUERY
    };
    (cmd) => {
        $crate::raft::state_machine::OpType::COMMAND
    };
    (sub) => {
        $crate::raft::state_machine::OpType::SUBSCRIBE
    };
}

#[macro_export]
macro_rules! raft_dispatch_fn {
    ($fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {{
        let decoded: ($($in_,)*) = $crate::utils::serde::deserialize($d).unwrap();
        let ($($arg,)*) = decoded;
        let f_result = $s.$fn_name($($arg),*).await;
        Some($crate::utils::serde::serialize(&f_result))
    }};
}

#[macro_export]
macro_rules! raft_dispatch_cmd {
    (cmd $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {
        raft_dispatch_fn!($fn_name $s $d( $( $arg : $in_ ),* ))
    };
    ($others:ident $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {None};
}

#[macro_export]
macro_rules! raft_dispatch_qry {
    (qry $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {
        raft_dispatch_fn!($fn_name $s $d( $( $arg : $in_ ),* ))
    };
    ($others:ident $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {None};
}

#[macro_export]
macro_rules! raft_sm_complete {
    () => {
        fn fn_dispatch_cmd<'a>(
            &'a mut self,
            fn_id: u64,
            data: &'a Vec<u8>,
        ) -> ::futures::future::BoxFuture<'a, Option<Vec<u8>>> {
            self.dispatch_cmd_(fn_id, data)
        }
        fn fn_dispatch_qry<'a>(
            &'a self,
            fn_id: u64,
            data: &'a Vec<u8>,
        ) -> ::futures::future::BoxFuture<'a, Option<Vec<u8>>> {
            self.dispatch_qry_(fn_id, data)
        }
        fn op_type(&mut self, fn_id: u64) -> Option<$crate::raft::state_machine::OpType> {
            self.op_type_(fn_id)
        }
    };
}

#[macro_export]
macro_rules! raft_state_machine {
    (
        $(
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) $(-> $out:ty)* ;
        )*
    ) => {
        raft_state_machine! {{
            $(
                $(#[$attr])*
                def $smt $fn_name( $( $arg : $in_ ),* ) $(-> $out)*;
            )*
        }}
    };
    (
        {
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ); // No return

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        raft_state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            def $smt $fn_name( $( $arg : $in_ ),* ) -> ();
        }
    };
    (
        {
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) -> $out:ty; //return, no error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        raft_state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            def $smt $fn_name( $( $arg : $in_ ),* ) -> $out;
        }
    };
    (
        {} // all expanded
        $(
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty;
        )*
    ) => {
        use futures::prelude::*;
        use futures::future::BoxFuture;

        pub mod commands {
            use super::*;
            use futures::prelude::*;
            use serde::{Serialize, Deserialize};
            $(
                #[derive(Serialize, Deserialize, Debug)]
                pub struct $fn_name {
                    pub data: Vec<u8>
                }
                impl $crate::raft::RaftMsg<$out> for $fn_name {
                    fn encode(self) -> (u64, $crate::raft::state_machine::OpType, Vec<u8>) {
                        (
                            ::bifrost_plugins::hash_ident!($fn_name) as u64,
                            raft_fn_op_type!($smt),
                            self.data
                        )
                    }
                    fn decode_return(data: &Vec<u8>) -> $out {
                        $crate::utils::serde::deserialize(data).unwrap()
                    }
                }
                impl $fn_name {
                    pub fn new($($arg:&$in_),*) -> $fn_name {
                        let req_data = ($($arg,)*);
                        $fn_name {
                            data: $crate::utils::serde::serialize(&req_data)
                        }
                    }
                }
            )*
        }

        pub trait StateMachineCmds: $crate::raft::state_machine::StateMachineCtl {
           $(
                $(#[$attr])*
                raft_trait_fn!($smt $fn_name( $( $arg : $in_ ),* ) -> $out);
           )*
           fn op_type_(&self, fn_id: u64) -> Option<$crate::raft::state_machine::OpType> {
                match fn_id as usize {
                   $(::bifrost_plugins::hash_ident!($fn_name) => {
                       Some(raft_fn_op_type!($smt))
                   }),*
                   _ => {
                       debug!("Undefined function id: {}", fn_id);
                       None
                   }
                }
           }
           fn dispatch_cmd_<'a>(&'a mut self, fn_id: u64, data: &'a Vec<u8>) -> BoxFuture<Option<Vec<u8>>> {
               async move {
                    match fn_id as usize {
                        $(::bifrost_plugins::hash_ident!($fn_name) => {
                            raft_dispatch_cmd!($smt $fn_name self data( $( $arg : $in_ ),* ))
                        }),*
                        _ => {
                            debug!("Undefined function id: {}. We have {}", fn_id, concat!(stringify!($($fn_name),*)));
                            None
                        }
                    }
               }.boxed()
           }
           fn dispatch_qry_<'a>(&'a self, fn_id: u64, data: &'a Vec<u8>) -> BoxFuture<Option<Vec<u8>>> {
               async move {
                    match fn_id as usize {
                        $(::bifrost_plugins::hash_ident!($fn_name) => {
                            raft_dispatch_qry!($smt $fn_name self data( $( $arg : $in_ ),* ))
                        }),*
                        _ => {
                            debug!("Undefined function id: {}", fn_id);
                            None
                        }
                    }
               }.boxed()
           }
        }
        pub mod client {
            use super::*;
            use std::sync::Arc;
            use super::commands::*;
            use crate::raft::client::*;
            use crate::raft::state_machine::master::ExecError;
            use crate::raft::state_machine::StateMachineClient;
            use crate::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};

            pub struct SMClient {
                client: Arc<RaftClient>,
                sm_id: u64
            }
            impl SMClient {
               $(
                  $(#[$attr])*
                  raft_client_fn!($smt $fn_name( $( $arg : &$in_ ),* ) -> $out);
               )*
               pub fn new(sm_id: u64, client: &Arc<RaftClient>) -> Self {
                    Self {
                        client: client.clone(),
                        sm_id: sm_id
                    }
               }
            }
            impl StateMachineClient for SMClient {
               fn new_instance (sm_id: u64, client: &Arc<RaftClient>) -> Self {
                    Self::new(sm_id, client)
               }
            }
        }
    };
}
