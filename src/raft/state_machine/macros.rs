//TODO: Use higher order macro to merge with rpc service! macro when possible to do this in Rust.
//Current major problem is inner repeated macro will be recognized as outer macro which breaks expand

#[macro_export]
macro_rules! raft_return_type {
    ($out: ty, $error: ty) => {::std::result::Result<$out, $error>};
}

#[macro_export]
macro_rules! raft_trait_fn {
    (qry $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        fn $fn_name(&self, $($arg:$in_),*) -> raft_return_type!($out, $error);
    };
    (cmd $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        fn $fn_name(&mut self, $($arg:$in_),*) -> raft_return_type!($out, $error);
    };
    (sub $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {}
}

#[macro_export]
macro_rules! raft_client_fn {
    (sub $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        pub fn $fn_name<F>(&self, f: F, $($arg:$in_),* )
        -> impl Future<Item = Result<SubscriptionReceipt, SubscriptionError>, Error = ExecError>
        where F: Fn(raft_return_type!($out, $error)) + 'static + Send + Sync
        {
            self.client.subscribe(
                self.sm_id,
                $fn_name::new($($arg,)*),
                f
            )
        }
    };
    ($others:ident $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        pub fn $fn_name(&self, $($arg:$in_),*)
        -> impl Future<Item = raft_return_type!($out, $error), Error = ExecError> {
            self.client.execute(
                self.sm_id,
                $fn_name::new($($arg,)*)
            )
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
        let decoded: ($($in_,)*) = $crate::utils::bincode::deserialize($d);
        let ($($arg,)*) = decoded;
        let f_result = $s.$fn_name($($arg),*);
        Some($crate::utils::bincode::serialize(&f_result))
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
        fn fn_dispatch_cmd(&mut self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {self.dispatch_cmd_(fn_id, data)}
        fn fn_dispatch_qry(&self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {self.dispatch_qry_(fn_id, data)}
        fn op_type(&mut self, fn_id: u64) -> Option<$crate::raft::state_machine::OpType> {self.op_type_(fn_id)}
    };
}

#[macro_export]
macro_rules! raft_state_machine {
    (
        $(
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) $(-> $out:ty)* $(| $error:ty)*;
        )*
    ) => {
        raft_state_machine! {{
            $(
                $(#[$attr])*
                def $smt $fn_name( $( $arg : $in_ ),* ) $(-> $out)* $(| $error)*;
            )*
        }}
    };
    (
        {
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ); // No return, no error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        raft_state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            def $smt $fn_name( $( $arg : $in_ ),* ) -> () | ();
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
            def $smt $fn_name( $( $arg : $in_ ),* ) -> $out | ();
        }
    };
    (
        {
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) | $error:ty; //no return, error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        raft_state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            def $smt $fn_name( $( $arg : $in_ ),* ) -> () | $error;
        }
    };
    (
        {
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty; //return, error

            $( $unexpanded:tt )*
        }
        $( $expanded:tt )*
    ) => {
        raft_state_machine! {
            { $( $unexpanded )* }

            $( $expanded )*

            $(#[$attr])*
            def $smt $fn_name( $( $arg : $in_ ),* ) -> $out | $error;
        }
    };
    (
        {} // all expanded
        $(
            $(#[$attr:meta])*
            def $smt:ident $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty;
        )*
    ) => {
        pub mod commands {
            use super::*;
            $(
                #[derive(Serialize, Deserialize, Debug)]
                pub struct $fn_name {
                    pub data: Vec<u8>
                }
                impl $crate::raft::RaftMsg<raft_return_type!($out, $error)> for $fn_name {
                    fn encode(self) -> (u64, $crate::raft::state_machine::OpType, Vec<u8>) {
                        (
                            hash_ident!($fn_name) as u64,
                            raft_fn_op_type!($smt),
                            self.data
                        )
                    }
                    fn decode_return(data: &Vec<u8>) -> raft_return_type!($out, $error) {
                        $crate::utils::bincode::deserialize(data)
                    }
                }
                impl $fn_name {
                    pub fn new($($arg:&$in_),*) -> $fn_name {
                        let req_data = ($($arg,)*);
                        $fn_name {
                            data: $crate::utils::bincode::serialize(&req_data)
                        }
                    }
                }
            )*
        }
        pub trait StateMachineCmds: $crate::raft::state_machine::StateMachineCtl {
           $(
                $(#[$attr])*
                raft_trait_fn!($smt $fn_name( $( $arg : $in_ ),* ) -> $out | $error);
           )*
           fn op_type_(&self, fn_id: u64) -> Option<$crate::raft::state_machine::OpType> {
                match fn_id as usize {
                   $(hash_ident!($fn_name) => {
                       Some(raft_fn_op_type!($smt))
                   }),*
                   _ => {
                       debug!("Undefined function id: {}", fn_id);
                       None
                   }
                }
           }
           fn dispatch_cmd_(&mut self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {
               match fn_id as usize {
                   $(hash_ident!($fn_name) => {
                        raft_dispatch_cmd!($smt $fn_name self data( $( $arg : $in_ ),* ))
                   }),*
                   _ => {
                       debug!("Undefined function id: {}", fn_id);
                       None
                   }
               }
           }
           fn dispatch_qry_(&self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {
               match fn_id as usize {
                   $(hash_ident!($fn_name) => {
                        raft_dispatch_qry!($smt $fn_name self data( $( $arg : $in_ ),* ))
                   }),*
                   _ => {
                       debug!("Undefined function id: {}", fn_id);
                       None
                   }
               }
           }
        }
        pub mod client {
            use std::sync::Arc;
            use futures::prelude::*;
            use $crate::raft::state_machine::master::ExecError;
            use $crate::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};

            use self::commands::*;
            use super::*;

            pub struct SMClient {
                client: Arc<RaftClient>,
                sm_id: u64
            }
            impl SMClient {
               $(
                  $(#[$attr])*
                  raft_client_fn!($smt $fn_name( $( $arg : &$in_ ),* ) -> $out | $error);
               )*
               pub fn new(sm_id: u64, client: &Arc<RaftClient>) -> SMClient {
                    SMClient {
                        client: client.clone(),
                        sm_id: sm_id
                    }
               }
            }
        }
    };
}
