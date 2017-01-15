//TODO: Use higher order macro to merge with rpc service! macro when possible to do this in Rust.
//Current major problem is inner repeated macro will be recognized as outer macro which breaks expand

macro_rules! return_type {
    ($out: ty, $error: ty) => {std::result::Result<$out, $error>};
}

macro_rules! trait_fn {
    (qry $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        fn $fn_name(&self, $($arg:$in_),*) -> return_type!($out, $error);
    };
    (cmd $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        fn $fn_name(&mut self, $($arg:$in_),*) -> return_type!($out, $error);
    };
}
macro_rules! client_fn {
    ($fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty $body:block) => {
        fn $fn_name(&self, $($arg:$in_),*)
        -> Result<return_type!($out, $error), ExecError> $body
    };
}

macro_rules! fn_op_type {
    (qry) => {$crate::raft::state_machine::OpType::QUERY};
    (cmd) => {$crate::raft::state_machine::OpType::COMMAND};
}

macro_rules! dispatch_fn {
    ($fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {{
        let decoded: commands::$fn_name = deserialize!($d);
        let f_result = $s.$fn_name($(decoded.$arg),*);
        Some(serialize!(&f_result))
    }};
}

macro_rules! dispatch_cmd {
    (qry $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {None};
    (cmd $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {
        dispatch_fn!($fn_name $s $d( $( $arg : $in_ ),* ))
    }
}

macro_rules! dispatch_qry {
    (cmd $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {None};
    (qry $fn_name:ident $s: ident $d: ident ( $( $arg:ident : $in_:ty ),* )) => {
        dispatch_fn!($fn_name $s $d( $( $arg : $in_ ),* ))
    }
}

#[macro_export]
macro_rules! sm_complete {
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
        use std;
        use byteorder::{ByteOrder, LittleEndian};
        use bincode::{SizeLimit, serde as bincode};

        pub mod commands {
            use super::*;
            $(
                #[derive(Serialize, Deserialize, Debug)]
                pub struct $fn_name {
                    $(pub $arg:$in_),*
                }
                impl $crate::raft::RaftMsg<return_type!($out, $error)> for $fn_name {
                    fn encode(&self) -> (u64, $crate::raft::state_machine::OpType, Vec<u8>) {
                        (
                            hash_ident!($fn_name) as u64,
                            fn_op_type!($smt),
                            serialize!(&self)
                        )
                    }
                    fn decode_return(&self, data: &Vec<u8>) -> return_type!($out, $error) {
                        deserialize!(data)
                    }
                }
            )*
        }
        pub trait StateMachineCmds: $crate::raft::state_machine::StateMachineCtl {
           $(
                $(#[$attr])*
                trait_fn!($smt $fn_name( $( $arg : $in_ ),* ) -> $out | $error);
           )*
           fn op_type_(&self, fn_id: u64) -> Option<$crate::raft::state_machine::OpType> {
                match fn_id as usize {
                   $(hash_ident!($fn_name) => {
                       Some(fn_op_type!($smt))
                   }),*
                   _ => {
                       println!("Undefined function id: {}", fn_id);
                       None
                   }
                }
           }
           fn dispatch_cmd_(&mut self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {
               match fn_id as usize {
                   $(hash_ident!($fn_name) => {
                        dispatch_cmd!($smt $fn_name self data( $( $arg : $in_ ),* ))
                   }),*
                   _ => {
                       println!("Undefined function id: {}", fn_id);
                       None
                   }
               }
           }
           fn dispatch_qry_(&self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {
               match fn_id as usize {
                   $(hash_ident!($fn_name) => {
                        dispatch_qry!($smt $fn_name self data( $( $arg : $in_ ),* ))
                   }),*
                   _ => {
                       println!("Undefined function id: {}", fn_id);
                       None
                   }
               }
           }
        }
        pub mod client {
            use std::sync::Arc;
            use $crate::raft::state_machine::master::ExecError;
            use $crate::raft::client::RaftClient;
            use self::commands::*;
            use super::*;

            pub struct SMClient {
                client: Arc<RaftClient>,
                sm_id: u64
            }
            impl SMClient {
               $(
                  $(#[$attr])*
                  pub fn $fn_name(&self, $($arg:$in_),*)
                  -> Result<return_type!($out, $error), ExecError> {
                      self.client.execute(
                          self.sm_id,
                          &$fn_name{$($arg:$arg),*}
                      )
                  }
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

#[cfg(test)]
mod syntax_test {
    raft_state_machine! {
        def qry get (key: u64) -> String | ();
        def cmd test(a: u32, b: u32) -> bool;
    }
}