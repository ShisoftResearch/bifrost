//TODO: Use higher order macro to merge with rpc service! macro when possible to do this in Rust.
//Current major problem is inner repeated macro will be recognized as outer macro which breaks expand

macro_rules! trait_fn {
    (qry $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        fn $fn_name(&self, $($arg:$in_),*) -> std::result::Result<$out, $error>;
    };
    (cmd $fn_name:ident ( $( $arg:ident : $in_:ty ),* ) -> $out:ty | $error:ty) => {
        fn $fn_name(&mut self, $($arg:$in_),*) -> std::result::Result<$out, $error>;
    };
}

macro_rules! trait_dispatch {
    () => {};
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

        mod sm_args {
            use super::*;
            $(
                #[derive(Serialize, Deserialize, Debug)]
                pub struct $fn_name {
                    $(pub $arg:$in_),*
                }
                impl $crate::raft::RaftMsg for $fn_name {
                    fn encode(&self) -> (usize, Vec<u8>) {
                        (
                            hash_ident!($fn_name),
                            bincode::serialize(&self, SizeLimit::Infinite).unwrap()
                        )
                    }
                }

            )*
        }
        mod sm_returns {
            $(
                #[derive(Serialize, Deserialize, Debug)]
                pub enum $fn_name {
                    Result($out),
                    Error($error)
                }
            )*
        }
        pub trait StateMachine: $crate::raft::state_machine::StateMachineInterface {
           $(
                $(#[$attr])*
                trait_fn!($smt $fn_name( $( $arg : $in_ ),* ) -> $out | $error);
           )*
           fn dispatch(&mut self, fn_id: u64, data: &Vec<u8>) -> Option<Vec<u8>> {
               match fn_id as usize {
                   $(hash_ident!($fn_name) => {
                       let decoded: sm_args::$fn_name = bincode::deserialize(data).unwrap();
                       let f_result = self.$fn_name($(decoded.$arg),*);
                       let s_result = match f_result {
                           Ok(v) => sm_returns::$fn_name::Result(v),
                           Err(e) => sm_returns::$fn_name::Error(e)
                       };
                       Some(bincode::serialize(&s_result, SizeLimit::Infinite).unwrap())
                   }),*
                   _ => {
                       println!("Undefined function id: {}", fn_id);
                       None
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