use bincode::{SizeLimit, serde as bincode};

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
        use byteorder::{ByteOrder, LittleEndian};
        use bincode::{SizeLimit, serde as bincode};

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
        mod rpc_returns {
            #[allow(unused_variables)]
            #[allow(unused_imports)]

            use super::*;
            $(
                #[allow(non_camel_case_types)]
                #[derive(Serialize, Deserialize, Debug)]
                pub enum $fn_name {
                    Result($out),
                    Error($error)
                }
            )*
        }
        pub trait Server: Clone + 'static {
           $(
                $(#[$attr])*
                fn $fn_name(&self, $($arg:$in_),*) -> std::result::Result<$out, $error>;
           )*

           fn listen(self, addr: &String) {
                $crate::rpc::Server::new(addr, Box::new(move|data, conn| {
                        let (mut head, mut body) = data.split_at_mut(8);
                        let func_id = LittleEndian::read_u64(&mut head);
                        match func_id as usize {
                            $(hash_ident!($fn_name) => {
                                let decoded: rpc_args::$fn_name = bincode::deserialize(&body).unwrap();
                                let f_result = self.$fn_name($(decoded.$arg),*);
                                let s_result = match f_result {
                                    Ok(v) => rpc_returns::$fn_name::Result(v),
                                    Err(e) => rpc_returns::$fn_name::Error(e)
                                };
                                let encoded: Vec<u8> = bincode::serialize(&s_result, SizeLimit::Infinite).unwrap();
                                conn.send_message(encoded).unwrap();
                            }),*
                            _ => {println!("Undefined function id: {}", func_id)}
                        }
                })).start();
            }
        }
        mod encoders {
            use bincode::{SizeLimit, serde as bincode};
            use byteorder::{ByteOrder, LittleEndian};
            use super::*;
            $(
                pub fn $fn_name($($arg:$in_),*) -> Vec<u8> {
                    let mut m_id_buf = [0u8; 8];
                    LittleEndian::write_u64(&mut m_id_buf, hash_ident!($fn_name) as u64);
                    let  obj = super::rpc_args::$fn_name {
                        $(
                            $arg: $arg
                        ),*
                    };
                    let mut data_vec = bincode::serialize(&obj, SizeLimit::Infinite).unwrap();
                    let mut r = Vec::with_capacity(data_vec.len() + 8);
                    r.extend_from_slice(&m_id_buf);
                    r.append(&mut data_vec);
                    r
                }
            )*
        }
        pub struct SyncClient {
            client: $crate::rpc::Client
        }
        impl SyncClient {
            pub fn new(addr: &String) -> SyncClient {
                SyncClient {
                    client: $crate::rpc::Client::new(addr)
                }
            }
           $(
                #[allow(non_camel_case_types)]
                $(#[$attr])*
                fn $fn_name(&mut self, $($arg:$in_),*) -> std::result::Result<$out, $error> {
                    let req_bytes = encoders::$fn_name($($arg),*);
                    let res_bytes = self.client.send_message(req_bytes);
                    let res: rpc_returns::$fn_name = bincode::deserialize(&res_bytes).unwrap();
                    match res {
                        rpc_returns::$fn_name::Result(v) => Ok(v),
                        rpc_returns::$fn_name::Error(e) => Err(e)
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