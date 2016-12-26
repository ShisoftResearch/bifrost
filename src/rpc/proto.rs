use bincode::{SizeLimit, serde as bincode};

#[macro_export]
macro_rules! serialize {
    ($e:expr) => {bincode::serialize($e, SizeLimit::Infinite).unwrap()};
}

#[macro_export]
macro_rules! deserialize {
    ($e:expr) => {bincode::deserialize($e).unwrap()};
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
        use byteorder::{ByteOrder, LittleEndian};
        use bincode::{SizeLimit, serde as bincode};
        use std::sync::Arc;
        use std::time::Duration;
        use std::io;

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
        pub trait Server: Sync + Send {
           $(
                $(#[$attr])*
                fn $fn_name(&self, $($arg:$in_),*) -> std::result::Result<$out, $error>;
           )*
        }
        fn listen(server: Arc<Server>, addr: &String) {
           $crate::tcp::server::Server::new(addr, Box::new(move |data| {
                    let func_id = LittleEndian::read_u64(&data);
                    let body: Vec<u8> = data.iter().skip(8).cloned().collect();
                    match func_id as usize {
                        $(hash_ident!($fn_name) => {
                            let decoded: rpc_args::$fn_name = deserialize!(&body);
                            let f_result = server.$fn_name($(decoded.$arg),*);
                            serialize!(&f_result)
                        }),*
                        _ => {
                            panic!("func_id not found, maybe version mismatch: {}", func_id)
                        }
                    }
            }));
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
                    let mut data_vec = serialize!(&obj);
                    let mut r = Vec::with_capacity(data_vec.len() + 8);
                    r.extend_from_slice(&m_id_buf);
                    r.append(&mut data_vec);
                    r
                }
            )*
        }
        pub struct SyncClient {
            client: $crate::tcp::client::Client,
            pub address: String
        }
        impl SyncClient {
            pub fn new(addr: &String) -> io::Result<SyncClient> {
                Ok(SyncClient {
                    client: $crate::tcp::client::Client::connect(addr)?,
                    address: addr.clone()
                })
            }
            pub fn with_timeout(addr: &String, timeout: Duration) -> io::Result<SyncClient> {
                Ok(SyncClient {
                    client: $crate::tcp::client::Client::connect_with_timeout(addr, timeout)?,
                    address: addr.clone()
                })
            }
           $(
                #[allow(non_camel_case_types)]
                $(#[$attr])*
                fn $fn_name(&mut self, $($arg:$in_),*) -> io::Result<std::result::Result<$out, $error>> {
                    let req_bytes = encoders::$fn_name($($arg),*);
                    let res_bytes = self.client.send(req_bytes);
                    if let Ok(res_bytes) = res_bytes {
                        Ok(deserialize!(res_bytes.as_slice()))
                    } else {
                        Err(res_bytes.err().unwrap())
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
