#![crate_type = "lib"]
#![feature(proc_macro_hygiene)]
#![feature(trait_alias)]

#[cfg(disable_shortcut)]
pub static DISABLE_SHORTCUT: bool = true;

#[cfg(not(disable_shortcut))]
pub static DISABLE_SHORTCUT: bool = false;

#[macro_use]
pub mod utils;
pub mod tcp;
#[macro_use]
pub mod rpc;
#[macro_use]
pub mod raft;
pub mod conshash;
pub mod hlc;
pub mod membership;
pub mod vector_clock;

#[macro_use]
extern crate log;

#[macro_use]
extern crate lazy_static;
pub extern crate bytes;

/// Test diagnostics. The feature removes all trace work when it is off.
#[macro_export]
macro_rules! doubt_trace {
    ($($arg:tt)*) => {{
        #[cfg(feature = "c3doubt_trace")]
        eprintln!("C3DOUBT {} us {}:{} {}",
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros(),
            file!(), line!(), format_args!($($arg)*));
    }};
}
