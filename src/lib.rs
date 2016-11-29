#![feature(plugin)]
#![plugin(bifrost_plugins)]
#![feature(proc_macro)]
#![crate_type = "lib"]

#[macro_use]
pub mod rpc;
pub mod raft;

extern crate mio;
extern crate byteorder;
extern crate slab;

extern crate bincode;
extern crate serde;
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate log;
extern crate env_logger;