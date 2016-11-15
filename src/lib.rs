#![feature(plugin)]
#![feature(proc_macro)]
#![plugin(bifrost_plugins)]
#![crate_type = "lib"]

pub mod rpc;

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