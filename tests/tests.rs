#![feature(plugin)]
#![plugin(bifrost_plugins)]

#![feature(proc_macro)]

#[macro_use]
extern crate bifrost;
extern crate byteorder;
extern crate bincode;

#[macro_use]
extern crate serde_derive;

mod rpc;