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
mod raft;

mod mutex {
    use std::sync::Mutex;

//    #[test]
//    fn reentering_lock() { //FAIL
//        let l = Mutex::new(false);
//        {
//            println!("Locking A");
//            let a = l.lock().unwrap();
//            println!("Locked A");
//            {
//                println!("Locking B");
//                let b = l.lock().unwrap();
//                println!("Locked B");
//            }
//            println!("Unlocked B");
//        }
//        println!("Unlocked A");
//    }
}