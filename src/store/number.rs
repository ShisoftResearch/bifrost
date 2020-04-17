#[macro_export]
macro_rules! def_store_number {
    ($m: ident, $t: ty) => {
        pub mod $m {
            use crate::raft::state_machine::StateMachineCtl;
            use bifrost_hasher::hash_str;
            use std::sync::Arc;
            use $crate::raft::state_machine::callback::server::SMCallback;
            use $crate::raft::RaftService;
            use futures::FutureExt;

            pub struct Number {
                pub num: $t,
                pub id: u64,
                callback: Option<SMCallback>,
            }
            raft_state_machine! {
                def cmd set(n: $t);
                def qry get() -> $t;

                def cmd get_and_add(n: $t) -> $t;
                def cmd add_and_get(n: $t) -> $t;

                def cmd get_and_minus(n: $t) -> $t;
                def cmd minus_and_get(n: $t) -> $t;

                def cmd get_and_incr() -> $t;
                def cmd incr_and_get() -> $t;

                def cmd get_and_decr() -> $t;
                def cmd decr_and_get() -> $t;

                def cmd get_and_multiply(n: $t) -> $t;
                def cmd multiply_and_get(n: $t) -> $t;

                def cmd get_and_divide(n: $t) -> $t;
                def cmd divide_and_get(n: $t) -> $t;

                def cmd compare_and_swap(original: $t, n: $t) -> $t;
                def cmd swap(n: $t) -> $t;

                def sub on_changed() -> ($t, $t);
            }
            impl StateMachineCmds for Number {
                fn set(&mut self, n: $t) -> BoxFuture<()> {
                    let on = self.num;
                    self.num = n;
                    if let Some(ref callback) = self.callback {
                        callback.notify(commands::on_changed::new(), (on, n));
                    }
                    future::ready(()).boxed()
                }
                fn get(&self) -> BoxFuture<$t> {
                    future::ready(self.num).boxed()
                }
                fn get_and_add(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on + n);
                    future::ready(on).boxed()
                }
                fn add_and_get(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on + n);
                    future::ready(self.num).boxed()
                }
                fn get_and_minus(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on - n);
                    future::ready(on).boxed()
                }
                fn minus_and_get(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on - n);
                    future::ready(self.num).boxed()
                }
                fn get_and_incr(&mut self) -> BoxFuture<$t> {
                    self.get_and_add(1 as $t)
                }
                fn incr_and_get(&mut self) -> BoxFuture<$t> {
                    self.add_and_get(1 as $t)
                }
                fn get_and_decr(&mut self) -> BoxFuture<$t> {
                    self.get_and_minus(1 as $t)
                }
                fn decr_and_get(&mut self) -> BoxFuture<$t> {
                    self.minus_and_get(1 as $t)
                }
                fn get_and_multiply(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on * n);
                    future::ready(on).boxed()
                }
                fn multiply_and_get(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on * n);
                    future::ready(self.num).boxed()
                }
                fn get_and_divide(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on / n);
                    future::ready(on).boxed()
                }
                fn divide_and_get(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(on / n);
                    future::ready(self.num).boxed()
                }
                fn compare_and_swap(&mut self, original: $t, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    if on == original {
                        self.set(n);
                    }
                    future::ready(on).boxed()
                }
                fn swap(&mut self, n: $t) -> BoxFuture<$t> {
                    let on = self.num;
                    self.set(n);
                    future::ready(on).boxed()
                }
            }
            impl StateMachineCtl for Number {
                raft_sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some($crate::utils::bincode::serialize(&self.num))
                }
                fn recover(&mut self, data: Vec<u8>) {
                    self.num = $crate::utils::bincode::deserialize(&data);
                }
                fn id(&self) -> u64 {
                    self.id
                }
            }
            impl Number {
                pub fn new(id: u64, val: $t) -> Number {
                    Number {
                        num: val,
                        id: id,
                        callback: None,
                    }
                }
                pub fn new_by_name(name: &String, num: $t) -> Number {
                    Number::new(hash_str(name), num)
                }
                pub async fn init_callback(&mut self, raft_service: &Arc<RaftService>) {
                    self.callback = Some(SMCallback::new(self.id(), raft_service.clone()).await);
                }
            }
        }
    };
}

def_store_number!(I8, i8);
// def_store_number!(I16, i16);
// def_store_number!(I32, i32);
// def_store_number!(I64, i64);
// def_store_number!(U8, u8);
// def_store_number!(U16, u16);
// def_store_number!(U32, u32);
// def_store_number!(U64, u64);
// def_store_number!(F64, f64);
// def_store_number!(F32, f32);

#[cfg(test)]
mod test {
    mod u32 {
        use bifrost::raft::client::RaftClient;
        use bifrost::raft::state_machine::callback::client::SubscriptionService;
        use bifrost::raft::*;
        use bifrost::rpc::Server;
        use bifrost::store::number::U32;
        use bifrost::store::number::U32::client::SMClient;
        use bifrost::store::number::U32::commands::{
            add_and_get, compare_and_swap, decr_and_get, divide_and_get, get, get_and_add,
            get_and_decr, get_and_divide, get_and_incr, get_and_minus, get_and_multiply, incr_and_get,
            minus_and_get, multiply_and_get, set, swap,
        };
        use futures::prelude::*;
        use crate::raft::client::RaftClient;
        use crate::raft::{DEFAULT_SERVICE_ID, RaftService, Storage, Options};
        use crate::rpc::Server;

        #[test]
        fn test() {
            let addr = String::from("127.0.0.1:2011");
            let mut num_sm = U32::Number::new_by_name(&String::from("test"), 0);
            let service = RaftService::new(Options {
                storage: Storage::default(),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });
            let sm_id = num_sm.id;
            let server = Server::new(&addr);
            server.register_service(DEFAULT_SERVICE_ID, &service);
            Server::listen_and_resume(&server);
            num_sm.init_callback(&service);
            assert!(RaftService::start(&service).await);
            service.register_state_machine(Box::new(num_sm)).await;
            service.bootstrap().await;

            let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID).unwrap();
            let sm_client = SMClient::new(sm_id, &client);
            RaftClient::prepare_subscription(&server);

            sm_client.on_changed(|res| {
                if let Ok((old, new)) = res {
                    println!("GOT NUM CHANGED: {} -> {}", old, new);
                }
            });

            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 0);
            sm_client.set(&1).wait().unwrap().unwrap();
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 1);
            assert_eq!(sm_client.get_and_add(&2).wait().unwrap().unwrap(), 1);
            assert_eq!(sm_client.add_and_get(&3).wait().unwrap().unwrap(), 6);
            assert_eq!(sm_client.get_and_minus(&4).wait().unwrap().unwrap(), 6);
            assert_eq!(sm_client.minus_and_get(&2).wait().unwrap().unwrap(), 0);
            assert_eq!(sm_client.get_and_incr().wait().unwrap().unwrap(), 0);
            assert_eq!(sm_client.incr_and_get().wait().unwrap().unwrap(), 2);
            assert_eq!(sm_client.get_and_multiply(&2).wait().unwrap().unwrap(), 2);
            assert_eq!(sm_client.multiply_and_get(&2).wait().unwrap().unwrap(), 8);
            assert_eq!(sm_client.get_and_divide(&2).wait().unwrap().unwrap(), 8);
            assert_eq!(sm_client.divide_and_get(&4).wait().unwrap().unwrap(), 1);
            assert_eq!(sm_client.swap(&5).wait().unwrap().unwrap(), 1);
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 5);
            assert_eq!(
                sm_client.compare_and_swap(&1, &10).wait().unwrap().unwrap(),
                5
            );
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 5);
            assert_eq!(
                sm_client.compare_and_swap(&5, &11).wait().unwrap().unwrap(),
                5
            );
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 11);
        }
    }

    mod f64 {
        use bifrost::raft::client::RaftClient;
        use bifrost::raft::*;
        use bifrost::rpc::Server;
        use bifrost::store::number::F64;
        use bifrost::store::number::F64::client::SMClient;
        use bifrost::store::number::F64::commands::{
            add_and_get, compare_and_swap, decr_and_get, divide_and_get, get, get_and_add,
            get_and_decr, get_and_divide, get_and_incr, get_and_minus, get_and_multiply, incr_and_get,
            minus_and_get, multiply_and_get, set, swap,
        };
        use futures::prelude::*;
        use crate::raft::client::RaftClient;
        use crate::raft::{RaftService, DEFAULT_SERVICE_ID, Storage, Options};
        use crate::rpc::Server;

        #[test]
        fn test() {
            let addr = String::from("127.0.0.1:2012");
            let num_sm = F64::Number::new_by_name(&String::from("test"), 0.0);
            let service = RaftService::new(Options {
                storage: Storage::default(),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });
            let sm_id = num_sm.id;
            let server = Server::new(&addr);
            server.register_service(DEFAULT_SERVICE_ID, &service);
            Server::listen_and_resume(&server);
            assert!(RaftService::start(&service).await);
            service.register_state_machine(Box::new(num_sm)).await;
            service.bootstrap().await;

            let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID).unwrap();
            let sm_client = SMClient::new(sm_id, &client);

            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 0.0);
            sm_client.set(&1.0).wait().unwrap().unwrap();
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 1.0);
            assert_eq!(sm_client.get_and_add(&2.0).wait().unwrap().unwrap(), 1.0);
            assert_eq!(sm_client.add_and_get(&3.0).wait().unwrap().unwrap(), 6.0);
            assert_eq!(sm_client.get_and_minus(&4.0).wait().unwrap().unwrap(), 6.0);
            assert_eq!(sm_client.minus_and_get(&2.0).wait().unwrap().unwrap(), 0.0);
            assert_eq!(sm_client.get_and_incr().wait().unwrap().unwrap(), 0.0);
            assert_eq!(sm_client.incr_and_get().wait().unwrap().unwrap(), 2.0);
            assert_eq!(
                sm_client.get_and_multiply(&2.0).wait().unwrap().unwrap(),
                2.0
            );
            assert_eq!(
                sm_client.multiply_and_get(&2.0).wait().unwrap().unwrap(),
                8.0
            );
            assert_eq!(sm_client.get_and_divide(&2.0).wait().unwrap().unwrap(), 8.0);
            assert_eq!(sm_client.divide_and_get(&4.0).wait().unwrap().unwrap(), 1.0);
            assert_eq!(sm_client.swap(&5.0).wait().unwrap().unwrap(), 1.0);
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 5.0);
            assert_eq!(
                sm_client
                    .compare_and_swap(&1.0, &10.0)
                    .wait()
                    .unwrap()
                    .unwrap(),
                5.0
            );
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 5.0);
            assert_eq!(
                sm_client
                    .compare_and_swap(&5.0, &11.0)
                    .wait()
                    .unwrap()
                    .unwrap(),
                5.0
            );
            assert_eq!(sm_client.get().wait().unwrap().unwrap(), 11.0);
        }
    }
}
