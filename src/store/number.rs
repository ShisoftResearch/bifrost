#[macro_export]
macro_rules! def_store_number {
    ($m: ident, $t: ty) => {
        pub mod $m {
            use crate::raft::state_machine::StateMachineCtl;
            use bifrost_hasher::hash_str;
            use futures::FutureExt;
            use std::sync::Arc;
            use $crate::raft::state_machine::callback::server::SMCallback;
            use $crate::raft::RaftService;

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
                    async move {
                        let on = self.num;
                        self.num = n;
                        if let Some(ref callback) = self.callback {
                            let _ = callback.notify(commands::on_changed::new(), (on, n)).await;
                        }
                    }.boxed()
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
                    Some($crate::utils::serde::serialize(&self.num))
                }
                fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
                    self.num = $crate::utils::serde::deserialize(&data).unwrap();
                    future::ready(()).boxed()
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
        use crate::raft::client::RaftClient;
        use crate::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
        use crate::rpc::Server;
        use futures::prelude::*;
        use U32::client::SMClient;

        def_store_number!(U32, u32);

        #[tokio::test(threaded_scheduler)]
        async fn test() {
            let _ = env_logger::try_init();
            let addr = String::from("127.0.0.1:2011");
            let mut num_sm = U32::Number::new_by_name(&String::from("test"), 0);
            let service = RaftService::new(Options {
                storage: Storage::default(),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });
            let sm_id = num_sm.id;
            let server = Server::new(&addr);
            server.register_service(DEFAULT_SERVICE_ID, &service).await;
            Server::listen_and_resume(&server).await;
            num_sm.init_callback(&service).await;
            assert!(RaftService::start(&service).await);
            service.register_state_machine(Box::new(num_sm)).await;
            service.bootstrap().await;

            let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
                .await
                .unwrap();
            let sm_client = SMClient::new(sm_id, &client);
            RaftClient::prepare_subscription(&server).await;

            sm_client.on_changed(|res| {
                let (old, new) = res;
                info!("GOT NUM CHANGED: {} -> {}", old, new);
                future::ready(()).boxed()
            });

            assert_eq!(sm_client.get().await.unwrap(), 0);
            sm_client.set(&1).await.unwrap();
            assert_eq!(sm_client.get().await.unwrap(), 1);
            assert_eq!(sm_client.get_and_add(&2).await.unwrap(), 1);
            assert_eq!(sm_client.add_and_get(&3).await.unwrap(), 6);
            assert_eq!(sm_client.get_and_minus(&4).await.unwrap(), 6);
            assert_eq!(sm_client.minus_and_get(&2).await.unwrap(), 0);
            assert_eq!(sm_client.get_and_incr().await.unwrap(), 0);
            assert_eq!(sm_client.incr_and_get().await.unwrap(), 2);
            assert_eq!(sm_client.get_and_multiply(&2).await.unwrap(), 2);
            assert_eq!(sm_client.multiply_and_get(&2).await.unwrap(), 8);
            assert_eq!(sm_client.get_and_divide(&2).await.unwrap(), 8);
            assert_eq!(sm_client.divide_and_get(&4).await.unwrap(), 1);
            assert_eq!(sm_client.swap(&5).await.unwrap(), 1);
            assert_eq!(sm_client.get().await.unwrap(), 5);
            assert_eq!(sm_client.compare_and_swap(&1, &10).await.unwrap(), 5);
            assert_eq!(sm_client.get().await.unwrap(), 5);
            assert_eq!(sm_client.compare_and_swap(&5, &11).await.unwrap(), 5);
            assert_eq!(sm_client.get().await.unwrap(), 11);
        }
    }

    mod f64 {
        use crate::raft::client::RaftClient;
        use crate::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
        use crate::rpc::Server;
        use F64::client::SMClient;

        def_store_number!(F64, f64);

        #[tokio::test(threaded_scheduler)]
        async fn test() {
            let addr = String::from("127.0.0.1:2012");
            let num_sm = F64::Number::new_by_name(&String::from("test"), 0.0);
            let service = RaftService::new(Options {
                storage: Storage::default(),
                address: addr.clone(),
                service_id: DEFAULT_SERVICE_ID,
            });
            let sm_id = num_sm.id;
            let server = Server::new(&addr);
            server.register_service(DEFAULT_SERVICE_ID, &service).await;
            Server::listen_and_resume(&server).await;
            assert!(RaftService::start(&service).await);
            service.register_state_machine(Box::new(num_sm)).await;
            service.bootstrap().await;

            let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
                .await
                .unwrap();
            let sm_client = SMClient::new(sm_id, &client);

            assert_eq!(sm_client.get().await.unwrap(), 0.0);
            sm_client.set(&1.0).await.unwrap();
            assert_eq!(sm_client.get().await.unwrap(), 1.0);
            assert_eq!(sm_client.get_and_add(&2.0).await.unwrap(), 1.0);
            assert_eq!(sm_client.add_and_get(&3.0).await.unwrap(), 6.0);
            assert_eq!(sm_client.get_and_minus(&4.0).await.unwrap(), 6.0);
            assert_eq!(sm_client.minus_and_get(&2.0).await.unwrap(), 0.0);
            assert_eq!(sm_client.get_and_incr().await.unwrap(), 0.0);
            assert_eq!(sm_client.incr_and_get().await.unwrap(), 2.0);
            assert_eq!(sm_client.get_and_multiply(&2.0).await.unwrap(), 2.0);
            assert_eq!(sm_client.multiply_and_get(&2.0).await.unwrap(), 8.0);
            assert_eq!(sm_client.get_and_divide(&2.0).await.unwrap(), 8.0);
            assert_eq!(sm_client.divide_and_get(&4.0).await.unwrap(), 1.0);
            assert_eq!(sm_client.swap(&5.0).await.unwrap(), 1.0);
            assert_eq!(sm_client.get().await.unwrap(), 5.0);
            assert_eq!(sm_client.compare_and_swap(&1.0, &10.0).await.unwrap(), 5.0);
            assert_eq!(sm_client.get().await.unwrap(), 5.0);
            assert_eq!(sm_client.compare_and_swap(&5.0, &11.0).await.unwrap(), 5.0);
            assert_eq!(sm_client.get().await.unwrap(), 11.0);
        }
    }
}
