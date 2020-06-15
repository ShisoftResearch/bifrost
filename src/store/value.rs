#[macro_export]
macro_rules! def_store_value {
    ($m: ident, $t: ty) => {
        pub mod $m {
            use bifrost_hasher::hash_str;
            use futures::FutureExt;
            use std::sync::Arc;
            use $crate::raft::state_machine::callback::server::SMCallback;
            use $crate::raft::state_machine::StateMachineCtl;
            use $crate::raft::RaftService;
            pub struct Value {
                pub val: $t,
                pub id: u64,
                callback: Option<SMCallback>,
            }
            raft_state_machine! {
                def cmd set(v: $t);
                def qry get() -> $t;
                def sub on_changed() -> ($t, $t);
            }
            impl StateMachineCmds for Value {
                fn set(&mut self, v: $t) -> ::futures::future::BoxFuture<()> {
                    async move {
                        if let Some(ref callback) = self.callback {
                            let old = self.val.clone();
                            let _ = callback
                                .notify(commands::on_changed::new(), (old, v.clone()))
                                .await;
                        }
                        self.val = v;
                    }
                    .boxed()
                }
                fn get(&self) -> ::futures::future::BoxFuture<$t> {
                    future::ready(self.val.clone()).boxed()
                }
            }
            impl StateMachineCtl for Value {
                raft_sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some($crate::utils::serde::serialize(&self.val))
                }
                fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
                    self.val = $crate::utils::serde::deserialize(&data).unwrap();
                    future::ready(()).boxed()
                }
                fn id(&self) -> u64 {
                    self.id
                }
            }
            impl Value {
                pub fn new(id: u64, val: $t) -> Value {
                    Value {
                        val: val,
                        id: id,
                        callback: None,
                    }
                }
                pub fn new_by_name(name: &String, val: $t) -> Value {
                    Value::new(hash_str(name), val)
                }
                pub async fn init_callback(&mut self, raft_service: &Arc<RaftService>) {
                    self.callback = Some(SMCallback::new(self.id(), raft_service.clone()).await);
                }
            }
        }
    };
}

#[cfg(test)]
mod test {
    use crate::raft::client::RaftClient;
    use crate::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
    use crate::rpc::Server;
    use string::client::SMClient;

    def_store_value!(string, String);

    #[tokio::test(threaded_scheduler)]
    async fn string() {
        let addr = String::from("127.0.0.1:2010");
        let original_string = String::from("The stored text");
        let altered_string = String::from("The altered text");
        let mut string_sm =
            string::Value::new_by_name(&String::from("test"), original_string.clone());
        let service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let sm_id = string_sm.id;
        let server = Server::new(&addr);
        string_sm.init_callback(&service).await;
        server.register_service(DEFAULT_SERVICE_ID, &service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&service).await);
        service.register_state_machine(Box::new(string_sm)).await;
        service.bootstrap().await;

        let client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        let sm_client = SMClient::new(sm_id, &client);
        RaftClient::prepare_subscription(&server).await;
        assert_eq!(&sm_client.get().await.unwrap(), &original_string);
        sm_client.set(&altered_string).await.unwrap();
        assert_eq!(&sm_client.get().await.unwrap(), &altered_string);
    }
}
