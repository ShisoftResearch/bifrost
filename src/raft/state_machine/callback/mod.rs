use bifrost_plugins::hash_ident;

pub mod client;
pub mod server;
//                (server_id, raft_sid, sm_id, fn_id, pattern_id)
pub type SubKey = (u64, u64, u64, u64);

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_SM_CALLBACK_DEFAULT_SERVICE) as u64;

service! {
    rpc notify(key: SubKey, data: Vec<u8>);
}

#[cfg(test)]
mod test {
    use std::sync::atomic::Ordering;
    use crate::raft::client::RaftClient;
    use std::sync::Arc;
    use crate::raft::{Storage, RaftService, Options, DEFAULT_SERVICE_ID};
    use crate::raft::state_machine::callback::server::SMCallback;
    use crate::rpc::Server;
    use crate::raft::state_machine::StateMachineCtl;
    use futures::FutureExt;

    pub struct Trigger {
        count: u64,
        callback: SMCallback,
    }

    raft_state_machine! {
    def cmd trigger();
    def sub on_trigged() -> u64;
}

    impl StateMachineCmds for Trigger {
        fn trigger(&mut self) -> BoxFuture<()> {
            async {
                self.count += 1;
                self.callback
                    .notify(commands::on_trigged::new(), self.count).await;
            }.boxed()
        }
    }

    impl StateMachineCtl for Trigger {
        raft_sm_complete!();
        fn id(&self) -> u64 {
            10
        }
        fn snapshot(&self) -> Option<Vec<u8>> {
            None
        }
        fn recover(&mut self, data: Vec<u8>) {}
    }


    #[tokio::test(threaded_scheduler)]
    async fn dummy() {
        println!("TESTING CALLBACK");
        let addr = String::from("127.0.0.1:2110");
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        let dummy_sm = Trigger {
            count: 0,
            callback: SMCallback::new(10, raft_service.clone()).await,
        };
        let sm_id = dummy_sm.id();
        server.register_service(DEFAULT_SERVICE_ID, &raft_service);
        Server::listen_and_resume(&server);
        RaftService::start(&raft_service).await;
        raft_service
            .register_state_machine(Box::new(dummy_sm))
            .await;
        raft_service.bootstrap().await;

        wait().await;

        let raft_client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID).await.unwrap();
        let sm_client = Arc::new(client::SMClient::new(sm_id, &raft_client));
        let loops = 10;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let sumer = Arc::new(AtomicUsize::new(0));
        let sumer_clone = sumer.clone();
        let mut expected_sum = 0;
        RaftClient::prepare_subscription(&server);
        sm_client
            .on_trigged(move |res: u64| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                sumer_clone.fetch_add(res as usize, Ordering::Relaxed);
                println!("CALLBACK TRIGGERED {}", res);
            })
            .await
            .unwrap();

        for i in 0..loops {
            let sm_client = sm_client.clone();
            expected_sum += i + 1;
            tokio::spawn(async {
                sm_client.trigger().await.unwrap();
            });
        }

        wait().await;

        assert_eq!(counter.load(Ordering::Relaxed), loops);
        assert_eq!(sumer.load(Ordering::Relaxed), expected_sum);
    }
}