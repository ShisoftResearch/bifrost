use bifrost_plugins::hash_ident;

use crate::raft::PlaneId;

pub mod client;
pub mod server;
pub use server::SMCallback;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct SubKey {
    pub service_id: u64,
    pub plane_id: PlaneId,
    pub sm_id: u64,
    pub fn_id: u64,
    pub pattern_id: u64,
}

impl SubKey {
    pub const fn new(
        service_id: u64,
        plane_id: PlaneId,
        sm_id: u64,
        fn_id: u64,
        pattern_id: u64,
    ) -> Self {
        Self {
            service_id,
            plane_id,
            sm_id,
            fn_id,
            pattern_id,
        }
    }
}

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_RAFT_SM_CALLBACK_DEFAULT_SERVICE) as u64;

service! {
    rpc notify(key: SubKey, data: &Vec<u8>);
}

#[cfg(test)]
mod test {
    use crate::raft::client::RaftClient;
    use crate::raft::state_machine::callback::server::SMCallback;
    use crate::raft::state_machine::StateMachineCtl;
    use crate::raft::{Options, PlaneId, PlaneSpec, RaftService, Storage, DEFAULT_SERVICE_ID};
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use future::FutureExt;
    use std::sync::atomic::*;
    use std::sync::Arc;

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
            self.count += 1;
            async move {
                self.callback
                    .notify(commands::on_trigged::new(), self.count)
                    .await
                    .unwrap();
            }
            .boxed()
        }
    }

    impl StateMachineCtl for Trigger {
        raft_sm_complete!();
        fn id(&self) -> u64 {
            10
        }
        fn snapshot(&self) -> Vec<u8> {
            unreachable!()
        }
        fn recover(&mut self, _: Vec<u8>) -> BoxFuture<()> {
            future::ready(()).boxed()
        }
        fn recoverable(&self) -> bool {
            false
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn graceful_shutdown_releases_root_callback_state_machine_cycle() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: "root-callback-shutdown-cycle".to_string(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let weak_service = Arc::downgrade(&raft_service);
        let state_machine = Trigger {
            count: 0,
            callback: SMCallback::new(10, raft_service.clone()).await,
        };
        raft_service
            .register_state_machine(Box::new(state_machine))
            .await;

        raft_service.shutdown().await;
        drop(raft_service);

        assert!(
            weak_service.upgrade().is_none(),
            "registered callback state machine kept the root RaftService alive after shutdown"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn graceful_shutdown_releases_type2_callback_state_machine_cycle() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let reserved = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reserved.local_addr().unwrap().to_string();
        drop(reserved);
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: address.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let weak_service = Arc::downgrade(&raft_service);
        let server = Server::new(&address);
        server.register_service(&raft_service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&raft_service, false).await);
        raft_service.bootstrap().await;
        let plane_id = PlaneId::type2(12).unwrap();
        let plane = raft_service
            .ensure_plane(PlaneSpec { plane_id })
            .await
            .expect("type-2 plane should be created");
        let state_machine = Trigger {
            count: 0,
            callback: plane
                .callback(10)
                .await
                .expect("type-2 callback should bind to its plane"),
        };
        plane
            .register_state_machine(Box::new(state_machine))
            .await
            .expect("type-2 callback state machine should register");

        raft_service.shutdown().await;
        server.shutdown().await;
        drop(plane);
        drop(server);
        drop(raft_service);

        assert!(
            weak_service.upgrade().is_none(),
            "registered callback state machine kept the type-2 RaftService alive after shutdown"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn callback_returns_not_leader_after_service_owner_is_dropped() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: "callback-after-owner-drop".to_string(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let weak_service = Arc::downgrade(&raft_service);
        let callback = SMCallback::new(10, raft_service.clone()).await;

        drop(raft_service);

        assert!(matches!(
            callback.notify(commands::on_trigged::new(), 1).await,
            Err(super::server::NotifyError::IsNotLeader)
        ));
        assert!(
            weak_service.upgrade().is_none(),
            "callback retained its dropped RaftService owner"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dummy() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let _ = env_logger::try_init();
        info!("TESTING CALLBACK");
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
        server.register_service(&raft_service).await;
        Server::listen_and_resume(&server).await;
        RaftService::start(&raft_service, false).await;
        raft_service
            .register_state_machine(Box::new(dummy_sm))
            .await;
        raft_service.bootstrap().await;

        async_wait_secs().await;

        let raft_client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        let sm_client = Arc::new(client::SMClient::new(sm_id, &raft_client));
        let loops = 10;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let sumer = Arc::new(AtomicUsize::new(0));
        let sumer_clone = sumer.clone();
        let mut expected_sum = 0;
        RaftClient::prepare_subscription(&server).await;
        sm_client
            .on_trigged(move |res: u64| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                sumer_clone.fetch_add(res as usize, Ordering::Relaxed);
                info!("CALLBACK TRIGGERED {}", res);
                future::ready(()).boxed()
            })
            .await
            .unwrap()
            .unwrap();

        for i in 0..loops {
            let sm_client = sm_client.clone();
            expected_sum += i + 1;
            tokio::spawn(async move {
                sm_client.trigger().await.unwrap();
            });
        }

        async_wait_secs().await;

        assert_eq!(counter.load(Ordering::Relaxed), loops);
        assert_eq!(sumer.load(Ordering::Relaxed), expected_sum);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dummy_type2_plane() {
        let _callback_guard = crate::raft::client::callback_test_support::CALLBACK_TEST_GUARD
            .write()
            .await;
        let _ = env_logger::try_init();
        let addr = String::from("127.0.0.1:2111");
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server.register_service(&raft_service).await;
        Server::listen_and_resume(&server).await;
        assert!(RaftService::start(&raft_service, false).await);
        raft_service.bootstrap().await;

        let plane_id = PlaneId::type2(11).unwrap();
        let plane = raft_service
            .ensure_plane(PlaneSpec { plane_id })
            .await
            .expect("plane should be created");
        let dummy_sm = Trigger {
            count: 0,
            callback: plane
                .callback(10)
                .await
                .expect("type-2 callback should bind to plane-local subscriptions"),
        };
        let sm_id = dummy_sm.id();
        plane
            .register_state_machine(Box::new(dummy_sm))
            .await
            .expect("type-2 state machine should register");
        plane
            .recover_after_register()
            .await
            .expect("type-2 plane should finish registration recovery");

        let raft_client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        let plane_client = raft_client.plane(plane_id);
        let sm_client = Arc::new(client::SMClient::new(sm_id, &plane_client));
        let loops = 5;
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let sumer = Arc::new(AtomicUsize::new(0));
        let sumer_clone = sumer.clone();
        let mut expected_sum = 0;
        RaftClient::prepare_subscription(&server).await;
        sm_client
            .on_trigged(move |res: u64| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                sumer_clone.fetch_add(res as usize, Ordering::Relaxed);
                future::ready(()).boxed()
            })
            .await
            .unwrap()
            .unwrap();

        for i in 0..loops {
            let sm_client = sm_client.clone();
            expected_sum += i + 1;
            tokio::spawn(async move {
                sm_client.trigger().await.unwrap();
            });
        }

        async_wait_secs().await;

        assert_eq!(counter.load(Ordering::Relaxed), loops);
        assert_eq!(sumer.load(Ordering::Relaxed), expected_sum);
    }
}
