use bifrost::raft::client::RaftClient;
use bifrost::raft::state_machine::callback::client::SubscriptionService;
use bifrost::raft::state_machine::callback::server::SMCallback;
use bifrost::raft::state_machine::master::ExecError;
use bifrost::raft::state_machine::StateMachineCtl;
use bifrost::raft::*;
use bifrost::rpc::Server;

use super::wait;

use futures::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

pub struct Trigger {
    count: u64,
    callback: SMCallback,
}

raft_state_machine! {
    def cmd trigger();
    def sub on_trigged() -> u64;
}

impl StateMachineCmds for Trigger {
    fn trigger(&mut self) -> Result<(), ()> {
        self.count += 1;
        self.callback
            .notify(commands::on_trigged::new(), Ok(self.count));
        Ok(())
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

#[test]
fn dummy() {
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
        callback: SMCallback::new(10, raft_service.clone()),
    };
    let sm_id = dummy_sm.id();
    server.register_service(DEFAULT_SERVICE_ID, &raft_service);
    Server::listen_and_resume(&server);
    RaftService::start(&raft_service);
    raft_service.register_state_machine(Box::new(dummy_sm));
    raft_service.bootstrap();

    wait();

    let raft_client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID).unwrap();
    let sm_client = Arc::new(client::SMClient::new(sm_id, &raft_client));
    let loops = 10;
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = counter.clone();
    let sumer = Arc::new(AtomicUsize::new(0));
    let sumer_clone = sumer.clone();
    let mut expected_sum = 0;
    RaftClient::prepare_subscription(&server);
    sm_client
        .on_trigged(move |res| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
            sumer_clone.fetch_add(res.unwrap() as usize, Ordering::Relaxed);
            println!("CALLBACK TRIGGERED {}", res.unwrap());
        })
        .wait()
        .unwrap();

    for i in 0..loops {
        let sm_client = sm_client.clone();
        expected_sum += i + 1;
        thread::spawn(move || {
            sm_client.trigger().wait().unwrap();
        });
    }

    wait();

    assert_eq!(counter.load(Ordering::Relaxed), loops);
    assert_eq!(sumer.load(Ordering::Relaxed), expected_sum);
}
