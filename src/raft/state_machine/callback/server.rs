use std::boxed::FnBox;
use std::collections::{HashMap, HashSet};
use std::sync::{RwLock, Arc};
use std::sync::atomic::{AtomicU64, Ordering};
use bifrost_hasher::{hash_str, hash_bytes};
use raft::RaftService;
use rpc;
use serde;
use super::super::{StateMachineCtl, OpType};
use super::super::super::RaftMsg;
use super::*;

lazy_static! {
    pub static ref SUBSCRIPTIONS: RwLock<HashMap<u64, Subscriptions>> = RwLock::new(HashMap::new());
}

pub struct Subscriber {
    pub session_id: u64,
    pub client: Arc<SyncServiceClient>
}

pub struct Subscriptions {
    next_id: u64,
    subscribers: HashMap<u64, Subscriber>,
    suber_subs: HashMap<u64, HashSet<u64>>, //suber_id -> sub_id
    subscriptions: HashMap<SubKey, HashSet<u64>>, // key -> sub_id
    sub_suber: HashMap<u64, u64>,
    sub_to_key: HashMap<u64, SubKey>, //sub_id -> sub_key
}

impl Subscriptions {

    pub fn new() -> Subscriptions {
        Subscriptions {
            next_id: 0,
            subscribers: HashMap::new(),
            suber_subs: HashMap::new(),
            subscriptions: HashMap::new(),
            sub_suber: HashMap::new(),
            sub_to_key: HashMap::new(),
        }
    }

    pub fn subscribe(&mut self, key: SubKey, address: String, session_id: u64) -> Result<u64, ()> {
        let sub_service_id = DEFAULT_SERVICE_ID;
        let suber_id = hash_str(address.clone());
        let suber_exists = self.subscribers.contains_key(&suber_id);
        let sub_id = self.next_id;
        let require_reload_suber = if suber_exists {
            let session_match = self.subscribers.get(&suber_id).unwrap().session_id == session_id;
            if !session_match {
                self.remove_subscriber(suber_id);
                true
            } else {false}
        } else {true};
        if !self.subscribers.contains_key(&suber_id) {
            self.subscribers.insert(suber_id, Subscriber {
                session_id: session_id,
                client: {
                    if let Ok(client) = rpc::DEFAULT_CLIENT_POOL.get(&address) {
                        SyncServiceClient::new(sub_service_id, client)
                    } else {
                        return Err(());
                    }
                }
            });
        }
        self.suber_subs.entry(suber_id).or_insert_with(|| HashSet::new()).insert(sub_id);
        self.subscriptions.entry(key).or_insert_with(|| HashSet::new()).insert(sub_id);
        self.sub_to_key.insert(sub_id, key);
        self.sub_suber.insert(sub_id, suber_id);

        self.next_id += 1;
        Ok(sub_id)
    }

    pub fn remove_subscriber(&mut self, suber_id: u64) {
        let suber_subs = if let Some(sub_ids) = self.suber_subs.get(&suber_id) {
            sub_ids.iter().cloned().collect()
        } else {Vec::<u64>::new()};
        for subs_id in suber_subs {
            self.remove_subscription(subs_id)
        }
        self.subscribers.remove(&suber_id);
        self.suber_subs.remove(&suber_id);
    }

    pub fn remove_subscription(&mut self, id: u64) {
        let sub_key = self.sub_to_key.remove(&id);
        if let Some(sub_key) = sub_key {
            if let Some(ref mut sub_subers) = self.subscriptions.get_mut(&sub_key) {
                sub_subers.remove(&id);
                self.sub_suber.remove(&id);
            }
        }
    }
}

pub struct SMCallback {
    pub raft_service: Arc<RaftService>,
    pub sm_id: u64,
}

impl SMCallback {
    pub fn new(state_machine_id: u64, raft_service: Arc<RaftService>) -> SMCallback {
        SMCallback {
            raft_service: raft_service,
            sm_id: state_machine_id,
        }
    }
    pub fn notify<R>(&self, func: &RaftMsg<R>, data: R) -> bool
    where R: serde::Serialize {
        let (fn_id, op_type, pattern_data) = func.encode();
        match op_type {
            OpType::SUBSCRIBE => {
                let pattern_id = hash_bytes(&pattern_data.as_slice());
                let raft_sid = self.raft_service.options.service_id;
                let sm_id = self.sm_id;
                let key = (raft_sid, sm_id, fn_id, pattern_id);
                let subscriptions_map = SUBSCRIPTIONS.read().unwrap();
                if let Some(subscriptions) = subscriptions_map.get(&raft_sid) {
                    if let Some(sub_ids) = subscriptions.subscriptions.get(&key) {
                        for sub_id in sub_ids {
                            if let Some(subscriber_id) = subscriptions.sub_suber.get(&sub_id) {
                                if let Some(subscriber) = subscriptions.subscribers.get(&subscriber_id) {
//                                    if let Ok(_) = subscriber.client.notify(
//                                        key, serialize!(&data)
//                                    ) {return true;} else {panic!("callback failed");}
                                    panic!("NOTIFY: {:?}", subscriber.client.notify(
                                        key, serialize!(&data)
                                    ))
                                }
                            }
                        }
                    }
                }
            },
            _ => {}
        }
        return false;
    }
}
