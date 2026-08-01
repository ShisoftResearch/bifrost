use super::super::OpType;
use super::*;
use crate::raft::{PlaneError, PlaneId, RaftMsg, RaftService};
use crate::rpc;
use async_std::sync::*;
use bifrost_hasher::{hash_bytes, hash_str};
use futures::stream::FuturesUnordered;
use serde;
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::{Arc, Weak};

pub struct Subscriber {
    pub session_id: u64,
    pub address: String,
    // Connected on first notification, not at registration. Subscribe
    // runs inside raft command apply: dialing the subscriber there made
    // the command's outcome depend on transient connectivity (and on
    // which replica applied it), so bootstrap-time subscriptions
    // spuriously failed under load.
    client: tokio::sync::OnceCell<Arc<AsyncServiceClient>>,
}

impl Subscriber {
    fn new(session_id: u64, address: String) -> Self {
        Self {
            session_id,
            address,
            client: tokio::sync::OnceCell::new(),
        }
    }

    pub async fn client(&self) -> io::Result<Arc<AsyncServiceClient>> {
        self.client
            .get_or_try_init(|| async {
                RPCClient::new_async(&self.address)
                    .await
                    .map(|client| AsyncServiceClient::new(&client))
            })
            .await
            .cloned()
    }
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

    pub async fn subscribe(
        &mut self,
        key: SubKey,
        address: &String,
        session_id: u64,
    ) -> Result<u64, ()> {
        let suber_id = hash_str(address);
        let suber_exists = self.subscribers.contains_key(&suber_id);
        let sub_id = self.next_id;
        debug!(
            "Subscription {:?} from {}, address {}, plane {}, fn {}, pattern {}",
            key,
            suber_id,
            address,
            key.plane_id.raw(),
            key.fn_id,
            key.pattern_id
        );
        let require_reload_suber = if suber_exists {
            match self.subscribers.get(&suber_id) {
                Some(subscriber) => {
                    let session_match = subscriber.session_id == session_id;
                    if !session_match {
                        self.remove_subscriber(suber_id);
                        true
                    } else {
                        false
                    }
                }
                None => {
                    error!("Subscriber {} exists flag is true but not found in map - data inconsistency", suber_id);
                    // Treat as if subscriber doesn't exist - require reload
                    true
                }
            }
        } else {
            true
        };
        if !suber_exists || require_reload_suber {
            self.subscribers
                .insert(suber_id, Subscriber::new(session_id, address.clone()));
        }
        self.suber_subs
            .entry(suber_id)
            .or_insert_with(|| HashSet::new())
            .insert(sub_id);
        self.subscriptions
            .entry(key)
            .or_insert_with(|| HashSet::new())
            .insert(sub_id);
        self.sub_to_key.insert(sub_id, key);
        self.sub_suber.insert(sub_id, suber_id);

        self.next_id += 1;
        Ok(sub_id)
    }

    pub fn remove_subscriber(&mut self, suber_id: u64) {
        debug!("Removing subscriber {}", suber_id);
        let suber_subs = if let Some(sub_ids) = self.suber_subs.get(&suber_id) {
            sub_ids.iter().cloned().collect()
        } else {
            Vec::<u64>::new()
        };
        for subs_id in suber_subs {
            self.remove_subscription(subs_id)
        }
        self.subscribers.remove(&suber_id);
        self.suber_subs.remove(&suber_id);
    }

    pub fn remove_subscription(&mut self, id: u64) {
        debug!("Removing subscription {}", id);
        let sub_key = self.sub_to_key.remove(&id);
        if let Some(sub_key) = sub_key {
            if let Some(ref mut sub_subers) = self.subscriptions.get_mut(&sub_key) {
                sub_subers.remove(&id);
                self.sub_suber.remove(&id);
            }
        }
    }
}

// used for raft services to subscribe directly from state machine instances
pub struct InternalSubscription {
    action: Box<dyn Fn(&dyn Any) + Sync + Send>,
}

pub struct SMCallback {
    pub subscriptions: Arc<RwLock<Subscriptions>>,
    pub raft_service: Weak<RaftService>,
    pub internal_subs: RwLock<HashMap<u64, Vec<InternalSubscription>>>,
    pub plane_id: PlaneId,
    pub sm_id: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum NotifyError {
    IsNotLeader,
    OpTypeNotSubscribe,
    CannotFindSubscription,
    CannotFindSubscribers,
    CannotFindSubscriber,
    CannotCastInternalSub,
    CannotConnectToSubscriber,
}

impl SMCallback {
    pub async fn new(state_machine_id: u64, raft_service: Arc<RaftService>) -> SMCallback {
        Self::new_on_plane(state_machine_id, PlaneId::type1(), raft_service)
            .await
            .expect("type-1 callback construction should not fail")
    }

    pub async fn new_on_plane(
        state_machine_id: u64,
        plane_id: PlaneId,
        raft_service: Arc<RaftService>,
    ) -> Result<SMCallback, PlaneError> {
        let subscriptions = raft_service.subscriptions_on_plane(plane_id).await?;
        Ok(SMCallback {
            subscriptions,
            raft_service: Arc::downgrade(&raft_service),
            plane_id,
            sm_id: state_machine_id,
            internal_subs: RwLock::new(HashMap::new()),
        })
    }

    pub async fn notify<M, R>(
        &self,
        msg: M,
        message: R,
    ) -> Result<(usize, Vec<NotifyError>, Vec<Result<(), rpc::RPCError>>), NotifyError>
    where
        R: serde::Serialize + Send + Sync + Clone + Any + Unpin + 'static,
        M: RaftMsg<R> + 'static,
    {
        let raft_service = match self.raft_service.upgrade() {
            Some(raft_service) => raft_service,
            None => {
                debug!(
                    "Will not send notification from state machine {} on plane {} because its Raft service has been retired",
                    self.sm_id,
                    self.plane_id.raw()
                );
                return Err(NotifyError::IsNotLeader);
            }
        };
        let is_leader = raft_service
            .is_leader_on_plane(self.plane_id)
            .await
            .unwrap_or(false);
        if !is_leader {
            debug!(
                "Will not send notification from {} on plane {} because this node is not a leader",
                raft_service.get_server_id(),
                self.plane_id.raw()
            );
            return Err(NotifyError::IsNotLeader);
        }
        let (fn_id, op_type, pattern_data) = msg.encode();
        return match op_type {
            OpType::SUBSCRIBE => {
                let pattern_id = hash_bytes(&pattern_data.as_slice());
                let raft_sid = raft_service.options.service_id;
                let sm_id = self.sm_id;
                let key = SubKey::new(raft_sid, self.plane_id, sm_id, fn_id, pattern_id);
                let internal_subs = self.internal_subs.read().await;
                let svr_subs = self.subscriptions.read().await;
                debug!(
                    "Sending notification, func {}, op: {:?}, pattern_id {}",
                    fn_id, op_type, pattern_id
                );
                if let Some(internal_subs) = internal_subs.get(&pattern_id) {
                    for is in internal_subs {
                        (is.action)(&message)
                    }
                } else {
                    trace!("Cannot found internal subs {}", pattern_id);
                }
                if let Some(sub_ids) = svr_subs.subscriptions.get(&key) {
                    let sub_result_futs: FuturesUnordered<_> = sub_ids
                        .iter()
                        .map(|sub_id| {
                            let message = Pin::new(&message);
                            async move {
                                let svr_subs = self.subscriptions.read().await;
                                if let Some(subscriber_id) = svr_subs.sub_suber.get(&sub_id) {
                                    if let Some(subscriber) =
                                        svr_subs.subscribers.get(&subscriber_id)
                                    {
                                        let data = crate::utils::serde::serialize(&*message);
                                        let client = match subscriber.client().await {
                                            Ok(client) => client,
                                            Err(e) => {
                                                debug!(
                                                    "Cannot reach subscriber {} at {}: {:?}",
                                                    subscriber_id, subscriber.address, e
                                                );
                                                return Err(
                                                    NotifyError::CannotConnectToSubscriber,
                                                );
                                            }
                                        };
                                        debug!(
                                            "Sending out callback notification to sub id {}",
                                            sub_id
                                        );
                                        let client_result = client.notify(key, &data).await;
                                        Ok(client_result)
                                    } else {
                                        Err(NotifyError::CannotFindSubscriber)
                                    }
                                } else {
                                    Err(NotifyError::CannotFindSubscribers)
                                }
                            }
                        })
                        .collect();
                    let sub_result: Vec<_> = sub_result_futs.collect().await;
                    let errors: Vec<NotifyError> = sub_result
                        .iter()
                        .filter_map(|r| {
                            if let Err(e) = r {
                                Some(e.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    let response: Vec<_> = sub_result
                        .into_iter()
                        .filter_map(|r| if let Ok(value) = r { Some(value) } else { None })
                        .collect();
                    Ok((sub_ids.len(), errors, response))
                } else {
                    Err(NotifyError::CannotFindSubscription)
                }
            }
            _ => Err(NotifyError::OpTypeNotSubscribe),
        };
    }
    pub async fn internal_subscribe<R, F, M>(&self, msg: M, trigger: F) -> Result<(), NotifyError>
    where
        M: RaftMsg<R>,
        F: Fn(&R) + Sync + Send + 'static,
        R: 'static,
    {
        let (_, op_type, pattern_data) = msg.encode();
        match op_type {
            OpType::SUBSCRIBE => {
                let pattern_id = hash_bytes(&pattern_data.as_slice());
                let mut internal_subs = self.internal_subs.write().await;
                internal_subs
                    .entry(pattern_id)
                    .or_insert_with(|| Vec::new())
                    .push(InternalSubscription {
                        action: Box::new(move |any: &dyn Any| match any.downcast_ref::<R>() {
                            Some(r) => trigger(r),
                            None => warn!("type mismatch in internal subscription"),
                        }),
                    });
                Ok(())
            }
            _ => Err(NotifyError::OpTypeNotSubscribe),
        }
    }
}

pub async fn notify<M, R, F>(callback: &Option<SMCallback>, msg: M, data: F)
where
    F: FnOnce() -> R,
    M: RaftMsg<R> + Send + 'static,
    R: serde::Serialize + Send + Sync + Clone + Unpin + Any + 'static,
{
    if let Some(ref callback) = *callback {
        match callback.notify(msg, data()).await {
            Ok(_) | Err(NotifyError::IsNotLeader) => {}
            Err(e) => warn!(
                "Cannot send nofication, failed after called due to: {:?}",
                e
            ),
        }
    } else {
        warn!("Cannot send notification, callback handler is empty");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscriptions_new() {
        let subs = Subscriptions::new();

        assert_eq!(subs.next_id, 0);
        assert!(subs.subscribers.is_empty());
        assert!(subs.suber_subs.is_empty());
        assert!(subs.subscriptions.is_empty());
        assert!(subs.sub_suber.is_empty());
        assert!(subs.sub_to_key.is_empty());
    }

    #[test]
    fn test_remove_subscription_nonexistent() {
        let mut subs = Subscriptions::new();

        // Remove non-existent subscription should not crash
        subs.remove_subscription(999);

        assert!(subs.sub_to_key.is_empty());
        assert!(subs.subscriptions.is_empty());
    }

    #[test]
    fn test_remove_subscription() {
        let mut subs = Subscriptions::new();

        // Manually add a subscription
        let sub_id = 1u64;
        let sub_key = SubKey::new(0, PlaneId::type1(), 0, 100, 200);

        subs.sub_to_key.insert(sub_id, sub_key);
        subs.subscriptions
            .entry(sub_key)
            .or_insert_with(HashSet::new)
            .insert(sub_id);
        subs.sub_suber.insert(sub_id, 42u64);

        // Now remove it
        subs.remove_subscription(sub_id);

        assert!(!subs.sub_to_key.contains_key(&sub_id));
        assert!(!subs.sub_suber.contains_key(&sub_id));
        if let Some(subs_set) = subs.subscriptions.get(&sub_key) {
            assert!(!subs_set.contains(&sub_id));
        }
    }

    #[test]
    fn test_remove_subscriber() {
        let mut subs = Subscriptions::new();

        let suber_id = 42u64;
        let sub_id = 1u64;
        let sub_key = SubKey::new(0, PlaneId::type1(), 0, 100, 200);

        // Manually set up subscriber with subscription
        subs.suber_subs
            .entry(suber_id)
            .or_insert_with(HashSet::new)
            .insert(sub_id);
        subs.sub_to_key.insert(sub_id, sub_key);
        subs.subscriptions
            .entry(sub_key)
            .or_insert_with(HashSet::new)
            .insert(sub_id);
        subs.sub_suber.insert(sub_id, suber_id);

        // Remove the subscriber
        subs.remove_subscriber(suber_id);

        assert!(!subs.suber_subs.contains_key(&suber_id));
        assert!(!subs.subscribers.contains_key(&suber_id));
        assert!(!subs.sub_to_key.contains_key(&sub_id));
        assert!(!subs.sub_suber.contains_key(&sub_id));
    }

    #[test]
    fn test_remove_subscriber_nonexistent() {
        let mut subs = Subscriptions::new();

        // Remove non-existent subscriber should not crash
        subs.remove_subscriber(999);

        assert!(subs.subscribers.is_empty());
    }

    #[test]
    fn test_notify_error_debug() {
        // Test that NotifyError can be debugged and cloned
        let error = NotifyError::IsNotLeader;
        let cloned = error.clone();

        assert!(matches!(cloned, NotifyError::IsNotLeader));

        // Test all variants
        let _ = NotifyError::OpTypeNotSubscribe;
        let _ = NotifyError::CannotFindSubscription;
        let _ = NotifyError::CannotFindSubscribers;
        let _ = NotifyError::CannotFindSubscriber;
        let _ = NotifyError::CannotCastInternalSub;
    }

    #[test]
    fn test_subscriptions_next_id_increment() {
        let mut subs = Subscriptions::new();

        assert_eq!(subs.next_id, 0);

        // Simulate what subscribe does with next_id
        let first_id = subs.next_id;
        subs.next_id += 1;

        let second_id = subs.next_id;
        subs.next_id += 1;

        assert_eq!(first_id, 0);
        assert_eq!(second_id, 1);
        assert_eq!(subs.next_id, 2);
    }

    #[test]
    fn test_subscriptions_multiple_subs_per_subscriber() {
        let mut subs = Subscriptions::new();

        let suber_id = 42u64;
        let sub_id1 = 1u64;
        let sub_id2 = 2u64;
        let sub_key1 = SubKey::new(0, PlaneId::type1(), 0, 100, 200);
        let sub_key2 = SubKey::new(0, PlaneId::type1(), 0, 101, 201);

        // Add two subscriptions for same subscriber
        subs.suber_subs
            .entry(suber_id)
            .or_insert_with(HashSet::new)
            .insert(sub_id1);
        subs.suber_subs
            .entry(suber_id)
            .or_insert_with(HashSet::new)
            .insert(sub_id2);

        subs.sub_to_key.insert(sub_id1, sub_key1);
        subs.sub_to_key.insert(sub_id2, sub_key2);
        subs.sub_suber.insert(sub_id1, suber_id);
        subs.sub_suber.insert(sub_id2, suber_id);

        // Verify both subscriptions are tracked
        let subscriber_subs = subs.suber_subs.get(&suber_id).unwrap();
        assert_eq!(subscriber_subs.len(), 2);
        assert!(subscriber_subs.contains(&sub_id1));
        assert!(subscriber_subs.contains(&sub_id2));

        // Remove the subscriber - should remove both subscriptions
        subs.remove_subscriber(suber_id);

        assert!(!subs.sub_to_key.contains_key(&sub_id1));
        assert!(!subs.sub_to_key.contains_key(&sub_id2));
        assert!(!subs.suber_subs.contains_key(&suber_id));
    }
}
