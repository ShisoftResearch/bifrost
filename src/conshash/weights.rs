use crate::raft::state_machine::{RecoverError, StateMachineCtl};
use crate::raft::RaftService;
use bifrost_plugins::hash_ident;
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_DHT_WEIGHTS) as u64;

raft_state_machine! {
    def cmd set_weight(group: u64, id: u64, weight: u64);
    def qry get_weights(group: u64) -> Option<HashMap<u64, u64>>;
    def qry get_weight(group: u64, id: u64) -> Option<u64>;
}
pub struct Weights {
    pub groups: HashMap<u64, HashMap<u64, u64>>,
    pub id: u64,
}
impl StateMachineCmds for Weights {
    fn set_weight(&mut self, group: u64, id: u64, weight: u64) -> BoxFuture<()> {
        *self
            .groups
            .entry(group)
            .or_insert_with(|| HashMap::new())
            .entry(id)
            .or_insert_with(|| 0) = weight;
        future::ready(()).boxed()
    }
    fn get_weights(&self, group: u64) -> BoxFuture<Option<HashMap<u64, u64>>> {
        future::ready(match self.groups.get(&group) {
            Some(m) => Some(m.clone()),
            None => None,
        })
        .boxed()
    }
    fn get_weight(&self, group: u64, id: u64) -> BoxFuture<Option<u64>> {
        future::ready(match self.groups.get(&group) {
            Some(m) => match m.get(&id) {
                Some(w) => Some(*w),
                None => None,
            },
            None => None,
        })
        .boxed()
    }
}
impl StateMachineCtl for Weights {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.id
    }
    fn snapshot(&self) -> Vec<u8> {
        crate::utils::serde::serialize(&self.groups)
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        if let Err(e) = self.restore(data) {
            // Raft recovery calls `try_recover`; this path cannot report the
            // failure, and keeps the current weights.
            error!("{}; keeping the current weights", e);
        }
        future::ready(()).boxed()
    }
    fn try_recover(&mut self, data: Vec<u8>) -> BoxFuture<Result<(), RecoverError>> {
        future::ready(self.restore(data)).boxed()
    }
    fn recoverable(&self) -> bool {
        true
    }
}
impl Weights {
    /// Restore the weights from their image; one that does not decode is an
    /// error, never an empty table (which would weigh every member at zero).
    fn restore(&mut self, data: Vec<u8>) -> Result<(), RecoverError> {
        self.groups =
            crate::utils::serde::deserialize::<HashMap<u64, HashMap<u64, u64>>>(data.as_slice())
                .ok_or_else(|| RecoverError {
                    sm_id: self.id,
                    reason: format!("the weights image ({} bytes) does not decode", data.len()),
                })?;
        Ok(())
    }

    pub async fn new_with_id(id: u64, raft_service: &Arc<RaftService>) {
        raft_service
            .register_state_machine(Box::new(Weights {
                groups: HashMap::new(),
                id,
            }))
            .await
    }
    pub async fn new(raft_service: &Arc<RaftService>) {
        Self::new_with_id(DEFAULT_SERVICE_ID, raft_service).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;

    #[test]
    fn snapshot_round_trips_under_the_active_codec() {
        let mut sm = Weights {
            groups: HashMap::new(),
            id: 1,
        };
        sm.groups.entry(7).or_default().insert(100, 3);
        sm.groups.entry(7).or_default().insert(200, 5);
        let snapshot = StateMachineCtl::snapshot(&sm);
        let mut restored = Weights {
            groups: HashMap::new(),
            id: 1,
        };
        block_on(StateMachineCtl::recover(&mut restored, snapshot));
        assert_eq!(restored.groups, sm.groups);
    }

    /// An image that does not decode stops recovery with a typed error; it
    /// never leaves the table empty (every member weighted zero).
    #[test]
    fn a_corrupt_image_stops_recovery_with_a_typed_error() {
        let mut sm = Weights {
            groups: HashMap::new(),
            id: 9,
        };
        sm.groups.entry(7).or_default().insert(100, 3);
        let before = sm.groups.clone();
        let err = block_on(StateMachineCtl::try_recover(&mut sm, vec![0xff, 0x00, 0x13]))
            .expect_err("garbage must not restore");
        assert_eq!(err.sm_id, 9);
        assert_eq!(sm.groups, before, "a refused image changes nothing");
    }
}
