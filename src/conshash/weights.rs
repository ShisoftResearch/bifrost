use raft::state_machine::StateMachineCtl;
use raft::RaftService;
use std::collections::HashMap;
use std::sync::Arc;
use utils::bincode;

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
    fn set_weight(&mut self, group: u64, id: u64, weight: u64) -> Result<(), ()> {
        *self
            .groups
            .entry(group)
            .or_insert_with(|| HashMap::new())
            .entry(id)
            .or_insert_with(|| 0) = weight;
        Ok(())
    }
    fn get_weights(&self, group: u64) -> Result<Option<HashMap<u64, u64>>, ()> {
        Ok(match self.groups.get(&group) {
            Some(m) => Some(m.clone()),
            None => None,
        })
    }
    fn get_weight(&self, group: u64, id: u64) -> Result<Option<u64>, ()> {
        Ok(match self.groups.get(&group) {
            Some(m) => match m.get(&id) {
                Some(w) => Some(*w),
                None => None,
            },
            None => None,
        })
    }
}
impl StateMachineCtl for Weights {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        self.id
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        Some(bincode::serialize(&self.groups))
    }
    fn recover(&mut self, data: Vec<u8>) {
        self.groups = bincode::deserialize(&data);
    }
}
impl Weights {
    pub fn new_with_id(id: u64, raft_service: &Arc<RaftService>) {
        raft_service.register_state_machine(Box::new(Weights {
            groups: HashMap::new(),
            id,
        }))
    }
    pub fn new(raft_service: &Arc<RaftService>) {
        Self::new_with_id(DEFAULT_SERVICE_ID, raft_service)
    }
}
