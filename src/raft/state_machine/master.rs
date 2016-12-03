use super::super::*;
use super::*;
use std::collections::HashMap;
use self::configs::{Configures, RaftMember, CONFIG_SM_ID};

#[derive(Serialize, Deserialize, Debug)]
pub enum AppendError {
    NOT_FOUND,
    NO_ENTRY,
}

pub enum RegisterResult {
    OK,
    EXISTED,
    RESERVED,
}

pub type AppendResult = Result<Option<Vec<u8>>, AppendError>;
pub type AppendResults = Vec<AppendResult>;
pub type SubStateMachine = Box<StateMachineCtl>;
pub type SnapshotDataItem = (u64, Vec<u8>);
pub type SnapshotDataItems = Vec<SnapshotDataItem>;
pub type BoxedConfig = Box<Configures>;

raft_state_machine! {
    def cmd append(entries: Option<LogEntries>) -> AppendResults;
}

pub struct MasterStateMachine {
    subs: HashMap<u64, SubStateMachine>,
    configs: Configures
}

impl StateMachineCmds for MasterStateMachine {
    fn append(&mut self, entries: Option<LogEntries>) -> Result<AppendResults, ()> {
        let mut rt: AppendResults = Vec::new();
        if let Some(entries) = entries {
            for entry in entries {
                let (sub_id, fn_id, data) = entry;
                rt.push(
                    if let Some(sm) = self.subs.get_mut(&sub_id) {
                        Ok(sm.as_mut().fn_dispatch(fn_id, &data))
                    } else {
                        Err(AppendError::NOT_FOUND)
                    }
                );
            }
            Ok(rt)
        } else {
            Err(())
        }
    }
}

impl StateMachineCtl for MasterStateMachine {
    sm_complete!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        let mut sms: SnapshotDataItems = Vec::with_capacity(self.subs.len());
        for (sm_id, smc) in self.subs.iter() {
            let sub_snapshot = smc.snapshot();
            if let Some(snapshot) = sub_snapshot {
                sms.push((*sm_id, snapshot));
            }
        }
        sms.push((self.configs.id(), self.configs.snapshot().unwrap()));
        let data = serialize!(&sms);
        Some(data)
    }
    fn recover(&mut self, data: Vec<u8>) {
        let mut sms: SnapshotDataItems = deserialize!(&data);
        for (sm_id, snapshot) in sms {
            if let Some(sm) = self.subs.get_mut(&sm_id) {
                sm.recover(snapshot);
            } else if sm_id == self.configs.id() {
                self.configs.recover(snapshot);
            }
        }
    }
    fn id(&self) -> u64 {0}
}

impl MasterStateMachine {
    pub fn new() -> MasterStateMachine {
        let mut msm = MasterStateMachine {
            subs: HashMap::new(),
            configs: Configures::new()
        };
        msm
    }

    pub fn register(&mut self, smc: SubStateMachine) -> RegisterResult {
        let id = smc.id();
        if id < 2 {return RegisterResult::RESERVED}
        if self.subs.contains_key(&id) {return RegisterResult::EXISTED};
        self.subs.insert(id, smc);
        RegisterResult::OK
    }

    pub fn members(&self) -> &HashMap<u64, RaftMember> {
        &self.configs.members
    }
}
