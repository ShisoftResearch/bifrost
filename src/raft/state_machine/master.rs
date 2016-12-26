use super::super::*;
use super::*;
use std::collections::HashMap;
use self::configs::{Configures, RaftMember, CONFIG_SM_ID};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExecError {
    SmNotFound,
    FnNotFound,
    ServerUnreachable
}

pub enum RegisterResult {
    OK,
    EXISTED,
    RESERVED,
}

pub type ExecOk = Vec<u8>;
pub type ExecResult = Result<ExecOk, ExecError>;
pub type SubStateMachine = Box<StateMachineCtl>;
pub type SnapshotDataItem = (u64, Vec<u8>);
pub type SnapshotDataItems = Vec<SnapshotDataItem>;

raft_state_machine! {}

pub struct MasterStateMachine {
    subs: HashMap<u64, SubStateMachine>,
    pub configs: Configures
}

impl StateMachineCmds for MasterStateMachine {}

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

    pub fn commit_cmd(&mut self, entry: &LogEntry) -> ExecResult {
        if let Some(sm) = self.subs.get_mut(&entry.sm_id) {
            match sm.as_mut().fn_dispatch_cmd(entry.fn_id, &entry.data) {
                Some(d) => Ok(d),
                None => Err(ExecError::FnNotFound)
            }
        } else {
            Err(ExecError::SmNotFound)
        }
    }
    pub fn exec_qry(&self, entry: &LogEntry) -> ExecResult {
        if let Some(sm) = self.subs.get(&entry.sm_id) {
            match sm.fn_dispatch_qry(entry.fn_id, &entry.data) {
                Some(d) => Ok(d),
                None => Err(ExecError::FnNotFound)
            }
        } else {
            Err(ExecError::SmNotFound)
        }
    }
}
