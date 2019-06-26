use self::configs::{Configures, RaftMember, CONFIG_SM_ID};
use super::super::*;
use super::*;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use utils::bincode;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExecError {
    SmNotFound,
    FnNotFound,
    ServersUnreachable,
    CannotConstructClient,
    NotCommitted,
    Unknown,
    TooManyRetry,
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
    pub configs: Configures,
}

impl StateMachineCmds for MasterStateMachine {}

impl StateMachineCtl for MasterStateMachine {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        0
    }
    fn snapshot(&self) -> Option<Vec<u8>> {
        let mut sms: SnapshotDataItems = Vec::with_capacity(self.subs.len());
        for (sm_id, smc) in self.subs.iter() {
            let sub_snapshot = smc.snapshot();
            if let Some(snapshot) = sub_snapshot {
                sms.push((*sm_id, snapshot));
            }
        }
        sms.push((self.configs.id(), self.configs.snapshot().unwrap()));
        let data = bincode::serialize(&sms);
        Some(data)
    }
    fn recover(&mut self, data: Vec<u8>) {
        let mut sms: SnapshotDataItems = bincode::deserialize(&data);
        for (sm_id, snapshot) in sms {
            if let Some(sm) = self.subs.get_mut(&sm_id) {
                sm.recover(snapshot);
            } else if sm_id == self.configs.id() {
                self.configs.recover(snapshot);
            }
        }
    }
}

fn parse_output(r: Option<Vec<u8>>) -> ExecResult {
    if let Some(d) = r {
        Ok(d)
    } else {
        Err(ExecError::FnNotFound)
    }
}

impl MasterStateMachine {
    pub fn new(service_id: u64) -> MasterStateMachine {
        let mut msm = MasterStateMachine {
            subs: HashMap::new(),
            configs: Configures::new(service_id),
        };
        msm
    }

    pub fn register(&mut self, smc: SubStateMachine) -> RegisterResult {
        let id = smc.id();
        if id < 2 {
            return RegisterResult::RESERVED;
        }
        if self.subs.contains_key(&id) {
            return RegisterResult::EXISTED;
        };
        self.subs.insert(id, smc);
        RegisterResult::OK
    }

    pub fn members(&self) -> &HashMap<u64, RaftMember> {
        &self.configs.members
    }

    pub fn commit_cmd(&mut self, entry: &LogEntry) -> ExecResult {
        match entry.sm_id {
            CONFIG_SM_ID => parse_output(self.configs.fn_dispatch_cmd(entry.fn_id, &entry.data)),
            _ => {
                if let Some(sm) = self.subs.get_mut(&entry.sm_id) {
                    parse_output(sm.as_mut().fn_dispatch_cmd(entry.fn_id, &entry.data))
                } else {
                    Err(ExecError::SmNotFound)
                }
            }
        }
    }
    pub fn exec_qry(&self, entry: &LogEntry) -> ExecResult {
        match entry.sm_id {
            CONFIG_SM_ID => parse_output(self.configs.fn_dispatch_qry(entry.fn_id, &entry.data)),
            _ => {
                if let Some(sm) = self.subs.get(&entry.sm_id) {
                    parse_output(sm.fn_dispatch_qry(entry.fn_id, &entry.data))
                } else {
                    Err(ExecError::SmNotFound)
                }
            }
        }
    }
    pub fn clear_subs(&mut self) {
        self.subs.clear()
    }
}

impl Error for ExecError {}
impl Display for ExecError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
