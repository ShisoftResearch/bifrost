use super::super::*;
use super::*;
use std::collections::HashMap;
use self::configs::Configures;

#[derive(Serialize, Deserialize, Debug)]
pub enum AppendError {
    NOT_FOUND,
    NO_ENTRY,
}

pub type AppendResult = Result<Option<Vec<u8>>, AppendError>;
pub type AppendResults = Vec<AppendResult>;

raft_state_machine! {
    def cmd append(entries: Option<LogEntries>) -> AppendResults;
}

pub struct MasterStateMachine {
    subs: HashMap<u64, Box<StateMachineCtl>>
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
    fn_dispatch!();
    fn snapshot(&self) -> Option<Vec<u8>> {
        None
    }
    fn id(&self) -> u64 {0}
}

impl MasterStateMachine {
    pub fn new() -> MasterStateMachine {
        let mut msm = MasterStateMachine {
            subs: HashMap::new()
        };
        msm.register(Box::new(Configures::new()));
        msm
    }

    pub fn register(&mut self, smc: Box<StateMachineCtl>) -> bool {
        let id = smc.id();
        if self.subs.contains_key(&id) {return false};
        self.subs.insert(id, smc);
        true
    }
}
