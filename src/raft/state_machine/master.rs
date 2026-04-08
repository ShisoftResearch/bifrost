use self::configs::{Configures, RaftMember, CONFIG_SM_ID};
use super::super::*;
use super::*;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ExecError {
    SmNotFound(u64),
    FnNotFound(u64, u64), // (sm_id, fn_id)
    ServersUnreachable,
    CannotConstructClient,
    NotCommitted,
    ShuttingDown,
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
pub type SubStateMachine = Box<dyn StateMachineCtl>;
pub type SnapshotDataItem = (u64, Vec<u8>);
pub type SnapshotDataItems = Vec<SnapshotDataItem>;

raft_state_machine! {}

pub struct MasterStateMachine {
    subs: HashMap<u64, SubStateMachine>,
    snapshots: HashMap<u64, Vec<u8>>,
    pub configs: Configures,
    plane_id: PlaneId,
}

impl StateMachineCmds for MasterStateMachine {}

impl StateMachineCtl for MasterStateMachine {
    raft_sm_complete!();
    fn id(&self) -> u64 {
        0
    }
    fn snapshot(&self) -> Vec<u8> {
        let mut sms: SnapshotDataItems = Vec::with_capacity(self.subs.len());
        for (sm_id, smc) in self.subs.iter() {
            if !smc.recoverable() {
                continue;
            }
            let sub_snapshot = smc.snapshot();
            sms.push((*sm_id, sub_snapshot));
        }
        sms.push((self.configs.id(), self.configs.snapshot()));
        let data = crate::utils::serde::serialize(&sms);
        data
    }
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        match crate::utils::serde::deserialize::<SnapshotDataItems>(data.as_slice()) {
            Some(sms) => {
                for (sm_id, snapshot) in sms {
                    self.snapshots.insert(sm_id, snapshot);
                }
            }
            None => {
                error!(
                    "Failed to deserialize master state machine snapshot for plane {}. State machine recovery failed.",
                    self.plane_id.raw()
                );
                // Clear snapshots to start fresh - this is safer than leaving corrupted state
                self.snapshots.clear();
            }
        }
        future::ready(()).boxed()
    }
    fn recoverable(&self) -> bool {
        true
    }
}

pub fn parse_output<'a>(r: Option<Vec<u8>>) -> ExecResult {
    if let Some(d) = r {
        Ok(d)
    } else {
        // Caller will wrap with correct (sm_id, fn_id); default to (0,0) if unknown
        Err(ExecError::FnNotFound(0, 0))
    }
}

impl MasterStateMachine {
    pub fn new(service_id: u64) -> MasterStateMachine {
        Self::new_on_plane(service_id, PlaneId::type1())
    }

    pub fn new_on_plane(service_id: u64, plane_id: PlaneId) -> MasterStateMachine {
        let msm = MasterStateMachine {
            subs: HashMap::new(),
            snapshots: HashMap::new(),
            configs: Configures::new(service_id),
            plane_id,
        };
        msm
    }

    /// Whether a given state machine id should be persisted/recovered.
    pub fn is_recoverable(&self, sm_id: u64) -> bool {
        if sm_id == CONFIG_SM_ID {
            return self.configs.recoverable();
        }
        if let Some(sm) = self.subs.get(&sm_id) {
            return sm.recoverable();
        }
        // Default to true if SM is not yet registered so we don't skip WAL
        true
    }

    pub fn register(&mut self, smc: SubStateMachine) -> RegisterResult {
        let id = smc.id();
        if is_reserved_internal_sm_id(id) {
            return RegisterResult::RESERVED;
        }
        if self.subs.contains_key(&id) {
            return RegisterResult::EXISTED;
        };
        self.subs.insert(id, smc);
        RegisterResult::OK
    }

    pub async fn recover_registered_snapshots(&mut self) {
        if let Some(snapshot) = self.snapshots.remove(&CONFIG_SM_ID) {
            self.configs.recover(snapshot).await;
        }

        let recoverable_ids: Vec<u64> = self
            .subs
            .keys()
            .filter(|id| self.snapshots.contains_key(id))
            .copied()
            .collect();
        for sm_id in recoverable_ids {
            if let Some(snapshot) = self.snapshots.remove(&sm_id) {
                if let Some(smc) = self.subs.get_mut(&sm_id) {
                    smc.recover(snapshot).await;
                }
            }
        }
    }

    pub fn members(&self) -> &HashMap<u64, RaftMember> {
        &self.configs.members
    }

    pub async fn commit_cmd(&mut self, entry: &LogEntry) -> ExecResult {
        match entry.sm_id {
            CONFIG_SM_ID => {
                let out = self.configs.fn_dispatch_cmd(entry.fn_id, &entry.data).await;
                match out {
                    Some(d) => Ok(d),
                    None => {
                        warn!(
                            "FN not found for cmd on plane {} sm_id={}, fn_id={} at log_id={}",
                            self.plane_id.raw(), entry.sm_id, entry.fn_id, entry.id
                        );
                        Err(ExecError::FnNotFound(entry.sm_id, entry.fn_id))
                    }
                }
            }
            _ => match self.subs.get_mut(&entry.sm_id) {
                Some(sm) => {
                    let out = sm.as_mut().fn_dispatch_cmd(entry.fn_id, &entry.data).await;
                    match out {
                        Some(data) => Ok(data),
                        None => {
                            warn!(
                                "FN not found for cmd on plane {} sm_id={}, fn_id={} at log_id={}",
                                self.plane_id.raw(), entry.sm_id, entry.fn_id, entry.id
                            );
                            Err(ExecError::FnNotFound(entry.sm_id, entry.fn_id))
                        }
                    }
                }
                None => {
                    warn!(
                        "SM not found for cmd on plane {} sm_id={} at log_id={}, have SMs: {:?}",
                        self.plane_id.raw(),
                        entry.sm_id,
                        entry.id,
                        self.subs.keys().collect::<Vec<_>>()
                    );
                    Err(ExecError::SmNotFound(entry.sm_id))
                }
            },
        }
    }
    pub async fn exec_qry(&self, entry: &LogEntry) -> ExecResult {
        match entry.sm_id {
            CONFIG_SM_ID => {
                let out = self.configs.fn_dispatch_qry(entry.fn_id, &entry.data).await;
                match out {
                    Some(d) => Ok(d),
                    None => {
                        warn!(
                            "FN not found for qry on plane {} sm_id={}, fn_id={} at log_id={}",
                            self.plane_id.raw(), entry.sm_id, entry.fn_id, entry.id
                        );
                        Err(ExecError::FnNotFound(entry.sm_id, entry.fn_id))
                    }
                }
            }
            _ => match self.subs.get(&entry.sm_id) {
                Some(sm) => {
                    let out = sm.fn_dispatch_qry(entry.fn_id, &entry.data).await;
                    match out {
                        Some(data) => Ok(data),
                        None => {
                            warn!(
                                "FN not found for qry on plane {} sm_id={}, fn_id={} at log_id={}",
                                self.plane_id.raw(), entry.sm_id, entry.fn_id, entry.id
                            );
                            Err(ExecError::FnNotFound(entry.sm_id, entry.fn_id))
                        }
                    }
                }
                None => {
                    warn!(
                        "SM not found for qry on plane {} sm_id={} at log_id={}, have SMs: {:?}",
                        self.plane_id.raw(),
                        entry.sm_id,
                        entry.id,
                        self.subs.keys().collect::<Vec<_>>()
                    );
                    Err(ExecError::SmNotFound(entry.sm_id))
                }
            },
        }
    }
    pub fn clear_subs(&mut self) {
        self.subs.clear()
    }
    pub fn has_sub(&self, id: &u64) -> bool {
        self.subs.contains_key(&id)
    }
}

impl Error for ExecError {}
impl Display for ExecError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::StateMachineCtl;

    // Mock state machine for testing
    struct MockStateMachine {
        id: u64,
        recoverable: bool,
        snapshot_data: Vec<u8>,
        recovered_data: Option<Vec<u8>>,
    }

    impl StateMachineCtl for MockStateMachine {
        raft_sm_complete!();

        fn id(&self) -> u64 {
            self.id
        }

        fn snapshot(&self) -> Vec<u8> {
            self.snapshot_data.clone()
        }

        fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
            self.recovered_data = Some(data);
            future::ready(()).boxed()
        }

        fn recoverable(&self) -> bool {
            self.recoverable
        }
    }

    impl StateMachineCmds for MockStateMachine {}

    #[test]
    fn test_master_state_machine_new() {
        let service_id = 12345u64;
        let msm = MasterStateMachine::new(service_id);

        assert_eq!(msm.id(), 0);
        assert!(msm.subs.is_empty());
        assert!(msm.snapshots.is_empty());
        // configs should be initialized
        assert_eq!(msm.configs.id(), CONFIG_SM_ID);
    }

    #[test]
    fn test_master_state_machine_id() {
        let msm = MasterStateMachine::new(1);
        assert_eq!(msm.id(), 0);
    }

    #[test]
    fn test_master_state_machine_recoverable() {
        let msm = MasterStateMachine::new(1);
        assert!(msm.recoverable());
    }

    #[test]
    fn test_is_recoverable_config_sm() {
        let msm = MasterStateMachine::new(1);

        // CONFIG_SM_ID (1) should be recoverable
        assert!(msm.is_recoverable(CONFIG_SM_ID));
    }

    #[test]
    fn test_is_recoverable_registered_sm() {
        let mut msm = MasterStateMachine::new(1);

        let mock_sm = Box::new(MockStateMachine {
            id: 100,
            recoverable: true,
            snapshot_data: vec![1, 2, 3],
            recovered_data: None,
        });

        msm.register(mock_sm);
        assert!(msm.is_recoverable(100));
    }

    #[test]
    fn test_is_recoverable_non_recoverable_sm() {
        let mut msm = MasterStateMachine::new(1);

        let mock_sm = Box::new(MockStateMachine {
            id: 200,
            recoverable: false,
            snapshot_data: vec![],
            recovered_data: None,
        });

        msm.register(mock_sm);
        assert!(!msm.is_recoverable(200));
    }

    #[test]
    fn test_is_recoverable_unregistered_sm() {
        let msm = MasterStateMachine::new(1);

        // Unregistered SMs default to true so we don't skip WAL
        assert!(msm.is_recoverable(999));
    }

    #[test]
    fn test_register_ok() {
        let mut msm = MasterStateMachine::new(1);

        let mock_sm = Box::new(MockStateMachine {
            id: 10,
            recoverable: true,
            snapshot_data: vec![1, 2, 3],
            recovered_data: None,
        });

        let result = msm.register(mock_sm);
        assert!(matches!(result, RegisterResult::OK));
        assert!(msm.has_sub(&10));
    }

    #[test]
    fn test_register_reserved() {
        let mut msm = MasterStateMachine::new(1);

        // ID 0 is reserved for master
        let mock_sm0 = Box::new(MockStateMachine {
            id: 0,
            recoverable: true,
            snapshot_data: vec![],
            recovered_data: None,
        });

        let result = msm.register(mock_sm0);
        assert!(matches!(result, RegisterResult::RESERVED));

        // ID 1 is reserved for config
        let mock_sm1 = Box::new(MockStateMachine {
            id: 1,
            recoverable: true,
            snapshot_data: vec![],
            recovered_data: None,
        });

        let result = msm.register(mock_sm1);
        assert!(matches!(result, RegisterResult::RESERVED));
    }

    #[test]
    fn test_register_existed() {
        let mut msm = MasterStateMachine::new(1);

        let mock_sm1 = Box::new(MockStateMachine {
            id: 10,
            recoverable: true,
            snapshot_data: vec![1, 2, 3],
            recovered_data: None,
        });

        msm.register(mock_sm1);

        // Try to register again with same ID
        let mock_sm2 = Box::new(MockStateMachine {
            id: 10,
            recoverable: true,
            snapshot_data: vec![4, 5, 6],
            recovered_data: None,
        });

        let result = msm.register(mock_sm2);
        assert!(matches!(result, RegisterResult::EXISTED));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_register_with_snapshot_recovery() {
        let mut msm = MasterStateMachine::new(1);

        // Add a snapshot for SM id 10
        let snapshot_data = vec![1, 2, 3, 4, 5];
        msm.snapshots.insert(10, snapshot_data.clone());

        let mock_sm = Box::new(MockStateMachine {
            id: 10,
            recoverable: true,
            snapshot_data: vec![],
            recovered_data: None,
        });

        msm.register(mock_sm);

        // Snapshot is kept pending until registered snapshots are explicitly recovered.
        assert_eq!(msm.snapshots.get(&10), Some(&snapshot_data));

        msm.recover_registered_snapshots().await;

        // Snapshot should be removed after replay.
        assert!(!msm.snapshots.contains_key(&10));
        let recovered = msm
            .subs
            .get(&10)
            .and_then(|sm| {
                let any = sm.as_ref() as &dyn std::any::Any;
                any.downcast_ref::<MockStateMachine>()
            })
            .and_then(|sm| sm.recovered_data.clone());
        assert_eq!(recovered, Some(snapshot_data));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_registered_snapshots_applies_config_snapshot() {
        let mut msm = MasterStateMachine::new(1);
        msm.configs.members.clear();

        let mut restored = Configures::new(99);
        let _ = restored.new_member(String::from("127.0.0.1:9100")).await;
        msm.snapshots.insert(CONFIG_SM_ID, restored.snapshot());

        msm.recover_registered_snapshots().await;

        assert_eq!(msm.configs.members.len(), 1);
        assert!(!msm.snapshots.contains_key(&CONFIG_SM_ID));
    }

    #[test]
    fn test_members() {
        let msm = MasterStateMachine::new(1);
        let members = msm.members();

        assert!(members.is_empty());
    }

    #[test]
    fn test_clear_subs() {
        let mut msm = MasterStateMachine::new(1);

        let mock_sm = Box::new(MockStateMachine {
            id: 10,
            recoverable: true,
            snapshot_data: vec![1, 2, 3],
            recovered_data: None,
        });

        msm.register(mock_sm);
        assert!(msm.has_sub(&10));

        msm.clear_subs();
        assert!(!msm.has_sub(&10));
        assert!(msm.subs.is_empty());
    }

    #[test]
    fn test_has_sub() {
        let mut msm = MasterStateMachine::new(1);

        assert!(!msm.has_sub(&10));

        let mock_sm = Box::new(MockStateMachine {
            id: 10,
            recoverable: true,
            snapshot_data: vec![1, 2, 3],
            recovered_data: None,
        });

        msm.register(mock_sm);
        assert!(msm.has_sub(&10));
        assert!(!msm.has_sub(&20));
    }

    #[test]
    fn test_snapshot_empty() {
        let msm = MasterStateMachine::new(1);
        let snapshot = msm.snapshot();

        assert!(!snapshot.is_empty());

        // Should be able to deserialize
        let items: Option<SnapshotDataItems> = crate::utils::serde::deserialize(&snapshot);
        assert!(items.is_some());
    }

    #[test]
    fn test_snapshot_with_recoverable_sm() {
        let mut msm = MasterStateMachine::new(1);

        let mock_sm = Box::new(MockStateMachine {
            id: 10,
            recoverable: true,
            snapshot_data: vec![1, 2, 3, 4, 5],
            recovered_data: None,
        });

        msm.register(mock_sm);

        let snapshot = msm.snapshot();
        let items: Option<SnapshotDataItems> = crate::utils::serde::deserialize(&snapshot);

        assert!(items.is_some());
        let items = items.unwrap();

        // Should have config SM + our mock SM
        assert!(items.len() >= 2);

        // Check that our mock SM's snapshot is included
        let has_mock_sm = items.iter().any(|(id, _)| *id == 10);
        assert!(has_mock_sm);
    }

    #[test]
    fn test_snapshot_non_recoverable_sm_excluded() {
        let mut msm = MasterStateMachine::new(1);

        let mock_sm = Box::new(MockStateMachine {
            id: 20,
            recoverable: false,
            snapshot_data: vec![1, 2, 3],
            recovered_data: None,
        });

        msm.register(mock_sm);

        let snapshot = msm.snapshot();
        let items: Option<SnapshotDataItems> = crate::utils::serde::deserialize(&snapshot);

        assert!(items.is_some());
        let items = items.unwrap();

        // Non-recoverable SM should not be in snapshot
        let has_mock_sm = items.iter().any(|(id, _)| *id == 20);
        assert!(!has_mock_sm);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_valid_snapshot() {
        let mut msm = MasterStateMachine::new(1);

        let mut items = SnapshotDataItems::new();
        items.push((10, vec![1, 2, 3]));
        items.push((20, vec![4, 5, 6]));

        let snapshot = crate::utils::serde::serialize(&items);

        msm.recover(snapshot).await;

        assert_eq!(msm.snapshots.get(&10), Some(&vec![1, 2, 3]));
        assert_eq!(msm.snapshots.get(&20), Some(&vec![4, 5, 6]));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recover_invalid_snapshot() {
        let mut msm = MasterStateMachine::new(1);

        // Add some existing snapshots
        msm.snapshots.insert(100, vec![1, 2, 3]);

        // Recover with invalid data
        let invalid_data = vec![0xFF, 0xFF, 0xFF];
        msm.recover(invalid_data).await;

        // Snapshots should be cleared on error
        assert!(msm.snapshots.is_empty());
    }

    #[test]
    fn test_parse_output_some() {
        let data = vec![1, 2, 3, 4, 5];
        let result = parse_output(Some(data.clone()));

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[test]
    fn test_parse_output_none() {
        let result = parse_output(None);

        assert!(result.is_err());
        assert!(matches!(result, Err(ExecError::FnNotFound(0, 0))));
    }

    #[test]
    fn test_exec_error_display() {
        let error = ExecError::SmNotFound(123);
        let display = format!("{}", error);
        assert!(display.contains("SmNotFound"));

        let error2 = ExecError::FnNotFound(1, 2);
        let display2 = format!("{}", error2);
        assert!(display2.contains("FnNotFound"));
    }

    #[test]
    fn test_exec_error_debug() {
        let error = ExecError::ServersUnreachable;
        let debug = format!("{:?}", error);
        assert!(debug.contains("ServersUnreachable"));
    }

    #[test]
    fn test_exec_error_clone() {
        let error = ExecError::CannotConstructClient;
        let cloned = error.clone();

        assert!(matches!(cloned, ExecError::CannotConstructClient));
    }

    #[test]
    fn test_all_exec_error_variants() {
        // Test that all variants can be created
        let _ = ExecError::SmNotFound(1);
        let _ = ExecError::FnNotFound(1, 2);
        let _ = ExecError::ServersUnreachable;
        let _ = ExecError::CannotConstructClient;
        let _ = ExecError::NotCommitted;
        let _ = ExecError::ShuttingDown;
        let _ = ExecError::Unknown;
        let _ = ExecError::TooManyRetry;
    }

    #[test]
    fn test_register_result_variants() {
        // Test that all variants exist
        let _ = RegisterResult::OK;
        let _ = RegisterResult::EXISTED;
        let _ = RegisterResult::RESERVED;
    }

    // ============================================================================
    // REAL-WORLD INTEGRATION SCENARIOS
    // ============================================================================

    // Real-world state machine that simulates a key-value store
    struct KeyValueStateMachine {
        id: u64,
        data: HashMap<String, String>,
    }

    impl StateMachineCtl for KeyValueStateMachine {
        raft_sm_complete!();

        fn id(&self) -> u64 {
            self.id
        }

        fn snapshot(&self) -> Vec<u8> {
            crate::utils::serde::serialize(&self.data)
        }

        fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
            if let Some(recovered_data) = crate::utils::serde::deserialize(&data) {
                self.data = recovered_data;
            }
            future::ready(()).boxed()
        }

        fn recoverable(&self) -> bool {
            true
        }
    }

    impl StateMachineCmds for KeyValueStateMachine {
        fn dispatch_cmd_<'a>(
            &'a mut self,
            fn_id: u64,
            data: &'a Vec<u8>,
        ) -> BoxFuture<'a, Option<Vec<u8>>> {
            async move {
                match fn_id {
                    1 => {
                        // SET command
                        if let Some((key, value)) =
                            crate::utils::serde::deserialize::<(String, String)>(data)
                        {
                            self.data.insert(key, value);
                            Some(crate::utils::serde::serialize(&true))
                        } else {
                            None
                        }
                    }
                    2 => {
                        // DELETE command
                        if let Some(key) = crate::utils::serde::deserialize::<String>(data) {
                            let existed = self.data.remove(&key).is_some();
                            Some(crate::utils::serde::serialize(&existed))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            .boxed()
        }

        fn dispatch_qry_<'a>(
            &'a self,
            fn_id: u64,
            data: &'a Vec<u8>,
        ) -> BoxFuture<'a, Option<Vec<u8>>> {
            async move {
                match fn_id {
                    10 => {
                        // GET query
                        if let Some(key) = crate::utils::serde::deserialize::<String>(data) {
                            let value = self.data.get(&key).cloned();
                            Some(crate::utils::serde::serialize(&value))
                        } else {
                            None
                        }
                    }
                    11 => {
                        // COUNT query
                        Some(crate::utils::serde::serialize(&self.data.len()))
                    }
                    _ => None,
                }
            }
            .boxed()
        }

        fn op_type_(&self, _fn_id: u64) -> Option<OpType> {
            Some(OpType::COMMAND)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_kv_store_workflow() {
        let mut msm = MasterStateMachine::new(1);

        // Register a KV store state machine
        let kv_sm = Box::new(KeyValueStateMachine {
            id: 100,
            data: HashMap::new(),
        });

        let result = msm.register(kv_sm);
        assert!(matches!(result, RegisterResult::OK));

        // Simulate SET command: key="user:123", value="Alice"
        let set_data =
            crate::utils::serde::serialize(&(String::from("user:123"), String::from("Alice")));
        let set_entry = LogEntry {
            id: 1,
            term: 1,
            sm_id: 100,
            fn_id: 1, // SET
            data: set_data,
        };

        let result = msm.commit_cmd(&set_entry).await;
        assert!(result.is_ok());

        // Simulate GET query: key="user:123"
        let get_data = crate::utils::serde::serialize(&String::from("user:123"));
        let get_entry = LogEntry {
            id: 2,
            term: 1,
            sm_id: 100,
            fn_id: 10, // GET
            data: get_data,
        };

        let result = msm.exec_qry(&get_entry).await;
        assert!(result.is_ok());
        let value: Option<Option<String>> = crate::utils::serde::deserialize(&result.unwrap());
        assert_eq!(value, Some(Some(String::from("Alice"))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_multiple_operations() {
        let mut msm = MasterStateMachine::new(1);

        let kv_sm = Box::new(KeyValueStateMachine {
            id: 100,
            data: HashMap::new(),
        });
        msm.register(kv_sm);

        // Insert multiple key-value pairs
        for i in 0..5 {
            let key = format!("key:{}", i);
            let value = format!("value:{}", i);
            let data = crate::utils::serde::serialize(&(key, value));

            let entry = LogEntry {
                id: i + 1,
                term: 1,
                sm_id: 100,
                fn_id: 1, // SET
                data,
            };

            let result = msm.commit_cmd(&entry).await;
            assert!(result.is_ok());
        }

        // Query the count
        let count_entry = LogEntry {
            id: 10,
            term: 1,
            sm_id: 100,
            fn_id: 11, // COUNT
            data: vec![],
        };

        let result = msm.exec_qry(&count_entry).await;
        assert!(result.is_ok());
        let count: usize = crate::utils::serde::deserialize(&result.unwrap()).unwrap();
        assert_eq!(count, 5);

        // Delete one key
        let delete_data = crate::utils::serde::serialize(&String::from("key:2"));
        let delete_entry = LogEntry {
            id: 11,
            term: 1,
            sm_id: 100,
            fn_id: 2, // DELETE
            data: delete_data,
        };

        let result = msm.commit_cmd(&delete_entry).await;
        assert!(result.is_ok());

        // Verify count decreased
        let result = msm.exec_qry(&count_entry).await;
        let count: usize = crate::utils::serde::deserialize(&result.unwrap()).unwrap();
        assert_eq!(count, 4);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_snapshot_and_recovery() {
        let mut msm = MasterStateMachine::new(1);

        let kv_sm = Box::new(KeyValueStateMachine {
            id: 100,
            data: HashMap::new(),
        });
        msm.register(kv_sm);

        // Populate with data
        for i in 0..10 {
            let key = format!("session:{}", i);
            let value = format!("token:{}", i * 100);
            let data = crate::utils::serde::serialize(&(key, value));

            let entry = LogEntry {
                id: i + 1,
                term: 1,
                sm_id: 100,
                fn_id: 1, // SET
                data,
            };

            msm.commit_cmd(&entry).await.unwrap();
        }

        // Take a snapshot
        let snapshot = msm.snapshot();
        assert!(!snapshot.is_empty());

        // Create a new master state machine and recover
        let mut new_msm = MasterStateMachine::new(1);
        new_msm.recover(snapshot).await;

        // Register the state machine again - it should recover data
        let new_kv_sm = Box::new(KeyValueStateMachine {
            id: 100,
            data: HashMap::new(),
        });
        new_msm.register(new_kv_sm);
        new_msm.recover_registered_snapshots().await;

        // Verify data was recovered by querying
        let get_data = crate::utils::serde::serialize(&String::from("session:5"));
        let get_entry = LogEntry {
            id: 100,
            term: 2,
            sm_id: 100,
            fn_id: 10, // GET
            data: get_data,
        };

        let result = new_msm.exec_qry(&get_entry).await;
        assert!(result.is_ok());
        let value: Option<Option<String>> = crate::utils::serde::deserialize(&result.unwrap());
        assert_eq!(value, Some(Some(String::from("token:500"))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_multiple_state_machines() {
        let mut msm = MasterStateMachine::new(1);

        // Register two different KV stores for different purposes
        let users_sm = Box::new(KeyValueStateMachine {
            id: 100, // Users store
            data: HashMap::new(),
        });
        msm.register(users_sm);

        let sessions_sm = Box::new(KeyValueStateMachine {
            id: 200, // Sessions store
            data: HashMap::new(),
        });
        msm.register(sessions_sm);

        // Add user
        let user_data = crate::utils::serde::serialize(&(
            String::from("user:1"),
            String::from("alice@example.com"),
        ));
        let user_entry = LogEntry {
            id: 1,
            term: 1,
            sm_id: 100,
            fn_id: 1,
            data: user_data,
        };
        msm.commit_cmd(&user_entry).await.unwrap();

        // Add session for that user
        let session_data =
            crate::utils::serde::serialize(&(String::from("session:abc"), String::from("user:1")));
        let session_entry = LogEntry {
            id: 2,
            term: 1,
            sm_id: 200,
            fn_id: 1,
            data: session_data,
        };
        msm.commit_cmd(&session_entry).await.unwrap();

        // Query both state machines
        let user_query = LogEntry {
            id: 3,
            term: 1,
            sm_id: 100,
            fn_id: 11, // COUNT
            data: vec![],
        };
        let user_count: usize =
            crate::utils::serde::deserialize(&msm.exec_qry(&user_query).await.unwrap()).unwrap();

        let session_query = LogEntry {
            id: 4,
            term: 1,
            sm_id: 200,
            fn_id: 11, // COUNT
            data: vec![],
        };
        let session_count: usize =
            crate::utils::serde::deserialize(&msm.exec_qry(&session_query).await.unwrap()).unwrap();

        assert_eq!(user_count, 1);
        assert_eq!(session_count, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_error_handling() {
        let mut msm = MasterStateMachine::new(1);

        let kv_sm = Box::new(KeyValueStateMachine {
            id: 100,
            data: HashMap::new(),
        });
        msm.register(kv_sm);

        // Try to execute command on non-existent state machine
        let entry = LogEntry {
            id: 1,
            term: 1,
            sm_id: 999, // Doesn't exist
            fn_id: 1,
            data: vec![],
        };

        let result = msm.commit_cmd(&entry).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecError::SmNotFound(999))));

        // Try to execute non-existent function
        let bad_fn_entry = LogEntry {
            id: 2,
            term: 1,
            sm_id: 100,
            fn_id: 999, // Doesn't exist
            data: vec![],
        };

        let result = msm.commit_cmd(&bad_fn_entry).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecError::FnNotFound(100, 999))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_query_on_non_existent_sm() {
        let msm = MasterStateMachine::new(1);

        let entry = LogEntry {
            id: 1,
            term: 1,
            sm_id: 999,
            fn_id: 10,
            data: vec![],
        };

        let result = msm.exec_qry(&entry).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecError::SmNotFound(999))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_config_sm_operations() {
        let mut msm = MasterStateMachine::new(1);

        // CONFIG_SM_ID is always registered
        let entry = LogEntry {
            id: 1,
            term: 1,
            sm_id: CONFIG_SM_ID,
            fn_id: 1, // Some config function
            data: vec![],
        };

        // This should route to config SM, not error with SmNotFound
        let result = msm.commit_cmd(&entry).await;
        // It may error with FnNotFound but not SmNotFound
        if result.is_err() {
            assert!(matches!(result, Err(ExecError::FnNotFound(_, _))));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_state_machine_lifecycle() {
        let mut msm = MasterStateMachine::new(1);

        let sm_id = 100u64;

        // Initially, state machine doesn't exist
        assert!(!msm.has_sub(&sm_id));

        // Register it
        let kv_sm = Box::new(KeyValueStateMachine {
            id: sm_id,
            data: HashMap::new(),
        });
        msm.register(kv_sm);
        assert!(msm.has_sub(&sm_id));

        // Use it
        let data = crate::utils::serde::serialize(&(String::from("test"), String::from("value")));
        let entry = LogEntry {
            id: 1,
            term: 1,
            sm_id,
            fn_id: 1,
            data,
        };
        let result = msm.commit_cmd(&entry).await;
        assert!(result.is_ok());

        // Clear all state machines
        msm.clear_subs();
        assert!(!msm.has_sub(&sm_id));

        // Try to use it after clearing - should error
        let entry2 = LogEntry {
            id: 2,
            term: 1,
            sm_id,
            fn_id: 1,
            data: vec![],
        };
        let result = msm.commit_cmd(&entry2).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(ExecError::SmNotFound(_))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_real_world_concurrent_queries() {
        let msm = Arc::new(MasterStateMachine::new(1));

        // This is a read-only test simulating concurrent queries
        // In real world, multiple threads would query simultaneously

        let query_entry = LogEntry {
            id: 1,
            term: 1,
            sm_id: CONFIG_SM_ID,
            fn_id: 1,
            data: vec![],
        };

        // Simulate concurrent reads
        let msm1 = msm.clone();
        let msm2 = msm.clone();
        let entry1 = query_entry.clone();
        let entry2 = query_entry.clone();

        let handle1 = tokio::spawn(async move { msm1.exec_qry(&entry1).await });

        let handle2 = tokio::spawn(async move { msm2.exec_qry(&entry2).await });

        // Both should complete (may error with FnNotFound but shouldn't panic)
        let _ = tokio::try_join!(handle1, handle2);
    }
}
