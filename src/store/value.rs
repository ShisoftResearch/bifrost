#[macro_export]
macro_rules! def_store_value {
    ($m: ident, $t: ty) => {
        pub mod $m {
            use bifrost_hasher::hash_str;
            use $crate::raft::state_machine::StateMachineCtl;
            use $crate::raft::state_machine::callback::server::SMCallback;
            use $crate::raft::RaftService;
            use std::sync::{Arc};
            pub struct Value {
                pub val: $t,
                pub id: u64,
                callback: Option<SMCallback>,
            }
            raft_state_machine! {
                def cmd set(v: $t);
                def qry get() -> $t;
                def sub on_changed() -> ($t, $t);
            }
            impl StateMachineCmds for Value {
                fn set(&mut self, v: $t) -> Result<(),()> {
                    if let Some(ref callback) = self.callback {
                        let old = self.val.clone();
                        callback.notify(&commands::on_changed::new(), Ok((old, v.clone())));
                    }
                    self.val = v;
                    Ok(())
                }
                fn get(&self) -> Result<$t, ()> {
                    Ok(self.val.clone())
                }
            }
            impl StateMachineCtl for Value {
                raft_sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some($crate::utils::bincode::serialize(&self.val))
                }
                fn recover(&mut self, data: Vec<u8>) {
                    self.val = $crate::utils::bincode::deserialize(&data);
                }
                fn id(&self) -> u64 {self.id}
            }
            impl Value {
                pub fn new(id: u64, val: $t) -> Value {
                    Value {
                        val: val,
                        id: id,
                        callback: None,
                    }
                }
                pub fn new_by_name(name: &String, val: $t) -> Value {
                    Value::new(hash_str(name), val)
                }
                pub fn init_callback(&mut self, raft_service: &Arc<Box<RaftService>>) {
                    self.callback = Some(SMCallback::new(self.id(), raft_service.clone()));
                }
            }
        }
    };
}

def_store_value!(string, String);