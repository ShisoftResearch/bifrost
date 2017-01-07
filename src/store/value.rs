macro_rules! def_store_value {
    ($m: ident, $t: ty) => {
        pub mod $m {
            use raft::state_machine::StateMachineCtl;
            use bifrost_hasher::hash_str;
            pub struct Value {
                pub val: $t,
                pub id: u64
            }
            raft_state_machine! {
                def cmd set(v: $t);
                def qry get() -> $t;
            }
            impl StateMachineCmds for Value {
                fn set(&mut self, v: $t) -> Result<(),()> {
                    self.val = v;
                    Ok(())
                }
                fn get(&self) -> Result<$t, ()> {
                    Ok(self.val.clone())
                }
            }
            impl StateMachineCtl for Value {
                sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some(serialize!(&self.val))
                }
                fn recover(&mut self, data: Vec<u8>) {
                    self.val = deserialize!(&data);
                }
                fn id(&self) -> u64 {self.id}
            }
            impl Value {
                pub fn new(id: u64, val: $t) -> Value {
                    Value {
                        val: val,
                        id: id,
                    }
                }
                pub fn new_by_name(name: String, val: $t) -> Value {
                    Value::new(hash_str(name), val)
                }
            }
        }
    };
}

def_store_value!(string, String);