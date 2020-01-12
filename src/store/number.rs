#[macro_export]
macro_rules! def_store_number {
    ($m: ident, $t: ty) => {
        pub mod $m {
            use bifrost_hasher::hash_str;
            use crate::raft::state_machine::StateMachineCtl;
            use std::sync::Arc;
            use $crate::raft::state_machine::callback::server::SMCallback;
            use $crate::raft::RaftService;
            pub struct Number {
                pub num: $t,
                pub id: u64,
                callback: Option<SMCallback>,
            }
            raft_state_machine! {
                def cmd set(n: $t);
                def qry get() -> $t;

                def cmd get_and_add(n: $t) -> $t;
                def cmd add_and_get(n: $t) -> $t;

                def cmd get_and_minus(n: $t) -> $t;
                def cmd minus_and_get(n: $t) -> $t;

                def cmd get_and_incr() -> $t;
                def cmd incr_and_get() -> $t;

                def cmd get_and_decr() -> $t;
                def cmd decr_and_get() -> $t;

                def cmd get_and_multiply(n: $t) -> $t;
                def cmd multiply_and_get(n: $t) -> $t;

                def cmd get_and_divide(n: $t) -> $t;
                def cmd divide_and_get(n: $t) -> $t;

                def cmd compare_and_swap(original: $t, n: $t) -> $t;
                def cmd swap(n: $t) -> $t;

                def sub on_changed() -> ($t, $t);
            }
            impl StateMachineCmds for Number {
                fn set(&mut self, n: $t) -> Result<(), ()> {
                    let on = self.num;
                    self.num = n;
                    if let Some(ref callback) = self.callback {
                        callback.notify(commands::on_changed::new(), Ok((on, n)));
                    }
                    Ok(())
                }
                fn get(&self) -> Result<$t, ()> {
                    Ok(self.num)
                }
                fn get_and_add(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on + n);
                    Ok(on)
                }
                fn add_and_get(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on + n);
                    Ok(self.num)
                }
                fn get_and_minus(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on - n);
                    Ok(on)
                }
                fn minus_and_get(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on - n);
                    Ok(self.num)
                }
                fn get_and_incr(&mut self) -> Result<$t, ()> {
                    self.get_and_add(1 as $t)
                }
                fn incr_and_get(&mut self) -> Result<$t, ()> {
                    self.add_and_get(1 as $t)
                }
                fn get_and_decr(&mut self) -> Result<$t, ()> {
                    self.get_and_minus(1 as $t)
                }
                fn decr_and_get(&mut self) -> Result<$t, ()> {
                    self.minus_and_get(1 as $t)
                }
                fn get_and_multiply(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on * n);
                    Ok(on)
                }
                fn multiply_and_get(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on * n);
                    Ok(self.num)
                }
                fn get_and_divide(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on / n);
                    Ok(on)
                }
                fn divide_and_get(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(on / n);
                    Ok(self.num)
                }
                fn compare_and_swap(&mut self, original: $t, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    if on == original {
                        self.set(n);
                    }
                    Ok(on)
                }
                fn swap(&mut self, n: $t) -> Result<$t, ()> {
                    let on = self.num;
                    self.set(n);
                    Ok(on)
                }
            }
            impl StateMachineCtl for Number {
                raft_sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some($crate::utils::bincode::serialize(&self.num))
                }
                fn recover(&mut self, data: Vec<u8>) {
                    self.num = $crate::utils::bincode::deserialize(&data);
                }
                fn id(&self) -> u64 {
                    self.id
                }
            }
            impl Number {
                pub fn new(id: u64, val: $t) -> Number {
                    Number {
                        num: val,
                        id: id,
                        callback: None,
                    }
                }
                pub fn new_by_name(name: &String, num: $t) -> Number {
                    Number::new(hash_str(name), num)
                }
                pub fn init_callback(&mut self, raft_service: &Arc<RaftService>) {
                    self.callback = Some(SMCallback::new(self.id(), raft_service.clone()));
                }
            }
        }
    };
}

def_store_number!(I8, i8);
// def_store_number!(I16, i16);
// def_store_number!(I32, i32);
// def_store_number!(I64, i64);
// def_store_number!(U8, u8);
// def_store_number!(U16, u16);
// def_store_number!(U32, u32);
// def_store_number!(U64, u64);
// def_store_number!(F64, f64);
// def_store_number!(F32, f32);
