#[macro_export]
macro_rules! def_store_hash_map {
    ($m: ident <$kt: ty, $vt: ty>) => {
        pub mod $m {
            use utils::BoxFutureResult;
            use $crate::raft::state_machine::StateMachineCtl;
            use $crate::raft::state_machine::callback::server::SMCallback;
            use $crate::raft::RaftService;
            use bifrost_hasher::hash_str;
            use std::collections::HashMap;
            use std::sync::{Arc};
            use super::*;
            pub struct Map {
                map: HashMap<$kt, $vt>,
                callback: Option<SMCallback>,
                pub id: u64
            }
            raft_state_machine! {
                def qry get(k: $kt) -> Option<$vt>;
                def cmd insert(k: $kt, v: $vt) -> Option<$vt>;
                def cmd insert_if_absent(k: $kt, v: $vt) -> $vt;
                def cmd remove(k: $kt) -> Option<$vt>;

                def qry is_empty() -> bool;
                def qry len() -> u64;
                def cmd clear();

                def qry keys() -> Vec<$kt>;
                def qry values() -> Vec<$vt>;
                def qry entries() -> Vec<($kt, $vt)>;
                def qry clone() -> HashMap<$kt, $vt>;

                def qry contains_key(k: $kt) -> bool;

                def sub on_inserted() -> ($kt, $vt);
                def sub on_key_inserted(k: $kt) -> $vt;
                def sub on_removed() -> ($kt, $vt);
                def sub on_key_removed(k: $kt) -> $vt;
            }
            impl StateMachineCmds for Map {
                fn get(&self, k: $kt) -> BoxFutureResult<Option<$vt>, ()> {
                    box future::ok(
                        if let Some(v) = self.map.get(&k) {
                            Some(v.clone())
                        } else {None}
                    )
                }
                fn insert(&mut self, k: $kt, v: $vt) -> BoxFutureResult<Option<$vt>, ()> {
                    if let Some(ref callback) = self.callback {
                        callback.notify(&commands::on_inserted::new(), Ok((k.clone(), v.clone())));
                        callback.notify(&commands::on_key_inserted::new(&k), Ok(v.clone()));
                    }
                    box future::ok(self.map.insert(k, v))
                }
                fn insert_if_absent(&mut self, k: $kt, v: $vt) -> BoxFutureResult<$vt, ()> {
                    if let Some(v) = self.map.get(&k) {
                        return Ok(v.clone())
                    }
                    box self.insert(k, v.clone()); future::ok(v)
                }
                fn remove(&mut self, k: $kt) -> BoxFutureResult<Option<$vt>, ()> {
                    let res = self.map.remove(&k);
                    if let Some(ref callback) = self.callback {
                        if let Some(ref v) = res {
                            callback.notify(&commands::on_removed::new(), Ok((k.clone(), v.clone())));
                            callback.notify(&commands::on_key_removed::new(&k), Ok(v.clone()));
                        }
                    }
                    box future::ok(res)
                }
                fn is_empty(&self) -> BoxFutureResult<bool, ()> {
                    box future::ok(self.map.is_empty())
                }
                fn len(&self) -> BoxFutureResult<u64, ()> {
                    box future::ok(self.map.len() as u64)
                }
                fn clear(&mut self) -> BoxFutureResult<(), ()> {
                    box future::ok(self.map.clear())
                }
                fn keys(&self) -> BoxFutureResult<Vec<$kt>, ()> {
                    box future::ok(self.map.keys().cloned().collect())
                }
                fn values(&self) -> BoxFutureResult<Vec<$vt>, ()> {
                    box future::ok(self.map.values().cloned().collect())
                }
                fn entries(&self) -> BoxFutureResult<Vec<($kt, $vt)>, ()> {
                    let mut r = Vec::new();
                    for (k, v) in self.map.iter() {
                        r.push((k.clone(), v.clone()));
                    }
                    box future::ok(r)
                }
                fn clone(&self) -> BoxFutureResult<HashMap<$kt, $vt>, ()> {
                    box future::ok(self.map.clone())
                }
                fn contains_key(&self, k: $kt) -> BoxFutureResult<bool, ()> {
                    box future::ok(self.map.contains_key(&k))
                }
            }
            impl StateMachineCtl for Map {
                raft_sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some($crate::utils::bincode::serialize(&self.map))
                }
                fn recover(&mut self, data: Vec<u8>) {
                    self.map = $crate::utils::bincode::deserialize(&data);
                }
                fn id(&self) -> u64 {self.id}
            }
            impl Map {
                pub fn new(id: u64) -> Map {
                    Map {
                        map: HashMap::new(),
                        callback: None,
                        id: id,
                    }
                }
                pub fn new_by_name(name: &String) -> Map {
                    Map::new(hash_str(name))
                }
                pub fn init_callback(&mut self, raft_service: &Arc<Box<RaftService>>) {
                    self.callback = Some(SMCallback::new(self.id(), raft_service.clone()));
                }
            }
        }
    };
}

def_store_hash_map!(string_u8vec_hashmap <String, Vec<u8>>);
def_store_hash_map!(string_string_hashmap <String, String>);