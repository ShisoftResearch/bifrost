#[macro_export]
macro_rules! def_store_hash_map {
    ($m: ident <$kt: ty, $vt: ty>) => {
        pub mod $m {
            use raft::state_machine::StateMachineCtl;
            use bifrost_hasher::hash_str;
            use std::collections::HashMap;
            pub struct Map {
                map: HashMap<$kt, $vt>,
                id: u64
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
            }
            impl StateMachineCmds for Map {
                fn get(&self, k: $kt) -> Result<Option<$vt>, ()> {
                    Ok(
                        if let Some(v) = self.map.get(&k) {
                            Some(v.clone())
                        } else {None}
                    )
                }
                fn insert(&mut self, k: $kt, v: $vt) -> Result<Option<$vt>, ()> {
                    Ok(self.map.insert(k, v))
                }
                fn insert_if_absent(&mut self, k: $kt, v: $vt) -> Result<$vt, ()> {
                    Ok(self.map.entry(k).or_insert(v).clone())
                }
                fn remove(&mut self, k: $kt) -> Result<Option<$vt>, ()> {
                    Ok(self.map.remove(&k))
                }
                fn is_empty(&self) -> Result<bool, ()> {
                    Ok(self.map.is_empty())
                }
                fn len(&self) -> Result<u64, ()> {
                    Ok(self.map.len() as u64)
                }
                fn clear(&mut self) -> Result<(), ()> {
                    Ok(self.map.clear())
                }
                fn keys(&self) -> Result<Vec<$kt>, ()> {
                    Ok(self.map.keys().cloned().collect())
                }
                fn values(&self) -> Result<Vec<$vt>, ()> {
                    Ok(self.map.values().cloned().collect())
                }
                fn entries(&self) -> Result<Vec<($kt, $vt)>, ()> {
                    let mut r = Vec::new();
                    for (k, v) in self.map.iter() {
                        r.push((k.clone(), v.clone()));
                    }
                    Ok(r)
                }
                fn clone(&self) -> Result<HashMap<$kt, $vt>, ()> {
                    Ok(self.map.clone())
                }
                fn contains_key(&self, k: $kt) -> Result<bool, ()> {
                    Ok(self.map.contains_key(&k))
                }
            }
            impl StateMachineCtl for Map {
                sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some(serialize!(&self.map))
                }
                fn recover(&mut self, data: Vec<u8>) {
                    self.map = deserialize!(&data);
                }
                fn id(&self) -> u64 {self.id}
            }
        }
    };
}

def_store_hash_map!(string_u8vec_hashmap <String, Vec<u8>>);