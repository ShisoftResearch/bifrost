#[macro_export]
macro_rules! def_store_hash_map {
    ($m: ident <$kt: ty, $vt: ty>) => {
        pub mod $m {
            use bifrost_hasher::hash_str;
            use futures::FutureExt;
            use std::collections::HashMap;
            use std::sync::Arc;
            use $crate::raft::state_machine::callback::server::SMCallback;
            use $crate::raft::state_machine::StateMachineCtl;
            use $crate::raft::RaftService;
            pub struct Map {
                map: HashMap<$kt, $vt>,
                callback: Option<SMCallback>,
                pub id: u64,
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
                fn get(&self, k: $kt) -> ::futures::future::BoxFuture<Option<$vt>> {
                    let value = if let Some(v) = self.map.get(&k) {
                        Some(v.clone())
                    } else {
                        None
                    };
                    future::ready(value).boxed()
                }
                fn insert(&mut self, k: $kt, v: $vt) -> ::futures::future::BoxFuture<Option<$vt>> {
                    async move {
                        if let Some(ref callback) = self.callback {
                            let _ = callback
                                .notify(commands::on_inserted::new(), (k.clone(), v.clone()))
                                .await;
                            let _ = callback
                                .notify(commands::on_key_inserted::new(&k), v.clone())
                                .await;
                        }
                        self.insert_to_map(k, v)
                    }
                    .boxed()
                }
                fn insert_if_absent(
                    &mut self,
                    k: $kt,
                    v: $vt,
                ) -> ::futures::future::BoxFuture<$vt> {
                    if let Some(v) = self.map.get(&k) {
                        return future::ready(v.clone()).boxed();
                    }
                    self.insert_to_map(k, v.clone());
                    future::ready(v).boxed()
                }
                fn remove(&mut self, k: $kt) -> ::futures::future::BoxFuture<Option<$vt>> {
                    async move {
                        let res = self.map.remove(&k);
                        if let Some(ref callback) = self.callback {
                            if let Some(ref v) = res {
                                let _ = callback
                                    .notify(commands::on_removed::new(), (k.clone(), v.clone()))
                                    .await;
                                let _ = callback
                                    .notify(commands::on_key_removed::new(&k), v.clone())
                                    .await;
                            }
                        }
                        res
                    }
                    .boxed()
                }
                fn is_empty(&self) -> ::futures::future::BoxFuture<bool> {
                    future::ready(self.map.is_empty()).boxed()
                }
                fn len(&self) -> ::futures::future::BoxFuture<u64> {
                    future::ready(self.map.len() as u64).boxed()
                }
                fn clear(&mut self) -> ::futures::future::BoxFuture<()> {
                    future::ready(self.map.clear()).boxed()
                }
                fn keys(&self) -> ::futures::future::BoxFuture<Vec<$kt>> {
                    future::ready(self.map.keys().cloned().collect()).boxed()
                }
                fn values(&self) -> ::futures::future::BoxFuture<Vec<$vt>> {
                    future::ready(self.map.values().cloned().collect()).boxed()
                }
                fn entries(&self) -> ::futures::future::BoxFuture<Vec<($kt, $vt)>> {
                    let mut r = Vec::new();
                    for (k, v) in self.map.iter() {
                        r.push((k.clone(), v.clone()));
                    }
                    future::ready(r).boxed()
                }
                fn clone(&self) -> ::futures::future::BoxFuture<HashMap<$kt, $vt>> {
                    future::ready(self.map.clone()).boxed()
                }
                fn contains_key(&self, k: $kt) -> ::futures::future::BoxFuture<bool> {
                    future::ready(self.map.contains_key(&k)).boxed()
                }
            }
            impl StateMachineCtl for Map {
                raft_sm_complete!();
                fn snapshot(&self) -> Option<Vec<u8>> {
                    Some($crate::utils::serde::serialize(&self.map))
                }
                fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
                    self.map = $crate::utils::serde::deserialize(&data).unwrap();
                    future::ready(()).boxed()
                }
                fn id(&self) -> u64 {
                    self.id
                }
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
                pub async fn init_callback(&mut self, raft_service: &Arc<RaftService>) {
                    self.callback = Some(SMCallback::new(self.id(), raft_service.clone()).await);
                }
                pub fn insert_to_map(&mut self, k: $kt, v: $vt) -> Option<$vt> {
                    self.map.insert(k, v)
                }
            }
        }
    };
}

def_store_hash_map!(string_u8vec_hashmap <String, Vec<u8>>);

#[cfg(test)]
mod test {
    use crate::raft::client::RaftClient;
    use crate::raft::{Options, RaftService, Storage, DEFAULT_SERVICE_ID};
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use futures::prelude::*;
    use std::collections::{HashMap, HashSet};
    use std::iter::FromIterator;
    use string_string_hashmap::client::SMClient;

    def_store_hash_map!(string_string_hashmap <String, String>);

    #[tokio::test(threaded_scheduler)]
    async fn hash_map() {
        let _ = env_logger::try_init();
        let addr = String::from("127.0.0.1:2013");
        let mut map_sm = string_string_hashmap::Map::new_by_name(&String::from("test"));
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: DEFAULT_SERVICE_ID,
        });
        let server = Server::new(&addr);
        server
            .register_service(DEFAULT_SERVICE_ID, &raft_service)
            .await;
        Server::listen_and_resume(&server).await;
        let sm_id = map_sm.id;
        map_sm.init_callback(&raft_service).await;
        assert!(RaftService::start(&raft_service).await);
        raft_service.register_state_machine(Box::new(map_sm)).await;
        raft_service.bootstrap().await;

        let raft_client = RaftClient::new(&vec![addr], DEFAULT_SERVICE_ID)
            .await
            .unwrap();
        let sm_client = SMClient::new(sm_id, &raft_client);
        RaftClient::prepare_subscription(&server).await;

        let sk1 = String::from("k1");
        let sk2 = String::from("k2");
        let sk3 = String::from("k3");
        let sk4 = String::from("k4");

        let sv1 = String::from("v1");
        let sv2 = String::from("v2");
        let sv3 = String::from("v3");
        let sv4 = String::from("v4");

        let mut inserted_stash = HashMap::new();
        inserted_stash.insert(sk1.clone(), sv1.clone());
        inserted_stash.insert(sk2.clone(), sv2.clone());
        inserted_stash.insert(sk3.clone(), sv3.clone());
        inserted_stash.insert(sk4.clone(), sv4.clone());

        let mut removed_stash = HashMap::new();
        removed_stash.insert(sk2.clone(), sv2.clone());

        sm_client.on_inserted(move |res| {
            let (key, value) = res;
            info!("GOT INSERT CALLBACK {:?} -> {:?}", key, value);
            assert_eq!(inserted_stash.get(&key).unwrap(), &value);
            future::ready(()).boxed()
        });
        sm_client.on_removed(move |res| {
            let (key, value) = res;
            info!("GOT REMOVED CALLBACK {:?} -> {:?}", key, value);
            assert_eq!(removed_stash.get(&key).unwrap(), &value);
            future::ready(()).boxed()
        });
        sm_client.on_key_inserted(
            |res| {
                info!("GOT K1 CALLBACK {:?}", res);
                assert_eq!(&String::from("v1"), &res);
                future::ready(()).boxed()
            },
            &sk1,
        );
        sm_client.on_key_removed(
            |res| {
                info!("GOT K2 CALLBACK {:?}", res);
                assert_eq!(&String::from("v2"), &res);
                future::ready(()).boxed()
            },
            &sk2,
        );
        assert!(sm_client.is_empty().await.unwrap());
        sm_client.insert(&sk1, &sv1).await.unwrap();
        sm_client.insert(&sk2, &sv2).await.unwrap();
        assert!(!sm_client.is_empty().await.unwrap());

        assert_eq!(sm_client.len().await.unwrap(), 2);
        assert_eq!(sm_client.get(&sk1).await.unwrap().unwrap(), sv1);
        assert_eq!(sm_client.get(&sk2).await.unwrap().unwrap(), sv2);

        sm_client
            .insert_if_absent(&sk2, &String::from("kv2"))
            .await
            .unwrap();
        assert_eq!(sm_client.len().await.unwrap(), 2);
        assert_eq!(sm_client.get(&sk2).await.unwrap().unwrap(), sv2);

        assert_eq!(sm_client.remove(&sk2).await.unwrap().unwrap(), sv2);
        assert_eq!(sm_client.len().await.unwrap(), 1);
        assert!(sm_client.get(&sk1).await.unwrap().is_some());
        assert!(sm_client.get(&sk2).await.unwrap().is_none());

        sm_client.clear().await.unwrap();
        assert_eq!(sm_client.len().await.unwrap(), 0);

        assert!(sm_client.insert(&sk1, &sv1).await.unwrap().is_none());
        assert!(sm_client.insert(&sk2, &sv2).await.unwrap().is_none());
        assert!(sm_client.insert(&sk3, &sv3).await.unwrap().is_none());
        assert!(sm_client.insert(&sk4, &sv4).await.unwrap().is_none());
        assert_eq!(sm_client.len().await.unwrap(), 4);

        let remote_keys = sm_client.keys().await.unwrap();
        let remote_keys_set = HashSet::<String>::from_iter(remote_keys.iter().cloned());
        assert_eq!(remote_keys_set.len(), 4);

        let remote_values = sm_client.values().await.unwrap();
        let remote_values_set = HashSet::<String>::from_iter(remote_values.iter().cloned());
        assert_eq!(remote_values_set.len(), 4);

        let remote_entries = sm_client.entries().await.unwrap();
        let remote_entries_set =
            HashSet::<(String, String)>::from_iter(remote_entries.iter().cloned());
        assert_eq!(remote_entries_set.len(), 4);

        let expected_keys: HashSet<_> = [sk1.clone(), sk2.clone(), sk3.clone(), sk4.clone()]
            .iter()
            .cloned()
            .collect();
        let expected_values: HashSet<_> = [sv1.clone(), sv2.clone(), sv3.clone(), sv4.clone()]
            .iter()
            .cloned()
            .collect();
        let expected_entries: HashSet<_> = [
            (sk1.clone(), sv1.clone()),
            (sk2.clone(), sv2.clone()),
            (sk3.clone(), sv3.clone()),
            (sk4.clone(), sv4.clone()),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(remote_keys_set.intersection(&expected_keys).count(), 4);
        assert_eq!(remote_values_set.intersection(&expected_values).count(), 4);
        assert_eq!(
            remote_entries_set.intersection(&expected_entries).count(),
            4
        );

        let mut expected_hashmap = HashMap::new();
        expected_hashmap.insert(sk1.clone(), sv1.clone());
        expected_hashmap.insert(sk2.clone(), sv2.clone());
        expected_hashmap.insert(sk3.clone(), sv3.clone());
        expected_hashmap.insert(sk4.clone(), sv4.clone());
        assert_eq!(expected_hashmap, sm_client.clone().await.unwrap());

        assert!(sm_client.contains_key(&sk1).await.unwrap());
        assert!(sm_client.contains_key(&sk2).await.unwrap());
        assert!(sm_client.contains_key(&sk3).await.unwrap());
        assert!(sm_client.contains_key(&sk4).await.unwrap());

        async_wait_secs().await;
    }
}
