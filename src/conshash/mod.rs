use futures::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use crate::conshash::weights::client::SMClient as WeightSMClient;
use crate::conshash::weights::DEFAULT_SERVICE_ID;
use crate::membership::client::{Member, ObserverClient as MembershipClient};
use crate::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use crate::raft::state_machine::master::ExecError;
use crate::utils::serde::serialize;
use async_std::sync::*;
use bifrost_hasher::{hash_bytes, hash_str};

pub mod weights;

#[derive(Debug)]
pub enum Action {
    Joined,
    Left,
}

#[derive(Debug)]
pub enum InitTableError {
    GroupNotExisted,
    NoWeightService(ExecError),
    NoWeightGroup,
    NoWeightInfo,
    Unknown,
}

#[derive(Debug)]
pub enum CHError {
    WatchError(Result<Result<SubscriptionReceipt, SubscriptionError>, ExecError>),
    InitTableError(InitTableError),
}

struct LookupTables {
    nodes: Vec<u64>,
    addrs: HashMap<u64, String>,
}

pub struct ConsistentHashing {
    tables: RwLock<LookupTables>,
    membership: Arc<MembershipClient>,
    weight_sm_client: WeightSMClient,
    group_name: String,
    watchers: RwLock<Vec<Box<Fn(&Member, &Action, &LookupTables, &Vec<u64>) + Send + Sync>>>,
    version: AtomicU64,
}

impl ConsistentHashing {
    pub async fn new_with_id(
        id: u64,
        group: &str,
        raft_client: &Arc<RaftClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        let membership = Arc::new(MembershipClient::new(raft_client));
        let ch = Arc::new(ConsistentHashing {
            tables: RwLock::new(LookupTables {
                nodes: Vec::new(),
                addrs: HashMap::new(),
            }),
            membership: membership.clone(),
            weight_sm_client: WeightSMClient::new(id, &raft_client),
            group_name: group.to_string(),
            watchers: RwLock::new(Vec::new()),
            version: AtomicU64::new(0),
        });
        {
            let ch = ch.clone();
            let res = membership
                .on_group_member_joined(
                    move |(member, version)| {
                        let ch = ch.clone();
                        server_joined(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        {
            let ch = ch.clone();
            let res = membership
                .on_group_member_online(
                    move |(member, version)| {
                        let ch = ch.clone();
                        server_joined(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        {
            let ch = ch.clone();
            let res = membership
                .on_group_member_left(
                    move |(member, version)| {
                        let ch = ch.clone();
                        server_left(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        {
            let ch = ch.clone();
            let res = membership
                .on_group_member_offline(
                    move |(member, version)| {
                        let ch = ch.clone();
                        server_left(ch, member, version).boxed()
                    },
                    group,
                )
                .await;
            if let Ok(Ok(_)) = res {
            } else {
                return Err(CHError::WatchError(res));
            }
        }
        Ok(ch)
    }
    pub async fn new(
        group: &str,
        raft_client: &Arc<RaftClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_with_id(DEFAULT_SERVICE_ID, group, raft_client).await
    }
    pub async fn new_client(
        group: &str,
        raft_client: &Arc<RaftClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_client_with_id(DEFAULT_SERVICE_ID, group, raft_client).await
    }
    pub async fn new_client_with_id(
        id: u64,
        group: &str,
        raft_client: &Arc<RaftClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        match ConsistentHashing::new_with_id(id, group, raft_client).await {
            Err(e) => Err(e),
            Ok(ch) => match ch.init_table().await {
                Err(e) => Err(CHError::InitTableError(e)),
                Ok(_) => Ok(ch.clone()),
            },
        }
    }
    pub async fn init_table(&self) -> Result<(), InitTableError> {
        let mut table = self.tables.write().await;
        self.init_table_(&mut table).await
    }
    pub async fn to_server_name(&self, server_id: u64) -> String {
        let lookup_table = self.tables.read().await;
        lookup_table.addrs.get(&server_id).unwrap().clone()
    }
    pub async fn to_server_name_option(&self, server_id: Option<u64>) -> Option<String> {
        if let Some(sid) = server_id {
            let lookup_table = self.tables.read().await;
            Some(lookup_table.addrs.get(&sid).unwrap().clone())
        } else {
            None
        }
    }
    pub async fn get_server_id(&self, hash: u64) -> Option<u64> {
        let lookup_table = self.tables.read().await;
        let nodes = &lookup_table.nodes;
        let slot_count = nodes.len();
        if slot_count == 0 {
            return None;
        }
        let result = nodes.get(self.jump_hash(slot_count, hash));
        debug!("Hash {} have been point to {:?}", hash, result);
        result.cloned()
    }
    pub fn jump_hash(&self, slot_count: usize, hash: u64) -> usize {
        let mut b: i64 = -1;
        let mut j: i64 = 0;
        let mut h = hash;
        while j < (slot_count as i64) {
            b = j;
            h = h.wrapping_mul(2862933555777941757).wrapping_add(1);
            j = (((b.wrapping_add(1)) as f64) * ((1i64 << 31) as f64)
                / (((h >> 33).wrapping_add(1)) as f64)) as i64;
        }
        debug!(
            "Jump hash point to index {} for {}, with slots {}",
            b, hash, slot_count
        );
        b as usize
    }
    pub async fn get_server(&self, hash: u64) -> Option<String> {
        self.to_server_name_option(self.get_server_id(hash).await)
            .await
    }
    pub async fn get_server_by_string(&self, string: &String) -> Option<String> {
        self.get_server(hash_str(string)).await
    }
    pub async fn get_server_by<T>(&self, obj: &T) -> Option<String>
    where
        T: serde::Serialize,
    {
        self.get_server(hash_bytes(serialize(obj).as_slice())).await
    }
    pub async fn get_server_id_by_string(&self, string: &String) -> Option<u64> {
        self.get_server_id(hash_str(string)).await
    }
    pub async fn get_server_id_by<T>(&self, obj: &T) -> Option<u64>
    where
        T: serde::Serialize,
    {
        self.get_server_id(hash_bytes(serialize(obj).as_slice()))
            .await
    }
    pub async fn rand_server(&self) -> Option<String> {
        let rand = rand::random::<u64>();
        self.get_server(rand).await
    }
    pub async fn nodes_count(&self) -> usize {
        let lookup_table = self.tables.read().await;
        return lookup_table.nodes.len();
    }
    pub async fn set_weight(&self, server_name: &String, weight: u64) -> Result<(), ExecError> {
        let group_id = hash_str(&self.group_name);
        let server_id = hash_str(server_name);
        self.weight_sm_client
            .set_weight(&group_id, &server_id, &weight)
            .await
    }
    pub async fn watch_all_actions<F>(&self, f: F)
    where
        F: Fn(&Member, &Action, &LookupTables, &Vec<u64>) + 'static + Send + Sync,
    {
        let mut watchers = self.watchers.write().await;
        watchers.push(Box::new(f));
    }
    pub async fn watch_server_nodes_range_changed<F>(&self, server: &String, f: F)
    // return ranges [...,...)
    where
        F: Fn((usize, u32)) + 'static + Send + Sync,
    {
        let server_id = hash_str(server);
        let wrapper = move |_: &Member, _: &Action, lookup_table: &LookupTables, _: &Vec<u64>| {
            let nodes = &lookup_table.nodes;
            let node_len = nodes.len();
            let mut weight = 0;
            let mut start = None;
            for ni in 0..node_len {
                let node = nodes[ni];
                if node == server_id {
                    weight += 1;
                    if start.is_none() {
                        start = Some(ni)
                    }
                }
            }
            if start.is_some() {
                f((start.unwrap(), weight));
            } else {
                warn!("No node exists for watch");
            }
        };
        self.watch_all_actions(wrapper).await;
    }

    async fn init_table_(
        &self,
        lookup_table: &mut RwLockWriteGuard<'_, LookupTables>,
    ) -> Result<(), InitTableError> {
        if let Ok(Some((mut members, version))) =
            self.membership.group_members(&self.group_name, true).await
        {
            let group_id = hash_str(&self.group_name);
            match self.weight_sm_client.get_weights(&group_id).await {
                Ok(Some(weights)) => {
                    if let Some(min_weight) = weights.values().min() {
                        let mut factors: BTreeMap<u64, u32> = BTreeMap::new();
                        let min_weight = *min_weight as f64;
                        for member in members.iter() {
                            let k = member.id;
                            let w = match weights.get(&k) {
                                Some(w) => *w as f64,
                                None => min_weight,
                            };
                            factors.insert(k, (w / min_weight) as u32);
                        }
                        let factor_sum: u32 = factors.values().sum();
                        lookup_table.nodes = Vec::with_capacity(factor_sum as usize);
                        for member in members.iter() {
                            lookup_table.addrs.insert(member.id, member.address.clone());
                        }
                        for (server_id, weight) in factors.into_iter() {
                            for i in 0..weight {
                                lookup_table.nodes.push(server_id);
                            }
                        }
                        self.version.store(version, Ordering::Relaxed);
                        Ok(())
                    } else {
                        Err(InitTableError::NoWeightInfo)
                    }
                }
                Err(e) => Err(InitTableError::NoWeightService(e)),
                Ok(None) => Err(InitTableError::NoWeightGroup),
                _ => Err(InitTableError::Unknown),
            }
        } else {
            Err(InitTableError::GroupNotExisted)
        }
    }
    pub fn membership(&self) -> Arc<MembershipClient> {
        self.membership.clone()
    }
}

async fn server_joined(ch: Arc<ConsistentHashing>, member: Member, version: u64) {
    server_changed(ch, member, Action::Joined, version).await;
}
async fn server_left(ch: Arc<ConsistentHashing>, member: Member, version: u64) {
    server_changed(ch, member, Action::Left, version).await;
}
async fn server_changed(ch: Arc<ConsistentHashing>, member: Member, action: Action, version: u64) {
    // let ch = ch.clone();
    let ch_version = ch.version.load(Ordering::Relaxed);
    if ch_version < version {
        let watchers = ch.watchers.read().await;
        let ch_version = ch.version.load(Ordering::Relaxed);
        if ch_version >= version {
            return;
        }
        {
            let mut lookup_table = ch.tables.write().await;
            let old_nodes = lookup_table.nodes.clone();
            ch.init_table_(&mut lookup_table).await;
            for watch in watchers.iter() {
                watch(&member, &action, &*lookup_table, &old_nodes);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::conshash::weights::Weights;
    use crate::conshash::ConsistentHashing;
    use crate::membership::client::ObserverClient;
    use crate::membership::member::MemberService;
    use crate::membership::server::Membership;
    use crate::raft::client::RaftClient;
    use crate::raft::{Options, RaftService, Storage};
    use crate::rpc::Server;
    use crate::utils::time::async_wait_secs;
    use std::collections::HashMap;
    use std::sync::atomic::*;
    use std::sync::Arc;

    #[tokio::test(threaded_scheduler)]
    async fn primary() {
        let addr = String::from("127.0.0.1:2200");
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: 0,
        });
        let server = Server::new(&addr);
        let heartbeat_service = Membership::new(&server, &raft_service).await;
        server.register_service(0, &raft_service).await;
        Server::listen_and_resume(&server).await;
        RaftService::start(&raft_service);
        raft_service.bootstrap();

        let group_1 = String::from("test_group_1");
        let group_2 = String::from("test_group_2");
        let group_3 = String::from("test_group_3");

        let server_1 = String::from("server1");
        let server_2 = String::from("server2");
        let server_3 = String::from("server3");

        let wild_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        let client = ObserverClient::new(&wild_raft_client);

        RaftClient::prepare_subscription(&server);

        client.new_group(&group_1).await.unwrap().unwrap();
        client.new_group(&group_2).await.unwrap().unwrap();
        client.new_group(&group_3).await.unwrap().unwrap();

        let member1_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        let member1_svr = MemberService::new(&server_1, &member1_raft_client).await;

        let member2_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        let member2_svr = MemberService::new(&server_2, &member2_raft_client).await;

        let member3_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        let member3_svr = MemberService::new(&server_3, &member3_raft_client).await;

        member1_svr.join_group(&group_1).await.unwrap();
        member2_svr.join_group(&group_1).await.unwrap();
        member3_svr.join_group(&group_1).await.unwrap();

        member1_svr.join_group(&group_2).await.unwrap();
        member2_svr.join_group(&group_2).await.unwrap();

        member1_svr.join_group(&group_3).await.unwrap();

        let weight_service = Weights::new(&raft_service);

        let ch1 = ConsistentHashing::new(&group_1, &wild_raft_client)
            .await
            .unwrap();
        let ch2 = ConsistentHashing::new(&group_2, &wild_raft_client)
            .await
            .unwrap();
        let ch3 = ConsistentHashing::new(&group_3, &wild_raft_client)
            .await
            .unwrap();

        ch1.set_weight(&server_1, 1).await.unwrap();
        ch1.set_weight(&server_2, 2).await.unwrap();
        ch1.set_weight(&server_3, 3).await.unwrap();

        ch2.set_weight(&server_1, 1).await.unwrap();
        ch2.set_weight(&server_2, 1).await.unwrap();

        ch3.set_weight(&server_1, 2).await.unwrap();

        ch1.init_table().await.unwrap();
        ch2.init_table().await.unwrap();
        ch3.init_table().await.unwrap();

        let ch1_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch1_server_node_changes_count_clone = Arc::new(AtomicUsize::new(0)).clone();
        ch1.watch_server_nodes_range_changed(&server_2, move |r| {
            ch1_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        let ch2_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch2_server_node_changes_count_clone = Arc::new(AtomicUsize::new(0)).clone();
        ch2.watch_server_nodes_range_changed(&server_2, move |r| {
            ch2_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        let ch3_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch3_server_node_changes_count_clone = Arc::new(AtomicUsize::new(0)).clone();
        ch3.watch_server_nodes_range_changed(&server_2, move |r| {
            ch3_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(ch1.nodes_count().await, 6);
        assert_eq!(ch2.nodes_count().await, 2);
        assert_eq!(ch3.nodes_count().await, 1);

        let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..30000usize {
            let k = format!("k - {}", i);
            let server = ch1.get_server_by_string(&k).await.unwrap();
            *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        assert_eq!(ch_1_mapping.get(&server_1).unwrap(), &4936);
        assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &9923);
        assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &15141); // hard coded due to constant

        let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..30000usize {
            let k = format!("k - {}", i);
            let server = ch2.get_server_by_string(&k).await.unwrap();
            *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        assert_eq!(ch_2_mapping.get(&server_1).unwrap(), &14967);
        assert_eq!(ch_2_mapping.get(&server_2).unwrap(), &15033);

        let mut ch_3_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..30000usize {
            let k = format!("k - {}", i);
            let server = ch3.get_server_by_string(&k).await.unwrap();
            *ch_3_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        assert_eq!(ch_3_mapping.get(&server_1).unwrap(), &30000);

        member1_svr.leave().await.unwrap();

        async_wait_secs().await;

        let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..30000usize {
            let k = format!("k - {}", i);
            let server = ch1.get_server_by_string(&k).await.unwrap();
            *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &11932);
        assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &18068);

        let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..30000usize {
            let k = format!("k - {}", i);
            let server = ch2.get_server_by_string(&k).await.unwrap();
            *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        assert_eq!(ch_2_mapping.get(&server_2).unwrap(), &30000);

        for i in 0..30000usize {
            let k = format!("k - {}", i);
            assert!(ch3.get_server_by_string(&k).await.is_none()); // no member
        }

        //    wait();
        //    wait();
        //    assert_eq!(ch1_server_node_changes_count.load(Ordering::Relaxed), 1);
        //    assert_eq!(ch2_server_node_changes_count.load(Ordering::Relaxed), 1);
        //    assert_eq!(ch3_server_node_changes_count.load(Ordering::Relaxed), 0);
    }
}
