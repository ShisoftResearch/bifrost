use futures::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::conshash::weights::client::SMClient as WeightSMClient;
use crate::conshash::weights::DEFAULT_SERVICE_ID;
use crate::membership::client::{Member, ObserverClient as MembershipClient};
use crate::raft::client::{RaftClient, SubscriptionError, SubscriptionReceipt};
use crate::raft::state_machine::master::ExecError;
use crate::utils::serde::serialize;
// use async_std::sync::*;
use parking_lot::*;
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
    watchers: RwLock<Vec<Box<dyn Fn(&Member, &Action, &Vec<u64>, &Vec<u64>) + Send + Sync>>>,
    update_lock: async_std::sync::Mutex<()>,
    version: AtomicU64,
}

impl ConsistentHashing {
    pub async fn new_with_id(
        id: u64,
        group: &str,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        let ch = Arc::new(ConsistentHashing {
            tables: RwLock::new(LookupTables {
                nodes: Vec::new(),
                addrs: HashMap::new(),
            }),
            membership: membership_client.clone(),
            weight_sm_client: WeightSMClient::new(id, &raft_client),
            group_name: group.to_string(),
            watchers: RwLock::new(Vec::new()),
            version: AtomicU64::new(0),
            update_lock: async_std::sync::Mutex::new(())
        });
        {
            let ch = ch.clone();
            let res = membership_client
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
            let res = membership_client
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
            let res = membership_client
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
            let res = membership_client
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
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_with_id(DEFAULT_SERVICE_ID, group, raft_client, membership_client).await
    }
    pub async fn new_client(
        group: &str,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        Self::new_client_with_id(DEFAULT_SERVICE_ID, group, raft_client, membership_client).await
    }
    pub async fn new_client_with_id(
        id: u64,
        group: &str,
        raft_client: &Arc<RaftClient>,
        membership_client: &Arc<MembershipClient>,
    ) -> Result<Arc<ConsistentHashing>, CHError> {
        match ConsistentHashing::new_with_id(id, group, raft_client, membership_client).await {
            Err(e) => Err(e),
            Ok(ch) => match ch.init_table().await {
                Err(e) => Err(CHError::InitTableError(e)),
                Ok(_) => Ok(ch.clone()),
            },
        }
    }
    pub fn to_server_name(&self, server_id: u64) -> String {
        let lookup_table = self.tables.read();
        trace!("Lookup table has {:?}", lookup_table.addrs);
        if let Some(name) = lookup_table.addrs.get(&server_id){
            name.to_owned()
        } else {
            panic!("Cannot find server name for server id {}", server_id);
        }
    }
    pub fn to_server_name_option(&self, server_id: Option<u64>) -> Option<String> {
        if let Some(sid) = server_id {
            let lookup_table = self.tables.read();
            Some(lookup_table.addrs.get(&sid).unwrap().clone())
        } else {
            None
        }
    }
    pub fn get_server_id(&self, hash: u64) -> Option<u64> {
        let lookup_table = self.tables.read();
        let nodes = &lookup_table.nodes;
        let slot_count = nodes.len();
        if slot_count == 0 {
            return None;
        }
        let result = nodes.get(self.jump_hash(slot_count, hash));
        // trace!("Hash {} have been point to {:?}", hash, result);
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
        // trace!(
        //     "Jump hash point to index {} for {}, with slots {}",
        //     b,
        //     hash,
        //     slot_count
        // );
        b as usize
    }
    pub fn get_server(&self, hash: u64) -> Option<String> {
        self.to_server_name_option(self.get_server_id(hash))
    }
    pub fn get_server_by_string(&self, string: &String) -> Option<String> {
        self.get_server(hash_str(string))
    }
    pub fn get_server_by<T>(&self, obj: &T) -> Option<String>
    where
        T: serde::Serialize,
    {
        self.get_server(hash_bytes(serialize(obj).as_slice()))
    }
    pub fn get_server_id_by_string(&self, string: &String) -> Option<u64> {
        self.get_server_id(hash_str(string))
    }
    pub fn get_server_id_by<T>(&self, obj: &T) -> Option<u64>
    where
        T: serde::Serialize,
    {
        self.get_server_id(hash_bytes(serialize(obj).as_slice()))
    }
    pub fn rand_server(&self) -> Option<String> {
        let rand = rand::random::<u64>();
        self.get_server(rand)
    }
    pub fn nodes_count(&self) -> usize {
        let lookup_table = self.tables.read();
        return lookup_table.nodes.len();
    }
    pub async fn set_weight(&self, server_name: &String, weight: u64) -> Result<(), ExecError> {
        let group_id = hash_str(&self.group_name);
        let server_id = hash_str(server_name);
        self.weight_sm_client
            .set_weight(&group_id, &server_id, &weight)
            .await
    }
    fn watch_all_actions<F>(&self, f: F)
    where
        F: Fn(&Member, &Action, &Vec<u64>, &Vec<u64>) + 'static + Send + Sync,
    {
        let mut watchers = self.watchers.write();
        watchers.push(Box::new(f));
    }
    pub fn watch_server_nodes_range_changed<F>(&self, server: &String, f: F)
    // return ranges [...,...)
    where
        F: Fn((usize, u32)) + 'static + Send + Sync,
    {
        let server_id = hash_str(server);
        let wrapper = move |_: &Member, _: &Action, nodes: &Vec<u64>, _: &Vec<u64>| {
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
        self.watch_all_actions(wrapper);
    }

    pub async fn init_table(
        &self
    ) -> Result<(), InitTableError> {
        let group_name = &self.group_name;
        let _lock = self.update_lock.lock().await;
        debug!(
            "Initializing table from membership group members for {}",
            group_name
        );
        debug!("Get group members for {}", group_name);
        let group_members = self.membership.group_members(group_name, true).await;
        debug!("Got group members for {}", group_name);
        if let Ok(Some((members, version))) = group_members {
            let group_id = hash_str(group_name);
            debug!("Getting weights for {}", group_name);
            match self.weight_sm_client.get_weights(&group_id).await {
                Ok(Some(weights)) => {
                    debug!("Group {} have {} weights", group_name, weights.len());
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
                        let mut lookup_table = self.tables.write();
                        lookup_table.nodes = Vec::with_capacity(factor_sum as usize);
                        for member in members.iter() {
                            lookup_table.addrs.insert(member.id, member.address.clone());
                        }
                        for (server_id, weight) in factors.into_iter() {
                            for _ in 0..weight {
                                lookup_table.nodes.push(server_id);
                            }
                        }
                        self.version.store(version, Ordering::Relaxed);
                        Ok(())
                    } else {
                        Err(InitTableError::NoWeightInfo)
                    }
                }
                Err(e) => {
                    error!("No weright service for group {}", group_name);
                    Err(InitTableError::NoWeightService(e))
                }
                Ok(None) => {
                    error!("No weight group for group {}", group_name);
                    Err(InitTableError::NoWeightGroup)
                }
            }
        } else {
            error!("No group {} existed in table", group_name);
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
    debug!(
        "Detected server membership change, member {:?}, action {:?}, version {}",
        member, action, version
    );
    let ch_version = ch.version.load(Ordering::Relaxed);
    if ch_version < version {
        {
            debug!("Obtaining conshash table write lock");
            let old_nodes = (&*ch.tables.read()).nodes.clone();
            debug!("Reinit conshash table");
            let reinit_res = ch.init_table().await;
            if !reinit_res.is_ok() {
                error!("Cannot reinit table {:?}", reinit_res.err().unwrap());
            }
            debug!("Triggering conshash watchers");
            let new_nodes = (&*ch.tables.read()).nodes.clone();
            for watch in ch.watchers.read().iter() {
                watch(&member, &action, &new_nodes, &old_nodes);
            }
        }
        debug!("Server change processing completed");
    } else {
        warn!("Server membership change too old to follow, member {:?}, action {:?}, version {}, expect {}", member, action, version, ch_version);
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
        let _ = env_logger::try_init();

        info!("Creating raft service");
        let addr = String::from("127.0.0.1:2200");
        let raft_service = RaftService::new(Options {
            storage: Storage::default(),
            address: addr.clone(),
            service_id: 0,
        });

        info!("Creating server");
        let server = Server::new(&addr);
        info!("Creating membership service");
        let _membership = Membership::new(&server, &raft_service).await;
        server.register_service(0, &raft_service).await;
        Server::listen_and_resume(&server).await;
        RaftService::start(&raft_service).await;
        raft_service.bootstrap().await;

        let group_1 = String::from("test_group_1");
        let group_2 = String::from("test_group_2");
        let group_3 = String::from("test_group_3");

        let server_1 = String::from("server1");
        let server_2 = String::from("server2");
        let server_3 = String::from("server3");

        info!("Create raft client");
        let wild_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("Create observer");
        let observer_client = Arc::new(ObserverClient::new(&wild_raft_client));

        info!("Create subscription");
        RaftClient::prepare_subscription(&server).await;

        info!("New group 1");
        observer_client.new_group(&group_1).await.unwrap().unwrap();
        info!("New group 2");
        observer_client.new_group(&group_2).await.unwrap().unwrap();
        info!("New group 3");
        observer_client.new_group(&group_3).await.unwrap().unwrap();

        info!("New raft client for member 1");
        let member1_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("New member service 1");
        let member1_svr = MemberService::new(&server_1, &member1_raft_client).await;

        info!("New raft client for member 2");
        let member2_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("New member service 2");
        let member2_svr = MemberService::new(&server_2, &member2_raft_client).await;

        info!("New raft client for member 3");
        let member3_raft_client = RaftClient::new(&vec![addr.clone()], 0).await.unwrap();
        info!("New member service 3");
        let member3_svr = MemberService::new(&server_3, &member3_raft_client).await;

        info!("Member 1 join group 1");
        member1_svr.join_group(&group_1).await.unwrap();
        info!("Member 1 join group 2");
        member2_svr.join_group(&group_1).await.unwrap();
        info!("Member 1 join group 3");
        member3_svr.join_group(&group_1).await.unwrap();

        info!("Member 1 join group 2");
        member1_svr.join_group(&group_2).await.unwrap();
        info!("Member 2 join group 2");
        member2_svr.join_group(&group_2).await.unwrap();

        info!("Member 1 join group 3");
        member1_svr.join_group(&group_3).await.unwrap();

        info!("New weight service");
        Weights::new(&raft_service).await;

        info!("New conshash for group 1");
        let ch1 = ConsistentHashing::new(&group_1, &wild_raft_client, &observer_client)
            .await
            .unwrap();

        info!("New conshash for group 2");
        let ch2 = ConsistentHashing::new(&group_2, &wild_raft_client, &observer_client)
            .await
            .unwrap();

        info!("New conshash for group 3");
        let ch3 = ConsistentHashing::new(&group_3, &wild_raft_client, &observer_client)
            .await
            .unwrap();

        info!("Set server 1 in group 1 to 1");
        ch1.set_weight(&server_1, 1).await.unwrap();
        info!("Set server 2 in group 1 to 2");
        ch1.set_weight(&server_2, 2).await.unwrap();
        info!("Set server 3 in group 1 to 3");
        ch1.set_weight(&server_3, 3).await.unwrap();

        info!("Set server 1 in group 2 to 1");
        ch2.set_weight(&server_1, 1).await.unwrap();
        info!("Set server 2 in group 2 to 1");
        ch2.set_weight(&server_2, 1).await.unwrap();

        info!("Set server 1 in group 3 to 2");
        ch3.set_weight(&server_1, 2).await.unwrap();

        info!("Init table for conshash 1");
        ch1.init_table().await.unwrap();
        info!("Init table for conshash 2");
        ch2.init_table().await.unwrap();
        info!("Init table for conshash 3");
        ch3.init_table().await.unwrap();

        let ch1_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch1_server_node_changes_count_clone = ch1_server_node_changes_count.clone();
        info!("Watch node change from conshash 1");
        ch1.watch_server_nodes_range_changed(&server_2, move |_| {
            ch1_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        let ch2_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch2_server_node_changes_count_clone = ch2_server_node_changes_count.clone();
        info!("Watch node change from conshash 2");
        ch2.watch_server_nodes_range_changed(&server_2, move |_| {
            ch2_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        let ch3_server_node_changes_count = Arc::new(AtomicUsize::new(0));
        let ch3_server_node_changes_count_clone = ch3_server_node_changes_count.clone();
        info!("Watch node change from conshash 3");
        ch3.watch_server_nodes_range_changed(&server_2, move |_| {
            ch3_server_node_changes_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        info!("Counting nodes for conshash 1");
        assert_eq!(ch1.nodes_count(), 6);
        info!("Counting nodes for conshash 2");
        assert_eq!(ch2.nodes_count(), 2);
        info!("Counting nodes for conshash 3");
        assert_eq!(ch3.nodes_count(), 1);

        info!("Batch get server by string from conshash 1");
        let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
        let data_set_size: usize = 30000;

        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch1.get_server_by_string(&k).unwrap();
            *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Counting distribution for conshash 1");
        assert_eq!(ch_1_mapping.get(&server_1).unwrap(), &4936);
        assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &9923);
        assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &15141); // hard coded due to constant

        info!("Batch get server by string from conshash 2");
        let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch2.get_server_by_string(&k).unwrap();
            *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Counting distribution for conshash 2");
        assert_eq!(ch_2_mapping.get(&server_1).unwrap(), &14967);
        assert_eq!(ch_2_mapping.get(&server_2).unwrap(), &15033);

        info!("Batch get server by string from conshash 3");
        let mut ch_3_mapping: HashMap<String, u64> = HashMap::new();
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch3.get_server_by_string(&k).unwrap();
            *ch_3_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Counting distribution for conshash 3");
        assert_eq!(ch_3_mapping.get(&server_1).unwrap(), &30000);

        info!("Close member 1");
        member1_svr.close();
        info!("Waiting");
        async_wait_secs().await;
        async_wait_secs().await;

        let mut ch_1_mapping: HashMap<String, u64> = HashMap::new();
        info!("Recheck get server by string for conshash 1");
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch1.get_server_by_string(&k).unwrap();
            *ch_1_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Recount distribution for conshash 1");
        assert_eq!(
            ch_1_mapping.get(&server_2).unwrap() + ch_1_mapping.get(&server_3).unwrap(),
            data_set_size as u64
        );
        assert_eq!(ch_1_mapping.get(&server_2).unwrap(), &11932);
        assert_eq!(ch_1_mapping.get(&server_3).unwrap(), &18068);

        let mut ch_2_mapping: HashMap<String, u64> = HashMap::new();
        info!("Recheck get server by string for conshash 2");
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            let server = ch2.get_server_by_string(&k).unwrap();
            *ch_2_mapping.entry(server.clone()).or_insert(0) += 1;
        }
        info!("Recount distribution for conshash 2");
        assert_eq!(
            ch_2_mapping.get(&server_2).unwrap(),
            &(data_set_size as u64)
        );

        info!("Cheching conshash 3 with no members");
        for i in 0..data_set_size {
            let k = format!("k - {}", i);
            assert!(ch3.get_server_by_string(&k).is_none()); // no member
        }

        info!("Waiting");
        async_wait_secs().await;
        async_wait_secs().await;
        info!("Testing callback counter");
        assert_eq!(ch1_server_node_changes_count.load(Ordering::Relaxed), 1);
        assert_eq!(ch2_server_node_changes_count.load(Ordering::Relaxed), 1);
        assert_eq!(ch3_server_node_changes_count.load(Ordering::Relaxed), 0);
        info!("Membership tests all done !");
    }
}
