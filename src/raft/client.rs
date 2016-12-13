use raft::{SyncClient, ClientClusterInfo, RaftMsg};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::Mutex;
use bifrost_plugins::hash_str;

struct QryMeta {
    last_log_id: u64,
    last_log_term: u64,
}

struct Members {
    clients: HashMap<u64, Mutex<SyncClient>>,
    id_map: HashMap<u64, String>,
}

pub struct RaftClient {
    qry_meta: QryMeta,
    members: Members,
    term: u64, // last member list update term
}

impl RaftClient {
    pub fn new(servers: Vec<String>) -> Option<RaftClient> {
        let mut client = RaftClient {
            qry_meta: QryMeta {
                last_log_id: 0,
                last_log_term: 0,
            },
            members: Members {
                clients: HashMap::new(),
                id_map: HashMap::new()
            },
            term: 0,
        };
        match client.update_info(&HashSet::from_iter(servers)) {
            Ok(_) => Some(client),
            Err(_) => None
        }
    }

    pub fn update_info(&mut self, addrs: &HashSet<String>) -> Result<(), ()> {
        let info: ClientClusterInfo;
        let mut servers = None;
        for server_addr in addrs {
            let id = hash_str(server_addr.clone());
            let mut client = self.members.clients.entry(id).or_insert_with(|| {
                Mutex::new(SyncClient::new(server_addr))
            }).lock().unwrap();
            if let Some(Ok(info)) = client.c_server_cluster_info() {
                servers = Some(info);
                break;
            }
        }
        match servers {
            Some(servers) => {
                let remote_members = servers.members;
                let mut remote_ids = HashSet::with_capacity(remote_members.len());
                self.members.id_map.clear();
                for (id, addr) in remote_members {
                    self.members.id_map.insert(id, addr);
                    remote_ids.insert(id);
                }
                let mut connected_ids = HashSet::with_capacity(self.members.clients.len());
                for id in self.members.clients.keys() {connected_ids.insert(*id);}
                let ids_to_remove = connected_ids.difference(&remote_ids);
                for id in ids_to_remove {self.members.clients.remove(id);}
                for id in remote_ids.difference(&connected_ids) {
                    let addr = self.members.id_map.get(id).unwrap();
                    self.members.clients.entry(id.clone()).or_insert_with(|| {
                        Mutex::new(SyncClient::new(addr))
                    }).lock().unwrap();
                }
                Ok(())
            },
            None => Err(()),
        }
    }

    pub fn execute(&mut self, msg: &RaftMsg) {

    }
}