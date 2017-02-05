pub mod client;
pub mod server;
pub mod member;

use membership::client::{Group as ClientGroup, Member as ClientMember};

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_MEMBERSHIP_SERVICE) as u64;

#[derive(Serialize, Deserialize, Debug)]
pub struct Member {
    pub id: u64,
    pub address: String,
    pub groups: Vec<u64>,
}

mod raft {
    use super::*;
    raft_state_machine! {
        def cmd hb_online_changed(online: Vec<u64>, offline: Vec<u64>);
        def cmd join(address: String) -> u64;
        def cmd leave(id: u64);
        def cmd join_group(group: u64, id: u64);
        def cmd leave_group(group: u64, id: u64);
        def qry group_leader(group: u64) -> Option<ClientMember>;
        def qry group_members (group: u64, online_only: bool) -> Vec<ClientMember>;
        def qry all_members (online_only: bool) -> Vec<ClientMember>;
        def sub on_group_member_offline(group: u64) -> ClientMember;
        def sub on_any_member_offline() -> Member;
        def sub on_group_member_online(group: u64) -> ClientMember;
        def sub on_any_member_online() -> Member;
        def sub on_group_member_joined(group: u64) -> ClientMember;
        def sub on_any_member_joined() -> Member;
        def sub on_group_member_left(group: u64) -> ClientMember;
        def sub on_any_member_left() -> Member;
        def sub on_group_leader_changed(group: u64) -> (ClientMember, ClientMember);
    }
}

mod heartbeat_rpc {
    service! {
        rpc ping(id: u64);
    }
}

