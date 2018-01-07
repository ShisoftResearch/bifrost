pub mod client;
pub mod server;
pub mod member;

use membership::client::{Member as ClientMember};
use futures::prelude::*;

pub static DEFAULT_SERVICE_ID: u64 = hash_ident!(BIFROST_MEMBERSHIP_SERVICE) as u64;

pub mod raft {
    use super::*;
    raft_state_machine! {
        def cmd hb_online_changed(online: Vec<u64>, offline: Vec<u64>);
        def cmd join(address: String) -> u64;
        def cmd leave(id: u64);
        def cmd join_group(group_name: String, id: u64);
        def cmd leave_group(group: u64, id: u64);
        def cmd new_group(name: String) -> u64 | u64;
        def cmd del_group(id: u64);
        def qry group_leader(group: u64) -> (Option<ClientMember>, u64);
        def qry group_members (group: u64, online_only: bool) -> (Vec<ClientMember>, u64);
        def qry all_members (online_only: bool) -> (Vec<ClientMember>, u64);
        def sub on_group_member_offline(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_offline() -> (ClientMember, u64); //
        def sub on_group_member_online(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_online() -> (ClientMember, u64); //
        def sub on_group_member_joined(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_joined() -> (ClientMember, u64); //
        def sub on_group_member_left(group: u64) -> (ClientMember, u64); //
        def sub on_any_member_left() -> (ClientMember, u64); //
        def sub on_group_leader_changed(group: u64) -> (Option<ClientMember>, Option<ClientMember>, u64);
    }
}

mod heartbeat_rpc {
    service! {
        rpc ping(id: u64);
    }
}

