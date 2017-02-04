pub mod client;
pub mod server;

#[derive(Serialize, Deserialize, Debug)]
pub enum Status {
    Online,
    Offline,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Member {
    pub id: u64,
    pub address: String,
    pub status: Status,
    pub groups: Vec<u64>,
}

mod raft {
    use super::*;
    raft_state_machine! {
        def cmd hb_online_changed(online: Vec<u64>, offline: Vec<u64>);
        def cmd join(group: u64, address: String);
        def cmd leave(group: u64, address: String);
        def qry members(group: u64) -> Vec<Member>;
        def qry leader(group: u64) -> Member;
        def qry group_members (group: u64) -> Vec<Member>;
        def qry all_members () -> Vec<Member>;
        def sub on_group_member_offline(group: u64) -> Member;
        def sub on_any_member_offline() -> Member;
        def sub on_group_member_online(group: u64) -> Member;
        def sub on_any_member_online() -> Member;
        def sub on_group_member_joined(group: u64) -> Member;
        def sub on_any_member_joined() -> Member;
        def sub on_group_member_left(group: u64) -> Member;
        def sub on_any_member_left() -> Member;
        def sub on_group_leader_changed(group: u64) -> (Member, Member);
    }
}

mod heartbeat_rpc {
    service! {
        rpc ping(id: u64);
    }
}

