//! Reproducers for two defects of configuration entries (RAFT_DESIGN.md, 3.1).
//! Each test states the required behaviour. The tests marked "control" pass on
//! the current code; the others fail on it.

use super::compaction_tests::scratch_dir;
use super::multi_member_tests::{add_entry, addresses, cluster, Member, NEVER};
use super::state_machine::configs::commands::new_member_;
use super::state_machine::configs::{Configures, CONFIG_SM_ID};
use super::*;
use bifrost_hasher::hash_str;

fn config_entry(address: &String) -> LogEntry {
    let (fn_id, _, data) = new_member_::new(address).encode();
    LogEntry {
        id: 0,
        term: 0,
        sm_id: CONFIG_SM_ID,
        fn_id,
        data,
    }
}

/// Three members; both followers stop; the leader gets `entry`.
async fn leader_without_a_quorum(tag: &str, entry: impl Fn(&String) -> LogEntry) -> ClientCmdResponse {
    let mut members = cluster(tag, 3, NEVER).await;
    let spare = Member::launch(&scratch_dir(&format!("{}_spare", tag)), &addresses(1)[0], NEVER).await;
    let f2 = members.pop().unwrap();
    let f1 = members.pop().unwrap();
    f1.stop().await;
    f2.stop().await;
    let leader = members.pop().unwrap();
    assert!(leader.is_leader().await);
    let response = Service::c_command(leader.svc.as_ref(), PlaneId::type1(), entry(&spare.addr)).await;
    spare.stop().await;
    leader.stop().await;
    response
}

/// Control: a data entry needs a quorum. It passes on the current code.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_data_entry_without_a_quorum_is_not_committed() {
    let response = leader_without_a_quorum("mc_data", |_| add_entry(0, 0, 1)).await;
    assert!(
        !matches!(response, ClientCmdResponse::Success { .. }),
        "a data entry was acknowledged without a quorum: {:?}",
        response
    );
}

/// Defect: a configuration entry is committed at once, without a quorum
/// (`try_sync_config_to_followers_on_plane`). Fails on the current code.
#[tokio::test(flavor = "multi_thread")]
async fn a_config_entry_without_a_quorum_is_not_committed() {
    let response = leader_without_a_quorum("mc_cfg", config_entry).await;
    assert!(
        !matches!(response, ClientCmdResponse::Success { .. }),
        "a configuration entry was acknowledged without a quorum: {:?}",
        response
    );
}

/// Control: with a quorum, the configuration entry commits.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_config_entry_with_a_quorum_commits() {
    let members = cluster("mc_cfg_ok", 3, NEVER).await;
    let spare = Member::launch(&scratch_dir("mc_cfg_ok_spare"), &addresses(1)[0], NEVER).await;
    let response = Service::c_command(members[0].svc.as_ref(), PlaneId::type1(), config_entry(&spare.addr)).await;
    assert!(matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
}

/// Defect: applying a configuration entry opens a connection and can fail,
/// so the voter set of a node depends on the network (`configs.rs:46`).
/// Fails on the current code: the unreachable address is not added.
#[tokio::test(flavor = "multi_thread")]
async fn applying_a_config_entry_does_not_depend_on_the_network() {
    let mut config = Configures::new(1);
    let unreachable = "127.0.0.1:9".to_string();
    config.new_member(unreachable.clone()).await;
    assert!(
        config.member_existed(hash_str(&unreachable)),
        "the entry was applied but the voter is missing"
    );
}

/// Control: with a reachable address the same call adds the voter.
#[tokio::test(flavor = "multi_thread")]
async fn control_applying_a_config_entry_for_a_reachable_address() {
    let spare = Member::launch(&scratch_dir("mc_apply"), &addresses(1)[0], NEVER).await;
    let mut config = Configures::new(1);
    config.new_member(spare.addr.clone()).await;
    assert!(config.member_existed(hash_str(&spare.addr)));
}
