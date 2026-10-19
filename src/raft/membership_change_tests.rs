//! Configuration entries (RAFT_DESIGN.md): the voters of a member are a
//! function of its snapshot and its log; the seed is an ordinary voter; a
//! configuration entry commits by quorum; one change at a time; a new leader
//! commits an entry of its term first. Tests marked "control" show that the
//! test can pass; the others failed before the change.

use super::compaction_tests::{disk_options, scratch_dir, snapshot_and_compact};
use super::multi_member_tests::{add_entry, addresses, cluster, Member, NEVER};
use super::state_machine::configs::commands::{del_member_, new_member_};
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
    let response = command_with_retry(&members[0], config_entry(&spare.addr)).await;
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

/// Restart one member in place: same directory, same address.
async fn restart(member: Member) -> Member {
    let (dir, addr) = member.stop().await;
    crate::utils::time::async_wait_secs().await;
    Member::launch(&dir, &addr, NEVER).await
}

fn removal_entry(address: &String) -> LogEntry {
    let (fn_id, _, data) = del_member_::new(address).encode();
    LogEntry {
        id: 0,
        term: 0,
        sm_id: CONFIG_SM_ID,
        fn_id,
        data,
    }
}

/// A command the way a client sends it: `NotCommitted` is retried (the leader
/// waits one second for each follower, and a new voter may be slow).
async fn command_with_retry(member: &Member, entry: LogEntry) -> ClientCmdResponse {
    let mut response = Service::c_command(member.svc.as_ref(), PlaneId::type1(), entry.clone()).await;
    for _ in 0..8 {
        if !matches!(response, ClientCmdResponse::NotCommitted { .. }) {
            break;
        }
        response = Service::c_command(member.svc.as_ref(), PlaneId::type1(), entry.clone()).await;
    }
    response
}

async fn voter_addresses(member: &Member) -> Vec<String> {
    let meta = member.svc.read_meta().await;
    let mut addresses: Vec<String> = meta.voters.values().cloned().collect();
    addresses.sort();
    addresses
}

async fn entries(member: &Member) -> Vec<LogEntry> {
    let meta = member.svc.read_meta().await;
    let logs = meta.logs.read().await;
    logs.values().cloned().collect()
}

/// Rule C5 and RC1: a restarted sole voter wins an election in a NEW term.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_sole_voter_leads_in_a_new_term() {
    let mut members = cluster("mc_sole", 1, NEVER).await;
    let member = members.pop().unwrap();
    member.add(5).await;
    let term = member.svc.read_meta().await.term;
    assert!(term >= 1, "the seed leads from term 1 on");
    let member = restart(member).await;
    {
        let meta = member.svc.read_meta().await;
        assert!(matches!(meta.membership, Membership::Leader(_)));
        assert!(meta.term > term, "the term was {} and is {}", term, meta.term);
    }
    assert_eq!(member.value().await, 5);
}

/// Rule C3: the seed is in the log, so every member folds the full voter set.
#[tokio::test(flavor = "multi_thread")]
async fn the_seed_is_a_voter_in_the_log_of_every_member() {
    let members = cluster("mc_seed_log", 3, NEVER).await;
    let mut expected: Vec<String> = members.iter().map(|m| m.addr.clone()).collect();
    expected.sort();
    for member in &members {
        let folded: Voters = {
            let meta = member.svc.read_meta().await;
            let logs = meta.logs.read().await;
            fold_voters(&Voters::new(), &logs, 0)
        };
        let mut from_log: Vec<String> = folded.values().cloned().collect();
        from_log.sort();
        assert_eq!(from_log, expected, "the log of {} lacks a voter", member.addr);
        assert_eq!(voter_addresses(member).await, expected);
    }
}

/// Cause 1 of the design (`rp-g`): a restarted joiner without a snapshot has
/// all voters from its log, so it does not lead alone.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_joiner_recovers_every_voter_and_does_not_lead() {
    let mut members = cluster("mc_joiner", 3, NEVER).await;
    let mut expected: Vec<String> = members.iter().map(|m| m.addr.clone()).collect();
    expected.sort();
    let joiner = members.remove(1);
    let joiner = restart(joiner).await;
    assert_eq!(voter_addresses(&joiner).await, expected);
    assert!(!joiner.is_leader().await, "a joiner with 3 voters led alone");
}

/// RC2: a voter grants its vote to a candidate it has not heard of.
#[tokio::test(flavor = "multi_thread")]
async fn a_voter_grants_a_vote_to_a_candidate_it_does_not_know() {
    let members = cluster("mc_unknown", 1, NEVER).await;
    let member = &members[0];
    let (last_id, last_term) = member.position().await;
    let term = member.svc.read_meta().await.term;
    let stranger = 0x5eed_u64;
    let answer = Service::request_vote(
        member.svc.as_ref(),
        PlaneId::type1(),
        term + 5,
        stranger,
        last_id,
        last_term,
    )
    .await;
    assert!(answer.1, "the vote was refused for an unknown candidate");
}

/// Rule C7: the first voter change of a new term commits a no-op first.
#[tokio::test(flavor = "multi_thread")]
async fn a_voter_change_waits_for_an_entry_of_the_new_term() {
    let mut members = cluster("mc_c7", 1, NEVER).await;
    let member = restart(members.pop().unwrap()).await;
    let spare = Member::launch(&scratch_dir("mc_c7_spare"), &addresses(1)[0], NEVER).await;
    let response = command_with_retry(&member, config_entry(&spare.addr)).await;
    assert!(matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
    let log = entries(&member).await;
    let noop = log.iter().position(|e| e.sm_id == NOOP_SM_ID);
    let change = log
        .iter()
        .rposition(|e| config_change(e) == Some(ConfigChange::Add(spare.addr.clone())));
    assert!(noop.is_some(), "no no-op entry before the change");
    assert!(noop < change, "the no-op must come before the change");
}

/// Control for C7: with an entry of the term already committed, no no-op is added.
#[tokio::test(flavor = "multi_thread")]
async fn control_no_noop_when_the_term_has_a_committed_entry() {
    let mut members = cluster("mc_c7_ctl", 1, NEVER).await;
    let member = restart(members.pop().unwrap()).await;
    member.add(1).await; // an entry of the new term, committed
    let spare = Member::launch(&scratch_dir("mc_c7_ctl_spare"), &addresses(1)[0], NEVER).await;
    let response = command_with_retry(&member, config_entry(&spare.addr)).await;
    assert!(matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
    assert!(entries(&member).await.iter().all(|e| e.sm_id != NOOP_SM_ID));
}

/// Rule C6: while one change is not committed, a different change is refused
/// and appends nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_voter_change_waits_for_the_first() {
    let mut members = cluster("mc_c6", 3, NEVER).await;
    let spare1 = Member::launch(&scratch_dir("mc_c6_s1"), &addresses(1)[0], NEVER).await;
    let spare2 = Member::launch(&scratch_dir("mc_c6_s2"), &addresses(1)[0], NEVER).await;
    let f2 = members.pop().unwrap();
    let f1 = members.pop().unwrap();
    f1.stop().await;
    f2.stop().await;
    let leader = &members[0];
    let first =
        Service::c_command(leader.svc.as_ref(), PlaneId::type1(), config_entry(&spare1.addr)).await;
    assert!(
        matches!(first, ClientCmdResponse::NotCommitted { .. }),
        "no quorum, so no commit: {:?}",
        first
    );
    let before = entries(leader).await.len();
    let second =
        Service::c_command(leader.svc.as_ref(), PlaneId::type1(), config_entry(&spare2.addr)).await;
    assert!(
        matches!(second, ClientCmdResponse::NotCommitted { .. }),
        "{:?}",
        second
    );
    assert_eq!(entries(leader).await.len(), before, "the second change was appended");
}

/// Rule C9: a leader that removes itself steps down once the change commits.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_that_removes_itself_steps_down() {
    let members = cluster("mc_c9", 3, NEVER).await;
    let leader = &members[0];
    assert!(leader.is_leader().await);
    let response = Service::c_command(
        leader.svc.as_ref(),
        PlaneId::type1(),
        removal_entry(&leader.addr),
    )
    .await;
    assert!(matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
    assert!(!leader.is_leader().await, "the removed leader still leads");
    assert!(!voter_addresses(leader).await.contains(&leader.addr));
}

/// R3: a committed command that does not decode stops the plane with a typed
/// error; it is not skipped and it does not panic the process.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_command_that_does_not_decode_stops_the_plane() {
    let members = cluster("mc_decode", 2, NEVER).await;
    let follower = &members[1];
    let term = follower.svc.read_meta().await.term;
    let (prev_id, prev_term) = follower.position().await;
    let mut bad = add_entry(prev_id + 1, term, 1);
    bad.data = vec![0xff, 0xff, 0xff];
    let applied_before = follower.svc.read_meta().await.last_applied;
    let leader_id = members[0].svc.id;
    let _ = Service::append_entries(
        follower.svc.as_ref(),
        PlaneId::type1(),
        term,
        leader_id,
        prev_id,
        prev_term,
        &Some(vec![bad]),
        prev_id + 1,
    )
    .await;
    let meta = follower.svc.read_meta().await;
    assert!(
        RaftService::lifecycle_is_stopping(meta.lifecycle),
        "the plane kept running over an entry it cannot read"
    );
    assert_eq!(meta.last_applied, applied_before, "the entry was skipped");
}

/// Control for R3: a good command applies.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_committed_command_that_decodes_applies() {
    let members = cluster("mc_decode_ctl", 2, NEVER).await;
    members[0].add(3).await;
    let all: Vec<&Member> = members.iter().collect();
    assert_eq!(
        super::multi_member_tests::wait_for_values(&all, 3).await,
        vec![3, 3]
    );
    assert!(!RaftService::lifecycle_is_stopping(
        members[1].svc.read_meta().await.lifecycle
    ));
}

/// Rule C1 and C11: the voters are the snapshot image plus the configuration
/// entries of the log after it.
#[tokio::test(flavor = "multi_thread")]
async fn voters_are_recovered_from_a_snapshot_and_the_log_suffix() {
    let addrs = addresses(2);
    let a = Member::launch(&scratch_dir("mc_snap_a"), &addrs[0], 1).await;
    a.svc.bootstrap().await;
    let b = Member::launch(&scratch_dir("mc_snap_b"), &addrs[1], 1).await;
    b.svc.join(&vec![addrs[0].clone()]).await.expect("join");
    for _ in 0..3 {
        a.add(1).await;
    }
    snapshot_and_compact(&a.svc).await; // the log map is empty, the image holds {a, b}
    let spare = Member::launch(&scratch_dir("mc_snap_spare"), &addresses(1)[0], 1).await;
    let response = command_with_retry(&a, config_entry(&spare.addr)).await;
    assert!(matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
    let mut expected = vec![addrs[0].clone(), addrs[1].clone(), spare.addr.clone()];
    expected.sort();
    assert_eq!(voter_addresses(&a).await, expected);
    // Restart: the image gives {a, b}; the entry above the snapshot adds the spare.
    let a = restart(a).await;
    assert_eq!(voter_addresses(&a).await, expected);
}

/// Rule C2 and C11: a member that receives a snapshot has the voters of its
/// image, then those of the entries after it.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_that_installs_a_snapshot_has_the_voters_of_its_image() {
    let addrs = addresses(2);
    let a = Member::launch(&scratch_dir("mc_inst_a"), &addrs[0], 1).await;
    a.svc.bootstrap().await;
    for _ in 0..10 {
        a.add(1).await;
    }
    snapshot_and_compact(&a.svc).await;
    let b = Member::launch(&scratch_dir("mc_inst_b"), &addrs[1], 1).await;
    b.svc.join(&vec![addrs[0].clone()]).await.expect("join after compaction");
    let all: Vec<&Member> = vec![&a, &b];
    assert_eq!(super::multi_member_tests::wait_for_values(&all, 10).await, vec![10, 10]);
    let mut expected = vec![addrs[0].clone(), addrs[1].clone()];
    expected.sort();
    assert!(b.svc.read_meta().await.last_snapshot_index > 0, "b did not install the snapshot");
    assert_eq!(voter_addresses(&b).await, expected);
    let b = restart(b).await;
    assert_eq!(voter_addresses(&b).await, expected);
}

/// Rule C2: a configuration entry that a new leader's entry replaces is gone
/// from the voters.
#[tokio::test(flavor = "multi_thread")]
async fn a_discarded_config_entry_leaves_the_voters() {
    let member = Member::launch(&scratch_dir("mc_trunc"), &addresses(1)[0], NEVER).await;
    let spare = "127.0.0.1:1".to_string();
    let stale_leader = 11u64;
    let first = |id, address: &String| {
        let mut entry = config_entry(address);
        entry.id = id;
        entry.term = 1;
        entry
    };
    let (_, result) = Service::append_entries(
        member.svc.as_ref(),
        PlaneId::type1(),
        1,
        stale_leader,
        0,
        0,
        &Some(vec![first(1, &member.addr), first(2, &spare)]),
        1,
    )
    .await;
    assert!(matches!(result, AppendEntriesResult::Ok), "{:?}", result);
    assert_eq!(voter_addresses(&member).await.len(), 2, "the held entry counts at once");
    // A leader of term 2 replaces entry 2.
    let mut replacement = add_entry(2, 2, 1);
    replacement.term = 2;
    let (_, result) = Service::append_entries(
        member.svc.as_ref(),
        PlaneId::type1(),
        2,
        12,
        1,
        1,
        &Some(vec![replacement]),
        1,
    )
    .await;
    assert!(matches!(result, AppendEntriesResult::Ok), "{:?}", result);
    assert_eq!(voter_addresses(&member).await, vec![member.addr.clone()]);
}

/// Observed in the campaign (K-GR3d): after both followers were restarted, a
/// stop of the leader left the plane without a leader for ever, because a
/// restarted member stayed in state `Undefined` and never timed out. A restarted
/// voter is a follower with an election timer.
#[tokio::test(flavor = "multi_thread")]
async fn restarted_followers_elect_a_leader_when_the_leader_stops() {
    let mut members = cluster("mc_elect", 3, NEVER).await;
    let leader = members.remove(0);
    let f2 = members.pop().unwrap();
    let f1 = members.pop().unwrap();
    let f1 = restart(f1).await;
    let f2 = restart(f2).await;
    leader.stop().await;
    // The election timeout is 10 to 30 s.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let mut elected = false;
    while std::time::Instant::now() < deadline {
        if f1.is_leader().await || f2.is_leader().await {
            elected = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(elected, "no leader 60 s after the leader stopped");
}

/// Observed in the campaign (K-GR5n): a restarted member asked a node that the
/// cluster named as leader, and that node answered `NotLeader(0)` for ever.
/// Read in the code: a leader that steps down because a follower reports a
/// higher term took the leader of that follower as its own leader. A follower
/// that became a candidate still names the old leader, which is the stepped
/// down member itself. The member must name no leader until a heartbeat of the
/// new term arrives.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_that_steps_down_on_a_higher_term_does_not_name_itself() {
    let mut members = cluster("mc_stepdown", 3, NEVER).await;
    let leader = members.remove(0);
    assert!(leader.is_leader().await);
    let leader_id = leader.svc.id;
    for follower in &members {
        // The follower campaigned in a term that the leader did not see. It
        // keeps the old leader as its leader, as `become_candidate` does.
        let mut meta = follower.svc.write_meta().await;
        assert_eq!(meta.leader_id, leader_id);
        meta.term += 5;
    }
    // A replication round learns the higher term from the followers. No
    // quorum answers, so the command is not acknowledged.
    let response = Service::c_command(leader.svc.as_ref(), PlaneId::type1(), add_entry(0, 0, 1)).await;
    assert!(!matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
    assert!(!leader.is_leader().await, "the leader did not step down");
    let meta = leader.svc.read_meta().await;
    assert_ne!(meta.leader_id, leader_id, "a follower names itself as leader");
    drop(meta);
    let info = leader.svc.cluster_info().await;
    assert_ne!(info.leader_id, leader_id);
}

/// A follower whose last heartbeat exceeds its election timeout names no leader.
/// Observed: without this rule, K-G3d completed 184 events, then server 0 timed
/// out at start during event 185. Its server-0-20.stderr records 2 redirects
/// from member 8393045598479251070 to itself, member 2412923645008276890.
/// Read: cluster_info rejects leader 0. This rule excludes an expired hint
/// from discovery; command replies still use the raw leader_id.
/// Hypothesis: the expired hint caused the start timeout. The log does not
/// record heartbeat ages or discovery replies. K-GR5n and K-X3n also timed
/// out, but their failed start logs contain no notleader-redirect line.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_names_no_leader_after_its_election_timeout() {
    let mut members = cluster("mc_stale_name", 3, NEVER).await;
    let _leader = members.remove(0);
    let follower = members.pop().unwrap();
    assert_ne!(follower.svc.cluster_info().await.leader_id, 0);
    {
        let mut meta = follower.svc.write_meta().await;
        meta.last_checked -= meta.timeout + 1;
    }
    assert_eq!(follower.svc.cluster_info().await.leader_id, 0);
}

/// IC1 (RC8): the first server that names a leader answers the cluster
/// information, so the cursor of a new client may come from a follower that
/// lags. `refresh_leader(0)` must raise the cursor to the leader's position.
/// Both followers lag in every round, so only a read from the leader gives the
/// right cursor. The probe order of `cluster_info` is random, so the test makes
/// 10 clients; without the change each round has a 2 in 3 chance to fail.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_leader_takes_the_cursor_from_the_leader() {
    let mut members = cluster("mc_cursor", 3, NEVER).await;
    let leader = members.remove(0);
    for value in 1..=3 {
        let response = command_with_retry(&leader, add_entry(0, 0, value)).await;
        assert!(matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
    }
    let leader_last = leader.svc.cluster_info().await.last_log_id;
    for round in 0..10 {
        for follower in &members {
            // The follower lags by one entry (a heartbeat restores it).
            let meta = follower.svc.write_meta().await;
            let mut logs = meta.logs.write().await;
            let last = *logs.keys().next_back().unwrap();
            if last >= leader_last {
                logs.remove(&last);
            }
        }
        let first = &members[round % members.len()];
        let client = RaftClient::new(&vec![first.addr.clone()], leader.svc.options.service_id)
            .await
            .unwrap();
        assert!(
            client.read_cursor().1 < leader_last,
            "test setup: the first answer must lag"
        );
        assert!(client.refresh_leader(0).await);
        assert!(
            client.read_cursor().1 >= leader_last,
            "round {}: the cursor {:?} is below the leader's log id {}",
            round,
            client.read_cursor(),
            leader_last
        );
    }
}

/// IC3: the restore refuses a store that names two or more voters, and it
/// changes nothing. With `force_new_cluster` it makes the member the only
/// voter. The second run changes nothing more (IC4).
#[tokio::test(flavor = "multi_thread")]
async fn the_restore_refuses_a_store_with_two_voters_unless_it_is_forced() {
    let mut members = cluster("mc_restore2", 2, NEVER).await;
    let second = members.pop().unwrap();
    let first = members.pop().unwrap();
    let (dir, _addr) = second.stop().await;
    first.stop().await;
    let new_address = addresses(1).remove(0);
    let storage = disk_options(&dir, NEVER, NEVER).storage;
    let refused = restore_sole_voter(storage.clone(), &new_address, DEFAULT_SERVICE_ID, false).await;
    match refused {
        Err(PlaneError::RestoreRefused { voters, .. }) => assert_eq!(voters.len(), 2),
        other => panic!("the restore of a 2-voter store was not refused: {:?}", other),
    }
    // The refusal wrote nothing: the store still names two voters.
    let again = restore_sole_voter(storage.clone(), &new_address, DEFAULT_SERVICE_ID, false).await;
    assert!(matches!(again, Err(PlaneError::RestoreRefused { .. })));
    let forced = restore_sole_voter(storage.clone(), &new_address, DEFAULT_SERVICE_ID, true)
        .await
        .unwrap();
    let removed: usize = forced.iter().map(|plane| plane.removed.len()).sum();
    assert_eq!(removed, 2, "{:?}", forced);
    // A second run (even without force: one voter is left) changes nothing.
    let second_run = restore_sole_voter(storage, &new_address, DEFAULT_SERVICE_ID, false)
        .await
        .unwrap();
    assert!(second_run.iter().all(|plane| plane.removed.is_empty()), "{:?}", second_run);
}

/// Control for C6: after the first change commits, a second change is admitted.
#[tokio::test(flavor = "multi_thread")]
async fn control_a_second_voter_change_after_the_first_commits_is_admitted() {
    let members = cluster("mc_c6_ok", 3, NEVER).await;
    let spare1 = Member::launch(&scratch_dir("mc_c6_ok_s1"), &addresses(1)[0], NEVER).await;
    let spare2 = Member::launch(&scratch_dir("mc_c6_ok_s2"), &addresses(1)[0], NEVER).await;
    let leader = &members[0];
    let first = command_with_retry(leader, config_entry(&spare1.addr)).await;
    assert!(matches!(first, ClientCmdResponse::Success { .. }), "{:?}", first);
    let second = command_with_retry(leader, config_entry(&spare2.addr)).await;
    assert!(matches!(second, ClientCmdResponse::Success { .. }), "{:?}", second);
    let voters = voter_addresses(leader).await;
    assert!(voters.contains(&spare1.addr) && voters.contains(&spare2.addr), "{:?}", voters);
}

/// C7 with several members: a leader of a new term has not committed an entry
/// of its term. Its first voter change commits a no-op first, and that no-op
/// reaches the followers before the change does. Without C7 two overlapping
/// changes across a leader change can give two leaders in one term (model
/// `control-no-own-term-commit-rule-4voters`).
#[tokio::test(flavor = "multi_thread")]
async fn a_voter_change_of_a_new_term_follows_a_committed_noop_on_every_follower() {
    let members = cluster("mc_c7_multi", 3, NEVER).await;
    let spare = Member::launch(&scratch_dir("mc_c7_multi_spare"), &addresses(1)[0], NEVER).await;
    let leader = &members[0];
    // The leader enters a new term without an entry of that term.
    {
        let mut meta = leader.svc.write_meta().await;
        meta.term += 1;
        persist_hard_state(&meta).await.unwrap();
    }
    for follower in &members[1..] {
        let mut meta = follower.svc.write_meta().await;
        meta.term += 1;
        persist_hard_state(&meta).await.unwrap();
    }
    let response = command_with_retry(leader, config_entry(&spare.addr)).await;
    assert!(matches!(response, ClientCmdResponse::Success { .. }), "{:?}", response);
    for member in &members {
        let log = entries(member).await;
        let noop = log.iter().position(|e| e.sm_id == NOOP_SM_ID);
        let change = log
            .iter()
            .rposition(|e| config_change(e) == Some(ConfigChange::Add(spare.addr.clone())));
        assert!(noop.is_some() && change.is_some(), "{}: noop {:?} change {:?}", member.addr, noop, change);
        assert!(noop < change, "{}: the change came before the no-op", member.addr);
    }
}
