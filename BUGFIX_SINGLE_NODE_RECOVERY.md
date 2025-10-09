# Bug Fix: Single-Node Raft Cluster Recovery

## Issue

Single-node Raft clusters failed to elect themselves as leader after recovering from persistent storage (disk). The node would recover its logs and state correctly, but remain in an undefined membership state with `leader_id = 0`, never transitioning to leader.

### Symptoms

```
[DEBUG] Discovered zero leader id from 127.0.0.1:16800
[DEBUG] This fail attempt have zero leader id, retry...
[ERROR] Cannot update info, cannot get cluster info
```

RaftClient would fail to connect because no leader was ever elected, causing applications to fail on restart.

## Root Cause

When a Raft service started with recovered state from disk, it:
1. ✅ Loaded logs from WAL correctly
2. ✅ Loaded snapshot if available
3. ✅ Recovered term, commit_index, last_applied
4. ✅ Detected it was the only cluster member
5. ❌ But stayed in `Membership::Undefined` state
6. ❌ Never transitioned to `Leader` role

The code had no logic to handle the single-node recovery case. Multi-node clusters would eventually elect a leader through the normal timeout+election process, but single-node clusters have no other nodes to coordinate with, so they need special handling.

## Solution

Added automatic leader transition for single-node clusters after recovery in `RaftService::start()`:

```rust
// FIX: Single-node cluster recovery from disk
// If we recovered state from disk and we're the only member,
// immediately become leader (no election needed)
let num_members = sm.configs.members.len();
let has_logs = !meta.logs.read().await.is_empty();
let has_term = meta.term > 0;

if (recovered_from_disk || has_logs || has_term) && num_members == 1 {
    info!(
        "Single-node cluster detected after recovery (term={}, logs={}, members={}). Becoming leader immediately.",
        meta.term, has_logs, num_members
    );
    let (last_log_id, _) = {
        let logs = meta.logs.read().await;
        get_last_log_info!(server, logs)
    };
    drop(sm); // Release state machine lock before become_leader
    server.become_leader(&mut meta, last_log_id).await;
    info!("Successfully transitioned to Leader state");
}
```

### Detection Logic

The fix detects single-node recovery by checking:
- `recovered_from_disk`: Snapshot was loaded from disk, OR
- `has_logs`: WAL logs were recovered (non-empty logs BTreeMap), OR
- `has_term`: Raft term > 0 (indicates previous state)

AND:
- `num_members == 1`: Only one member in the cluster

If all conditions are met, the node immediately transitions to Leader without waiting for timeout or election.

## Files Modified

- **`src/raft/mod.rs`**: Added single-node recovery logic in `start()` method (lines 407-443)
- **`tests/single_node_recovery_test.rs`**: New comprehensive test suite

## Testing

Created two new tests that verify the fix:

### Test 1: `test_single_node_cluster_recovery_becomes_leader` ✅

Tests basic single-node recovery:
1. Creates single-node cluster with disk storage
2. Generates logs via state machine commands
3. Shuts down gracefully
4. Restarts with same storage
5. **Verifies node becomes leader immediately** ✅
6. Verifies RaftClient can connect

**Result**: PASS

### Test 2: `test_single_node_multiple_restart_cycles` ✅

Tests multiple restart cycles:
1. Performs 3 complete restart cycles
2. Each cycle generates and persists more logs
3. Verifies leader election succeeds in all cycles
4. Confirms cumulative log recovery (2, 4, 6 logs)

**Result**: PASS

### Running the Tests

```bash
# Run both tests
cargo test --test single_node_recovery_test -- --test-threads=1

# Results:
test test_single_node_cluster_recovery_becomes_leader ... ok
test test_single_node_multiple_restart_cycles ... ok
test result: ok. 2 passed; 0 failed; 0 ignored
```

## Impact

This fix enables:
- ✅ Production deployments with single-node Raft clusters
- ✅ Development and testing with persistent storage
- ✅ Disaster recovery scenarios for single-node setups
- ✅ Server restarts without manual intervention

## Standard Raft Behavior

This implementation follows standard Raft protocol:
- **Single-node clusters**: Skip election, immediately become leader
- **Multi-node clusters**: Use normal timeout + election process

This is the expected behavior in Raft implementations like etcd, Consul, and others.

## Before vs After

### Before (Broken)
```
Server restarts
  ↓
Loads state from disk (term=2, logs=2)
  ↓
Membership = Undefined
  ↓
Checker task runs
  ↓
Never times out (waiting for leader)
  ↓
❌ leader_id stays 0 forever
  ↓
❌ RaftClient cannot connect
```

### After (Fixed)
```
Server restarts
  ↓
Loads state from disk (term=2, logs=2)
  ↓
Detects single-node + recovered state
  ↓
✅ Immediately becomes Leader
  ↓
Membership = Leader
  ↓
✅ leader_id = self.id
  ↓
✅ RaftClient connects successfully
```

## Verification

The fix was verified with debug logging showing the exact flow:

```
[DEBUG] Post-recovery check: recovered_from_disk=false, num_members=1, 
        has_logs=true, has_term=true, membership="Undefined"
[INFO] Single-node cluster detected after recovery (term=2, logs=true, members=1). 
       Becoming leader immediately.
[INFO] Successfully transitioned to Leader state
```

## Related Documentation

- **WAL_GUIDE.md**: Write-Ahead Log implementation
- **SNAPSHOT_GUIDE.md**: Snapshot and recovery process
- **GRACEFUL_SHUTDOWN.md**: (deleted) Shutdown procedures

---

**Fixed**: October 9, 2025  
**Affects**: Bifrost v0.1.0 and earlier  
**Severity**: Critical for single-node deployments

