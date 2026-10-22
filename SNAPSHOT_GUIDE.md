# Snapshot, Checkpointing, and Recovery Guide

## Overview

Bifrost's Raft implementation now includes production-ready snapshot, checkpointing, and recovery functionality. This prevents unbounded memory growth and enables fast recovery after restarts.

## Features

✅ **Automatic Snapshot Creation**: Triggered by configurable log count thresholds  
✅ **Persistent Storage**: Atomic writes with CRC32 corruption detection  
✅ **Crash Recovery**: Automatically loads snapshots on restart  
✅ **Log Compaction**: Removes old logs from memory after snapshots  
✅ **Follower Catch-up**: Automatically sends snapshots to lagging nodes  
✅ **Corruption Handling**: Graceful fallback when snapshot files are corrupted  

## Quick Start

### 1. Basic Setup with Snapshots

```rust
use bifrost::raft::{RaftService, Options, Storage, DEFAULT_SERVICE_ID};
use bifrost::raft::disk::DiskOptions;
use bifrost::rpc::Server;

#[tokio::main]
async fn main() {
    // Create Raft service with disk storage
    let service = RaftService::new(Options {
        storage: Storage::DISK(DiskOptions::new("/var/lib/myapp/raft".to_string())),
        address: "127.0.0.1:5000".to_string(),
        service_id: DEFAULT_SERVICE_ID,
    });
    
    let server = Server::new(&"127.0.0.1:5000");
    server.register_service(&service).await;
    Server::listen_and_resume(&server).await;
    
    // Start automatically recovers from snapshot if it exists!
    RaftService::start(&service).await;
    
    // Bootstrap or join cluster
    service.bootstrap().await;
    // OR: service.join(&vec!["existing-node:5000".to_string()]).await;
}
```

### 2. Custom Configuration

```rust
use bifrost::raft::disk::DiskOptions;

let custom_opts = DiskOptions {
    path: "/data/raft".to_string(),
    take_snapshots: true,              // Enable snapshots
    append_logs: true,                  // Enable log persistence
    trim_logs: true,                    // Enable log trimming
    snapshot_log_threshold: 5000,       // Snapshot every 5000 applied logs
    log_compaction_threshold: 10000,    // Compact when > 10000 logs
};

let service = RaftService::new(Options {
    storage: Storage::DISK(custom_opts),
    address: "127.0.0.1:5000".to_string(),
    service_id: DEFAULT_SERVICE_ID,
});
```

## How It Works

### Automatic Snapshot Creation

**When**: After the leader applies `snapshot_log_threshold` logs since the last snapshot

**What happens**:
1. Leader generates snapshot from all state machines
2. Persists snapshot to disk with CRC32 checksum
3. Updates snapshot metadata (index, term)
4. Compacts old logs from memory (if > compaction_threshold)

```rust
// Triggered automatically in try_sync_log_to_followers()
// After successfully committing logs to followers
if should_take_snapshot() {
    take_snapshot();  // Generates, persists, and compacts
}
```

### Startup Recovery

**When**: Every time `RaftService::start()` is called

**What happens**:
1. Checks if snapshot file exists on disk
2. Validates CRC32 checksum
3. Deserializes snapshot data
4. Calls `state_machine.recover(snapshot_data)`
5. Updates indices and metadata
6. Compacts logs already covered by snapshot
7. Stages the image of each state machine that is not registered yet; it is
   restored when the state machine registers, before the commands buffered
   for it are replayed
8. Continues normal operation

Until `start` loads the snapshot, the member answers no AppendEntries,
InstallSnapshot or RequestVote. Until `recover_after_register` completes, the
leader commits nothing on its own (the no-op of a new term waits).

```rust
// Automatically called in RaftService::start()
load_snapshot_on_startup();
```

### Follower Catch-up with Snapshots

**When**: A follower needs logs that the leader has already compacted

**Scenarios**:
- New node joining the cluster
- Node was offline during log compaction
- Node is too slow and fell far behind

**What happens**:
1. Leader detects: `follower.next_index <= leader.last_snapshot_index`
2. Leader reads its PERSISTED snapshot (`snapshot.dat`): image, index and term
   were written together. Never the live state machine: it is ahead of the
   snapshot index, and a follower would apply the entries in between twice.
3. Leader sends it via the `install_snapshot` RPC, labelled with the
   snapshot's own index and term
4. Follower refuses it from a stale term, and ignores it when it has already
   applied up to that index (a retransmitted or older snapshot changes nothing)
5. Follower persists the snapshot (a failed write installs nothing; the
   leader sends it again)
6. Follower keeps its log after the snapshot index only if it holds the
   snapshot's last entry with the same term; otherwise its whole log is
   superseded, in memory and in `log.dat`
7. Follower recovers its registered state machines and member list from it
   (later registrations recover at registration)
8. Follower continues with normal log replication from `index + 1`

```rust
// In send_follower_heartbeat()
if follower.next_index <= last_snapshot_index {
    let snapshot = StorageEntity::read_snapshot_at(dir, plane_id).await?;
    rpc.install_snapshot(
        plane_id,
        term,
        leader_id,
        snapshot.last_included_index,
        snapshot.last_included_term,
        snapshot.snapshot,
    ).await;
}
```

## Implementing Snapshotable State Machines

Your state machines must implement `snapshot()` and `recover()`:

```rust
use bifrost::raft::state_machine::StateMachineCtl;
use futures::future::BoxFuture;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct MyState {
    counter: i64,
    data: HashMap<String, String>,
}

struct MyStateMachine {
    state: MyState,
}

impl StateMachineCtl for MyStateMachine {
    fn id(&self) -> u64 { 42 }
    
    fn snapshot(&self) -> Option<Vec<u8>> {
        // Serialize your entire state
        let data = bincode::serialize(&self.state).ok()?;
        Some(data)
    }
    
    fn recover(&mut self, data: Vec<u8>) -> BoxFuture<()> {
        // Deserialize and restore state
        if !data.is_empty() {
            if let Ok(state) = bincode::deserialize(&data) {
                self.state = state;
                println!("State machine recovered: counter={}", self.state.counter);
            }
        }
        Box::pin(async {})
    }
    
    // ... command and query handlers ...
}
```

## File Layout

When using disk storage, the following files are created:

```
/var/lib/myapp/raft/
├── log.dat           # Persisted Raft log entries
├── commit.idx        # Commit index (replay bound after a restart)
├── hardstate.dat     # Current term and vote (Raft's persistent state)
├── snapshot.dat      # Latest snapshot with CRC32
└── *.tmp             # Temporary files during atomic replacement
```

### Hard State (`hardstate.dat`)

The current term and the vote cast in it, as Raft requires them to survive a
crash. A fixed 25-byte record -- `"RHS3"`, term (u64), vote flag (u8), vote
(u64), CRC32 of the preceding bytes -- replaced whole: temp file, fsync,
rename, fsync of the directory. It is written only when the term or vote
changes, and always before anything depends on it: a vote is granted, a
candidate sends RequestVote, a member answers in an adopted term, a leader
appends in its term.

On a restart the persisted term is authoritative; "the term is at least every
log entry's term and the snapshot's term" is checked, and violations are
logged and raised. A brand-new store creates `hardstate.dat` at term 0; a
store with a log, snapshot or commit index but no hard state refuses to start
(`MissingHardState`) -- there is no derivation for stores written before the
hard state existed, they are reimported. A damaged file is an error at
startup too: guessing the term or forgetting the vote is how a member votes
twice.

The last byte of the magic is the version of the whole raft store of the
plane (`RHS3`). A store of another version (`RHS1`, `RHS2`) gives the typed
`BadHardState` version error before the WAL is read, and its files are not
changed. Such a store is imported again; there is no decoder for an old
version.

### WAL Start Rules (`log.dat`)

A store opens in this order: the hard state (the version), the check for
history without a hard state, the durable commit index (`commit.idx`), then
the WAL. The WAL has a record at every log id: a command of a state machine
that is not recoverable is kept as a no-op record with the same id and term.

| WAL state at start | Result |
|---|---|
| A damaged record (length, CRC or decode) that more records follow | `DamagedWal`: the start stops, the file is not cut |
| A damaged tail (the last record, a length past the file end, or a zero-filled rest), and the durable commit index is at or below the last intact record or the snapshot index | The tail is cut; the next append starts after the last intact record |
| A damaged tail, and the durable commit index is above both the last intact record and the snapshot index | `DamagedCommittedTail`: the start stops, the file is not cut |
| A hole in the log above the snapshot, or a log that ends below the commit index | `LogHole`: the start stops |

Each error is a typed `io::Error` (`PlaneError::StorageInit` for a plane).
Entries above the durable commit index can still be cut with a damaged tail.
A follower writes `commit.idx` only at flush or shutdown.

### Snapshot File Format

```
[4 bytes]  CRC32 checksum
[8 bytes]  Data length
[N bytes]  Serialized SnapshotEntity:
           {
               last_included_index: u64,
               last_included_term: u64,
               snapshot: Vec<u8>  // Serialized state machine data
           }
```

## Monitoring

### Check Snapshot Status

```rust
let meta = service.read_meta().await;
println!("Last snapshot: index={}, term={}", 
    meta.last_snapshot_index,
    meta.last_snapshot_term);

let num_logs = service.num_logs().await;
println!("Logs in memory: {}", num_logs);
```

### Manually Trigger Snapshot (Advanced)

```rust
// Normally automatic, but you can manually trigger:
let mut meta = service.write_meta().await;
service.take_snapshot(&mut meta).await;
```

## Safety Guarantees

### 1. **Crash Safety**
- Atomic writes using temp file + rename pattern
- If process crashes during snapshot write, old snapshot remains intact

### 2. **Corruption Detection**
- CRC32 checksum on snapshots and the hard state
- A damaged snapshot, hard state or commit index stops the plane at startup
  instead of being ignored: each holds state the log alone cannot rebuild (an
  installed snapshot, a vote, the replay bound). An empty `snapshot.dat`
  means "no snapshot yet".

### 2a. **Persistence Failures Are Refused, Never Acknowledged**
- A follower whose WAL write, fsync, suffix cut or hard-state write fails
  answers AppendEntries `NotPersisted` and does not move its commit index; a
  failed InstallSnapshot write installs nothing.
- A leader whose WAL append, fsync, commit-index write or hard-state write
  fails answers the client `StorageFailure` (`ExecError::StorageFailure`) and
  does not commit; three failures in a row make it step down.
- A failed WAL batch is cut back off the file; if that cut fails too, the WAL
  refuses all writes until a restart replays what is on disk.
- A vote whose write fails is answered "not granted".

### 3. **Raft Correctness**
- Snapshots track correct term and index
- No safety violations from compaction
- Follows Raft paper specifications

### 4. **Consistency**
- Followers always get consistent state via snapshots
- State machine recovery is deterministic
- All nodes eventually converge to same state

## Configuration Recommendations

### Small Applications (< 1000 ops/sec)
```rust
snapshot_log_threshold: 1000,
log_compaction_threshold: 2000,
```

### Medium Applications (1000-10000 ops/sec)
```rust
snapshot_log_threshold: 5000,
log_compaction_threshold: 10000,
```

### Large Applications (> 10000 ops/sec)
```rust
snapshot_log_threshold: 10000,
log_compaction_threshold: 20000,
```

### Memory-Constrained Systems
```rust
snapshot_log_threshold: 500,   // Snapshot more frequently
log_compaction_threshold: 1000, // Compact aggressively
```

## Troubleshooting

### Issue: Logs keep growing
**Solution**: Check that `take_snapshots: true` and thresholds are set appropriately

### Issue: Snapshot file not created
**Solution**: 
- Verify disk permissions on path
- Ensure state machines implement `snapshot()` correctly
- Check logs for error messages

### Issue: Follower doesn't catch up
**Solution**:
- Check network connectivity
- Verify `install_snapshot` RPC is working
- Check follower logs for error messages

### Issue: Corrupted snapshot detected
The plane refuses to start rather than run without it.
**Solution**:
- Investigate disk issues first
- Only on a member whose log still holds every entry (the WAL is never
  trimmed on disk, but a member that received its state by InstallSnapshot
  does not hold the entries below it) is deleting `snapshot.dat` safe: it
  then rebuilds from the log. Otherwise restore the member from a healthy
  replica.

## Performance Considerations

### Snapshot Creation Cost
- **Time**: O(state_size) to serialize state
- **Disk I/O**: One sequential write
- **Memory**: Temporary copy of state during serialization

### Log Compaction Cost
- **Time**: O(logs_to_remove) to filter BTreeMap
- **Memory**: Immediate reduction after compaction

### Recovery Cost
- **Time**: O(state_size) to deserialize snapshot + O(remaining_logs)
- **Disk I/O**: One sequential read

## Testing

Run all snapshot tests:

```bash
cargo test --lib test_snapshot test_log_compaction test_state_machine_snapshot test_install
```

Individual tests:
- `test_snapshot_write_and_read` - I/O functionality
- `test_snapshot_corruption_detection` - CRC validation
- `test_log_compaction_removes_old_logs` - Memory reduction
- `test_snapshot_threshold_configuration` - Threshold logic
- `test_state_machine_snapshot_and_recovery` - SM serialization
- `test_install_snapshot_compacts_logs` - Follower catch-up
- `snapshot_disk_persistence` - End-to-end persistence
- `snapshot_persistence_and_recovery` - Full recovery cycle

## Example: Multi-Server Deployment

```rust
// server1.rs (Leader)
let service = RaftService::new(Options {
    storage: Storage::DISK(DiskOptions::new("/data/node1".to_string())),
    address: "10.0.1.10:5000".to_string(),
    service_id: DEFAULT_SERVICE_ID,
});
// ... setup ...
service.bootstrap().await;

// server2.rs (Follower)
let service = RaftService::new(Options {
    storage: Storage::DISK(DiskOptions::new("/data/node2".to_string())),
    address: "10.0.1.11:5000".to_string(),
    service_id: DEFAULT_SERVICE_ID,
});
// ... setup ...
service.join(&vec!["10.0.1.10:5000".to_string()]).await;

// server3.rs (New node joining later - will get snapshot automatically!)
let service = RaftService::new(Options {
    storage: Storage::DISK(DiskOptions::new("/data/node3".to_string())),
    address: "10.0.1.12:5000".to_string(),
    service_id: DEFAULT_SERVICE_ID,
});
// ... setup ...
service.join(&vec!["10.0.1.10:5000".to_string()]).await;
// ✅ Will automatically receive snapshot if logs are compacted!
```

## Summary

The Raft framework now has **industrial-grade snapshot and recovery capabilities**:

- ✅ Automatic snapshot creation based on thresholds
- ✅ Crash-safe atomic writes to disk
- ✅ Automatic recovery on restart
- ✅ Log compaction to prevent memory leaks
- ✅ Automatic snapshot transfer to lagging/new nodes
- ✅ Corruption detection and handling
- ✅ Fully tested with comprehensive test suite

New nodes joining the cluster automatically receive snapshots if they're too far behind - no manual intervention needed!

