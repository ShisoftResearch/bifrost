# Membership Guide

This document explains how membership works in Bifrost and the difference between **Raft Cluster Membership** and the **Membership Service**.

## Two Types of Membership

Bifrost has two distinct membership systems that serve different purposes:

### 1. Raft Cluster Membership (PERSISTED ✅)

**Location**: `src/raft/state_machine/configs.rs`

**Purpose**: Tracks which servers are part of the Raft consensus cluster

**Persistence**: **YES** - Fully persisted to disk via:
- Write-Ahead Log (WAL)
- Snapshots

**Members**: Raft servers that participate in consensus (leader election, log replication)

**Operations**:
- `new_member_(address)` - Add a Raft server to the cluster
- `del_member_(address)` - Remove a Raft server from the cluster
- `member_address()` - Query all Raft cluster members

**Recovery**: On restart, Raft cluster membership is recovered from:
1. Latest snapshot on disk
2. WAL log replay

**Why Persisted?**: Critical for Raft consensus. The cluster must know its membership to:
- Calculate quorum (majority)
- Elect leaders
- Replicate logs correctly

**Code Example**:
```rust
// These members are persisted and recovered on restart
service.join(&vec!["node1:5000".to_string()]).await;
```

### 2. Membership Service (NOT PERSISTED ❌)

**Location**: `src/membership/server.rs`

**Purpose**: Tracks member groups, heartbeat status, and online/offline state

**Persistence**: **NO** - Intentionally ephemeral

**Members**: Applications or clients using the membership service for:
- Group membership
- Leader election within groups
- Liveness tracking
- Membership change notifications

**Operations**:
- `join(address)` - Join as a member
- `leave(id)` - Leave the membership service
- `join_group(group_name, id)` - Join a group
- `leave_group(group, id)` - Leave a group
- `ping(id)` - Send heartbeat

**Recovery**: On restart, starts with **empty state** and rebuilds through:
1. Members calling `join()` again
2. Heartbeat `ping()` messages
3. Group operations

**Why NOT Persisted?**: 
- Membership should reflect **current network reality**
- Stale disk state would be misleading after crashes
- Members must actively rejoin to prove they're alive
- Groups are transient application-level constructs

**Code Example**:
```rust
// After restart, this state is gone - members must rejoin
let client = MemberClient::new(...).await;
client.join().await;  // Must be called again after restart
client.join_group("workers".to_string()).await;
```

## Comparison Table

| Feature | Raft Cluster Membership | Membership Service |
|---------|------------------------|-------------------|
| **Persisted** | ✅ Yes (WAL + Snapshot) | ❌ No (Always fresh) |
| **Purpose** | Raft consensus | Application groups/heartbeats |
| **Scope** | Cluster-wide | Per-service |
| **Recovery** | From disk | From network rediscovery |
| **State Machine ID** | `CONFIG_SM_ID` (1) | `DEFAULT_SERVICE_ID` |
| **Critical for Raft** | ✅ Yes | ❌ No |
| **Survives Restart** | ✅ Yes | ❌ No |

## How They Work Together

```
┌─────────────────────────────────────────────────────────┐
│                  Bifrost Cluster                        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Raft Cluster Membership (Persisted)                   │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐               │
│  │ Server1 │  │ Server2 │  │ Server3 │               │
│  │ :5000   │  │ :5001   │  │ :5002   │               │
│  └────┬────┘  └────┬────┘  └────┬────┘               │
│       │            │            │                      │
│       ├────────────┴────────────┤                      │
│       │   Raft Consensus        │                      │
│       │   (Leader Election,     │                      │
│       │    Log Replication)     │                      │
│       └─────────────────────────┘                      │
│                                                         │
│  Membership Service (NOT Persisted - Fresh on restart) │
│  ┌─────────────────────────────────────────────┐      │
│  │  Members: {                                  │      │
│  │    App1 -> online, groups: [workers]        │      │
│  │    App2 -> online, groups: [workers]        │      │
│  │    App3 -> offline, groups: [storage]       │      │
│  │  }                                           │      │
│  │  Groups: {                                   │      │
│  │    workers -> leader: App1, members: [1,2]  │      │
│  │    storage -> leader: None, members: [3]    │      │
│  │  }                                           │      │
│  └─────────────────────────────────────────────┘      │
│           ↑ This is cleared on restart                 │
└─────────────────────────────────────────────────────────┘
```

## Startup Sequence After Restart

### Raft Cluster Membership (Automatic)
```rust
// Server restarts
let raft_service = RaftService::new(options);
RaftService::start(&raft_service).await;

// ✅ Cluster membership automatically recovered from disk
// ✅ Knows about Server1, Server2, Server3
// ✅ Can participate in consensus immediately
```

### Membership Service (Manual Rejoin Required)
```rust
// Server restarts
let membership_client = MemberClient::new(...).await;

// ❌ Membership service starts EMPTY
// ❌ Previous groups/members are forgotten

// ✅ Applications must rejoin
membership_client.join().await;           // Rejoin as member
membership_client.join_group("workers").await;  // Rejoin group
membership_client.start_heartbeat();      // Start sending pings

// ✅ Membership service rebuilds state from these actions
```

## Why This Design?

### Raft Cluster Membership: Persisted
- **Safety**: Raft consensus requires consistent membership for quorum
- **Correctness**: Must survive crashes to maintain cluster integrity
- **Availability**: Cluster can restart without manual intervention

### Membership Service: NOT Persisted
- **Freshness**: Ensures membership reflects current reality
- **Simplicity**: No stale data to reconcile
- **Self-Healing**: Dead members naturally disappear (no heartbeat = offline)
- **Flexibility**: Applications control their own membership lifecycle

## Code Examples

### Example 1: Raft Member Survives Restart

```rust
// Initial setup
let raft = RaftService::new(Options {
    storage: Storage::DISK(disk_opts),
    address: "node1:5000".to_string(),
    service_id: DEFAULT_SERVICE_ID,
});
raft.join(&vec!["node2:5000".to_string()]).await;

// --- CRASH AND RESTART ---

// After restart
let raft = RaftService::new(Options {
    storage: Storage::DISK(disk_opts),  // Same disk path
    address: "node1:5000".to_string(),
    service_id: DEFAULT_SERVICE_ID,
});
RaftService::start(&raft).await;

// ✅ Still knows about node2:5000 (recovered from disk)
// ✅ Can participate in cluster immediately
```

### Example 2: Membership Service Starts Fresh

```rust
// Initial setup
let member = MemberClient::new(...).await;
member.join().await;
member.join_group("workers").await;

// --- CRASH AND RESTART ---

// After restart
let member = MemberClient::new(...).await;

// ❌ Not in any groups
// ❌ Not registered as a member

// Must rejoin explicitly
member.join().await;                  // Required
member.join_group("workers").await;   // Required
member.start_heartbeat();             // Required
```

## Best Practices

### For Raft Cluster Members
1. Use `Storage::DISK` for production deployments
2. Membership changes are committed via Raft consensus
3. No need to rejoin after restart

### For Membership Service Users
1. **Always rejoin after restart**:
   ```rust
   async fn on_startup() {
       member_client.join().await;
       for group in my_groups {
           member_client.join_group(group).await;
       }
       member_client.start_heartbeat();
   }
   ```

2. **Handle disconnections gracefully** - May need to rejoin

3. **Monitor membership changes** via subscriptions:
   ```rust
   client.on_any_member_joined(|member, version| {
       println!("New member joined: {:?}", member);
   }).await;
   ```

## Summary

- **Raft Cluster Membership**: Persisted for consensus correctness ✅
- **Membership Service**: NOT persisted, learns from network ❌
- Both serve different purposes and have different persistence requirements
- This design ensures both safety (for Raft) and freshness (for membership)

