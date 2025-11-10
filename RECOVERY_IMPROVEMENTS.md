# Node Recovery and Temporary Failure Handling Improvements

## Overview

This document describes the improvements made to handle nodes that temporarily miss heartbeats due to being under load, ensuring they can properly recover and rejoin the cluster either as a leader or follower.

## Problem Statement

When running under heavy load, nodes may temporarily fail to respond to heartbeats, leading to:

1. **Premature offline marking**: Nodes marked offline after a single timeout
2. **Leadership churn**: Rapid leadership changes causing instability  
3. **Flapping**: Nodes bouncing between online/offline states
4. **Panic on errors**: Unwrap() calls causing crashes during transient failures

## Solutions Implemented

### 1. Grace Period with Consecutive Failure Tracking

**New Configuration Constants:**
```rust
static MAX_TIMEOUT: i64 = 10_000;              // 10 seconds before considering potentially offline
static OFFLINE_GRACE_CHECKS: u8 = 3;           // Require 3 consecutive failures before marking offline
static ONLINE_GRACE_CHECKS: u8 = 2;            // Require 2 consecutive successes before marking online
static MIN_STATE_CHANGE_INTERVAL: i64 = 5_000; // Minimum 5 seconds between state changes
```

**Benefits:**
- **Resilience**: Tolerates temporary hiccups (up to 3 timeout checks × 500ms = ~1.5 seconds grace)
- **Anti-flapping**: Minimum 5 second interval prevents rapid state oscillation
- **Smooth recovery**: Requires 2 consecutive successful heartbeats before marking node back online

### 2. Enhanced HeartbeatStatus Tracking

**New HBStatus Fields:**
```rust
struct HBStatus {
    last_updated: i64,
    online: bool,
    consecutive_failures: u8,      // Count of consecutive timeout checks
    consecutive_successes: u8,     // Count of consecutive successful checks  
    last_state_change: i64,        // Timestamp of last state transition
}
```

**Behavior:**
- **Online → Offline**: Tracks consecutive timeouts, only transitions after reaching threshold AND minimum interval
- **Offline → Online**: Tracks consecutive successful heartbeats, transitions after reaching threshold AND minimum interval
- **Stable states**: Resets counters when nodes are consistently responsive

### 3. Improved Error Handling

All `.unwrap()` calls replaced with proper error handling:

**Fixed Functions:**
- `compose_client_member`: Now returns `Option<ClientMember>` instead of panicking
- `group_leader_candidate_available`: Logs errors instead of panicking
- `group_leader_candidate_unavailable`: Handles all failure cases gracefully
- `notify_for_member_*`: Early returns with error logging on failures
- Mutex lock failures: Gracefully handled with error logging

**Result:**
- No more panics during transient failures
- Clear error logs for debugging
- System continues operating even when individual operations fail

### 4. Leadership Transfer Grace Period

When leadership transfers (e.g., during reelection):
```rust
async fn transfer_leadership(&self) {
    // Give all online members fresh timestamps
    // Reset all failure/success counters
    // Prevents immediate timeout after leadership change
}
```

**Benefits:**
- New leader gets time to stabilize before checking heartbeats
- Prevents cascading failures during leadership transitions
- All members get a "fresh start" under new leadership

## Recovery Scenarios

### Scenario 1: Node Under Temporary Load

**Timeline:**
1. Node A is leader and becomes overloaded
2. Misses heartbeat at T+10s (consecutive_failures = 1)
3. Misses heartbeat at T+10.5s (consecutive_failures = 2)  
4. Misses heartbeat at T+11s (consecutive_failures = 3)
5. **Now marked offline** (after 3 consecutive failures)
6. Leadership election: Node B becomes leader
7. Node A recovers, starts sending heartbeats again
8. Receives heartbeat at T+15s (consecutive_successes = 1)
9. Receives heartbeat at T+15.5s (consecutive_successes = 2)
10. **Marked back online** (after 2 consecutive successes AND 5s minimum interval)
11. Node A becomes follower of Node B

**Key Points:**
- ~1.5 second tolerance before marking offline (3 × 500ms checks)
- Minimum 5 second offline period (anti-flapping protection)
- Node A does NOT automatically reclaim leadership (stability)
- Node A properly syncs as follower under Node B

### Scenario 2: Brief Network Hiccup

**Timeline:**
1. Node experiences single timeout (consecutive_failures = 1)
2. Next heartbeat succeeds (consecutive_failures reset to 0)
3. **Node remains online** - no state change

**Key Points:**
- Single hiccups don't trigger state changes
- Prevents unnecessary leadership elections
- Maintains cluster stability

### Scenario 3: Persistent Failure

**Timeline:**
1. Node genuinely fails (hardware/crash)
2. Consecutive failures accumulate: 1, 2, 3
3. Marked offline after 3 checks
4. Leadership transfers to healthy node
5. Eventually removed from cluster if doesn't recover

**Key Points:**
- Real failures still detected quickly (~1.5 seconds)
- System continues with remaining healthy nodes
- No false positives from temporary load

## Monitoring and Observability

### New Log Messages

**During failure detection:**
```
DEBUG: Member 12345 timeout check 1/3 (10500ms since last update, 2000ms since last state change)
DEBUG: Member 12345 timeout check 2/3 (11000ms since last update, 2500ms since last state change)  
WARN:  Marking member 12345 as offline after 3 consecutive timeout checks (11500ms since last update)
```

**During recovery:**
```
DEBUG: Member 12345 recovery check 1/2 (3000ms since last state change)
INFO:  Marking member 12345 as back online after 2 consecutive successful checks
```

**Error scenarios:**
```
ERROR: Failed to change leader for group 789 to member 12345
ERROR: Failed to find online member for group 789 after member 12345 became unavailable
ERROR: Failed to compose client member 12345 for online notification
```

## Configuration Tuning

You can adjust these constants based on your needs:

- **Increase `OFFLINE_GRACE_CHECKS`**: More tolerance for slow responses (longer detection time)
- **Decrease `OFFLINE_GRACE_CHECKS`**: Faster failure detection (less tolerance)
- **Increase `MIN_STATE_CHANGE_INTERVAL`**: More aggressive anti-flapping (longer recovery time)
- **Decrease `MIN_STATE_CHANGE_INTERVAL`**: Faster recovery (more risk of flapping)
- **Increase `MAX_TIMEOUT`**: More lenient heartbeat requirements
- **Decrease `MAX_TIMEOUT`**: Stricter heartbeat requirements

## Testing Recommendations

1. **Load testing**: Verify nodes can recover under realistic load
2. **Network partition**: Test with simulated network splits
3. **Chaos testing**: Randomly kill/restart nodes to test recovery paths
4. **Long-running stability**: Monitor for log growth and state flapping

## Backward Compatibility

All changes are backward compatible:
- Wire protocol unchanged
- State machine behavior unchanged (only timing/resilience improved)
- Existing clusters will benefit immediately upon upgrade

## Performance Impact

- **Minimal CPU overhead**: Simple counter increments
- **Minimal memory overhead**: 3 extra bytes per member (2 u8 counters + i64 timestamp)
- **Reduced network churn**: Fewer unnecessary state changes = less Raft log entries
- **Improved stability**: Less leadership churn = better overall performance

## Future Enhancements

Potential future improvements:
1. **Configurable parameters**: Make timeouts/thresholds runtime-configurable
2. **Adaptive timeouts**: Adjust based on observed network latency
3. **Priority-based leader election**: Prefer certain nodes as leaders
4. **Health scoring**: Multi-factor health beyond just heartbeats
5. **Metrics export**: Prometheus/OpenTelemetry integration for monitoring

