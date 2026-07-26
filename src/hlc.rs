//! Hybrid logical clock (HLC).
//!
//! An `Hlc` packs a 48-bit wall-clock millisecond timestamp above a 16-bit
//! logical counter into a single `u64`, so the derived lexicographic order
//! over `(ts, node)` is total and message chains strictly increase `ts`
//! (the causality-consistency property HLC is built for).

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub const LOGICAL_BITS: u32 = 16;

/// Hybrid logical clock value. `ts` packs 48 bits of wall-clock milliseconds
/// above a 16-bit logical counter, so the derived lexicographic order
/// `(ts, node)` is total, and message chains strictly increase `ts`
/// (causality consistency). 16 bytes, `Copy`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub struct Hlc {
    pub ts: u64,
    pub node: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HlcError {
    Exhausted,
}

impl Hlc {
    /// Wall-clock milliseconds this value is anchored to.
    pub fn wall_ms(&self) -> u64 {
        self.ts >> LOGICAL_BITS
    }
}

/// Per-process clock source. Both operations are single-CAS-loop updates of
/// the packed `ts`, so values from one source strictly increase — `(ts, node)`
/// is unique and usable as a transaction id.
pub struct HlcSource {
    node: u64,
    ts: AtomicU64,
}

impl HlcSource {
    pub fn new(node: u64) -> Self {
        Self {
            node,
            ts: AtomicU64::new(0),
        }
    }

    pub fn node(&self) -> u64 {
        self.node
    }

    fn advance_checked(&self, floor: u64) -> Result<Hlc, HlcError> {
        let phys = Self::packed_phys_ms_checked()?;
        let floor_next = floor.checked_add(1).ok_or(HlcError::Exhausted)?;
        let mut current = self.ts.load(Ordering::Relaxed);
        loop {
            let local_next = current.checked_add(1).ok_or(HlcError::Exhausted)?;
            let next = local_next.max(floor_next).max(phys);
            match self
                .ts
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Ok(Hlc {
                        ts: next,
                        node: self.node,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn packed_phys_ms_checked() -> Result<u64, HlcError> {
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0);
        Self::pack_phys_ms_checked(ms)
    }

    fn pack_phys_ms_checked(ms: u128) -> Result<u64, HlcError> {
        if ms > (u64::MAX >> LOGICAL_BITS) as u128 {
            return Err(HlcError::Exhausted);
        }
        Ok((ms as u64) << LOGICAL_BITS)
    }

    pub fn try_now(&self) -> Result<Hlc, HlcError> {
        self.advance_checked(0)
    }

    pub fn try_observe(&self, remote: Hlc) -> Result<Hlc, HlcError> {
        self.advance_checked(remote.ts)
    }

    /// Local/send event: a fresh value strictly greater than any previously
    /// issued by this source.
    pub fn now(&self) -> Hlc {
        self.try_now().expect("HLC timestamp space exhausted")
    }

    /// Receive event: merge a remote value; the result strictly exceeds both
    /// the remote and all prior local values.
    pub fn observe(&self, remote: Hlc) -> Hlc {
        self.try_observe(remote)
            .expect("HLC timestamp space exhausted")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_advance_refuses_local_wrap() {
        let source = HlcSource::new(7);
        source.ts.store(u64::MAX, Ordering::Relaxed);

        assert_eq!(source.try_now(), Err(HlcError::Exhausted));
        assert_eq!(source.ts.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn checked_observe_refuses_remote_wrap() {
        let source = HlcSource::new(7);

        assert_eq!(
            source.try_observe(Hlc {
                ts: u64::MAX,
                node: 9,
            }),
            Err(HlcError::Exhausted)
        );
        assert_eq!(source.ts.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn checked_physical_packing_refuses_out_of_range_milliseconds() {
        let max_ms = (u64::MAX >> LOGICAL_BITS) as u128;

        assert_eq!(
            HlcSource::pack_phys_ms_checked(max_ms),
            Ok((max_ms as u64) << LOGICAL_BITS)
        );
        assert_eq!(
            HlcSource::pack_phys_ms_checked(max_ms + 1),
            Err(HlcError::Exhausted)
        );
        assert_eq!(
            HlcSource::pack_phys_ms_checked(u64::MAX as u128 + 1),
            Err(HlcError::Exhausted)
        );
    }

    #[test]
    fn hlc_orders_by_ts_then_node() {
        let a = Hlc { ts: 10, node: 2 };
        let b = Hlc { ts: 10, node: 3 };
        let c = Hlc { ts: 11, node: 1 };
        assert!(a < b && b < c && a < c);
    }

    #[test]
    fn now_is_strictly_monotonic_and_unique() {
        let source = HlcSource::new(7);
        let mut prev = source.now();
        for _ in 0..10_000 {
            let next = source.now();
            assert!(next > prev, "now() must strictly increase");
            assert_eq!(next.node, 7);
            prev = next;
        }
    }

    #[test]
    fn observe_dominates_remote_and_local() {
        let source = HlcSource::new(1);
        let local_before = source.now();
        // A remote far ahead of us (e.g. 1 hour of wall clock).
        let remote = Hlc {
            ts: local_before.ts + (3_600_000 << LOGICAL_BITS),
            node: 2,
        };
        let after = source.observe(remote);
        assert!(after > remote, "observe must exceed the remote");
        assert!(after > local_before, "observe must exceed local history");
        assert!(source.now() > after, "later events keep increasing");
    }

    #[test]
    fn causality_chains_imply_strict_hlc_order() {
        // Linear-extension property: send/receive chains strictly increase.
        let a = HlcSource::new(1);
        let b = HlcSource::new(2);
        let c = HlcSource::new(3);
        let mut last = a.now();
        for source in [&b, &c, &a, &c, &b] {
            let received = source.observe(last);
            assert!(received > last);
            last = source.now();
            assert!(last > received);
        }
    }

    #[test]
    fn regression_sparse_lex_cycle_is_transitively_ordered() {
        // The vector-clock counterexample (A=[(2,5)], B=[(1,1),(2,5)], C=[(1,2)])
        // produced A<B, B<C, C<A under causal+deterministic_cmp. Any three Hlc
        // values are transitively ordered by construction; encode the shape.
        let a = Hlc { ts: 5, node: 2 };
        let b = Hlc { ts: 6, node: 1 };
        let c = Hlc { ts: 7, node: 1 };
        let mut v = [c, a, b];
        v.sort();
        assert_eq!(v, [a, b, c]);
    }

    #[test]
    fn serde_roundtrip() {
        let x = Hlc {
            ts: 0xABCD_EF01_2345,
            node: 42,
        };
        let json = serde_json::to_vec(&x).unwrap();
        let y: Hlc = serde_json::from_slice(&json).unwrap();
        assert_eq!(x, y);
    }
}
