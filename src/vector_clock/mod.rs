use bifrost_hasher::hash_str;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub enum Relation {
    Equal,
    Before,
    After,
    Concurrent,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash)]
pub struct VectorClock<S: std::hash::Hash + Ord + Eq + Copy> {
    map: Vec<(S, u64)>,
}

impl<S: std::hash::Hash + Eq + Copy + Ord> PartialOrd for VectorClock<S> {
    fn partial_cmp(&self, other: &VectorClock<S>) -> Option<Ordering> {
        let rel = self.relation(other);
        match rel {
            Relation::Before => Some(Ordering::Less),
            Relation::After => Some(Ordering::Greater),
            Relation::Equal => Some(Ordering::Equal),
            Relation::Concurrent => None,
        }
    }
}

impl<S: std::hash::Hash + Eq + Copy + Ord> Ord for VectorClock<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        let rel = self.relation(other);
        match rel {
            Relation::Before => Ordering::Less,
            Relation::After => Ordering::Greater,
            _ => Ordering::Equal, // not justified, but sufficient for BTreeSet data structure
        }
    }
}

impl<S: std::hash::Hash + Eq + Copy + Ord> PartialEq for VectorClock<S> {
    fn eq(&self, other: &VectorClock<S>) -> bool {
        let rel = self.relation(other);
        rel == Relation::Equal
    }
}

impl<S: std::hash::Hash + Ord + Eq + Copy> VectorClock<S> {
    pub fn new() -> VectorClock<S> {
        VectorClock {
            map: vec![],
        }
    }

    pub fn inc(&mut self, server: S) {
        let idx = self.map.binary_search_by_key(&server, |(k, _)| *k);
        match idx {
            Ok(idx) => {
                *(&mut self.map[idx].1) += 1;
            }
            Err(idx) => {
                self.map.insert(idx, (server, 1));
            }
        }
    }

    pub fn happened_before(&self, clock_b: &VectorClock<S>) -> bool {
        let mut ai = 0;
        let mut bi = 0;
        let al = self.map.len();
        let bl = clock_b.map.len();
        if al == 0 {
            return clock_b.map.iter().any(|(_, n)| *n > 0);
        }
        if bl == 0 {
            return false;
        }
        let mut a_lt_b = false;
        while ai < al || bi < bl {
            if ai >= al {
                // No need to check follwoing entries
                break;
            }
            if bi >= bl {
                bi = bl - 1;
            }
            let (ak, an) = &self.map[ai];
            let (bk, bn) = &clock_b.map[bi];
            if ak == bk {
                // Two vector have the same key, compare their values
                if an > bn {
                    return false;
                }
                ai += 1;
                bi += 1;
                a_lt_b = a_lt_b || *an < *bn;
            } else if ak > bk {
                // Clock b have a server that a does not have
                // b should either equal or happend after a
                bi += 1;
                a_lt_b = a_lt_b || 0 < *bn;
            } else if ak < bk {
                // Clock a have a server that b does not have
                // if a have thick greater than 0, it happened after b, which should return false
                if *an > 0 {
                    return false;
                }
                ai += 1;
            } else {
                unreachable!();
            }
        }
        return a_lt_b;
    }

    pub fn equals(&self, clock_b: &VectorClock<S>) -> bool {
        let mut ai = 0;
        let mut bi = 0;
        let al = self.map.len();
        let bl = clock_b.map.len();
        if al == 0 && al == bl {
            return true;
        }
        if al != bl || (al == 0 || bl == 0) {
            return false;
        }
        while ai < al || bi < bl {
            if ai >= al {
                ai = al - 1;
            }
            if bi >= bl {
                bi = bl - 1;
            }
            let (ak, an) = &self.map[ai];
            let (bk, bn) = &clock_b.map[bi];
            if ak == bk {
                // Two vector have the same key, compare their values
                if an != bn {
                    return false;
                }
                ai += 1;
                bi += 1;
            } else if ak > bk {
                // Clock b have a server that a does not have
                // b should either equal or happend after a
                bi += 1;
            } else if ak < bk {
                // Clock a have a server that b does not have
                ai += 1;
            } else {
                unreachable!();
            }
        }
        return true;
    }

    pub fn relation(&self, clock_b: &VectorClock<S>) -> Relation {
        if self.equals(clock_b) {
            return Relation::Equal;
        }
        if self.happened_before(clock_b) {
            return Relation::Before;
        }
        if clock_b.happened_before(self) {
            return Relation::After;
        }
        return Relation::Concurrent;
    }

    pub fn merge_with(&mut self, clock_b: &VectorClock<S>) {
        // merge_with is used to update counter for other servers (also learn from it)
        let mut ai = 0;
        let mut bi = 0;
        let al = self.map.len();
        let bl = clock_b.map.len();
        if bl == 0 {
            return;
        }
        if al == 0 {
            self.map = clock_b.map.clone();
            return;
        }
        let mut new_map = Vec::with_capacity(self.map.len() + clock_b.map.len());
        while ai < al || bi < bl {
            if ai >= al {
                ai = al - 1;
            }
            if bi >= bl {
                bi = bl - 1;
            }
            let (ak, an) = &self.map[ai];
            let (bk, bn) = &clock_b.map[bi];
            if ak == bk {
                // Two vector have the same key, compare their values
                if an < bn {
                    new_map.push((*ak, *bn));
                }
                ai += 1;
                bi += 1;
            } else if ak > bk {
                // Clock b have a server that a does not have
                new_map.push((*bk, *bn));
                bi += 1;
            } else if ak < bk {
                // Clock a have a server that b does not have
                ai += 1;
            } else {
                unreachable!();
            }
        }
        self.map = new_map;
    }

    pub fn learn_from(&mut self, clock_b: &VectorClock<S>) {
        // learn_from only insert missing servers into the clock
        let mut ai = 0;
        let mut bi = 0;
        let al = self.map.len();
        let bl = clock_b.map.len();
        if bl == 0 {
            return;
        }
        if al == 0 {
            self.map = clock_b.map.clone();
            return;
        }
        let mut new_map = Vec::with_capacity(self.map.len() + clock_b.map.len());
        while ai < al || bi < bl {
            if ai >= al {
                ai = al - 1;
            }
            if bi >= bl {
                bi = bl - 1;
            }
            let (ak, _an) = &self.map[ai];
            let (bk, bn) = &clock_b.map[bi];
            if ak == bk {
                // Two vector have the same key, compare their values
                ai += 1;
                bi += 1;
            } else if ak > bk {
                // Clock b have a server that a does not have
                new_map.push((*bk, *bn));
                bi += 1;
            } else if ak < bk {
                // Clock a have a server that b does not have
                ai += 1;
            } else {
                unreachable!();
            }
        }
        self.map = new_map;
    }
}

pub struct ServerVectorClock {
    server: u64,
    clock: RwLock<StandardVectorClock>,
}

impl ServerVectorClock {
    pub fn new(server_address: &String) -> ServerVectorClock {
        ServerVectorClock {
            server: hash_str(server_address),
            clock: RwLock::new(VectorClock::new()),
        }
    }
    pub fn inc(&self) -> StandardVectorClock {
        let mut clock = self.clock.write();
        clock.inc(self.server);
        clock.clone()
    }

    pub fn happened_before(&self, clock_b: &StandardVectorClock) -> bool {
        let clock = self.clock.read();
        clock.happened_before(clock_b)
    }
    pub fn equals(&self, clock_b: &StandardVectorClock) -> bool {
        let clock = self.clock.read();
        clock.equals(clock_b)
    }
    pub fn relation(&self, clock_b: &StandardVectorClock) -> Relation {
        let clock = self.clock.read();
        clock.relation(clock_b)
    }
    pub fn merge_with(&self, clock_b: &StandardVectorClock) {
        let mut clock = self.clock.write();
        clock.merge_with(clock_b)
    }
    pub fn learn_from(&self, clock_b: &StandardVectorClock) {
        let mut clock = self.clock.write();
        clock.learn_from(clock_b)
    }
    pub fn to_clock(&self) -> StandardVectorClock {
        let clock = self.clock.read();
        clock.clone()
    }
}

pub type StandardVectorClock = VectorClock<u64>;

#[cfg(test)]
mod test {
    use crate::vector_clock::{StandardVectorClock, Relation};

    #[test]
    fn general() {
        let _ = env_logger::try_init();
        let mut clock = StandardVectorClock::new();
        let blank_clock = StandardVectorClock::new();
        clock.inc(1);
        clock.inc(3);
        let old_clock = clock.clone();
        clock.inc(1);
        clock.inc(2);
        info!("{:?}", clock.relation(&blank_clock));
        assert!(clock > blank_clock);
        assert!(blank_clock < clock);
        assert!(blank_clock != clock);
        assert!(old_clock.happened_before(&clock), "old {:?}, new {:?}", old_clock, clock);
        assert!(!clock.happened_before(&old_clock), "old {:?}, new {:?}", old_clock, clock);
        assert!(!clock.equals(&old_clock), "old {:?}, new {:?}", old_clock, clock);
        assert_eq!(clock.relation(&old_clock), Relation::After, "old {:?}, new {:?}", old_clock, clock);
        assert_eq!(old_clock.relation(&clock), Relation::Before, "old {:?}, new {:?}", old_clock, clock);
        let blank_clock_2 = StandardVectorClock::new();
        assert!(blank_clock == blank_clock_2);
    }
}
