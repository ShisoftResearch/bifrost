use bifrost_hasher::hash_str;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum Relation {
    Equal,
    Before,
    After,
    Concurrent,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq)]
pub struct VectorClock<S: std::hash::Hash + Ord + Eq + Copy> {
    map: HashMap<S, u64>,
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
            map: HashMap::new(),
        }
    }

    pub fn inc(&mut self, server: S) -> VectorClock<S> {
        *self.map.entry(server).or_insert(0) += 1;
        self.clone()
    }

    pub fn happened_before(&self, clock_b: &VectorClock<S>) -> bool {
        let mut a_lt_b = false;
        for (server, ai) in self.map.iter() {
            let bi = *clock_b.map.get(server).unwrap_or(&0);
            if *ai > bi {
                return false;
            }
            a_lt_b = a_lt_b || *ai < bi;
        }
        for (server, bi) in clock_b.map.iter() {
            let ai = *self.map.get(server).unwrap_or(&0);
            if ai > *bi {
                return false;
            }
            a_lt_b = a_lt_b || ai < *bi;
        }
        return a_lt_b;
    }
    pub fn equals(&self, clock_b: &VectorClock<S>) -> bool {
        let b_keys: Vec<_> = clock_b.map.keys().collect();
        let self_keys: Vec<_> = self.map.keys().collect();
        if b_keys != self_keys {
            return false;
        }
        for (server, ac) in self.map.iter() {
            let bc: u64 = {
                match clock_b.map.get(server) {
                    Some(v) => *v,
                    None => {
                        return false;
                    }
                }
            };
            if *ac != bc {
                return false;
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
        for (server, bc) in clock_b.map.iter() {
            let ba = self.map.entry(*server).or_insert(0);
            if *ba < *bc {
                *ba = *bc
            }
        }
    }
    pub fn learn_from(&mut self, clock_b: &VectorClock<S>) {
        // learn_from only insert missing servers into the clock
        for (server, bc) in clock_b.map.iter() {
            self.map.entry(*server).or_insert(*bc);
        }
    }
}

pub struct ServerVectorClock {
    server: u64,
    clock: RwLock<VectorClock<u64>>,
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
        clock.inc(self.server)
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
    use crate::vector_clock::StandardVectorClock;

    #[test]
    fn test() {
        let _ = env_logger::try_init();
        let mut clock = StandardVectorClock::new();
        let blank_clock = StandardVectorClock::new();
        clock.inc(1);
        info!("{:?}", clock.relation(&blank_clock));
        assert!(clock > blank_clock);
        assert!(blank_clock < clock);
        assert!(blank_clock != clock);
    }
}
