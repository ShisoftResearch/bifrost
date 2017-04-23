use std::hash::Hash;
use std::collections::{BTreeMap, HashSet};
use std::cmp::Ordering;
use parking_lot::RwLock;
use bifrost_hasher::hash_str;

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum Relation {
    Equal,
    Before,
    After,
    Concurrent
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash)]
pub struct VectorClock<S: Ord + Eq + Copy> {
    map: BTreeMap<S, u64>
}

impl <S: Eq + Copy + Ord> PartialOrd for VectorClock<S> {
    fn partial_cmp(&self, other: &VectorClock<S>) -> Option<Ordering> {
        let rel = self.relation(other);
        match rel {
            Relation::Before => Some(Ordering::Less),
            Relation::After => Some(Ordering::Greater),
            Relation::Equal => Some(Ordering::Equal),
            Relation::Concurrent => None
        }
    }

    fn lt(&self, other: &VectorClock<S>) -> bool {
        self.relation(other) == Relation::Before
    }

    fn le(&self, other: &VectorClock<S>) -> bool {
        let rel = self.relation(other);
        rel == Relation::Before || rel == Relation::Equal
    }

    fn gt(&self, other: &VectorClock<S>) -> bool {
        self.relation(other) == Relation::After
    }

    fn ge(&self, other: &VectorClock<S>) -> bool {
        let rel = self.relation(other);
        rel == Relation::After || rel == Relation::Equal
    }
}

impl <S: Eq + Copy + Ord> PartialEq for VectorClock<S> {
    fn eq(&self, other: &VectorClock<S>) -> bool {
        let rel = self.relation(other);
        rel == Relation::Equal
    }

    fn ne(&self, other: &VectorClock<S>) -> bool {
        let rel = self.relation(other);
        !(rel == Relation::After || rel == Relation::Before)
    }
}

impl <S: Ord + Eq + Copy>VectorClock<S> {
    pub fn new() -> VectorClock<S> {
        VectorClock {
            map: BTreeMap::new()
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
            if *ai > bi {return false;}
            a_lt_b = a_lt_b || *ai < bi;
        }
        for (server, bi) in clock_b.map.iter() {
            let ai = *self.map.get(server).unwrap_or(&0);
            if ai > *bi {return false;}
            a_lt_b = a_lt_b || ai < *bi;
        }
        return a_lt_b;
    }
    pub fn equals(&self, clock_b: &VectorClock<S>) -> bool {
        for (server, ac) in self.map.iter() {
            if let Some(bc) = clock_b.map.get(server) {
                if ac != bc {return false;}
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
            let mut ba = self.map.entry(*server).or_insert(0);
            if *ba < *bc {*ba = *bc}
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
    clock: RwLock<VectorClock<u64>>
}

impl ServerVectorClock {
    pub fn new(server_address: &String) -> ServerVectorClock {
        ServerVectorClock {
            server: hash_str(server_address),
            clock: RwLock::new(VectorClock::new())
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