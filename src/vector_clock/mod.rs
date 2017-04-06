use std::hash::Hash;
use std::collections::{HashMap, HashSet};
use parking_lot::RwLock;
use bifrost_hasher::hash_str;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Relation {
    Equal,
    Before,
    After,
    Concurrent
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct VectorClock<S: Hash + Eq + Copy> {
    map: HashMap<S, u64>
}

impl <S: Hash + Eq + Copy>VectorClock<S> {
    pub fn new() -> VectorClock<S> {
        VectorClock {
            map: HashMap::new()
        }
    }

    pub fn inc(&mut self, server: S) {
        *self.map.entry(server).or_insert(0) += 1;
    }

    pub fn happened_before(&self, clock_b: &VectorClock<S>) -> bool {
        for (server, ac) in self.map.iter() {
            let bc = *clock_b.map.get(server).unwrap_or(&0);
            if *ac >= bc {return false;}
        }
        for (server, bc) in clock_b.map.iter() {
            let ac = *self.map.get(server).unwrap_or(&0);
            if ac >= *bc {return false;}
        }
        return false;
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
    pub fn inc(&self) {
        let mut clock = self.clock.write();
        clock.inc(self.server)
    }

    pub fn happened_before(&self, clock_b: &VectorClock<u64>) -> bool {
        let clock = self.clock.read();
        clock.happened_before(clock_b)
    }
    pub fn equals(&self, clock_b: &VectorClock<u64>) -> bool {
        let clock = self.clock.read();
        clock.equals(clock_b)
    }
    pub fn relation(&self, clock_b: &VectorClock<u64>) -> Relation {
        let clock = self.clock.read();
        clock.relation(clock_b)
    }
    pub fn merge_with(&self, clock_b: &VectorClock<u64>) {
        let mut clock = self.clock.write();
        clock.merge_with(clock_b)
    }
    pub fn learn_from(&self, clock_b: &VectorClock<u64>) {
        let mut clock = self.clock.write();
        clock.learn_from(clock_b)
    }
    pub fn to_clock(&self) -> VectorClock<u64> {
        let clock = self.clock.read();
        clock.clone()
    }
}
