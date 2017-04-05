use std::hash::Hash;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Relation {
    Equal,
    Before,
    After,
    Concurrent
}

pub struct VectorClock<S> {
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

    pub fn happened_after(&self, clock_b: &VectorClock<S>) -> bool {
        for (server, ac) in self.map.iter() {
            if let Some(bc) = clock_b.map.get(server) {
                // We only check servers that existed in the clock
                // Because we can't tell sequences from missing servers
                if ac > bc {return true;}
            }
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
        if self.happened_after(clock_b) {
            return Relation::After;
        }
        if clock_b.happened_after(self) {
            return Relation::Before;
        }
        return Relation::Concurrent;
    }
    pub fn merge_with(&mut self, clock_b: &VectorClock<S>) {
        for (server, bc) in clock_b.map.iter() {
            let mut ba = self.map.entry(*server).or_insert(0);
            if *ba < *bc {*ba = *bc}
        }
    }
}
