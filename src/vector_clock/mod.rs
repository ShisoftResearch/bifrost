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

#[derive(Serialize, Deserialize, Debug, Clone, Eq)]
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
        self.equals(other)
    }
}

impl<S: std::hash::Hash + Ord + Eq + Copy> VectorClock<S> {
    fn canonicalize(mut map: Vec<(S, u64)>) -> Vec<(S, u64)> {
        map.retain(|(_, counter)| *counter > 0);
        map.sort_unstable_by_key(|(server, _)| *server);

        let mut canonical: Vec<(S, u64)> = Vec::with_capacity(map.len());
        for (server, counter) in map {
            match canonical.last_mut() {
                Some((last_server, last_counter)) if *last_server == server => {
                    *last_counter = (*last_counter).max(counter);
                }
                _ => canonical.push((server, counter)),
            }
        }

        canonical
    }

    fn compare_canonical(a: &[(S, u64)], b: &[(S, u64)]) -> Relation {
        let mut ai = 0;
        let mut bi = 0;
        let mut a_lt_b = false;
        let mut b_lt_a = false;

        while ai < a.len() || bi < b.len() {
            match (a.get(ai), b.get(bi)) {
                (Some((ak, an)), Some((bk, bn))) => match ak.cmp(bk) {
                    Ordering::Equal => {
                        if an < bn {
                            a_lt_b = true;
                        } else if an > bn {
                            b_lt_a = true;
                        }
                        ai += 1;
                        bi += 1;
                    }
                    Ordering::Less => {
                        b_lt_a = true;
                        ai += 1;
                    }
                    Ordering::Greater => {
                        a_lt_b = true;
                        bi += 1;
                    }
                },
                (Some(_), None) => {
                    b_lt_a = true;
                    ai += 1;
                }
                (None, Some(_)) => {
                    a_lt_b = true;
                    bi += 1;
                }
                (None, None) => break,
            }

            if a_lt_b && b_lt_a {
                return Relation::Concurrent;
            }
        }

        match (a_lt_b, b_lt_a) {
            (false, false) => Relation::Equal,
            (true, false) => Relation::Before,
            (false, true) => Relation::After,
            (true, true) => Relation::Concurrent,
        }
    }

    fn canonical_relation(&self, clock_b: &VectorClock<S>) -> Relation {
        let clock_a = Self::canonicalize(self.map.clone());
        let clock_b = Self::canonicalize(clock_b.map.clone());
        Self::compare_canonical(&clock_a, &clock_b)
    }

    pub fn new() -> VectorClock<S> {
        VectorClock { map: vec![] }
    }

    pub fn from_vec(vec: Vec<(S, u64)>) -> Self {
        Self {
            map: Self::canonicalize(vec),
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
        self.canonical_relation(clock_b) == Relation::Before
    }

    pub fn equals(&self, clock_b: &VectorClock<S>) -> bool {
        self.canonical_relation(clock_b) == Relation::Equal
    }

    pub fn relation(&self, clock_b: &VectorClock<S>) -> Relation {
        self.canonical_relation(clock_b)
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
                } else {
                    new_map.push((*ak, *an));
                }
                ai += 1;
                bi += 1;
            } else if ak > bk {
                // Clock b have a server that a does not have
                new_map.push((*bk, *bn));
                bi += 1;
            } else if ak < bk {
                // Clock a have a server that b does not have
                new_map.push((*ak, *an));
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
            let (ak, an) = &self.map[ai];
            let (bk, bn) = &clock_b.map[bi];
            if ak == bk {
                // Two vector have the same key, compare their values
                ai += 1;
                bi += 1;
                new_map.push((*ak, *an));
            } else if ak > bk {
                // Clock b have a server that a does not have
                new_map.push((*bk, *bn));
                bi += 1;
            } else if ak < bk {
                // Clock a have a server that b does not have
                new_map.push((*ak, *an));
                ai += 1;
            } else {
                unreachable!();
            }
        }
        self.map = new_map;
    }
}

impl<S: std::hash::Hash + Eq + Copy + Ord> std::hash::Hash for VectorClock<S> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let canonical = Self::canonicalize(self.map.clone());
        canonical.len().hash(state);
        canonical.hash(state);
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
    use crate::vector_clock::{Relation, StandardVectorClock};
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn clock_hash(clock: &StandardVectorClock) -> u64 {
        let mut hasher = DefaultHasher::new();
        clock.hash(&mut hasher);
        hasher.finish()
    }

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
        assert!(
            old_clock.happened_before(&clock),
            "old {:?}, new {:?}",
            old_clock,
            clock
        );
        assert!(
            !clock.happened_before(&old_clock),
            "old {:?}, new {:?}",
            old_clock,
            clock
        );
        assert!(
            !clock.equals(&old_clock),
            "old {:?}, new {:?}",
            old_clock,
            clock
        );
        assert_eq!(
            clock.relation(&old_clock),
            Relation::After,
            "old {:?}, new {:?}",
            old_clock,
            clock
        );
        assert_eq!(
            old_clock.relation(&clock),
            Relation::Before,
            "old {:?}, new {:?}",
            old_clock,
            clock
        );
        let blank_clock_2 = StandardVectorClock::new();
        assert!(blank_clock == blank_clock_2);
    }

    #[test]
    fn unaligned_clock_eq() {
        let _ = env_logger::try_init();
        let clock_a = StandardVectorClock::from_vec(vec![(1, 2), (2, 3), (3, 4), (4, 5), (5, 6)]);
        let clock_b = StandardVectorClock::from_vec(vec![(2, 3), (4, 5)]);
        assert!(!clock_a.equals(&clock_b));
        assert!(!clock_b.equals(&clock_a));
        assert!(!clock_a.happened_before(&clock_b));
        assert!(clock_b.happened_before(&clock_a));
        assert_eq!(clock_a.relation(&clock_b), Relation::After);
        assert_eq!(clock_b.relation(&clock_a), Relation::Before);
    }

    #[test]
    fn missing_trailing_component_is_before() {
        let clock_a = StandardVectorClock::from_vec(vec![(1, 1)]);
        let clock_b = StandardVectorClock::from_vec(vec![(1, 1), (2, 1)]);

        assert!(!clock_a.equals(&clock_b));
        assert!(!clock_b.equals(&clock_a));
        assert!(clock_a.happened_before(&clock_b));
        assert!(!clock_b.happened_before(&clock_a));
        assert_eq!(clock_a.relation(&clock_b), Relation::Before);
        assert_eq!(clock_b.relation(&clock_a), Relation::After);
    }

    #[test]
    fn missing_leading_and_disjoint_components_compare_over_union() {
        let leading_a = StandardVectorClock::from_vec(vec![(2, 1)]);
        let leading_b = StandardVectorClock::from_vec(vec![(1, 1), (2, 1)]);
        assert_eq!(leading_a.relation(&leading_b), Relation::Before);
        assert_eq!(leading_b.relation(&leading_a), Relation::After);

        let concurrent_a = StandardVectorClock::from_vec(vec![(1, 2), (3, 1)]);
        let concurrent_b = StandardVectorClock::from_vec(vec![(1, 1), (2, 1)]);
        assert!(!concurrent_a.happened_before(&concurrent_b));
        assert!(!concurrent_b.happened_before(&concurrent_a));
        assert_eq!(concurrent_a.relation(&concurrent_b), Relation::Concurrent);
        assert_eq!(concurrent_b.relation(&concurrent_a), Relation::Concurrent);
    }

    #[test]
    fn from_vec_canonicalizes_unsorted_duplicates_and_zeroes() {
        let clock = StandardVectorClock::from_vec(vec![(3, 0), (2, 1), (1, 2), (2, 3), (1, 0)]);

        assert_eq!(clock.map, vec![(1, 2), (2, 3)]);
        assert_eq!(clock, StandardVectorClock::from_vec(vec![(1, 2), (2, 3)]));
    }

    #[test]
    fn deserialized_noncanonical_equal_clocks_hash_the_same() {
        let canonical = StandardVectorClock::from_vec(vec![(1, 2), (2, 3)]);
        let deserialized: StandardVectorClock =
            serde_json::from_str(r#"{"map":[[2,1],[1,2],[2,3],[3,0],[2,0]]}"#).unwrap();

        assert_eq!(deserialized.relation(&canonical), Relation::Equal);
        assert_eq!(deserialized, canonical);
        assert_eq!(clock_hash(&deserialized), clock_hash(&canonical));
    }

    #[test]
    fn unaligned_clock_rel_disjoint_concurrent() {
        let _ = env_logger::try_init();
        let clock_a = StandardVectorClock::from_vec(vec![(1, 2), (3, 4), (5, 6)]);
        let clock_b = StandardVectorClock::from_vec(vec![(0, 1), (2, 3), (7, 8), (9, 10)]);
        assert!(!clock_a.equals(&clock_b));
        assert!(!clock_b.equals(&clock_a));
        assert!(!clock_a.happened_before(&clock_b));
        assert!(!clock_b.happened_before(&clock_a));
        assert_eq!(clock_a.relation(&clock_b), Relation::Concurrent);
    }

    #[test]
    fn unaligned_clock_rel_joint_concurrent() {
        let _ = env_logger::try_init();
        let clock_a = StandardVectorClock::from_vec(vec![(1, 2), (3, 4)]);
        let clock_b = StandardVectorClock::from_vec(vec![(1, 3), (3, 3)]);
        assert!(!clock_a.equals(&clock_b));
        assert!(!clock_b.equals(&clock_a));
        assert!(!clock_a.happened_before(&clock_b));
        assert!(!clock_b.happened_before(&clock_a));
        assert_eq!(clock_a.relation(&clock_b), Relation::Concurrent);
    }

    #[test]
    fn test_merge_with() {
        let _ = env_logger::try_init();
        let mut clock_a = StandardVectorClock::from_vec(vec![(1, 2), (3, 4)]);
        let clock_b = StandardVectorClock::from_vec(vec![(1, 5), (2, 3), (3, 1)]);

        clock_a.merge_with(&clock_b);

        // After merge, clock_a should have max values
        assert_eq!(
            clock_a,
            StandardVectorClock::from_vec(vec![(1, 5), (2, 3), (3, 4)])
        );
    }

    #[test]
    fn test_merge_with_empty() {
        let mut clock_a = StandardVectorClock::from_vec(vec![(1, 2), (3, 4)]);
        let clock_b = StandardVectorClock::new();

        let expected = clock_a.clone();
        clock_a.merge_with(&clock_b);

        assert_eq!(clock_a, expected);
    }

    #[test]
    fn test_merge_with_into_empty() {
        let mut clock_a = StandardVectorClock::new();
        let clock_b = StandardVectorClock::from_vec(vec![(1, 2), (3, 4)]);

        clock_a.merge_with(&clock_b);

        assert_eq!(clock_a, clock_b);
    }

    #[test]
    fn test_learn_from() {
        let mut clock_a = StandardVectorClock::from_vec(vec![(1, 5)]);
        let clock_b = StandardVectorClock::from_vec(vec![(1, 2)]);

        clock_a.learn_from(&clock_b);

        // After learn_from, clock_a keeps its own values for existing keys
        assert_eq!(clock_a, StandardVectorClock::from_vec(vec![(1, 5)]));
    }

    #[test]
    fn test_learn_from_empty() {
        let mut clock_a = StandardVectorClock::from_vec(vec![(1, 2), (3, 4)]);
        let clock_b = StandardVectorClock::new();

        let expected = clock_a.clone();
        clock_a.learn_from(&clock_b);

        assert_eq!(clock_a, expected);
    }

    #[test]
    fn test_learn_from_into_empty() {
        let mut clock_a = StandardVectorClock::new();
        let clock_b = StandardVectorClock::from_vec(vec![(1, 2), (3, 4)]);

        clock_a.learn_from(&clock_b);

        assert_eq!(clock_a, clock_b);
    }

    #[test]
    fn test_server_vector_clock() {
        use super::ServerVectorClock;

        let addr = String::from("127.0.0.1:8080");
        let svc = ServerVectorClock::new(&addr);

        // Test inc
        let clock1 = svc.inc();
        let clock2 = svc.inc();

        assert!(clock1.happened_before(&clock2));
        assert_eq!(clock1.relation(&clock2), Relation::Before);

        // Test happened_before with clock1 (svc is now at clock2, so clock1 is before svc)
        assert!(clock1.happened_before(&svc.to_clock()));

        // Test equals - svc should equal clock2
        assert!(svc.equals(&clock2));

        // Test to_clock
        let current = svc.to_clock();
        assert!(!current.map.is_empty());
    }

    #[test]
    fn test_server_vector_clock_relation() {
        use super::ServerVectorClock;

        let addr = String::from("127.0.0.1:9090");
        let svc = ServerVectorClock::new(&addr);

        let clock1 = svc.inc();

        let external_clock = StandardVectorClock::new();

        let rel = svc.relation(&external_clock);
        assert_eq!(rel, Relation::After);
    }

    #[test]
    fn test_inc_new_server() {
        let mut clock = StandardVectorClock::new();
        clock.inc(10);
        clock.inc(5);
        clock.inc(10);

        // Should be sorted and properly counted
        assert_eq!(clock, StandardVectorClock::from_vec(vec![(5, 1), (10, 2)]));
    }

    #[test]
    fn test_partial_ord() {
        let clock_a = StandardVectorClock::from_vec(vec![(1, 1)]);
        let clock_b = StandardVectorClock::from_vec(vec![(1, 2)]);
        let clock_c = StandardVectorClock::from_vec(vec![(2, 1)]);

        assert!(clock_a < clock_b);
        assert!(clock_b > clock_a);
        assert!(clock_a.partial_cmp(&clock_c).is_none()); // Concurrent
    }

    #[test]
    fn test_ord() {
        use std::cmp::Ordering;

        let clock_a = StandardVectorClock::from_vec(vec![(1, 1)]);
        let clock_b = StandardVectorClock::from_vec(vec![(1, 2)]);
        let clock_c = StandardVectorClock::from_vec(vec![(2, 1)]);

        assert_eq!(clock_a.cmp(&clock_b), Ordering::Less);
        assert_eq!(clock_b.cmp(&clock_a), Ordering::Greater);
        assert_eq!(clock_a.cmp(&clock_c), Ordering::Equal); // Concurrent treated as equal
    }

    #[test]
    fn test_happened_before_empty_clocks() {
        let clock_a = StandardVectorClock::new();
        let mut clock_b = StandardVectorClock::new();
        clock_b.inc(1);

        assert!(clock_a.happened_before(&clock_b));
        assert!(!clock_b.happened_before(&clock_a));
    }

    #[test]
    fn test_equals_empty_clocks() {
        let clock_a = StandardVectorClock::new();
        let clock_b = StandardVectorClock::new();

        assert!(clock_a.equals(&clock_b));
    }

    #[test]
    fn test_equals_with_zero_values() {
        let clock_a = StandardVectorClock::new();
        let clock_b = StandardVectorClock::from_vec(vec![(1, 0), (2, 0)]);

        assert!(clock_a.equals(&clock_b));
        assert!(clock_b.equals(&clock_a));
    }
}
