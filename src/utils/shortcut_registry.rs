use lightning::map::{Map, PtrHashMap, SwiftKeyProbe};
use std::hash::Hash;

/// Registry support for exported service macros. The private map exposes only
/// lock-free lookup, publication, and conditional removal.
#[doc(hidden)]
pub struct ShortcutRegistry<K: Clone + Hash + SwiftKeyProbe, V: Clone> {
    entries: PtrHashMap<K, V>,
}

impl<K: Clone + Hash + SwiftKeyProbe, V: Clone> ShortcutRegistry<K, V> {
    pub fn new() -> Self {
        Self {
            entries: PtrHashMap::with_capacity(16),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.entries.get(key)
    }

    pub fn register(&self, key: K, value: V) {
        self.entries.insert(key, value);
    }

    pub fn unregister_if(&self, key: &K, matches: impl FnMut(&V) -> bool) {
        drop(self.entries.remove_if(key, matches));
        // A removed callback can own a server. Release retired values once
        // readers have left their read sections, including after a stale token.
        self.entries.reclaim_pending();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn bflock_replacement_during_remove_predicate_survives() {
        let registry = Arc::new(ShortcutRegistry::new());
        let old = Arc::new(());
        let new = Arc::new(());
        registry.register(1_u64, old.clone());
        let entered = Arc::new(Barrier::new(2));
        let replaced = Arc::new(Barrier::new(2));
        let remover = {
            let registry = registry.clone();
            let entered = entered.clone();
            let replaced = replaced.clone();
            std::thread::spawn(move || {
                registry.unregister_if(&1, |current| {
                    if Arc::ptr_eq(current, &old) {
                        entered.wait();
                        replaced.wait();
                        true
                    } else {
                        false
                    }
                });
            })
        };
        entered.wait();
        registry.register(1, new.clone());
        replaced.wait();
        remover.join().unwrap();
        assert!(Arc::ptr_eq(&registry.get(&1).unwrap(), &new));
        registry.unregister_if(&1, |current| Arc::ptr_eq(current, &new));
        assert!(registry.get(&1).is_none());
    }
}
