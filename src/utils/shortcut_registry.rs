use lightning::map::{Map, PtrHashMap, SwiftKeyProbe};
use std::hash::Hash;
use std::sync::Arc;

/// Registry support for exported service macros. The private map exposes only
/// lock-free lookup, publication, and conditional removal. An Arc shares each
/// immutable registration so lookups do not clone its unused token or Weak.
#[doc(hidden)]
pub struct ShortcutRegistry<K: Clone + Hash + SwiftKeyProbe, V: Clone> {
    entries: PtrHashMap<K, Arc<V>>,
}

impl<K: Clone + Hash + SwiftKeyProbe, V: Clone> ShortcutRegistry<K, V> {
    pub fn new() -> Self {
        Self {
            entries: PtrHashMap::with_capacity(16),
        }
    }

    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        self.entries.get(key)
    }

    pub fn register(&self, key: K, value: V) {
        self.entries.insert(key, Arc::new(value));
    }

    pub fn unregister_if(&self, key: &K, mut matches: impl FnMut(&V) -> bool) {
        drop(self.entries.remove_if(key, |entry| matches(entry.as_ref())));
        // A removed callback can own a server. Release retired values once
        // readers have left their read sections, including after a stale token.
        self.entries.reclaim_pending();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    struct CloneTracked {
        clones: Arc<std::sync::atomic::AtomicUsize>,
        payload: u64,
    }
    impl Clone for CloneTracked {
        fn clone(&self) -> Self {
            self.clones
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Self {
                clones: self.clones.clone(),
                payload: self.payload,
            }
        }
    }

    #[test]
    fn bflock_lookup_does_not_clone_registration_payload() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let clones = Arc::new(AtomicUsize::new(0));
        let registry = ShortcutRegistry::new();
        registry.register(
            1_u64,
            CloneTracked {
                clones: clones.clone(),
                payload: 47,
            },
        );
        clones.store(0, Ordering::SeqCst);
        let registration = registry.get(&1).unwrap();
        assert_eq!(registration.payload, 47);
        assert_eq!(
            clones.load(Ordering::SeqCst),
            0,
            "lookup must share the immutable registration payload"
        );
    }

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
        assert!(Arc::ptr_eq(registry.get(&1).unwrap().as_ref(), &new));
        registry.unregister_if(&1, |current| Arc::ptr_eq(current, &new));
        assert!(registry.get(&1).is_none());
    }
}
