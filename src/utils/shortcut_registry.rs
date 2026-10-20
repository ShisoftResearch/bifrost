use lightning::map::{Map, PtrHashMap, SwiftKeyProbe};
use std::hash::Hash;

/// Registry support for exported service macros. The map stays private so all
/// writers follow the same key-guard protocol. Readers never acquire a guard.
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
        // Unlike get_or_insert, this publishes a complete value for a new key.
        let guard = match self.entries.locked_with_upsert(key, value) {
            Ok((guard, _)) | Err(guard) => guard,
        };
        drop(guard);
    }

    pub fn unregister_if(&self, key: &K, matches: impl FnOnce(&V) -> bool) {
        if let Some(guard) = self.entries.lock(key) {
            if matches(&guard) {
                drop(guard.remove());
            }
        }
        // A removed callback can own a server. Release retired values once
        // readers have left their read sections, including after a stale token.
        self.entries.reclaim_pending();
    }
}
