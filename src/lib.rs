//! kvs provides a key-vale store in memory.
#![deny(missing_docs)]
use std::collections::HashMap;

/// KvStore is an in-memory key-value store.
pub struct KvStore {
    m: HashMap<String, String>,
}

impl KvStore {
    /// Create a key-value store.
    pub fn new() -> KvStore {
        KvStore { m: HashMap::new() }
    }

    /// Set a value associated with key.
    pub fn set(&mut self, key: String, value: String) {
        self.m.insert(key, value);
    }

    /// Get a value by key.
    pub fn get(&mut self, key: String) -> Option<String> {
        match self.m.get(&key) {
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    /// Remove a value by key.
    pub fn remove(&mut self, key: String) {
        self.m.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
