//! kvs provides a key-vale store in memory.
#![deny(missing_docs)]
use std::path::Path;

/// KvStore is an in-memory key-value store.
#[derive(Default)]
pub struct KvStore {}

impl KvStore {
    /// Open a key-value store from a file at path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        unimplemented!()
    }

    /// Set a value associated with key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        unimplemented!()
    }

    /// Get a value by key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        unimplemented!()
    }

    /// Remove a value by key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!()
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for kvs.
#[derive(Debug)]
pub enum Error {}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
