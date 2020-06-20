//! kvs provides a key-vale store in memory.
#![deny(missing_docs)]
use std::collections::BTreeMap;
use std::fs;
use std::io::prelude::*;
use std::path::Path;

/// KvStore is an in-memory key-value store.
pub struct KvStore {
    datafile: std::path::PathBuf,
    bw: std::io::BufWriter<std::fs::File>,
    index: BTreeMap<String, String>,
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

impl KvStore {
    /// Opens a `KvStore` from the directory at path.
    ///
    /// This will create a new directory if the given one does not exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        fs::create_dir_all(path.as_ref())?;
        let datafile = path.as_ref().join("datafile");
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&datafile)?;
        Ok(KvStore {
            datafile,
            bw: std::io::BufWriter::new(file),
            index: BTreeMap::new(),
        })
    }

    /// Set a value associated with key.
    ///
    /// Commands are serialized in JSON format for easier debugging.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log = Command::Set { key, value };
        serde_json::to_writer(&mut self.bw, &log)?;
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.build_index()?;
        Ok(self.index.get(&key).cloned())
    }

    /// Remove a value by key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.build_index()?;
        match self.index.get(&key) {
            Some(_) => {
                let log = Command::Rm { key };
                serde_json::to_writer(&mut self.bw, &log)?;
                Ok(())
            }
            None => Err(Error::KeyNotFound),
        }
    }

    fn build_index(&mut self) -> Result<()> {
        self.bw.flush()?;
        let file = fs::File::open(&self.datafile)?;
        let de = serde_json::Deserializer::from_reader(&file);
        for command in de.into_iter::<Command>() {
            let command = command?;
            match command {
                Command::Set { key, value } => {
                    self.index.insert(key, value);
                }
                Command::Rm { key } => {
                    self.index.remove(&key);
                }
            }
        }
        Ok(())
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, Error>;

use thiserror::Error;

/// Error type for kvs.
#[derive(Error, Debug)]
pub enum Error {
    /// IO error.
    #[error("`IO error occurred: {0}`")]
    Io(#[from] std::io::Error),
    /// Serde error.
    #[error("Serde error occurred: {0}")]
    Serde(#[from] serde_json::Error),
    /// Key is not found.
    #[error("Key not found")]
    KeyNotFound,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
