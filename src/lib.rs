//! kvs provides a key-vale store in memory.
#![deny(missing_docs)]
use std::collections::BTreeMap;
use std::io::{self, prelude::*};
use std::{fs, path::Path};

/// KvStore is an in-memory key-value store.
pub struct KvStore {
    datafile: std::path::PathBuf,
    w: io::BufWriter<fs::File>,
    /// a map from key to log pointer which is  represented as a file offset.
    index: BTreeMap<String, u64>,
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
            w: io::BufWriter::new(file),
            index: BTreeMap::new(),
        })
    }

    /// Set a value associated with key.
    ///
    /// Commands are serialized in JSON format for easier debugging.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log = Command::Set { key, value };
        serde_json::to_writer(&mut self.w, &log)?;
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.build_index()?;
        let op = self.index.get(&key);
        let p = match op {
            Some(p) => p.clone(),
            None => {
                return Ok(None);
            }
        };
        let mut file = fs::File::open(&self.datafile)?;
        file.seek(io::SeekFrom::Start(p))?;
        let mut stream = serde_json::Deserializer::from_reader(&file).into_iter::<Command>();
        let command = match stream.next() {
            Some(result) => result?,
            None => return Err(Error::UnexpectedError),
        };

        if let Command::Set { key: k, value: v } = command {
            if k != key {
                Ok(None)
            } else {
                Ok(Some(v))
            }
        } else {
            Err(Error::UnexpectedError)
        }
    }

    /// Remove a value by key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.build_index()?;
        match self.index.get(&key) {
            Some(_) => {
                let log = Command::Rm { key };
                serde_json::to_writer(&mut self.w, &log)?;
                Ok(())
            }
            None => Err(Error::KeyNotFound),
        }
    }

    fn build_index(&mut self) -> Result<()> {
        self.w.flush()?;
        let mut file = fs::File::open(&self.datafile)?;
        let mut stream = serde_json::Deserializer::from_reader(&mut file).into_iter::<Command>();
        let mut offset = stream.byte_offset() as u64;
        while let Some(command) = stream.next() {
            let command = command?;
            match command {
                Command::Set { key, value: _ } => {
                    self.index.insert(key, offset as u64);
                }
                Command::Rm { key } => {
                    self.index.remove(&key);
                }
            }
            offset = stream.byte_offset() as u64;
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
    Io(#[from] io::Error),
    /// Serde error.
    #[error("Serde error occurred: {0}")]
    Serde(#[from] serde_json::Error),
    /// Key is not found.
    #[error("Key not found")]
    KeyNotFound,
    /// Unexpected error.
    #[error("Unexpected error")]
    UnexpectedError,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
