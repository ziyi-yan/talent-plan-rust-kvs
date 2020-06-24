//! kvs provides a key-vale store in memory.
#![deny(missing_docs)]
use std::collections::BTreeMap;
use std::io::{self, prelude::*};
use std::{fs, path::Path};

/// KvStore is an in-memory key-value store.
pub struct KvStore {
    datafile: std::path::PathBuf,
    writer: PositionedWriter<io::BufWriter<fs::File>>,
    /// a map from key to log pointer which is  represented as a file offset.
    index: BTreeMap<String, u64>,
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

const DATA_FILE_NAME: &str = "datafile";

impl KvStore {
    /// Opens a `KvStore` from the directory at path.
    ///
    /// This will create a new directory if the given one does not exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<KvStore> {
        fs::create_dir_all(path.as_ref())?;

        let datafile = path.as_ref().join(DATA_FILE_NAME);
        let mut index = BTreeMap::new();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&datafile)?;
        let offset = file.seek(io::SeekFrom::End(0))?;

        build_index(&datafile, &mut index)?;

        Ok(KvStore {
            datafile,
            writer: PositionedWriter {
                w: io::BufWriter::new(file),
                offset,
            },
            index,
        })
    }

    /// Set a value associated with key.
    ///
    /// Commands are serialized in JSON format for easier debugging.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set { key, value };
        let offset = self.writer.offset;
        serde_json::to_writer(&mut self.writer, &command)?;
        if let Command::Set { key, .. } = command {
            self.index.insert(key, offset);
        }
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.writer.flush()?;
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
        match self.index.get(&key) {
            Some(_) => {
                let command = Command::Remove { key };
                serde_json::to_writer(&mut self.writer, &command)?;
                if let Command::Remove { key } = command {
                    self.index.remove(&key);
                }
                Ok(())
            }
            None => Err(Error::KeyNotFound),
        }
    }
}

fn build_index(datafile: impl AsRef<Path>, index: &mut BTreeMap<String, u64>) -> Result<()> {
    let mut file = fs::File::open(datafile)?;
    let mut stream = serde_json::Deserializer::from_reader(&mut file).into_iter::<Command>();
    let mut offset = stream.byte_offset() as u64;
    while let Some(command) = stream.next() {
        let command = command?;
        match command {
            Command::Set { key, .. } => {
                index.insert(key, offset as u64);
            }
            Command::Remove { key } => {
                index.remove(&key);
            }
        }
        offset = stream.byte_offset() as u64;
    }
    Ok(())
}

/// PositionedWriter tracks the current writing position as a offset in bytes from the start of the stream.
struct PositionedWriter<W: Write> {
    w: W,
    offset: u64,
}

impl<W: io::Write> io::Write for PositionedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.w.write(buf)?;
        self.offset += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
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
