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
    num_dead_keys: u64,
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

const DATA_FILE_NAME: &str = "datafile";

const COMPACTION_DEAD_KEYS_RATIO: f64 = 0.4;

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

        let num_dead_keys = build_index(&datafile, &mut index)?;

        Ok(KvStore {
            datafile,
            writer: PositionedWriter {
                w: io::BufWriter::new(file),
                offset,
            },
            index,
            num_dead_keys,
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
            if let Some(_) = self.index.insert(key, offset) {
                self.num_dead_keys += 1;
                self.compact()?;
            }
        }
        Ok(())
    }

    /// Get a value by key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.writer.flush()?;
        do_get(&self.index, &self.datafile, key)
    }

    /// Remove a value by key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.index.get(&key) {
            Some(_) => {
                let command = Command::Remove { key };
                serde_json::to_writer(&mut self.writer, &command)?;
                if let Command::Remove { key } = command {
                    if let Some(_) = self.index.remove(&key) {
                        self.num_dead_keys += 1;
                        self.compact()?;
                    }
                }
                Ok(())
            }
            None => Err(Error::KeyNotFound),
        }
    }

    fn compact(&mut self) -> Result<()> {
        let dead_keys_ratio = self.num_dead_keys as f64 / self.index.len() as f64;
        if dead_keys_ratio > COMPACTION_DEAD_KEYS_RATIO {
            self._compact()?;
        }
        Ok(())
    }

    // TODO need some refactor and address those questions from project-2 about file handling managment and copying.
    // TODO try to split data into multiple files and only compaction inactive files.
    fn _compact(&mut self) -> Result<()> {
        self.writer.flush()?;
        // Overwrite the data file with new bunch of Command::Set commands based on current index in memory
        let mut buf = Vec::new();
        let mut index = BTreeMap::new();
        let mut writer = PositionedWriter {
            w: &mut buf,
            offset: 0,
        };
        let mut offset = 0;
        for key in self.index.keys() {
            let command = Command::Set {
                key: key.to_owned(),
                value: do_get(&self.index, &self.datafile, key.to_owned())?.unwrap(),
            };
            serde_json::to_writer(&mut writer, &command)?;
            // Update index keys with new offset
            index.insert(key.to_owned(), offset);
            offset = writer.offset;
        }
        self.index = index;
        // Update data file content
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.datafile)?;

        // Update writer
        self.writer = PositionedWriter {
            w: io::BufWriter::new(file),
            offset: 0,
        };
        self.writer.write_all(buf.as_ref())?;
        Ok(())
    }
}

fn do_get(
    index: &BTreeMap<String, u64>,
    datafile: impl AsRef<Path>,
    key: String,
) -> Result<Option<String>> {
    let op = index.get(&key);
    let p = match op {
        Some(p) => p.clone(),
        None => {
            return Ok(None);
        }
    };
    let mut file = fs::File::open(datafile)?;
    file.seek(io::SeekFrom::Start(p))?;
    let mut stream = serde_json::Deserializer::from_reader(&file).into_iter::<Command>();
    let command = match stream.next() {
        Some(result) => result?,
        None => {
            return Err(Error::UnexpectedError(format!(
                "no command from offset {}",
                p
            )))
        }
    };

    if let Command::Set { key: k, value: v } = command {
        if k != key {
            Ok(None)
        } else {
            Ok(Some(v))
        }
    } else {
        Err(Error::UnexpectedError(
            "read command is a not Command::Set".to_owned(),
        ))
    }
}

fn build_index(datafile: impl AsRef<Path>, index: &mut BTreeMap<String, u64>) -> Result<u64> {
    let mut num_dead_keys = 0;
    let mut file = fs::File::open(datafile)?;
    let mut stream = serde_json::Deserializer::from_reader(&mut file).into_iter::<Command>();
    let mut offset = stream.byte_offset() as u64;
    while let Some(command) = stream.next() {
        let command = command?;
        match command {
            Command::Set { key, .. } => {
                if let Some(_) = index.insert(key, offset as u64) {
                    num_dead_keys += 1;
                }
            }
            Command::Remove { key } => {
                if let Some(_) = index.remove(&key) {
                    num_dead_keys += 1;
                }
                // Because the remove command will always be deleted in a compaction.
                num_dead_keys += 1;
            }
        }
        offset = stream.byte_offset() as u64;
    }
    Ok(num_dead_keys)
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
    #[error("Unexpected error: {0}")]
    UnexpectedError(String),
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
