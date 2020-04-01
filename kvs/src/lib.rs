#![deny(missing_docs)]

//! A library for in-memory key-value store

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{prelude::*, BufReader};
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Error kinds enum for KvStore operations
#[derive(Debug, Error)]
#[allow(missing_docs)] // descriptions are provided through macro
pub enum KvStoreError {
  #[error("I/O operation failed")]
  IoError(#[from] std::io::Error),
  #[error("Replay existing log file failed: {0}")]
  ReplayError(String),
  #[error("Serializing command failed")]
  SerError(#[from] ron::ser::Error),
  #[error("Deserializing command failed")]
  DeError(#[from] ron::de::Error),
  #[error("Key not found during remove")]
  RmKeyNotFoundError,
}

/// Result wrapper for KvStore operations
pub type Result<T> = std::result::Result<T, KvStoreError>;

// the in-memory index type
type Index = HashMap<String, String>;
// the log type
type Log = File;

/// KvStore is an in-memory key-value store
pub struct KvStore {
  index: Index,
  log: Log,
}

type Key = String;
type Value = String;
#[derive(Debug, Serialize, Deserialize)]
enum KvCommand {
  Set(Key, Value),
  Rm(Key),
}

impl KvStore {
  /// Creates a new key-value store
  pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
    let log_path = path.into().join("kvs.log");
    let index = Self::replay(&log_path)?;
    let log = OpenOptions::new()
      .append(true)
      .read(true)
      .create(true)
      .open(&log_path)?;

    Ok(Self { index, log })
  }

  /// Get the value associated with the given key in the key-value store
  pub fn get(&self, key: String) -> Result<Option<String>> {
    Ok(self.index.get(&key).map(|s| s.to_owned()))
  }

  /// Set the value associated with the given key in the key-value store
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    // write log
    let cmd = KvCommand::Set(key.clone(), value.clone());
    write!(self.log, "{}\n", ron::ser::to_string(&cmd)?)?;

    // update in-memory index
    self.index.insert(key, value);

    Ok(())
  }

  /// Remove the given key and its associated value from the key-value store
  pub fn remove(&mut self, key: String) -> Result<()> {
    // check exist
    if !self.index.contains_key(&key) {
      return Err(KvStoreError::RmKeyNotFoundError);
    }

    // write log
    let cmd = KvCommand::Rm(key.clone());
    write!(self.log, "{}\n", ron::ser::to_string(&cmd)?)?;

    // update in-memory index
    self.index.remove(&key);

    Ok(())
  }

  fn replay<P: AsRef<Path>>(path: P) -> Result<Index> {
    let mut index = Index::new();

    match File::open(path) {
      Ok(f) => {
        let reader = BufReader::new(f);
        for cmd_string in reader.lines().map(|l| l.unwrap()) {
          let cmd = ron::de::from_str(&cmd_string)?;
          match cmd {
            KvCommand::Set(key, value) => {
              index.insert(key, value);
            }
            KvCommand::Rm(key) => {
              index.remove(&key);
            }
          }
        }

        Ok(index)
      }
      Err(e) => {
        if e.kind() == std::io::ErrorKind::NotFound {
          Ok(index)
        } else {
          Err(KvStoreError::IoError(e))
        }
      }
    }
  }
}
