#![deny(missing_docs)]

//! A library for in-memory key-value store

use rmp_serde::decode::{Deserializer, ReadReader};
use rmp_serde::encode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Seek, SeekFrom, Write};
use std::path::PathBuf;
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
  EncodeError(#[from] rmp_serde::encode::Error),
  #[error("Deserializing command failed")]
  DecodeError(#[from] rmp_serde::decode::Error),
  #[error("Key not found when attempting remove")]
  RmKeyNotFoundError,
  #[error("Error when attempting get")]
  GetError,
}

/// Result wrapper for KvStore operations
pub type Result<T> = std::result::Result<T, KvStoreError>;

// the in-memory index type (key -> log pointer)
type Index = HashMap<String, u64>;
// the log type
type Log = Deserializer<ReadReader<BufReader<File>>>;

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
  pub fn open(directory: impl Into<PathBuf>) -> Result<Self> {
    let log_path = directory.into().join("kvs.log");

    let log_file = OpenOptions::new().write(true).read(true).create(true).open(&log_path)?;

    let reader = BufReader::new(log_file);
    let mut log = Deserializer::new(reader);

    let mut index = HashMap::new();
    log.get_mut().seek(SeekFrom::Start(0))?;

    loop {
      let pos = log.get_mut().seek(SeekFrom::Current(0))?;
      if let Ok(cmd) = KvCommand::deserialize(&mut log) {
        match cmd {
          KvCommand::Set(key, _value) => {
            index.insert(key, pos);
          }
          KvCommand::Rm(key) => {
            index.remove(&key);
          }
        }
      } else {
        // TODO check for EoF and error out otherwise
        break;
      }
    }

    Ok(Self { index, log })
  }

  /// Get the value associated with the given key in the key-value store
  pub fn get(&mut self, key: String) -> Result<Option<String>> {
    self
      .index
      .get(&key)
      .map(|v| *v) //
      .map(|log_pointer| {
        self.log.get_mut().seek(SeekFrom::Start(log_pointer))?;

        if let Ok(KvCommand::Set(key_in_log, value)) = KvCommand::deserialize(&mut self.log) {
          if key_in_log == key {
            Ok(value)
          } else {
            Err(KvStoreError::GetError)
          }
        } else {
          Err(KvStoreError::GetError)
        }
      })
      .transpose()
  }

  /// Set the value associated with the given key in the key-value store
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    // write log
    let cmd = KvCommand::Set(key.clone(), value);
    let log_pointer = self.write_log(cmd)?;

    // update in-memory index
    self.index.insert(key, log_pointer);

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
    self.write_log(cmd)?;

    // update in-memory index
    self.index.remove(&key);

    Ok(())
  }

  fn write_log(&mut self, cmd: KvCommand) -> Result<u64> {
    // Go to file tail
    let pos = self.log.get_mut().seek(SeekFrom::End(0))?;

    // Write command
    let bytes = encode::to_vec(&cmd)?;
    self.log.get_mut().get_mut().write_all(&bytes)?;

    Ok(pos)
  }
}
