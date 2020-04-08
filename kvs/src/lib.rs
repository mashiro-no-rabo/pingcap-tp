#![deny(missing_docs)]

//! A library for in-memory key-value store

use rmp_serde::decode::{Deserializer, ReadReader};
use rmp_serde::encode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
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
  #[error("Error during compaction")]
  CompactionError,
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
  log_dir: PathBuf,
  log: Log,
  garbage: u32,
}

// Trigger compaction when garbages exceeding this value
const COMPACTION_THRESHOLD: u32 = 100;

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
    let log_dir = directory.into();
    let log_path = log_dir.clone().join("kvs.log");

    let log_file = OpenOptions::new().write(true).read(true).create(true).open(&log_path)?;

    let reader = BufReader::new(log_file);
    let mut log = Deserializer::new(reader);

    let mut index = HashMap::new();
    let mut garbage = 0;
    log.get_mut().seek(SeekFrom::Start(0))?;

    loop {
      let pos = log.get_mut().seek(SeekFrom::Current(0))?;
      if let Ok(cmd) = KvCommand::deserialize(&mut log) {
        match cmd {
          KvCommand::Set(key, _value) => {
            if index.insert(key, pos).is_some() {
              // key is replaced
              garbage += 1;
            }
          }
          KvCommand::Rm(key) => {
            index.remove(&key);
            // rm is always garbage
            garbage += 1;
          }
        }
      } else {
        // TODO check for EoF and error out otherwise
        break;
      }
    }

    let mut kvs = Self {
      index,
      log_dir,
      log,
      garbage,
    };
    kvs.maybe_compact_logs()?;

    Ok(kvs)
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
    if self.index.insert(key, log_pointer).is_some() {
      self.garbage += 1;
      self.maybe_compact_logs()?;
    }

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
    self.garbage += 1;
    self.maybe_compact_logs()?;

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

  fn maybe_compact_logs(&mut self) -> Result<()> {
    if self.garbage < COMPACTION_THRESHOLD {
      return Ok(());
    }

    // write a new log with only Set commands
    let clog_path = self.log_dir.clone().join("kvs-comp.log");

    // use a block to close new file (?)
    let new_index = {
      let mut clog_file = OpenOptions::new().write(true).create(true).open(&clog_path)?;

      let mut new_pos = 0;
      let mut index = self.index.clone();
      for (key, log_pointer) in index.iter_mut() {
        self.log.get_mut().seek(SeekFrom::Start(*log_pointer))?;

        if let Ok(KvCommand::Set(_, value)) = KvCommand::deserialize(&mut self.log) {
          *log_pointer = new_pos;

          let cmd = KvCommand::Set(key.to_owned(), value);
          let bytes = encode::to_vec(&cmd)?;
          clog_file.write_all(&bytes)?;
          new_pos += bytes.len() as u64;
        } else {
          return Err(KvStoreError::CompactionError);
        }
      }
      clog_file.sync_all()?;

      index
    };

    // move (rename) the log and reopen it
    let log_path = self.log_dir.clone().join("kvs.log");
    fs::rename(clog_path, &log_path)?;
    let log_file = OpenOptions::new().write(true).read(true).open(&log_path)?;
    let reader = BufReader::new(log_file);
    let new_log = Deserializer::new(reader);

    // reset struct fields
    self.index = new_index;
    self.log = new_log;
    self.garbage = 0;

    Ok(())
  }
}
