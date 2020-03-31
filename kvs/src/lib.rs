#![deny(missing_docs)]

//! A library for in-memory key-value store

use std::collections::HashMap;
use std::path::Path;
use thiserror::Error;

/// Error kinds enum for KvStore operations
#[derive(Debug, Error)]
#[allow(missing_docs)] // descriptions are provided through macro
pub enum KvStoreError {
  #[error("I/O operation failed")]
  IoError(#[from] std::io::Error),
}

/// Result wrapper for KvStore operations
pub type Result<T> = std::result::Result<T, KvStoreError>;

/// KvStore is an in-memory key-value store
pub struct KvStore {
  store: HashMap<String, String>,
}

impl KvStore {
  /// Creates a new key-value store
  pub fn open<P: AsRef<Path>>(_: P) -> Result<Self> {
    Ok(Self { store: HashMap::new() })
  }

  /// Get the value associated with the given key in the key-value store
  pub fn get(&self, key: String) -> Result<Option<String>> {
    Ok(self.store.get(&key).map(|s| s.to_owned()))
  }

  /// Set the value associated with the given key in the key-value store
  pub fn set(&mut self, key: String, value: String) -> Result<()> {
    self.store.insert(key, value);
    Ok(())
  }

  /// Remove the given key and its associated value from the key-value store
  pub fn remove(&mut self, key: String) -> Result<()> {
    self.store.remove(&key);
    Ok(())
  }
}
