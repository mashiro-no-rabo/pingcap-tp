#![deny(missing_docs)]

//! A library for in-memory key-value store

use std::collections::HashMap;

/// KvStore is an in-memory key-value store
pub struct KvStore {
  store: HashMap<String, String>,
}

impl KvStore {
  /// Creates a new key-value store
  pub fn new() -> Self {
    Self { store: HashMap::new() }
  }

  /// Get the value associated with the given key in the key-value store
  pub fn get(&self, key: String) -> Option<String> {
    self.store.get(&key).map(|s| s.to_owned())
  }

  /// Set the value associated with the given key in the key-value store
  pub fn set(&mut self, key: String, value: String) {
    self.store.insert(key, value);
  }

  /// Remove the given key and its associated value from the key-value store
  pub fn remove(&mut self, key: String) {
    self.store.remove(&key);
  }
}
