use anyhow::Result;
use structopt::StructOpt;

use kvs::*;

#[derive(Debug, StructOpt)]
#[structopt(
  author = env!("CARGO_PKG_AUTHORS"),
  about = env!("CARGO_PKG_DESCRIPTION"),
)]
enum Kv {
  Get { key: String },
  Set { key: String, value: String },
  Rm { key: String },
}

fn main() -> Result<()> {
  match Kv::from_args() {
    Kv::Get { key } => {
      let store = KvStore::open(".")?;
      match store.get(key)? {
        Some(vv) => println!("{}", vv),
        None => println!("Key not found"),
      }
      Ok(())
    }
    Kv::Set { key, value } => {
      let mut store = KvStore::open(".")?;
      store.set(key, value)?;
      Ok(())
    }
    Kv::Rm { key } => {
      let mut store = KvStore::open(".")?;
      let handled_not_found = match store.remove(key) {
        Err(KvStoreError::RmKeyNotFoundError) => {
          println!("Key not found");
          std::process::exit(1);
        }
        other => other,
      };

      handled_not_found?;

      Ok(())
    }
  }
}
