use anyhow::{bail, Result};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
  name = "kv",
  version = env!("CARGO_PKG_VERSION")
)]
enum Kv {
  Get { key: String },
  Set { key: String, value: String },
  Rm { key: String },
}

fn main() -> Result<()> {
  match Kv::from_args() {
    Kv::Get { key: _ } => bail!("unimplemented"),
    Kv::Set { key: _, value: _ } => bail!("unimplemented"),
    Kv::Rm { key: _ } => bail!("unimplemented"),
  }
}
