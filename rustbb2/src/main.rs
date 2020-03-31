use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Serialize, Deserialize)]
enum Direction {
  Up,
  Down,
  Left,
  Right,
}

#[derive(Debug, Serialize, Deserialize)]
struct Move {
  dir: Direction,
  dist: i32,
}

fn main() -> Result<()> {
  let a = Move {
    dir: Direction::Up,
    dist: 10,
  };

  println!("[exercise1] a: {:?}", &a);

  let f = File::create("exercise1")?;
  serde_json::to_writer(f, &a)?;

  let f = File::open("exercise1")?;
  let b: Move = serde_json::from_reader(f)?;

  println!("[exercise1] b: {:?}", &b);

  Ok(())
}
