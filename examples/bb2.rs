use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Cursor, Write};

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

type Key = String;
type Value = String;
#[derive(Debug, Serialize, Deserialize)]
enum KvCommand {
  Set(Key, Value),
  Rm(Key),
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

  let v8 = serde_json::to_vec(&a)?;
  let b_v8: Move = serde_json::from_slice(&v8)?;

  println!("[exercise2] b from Vec<u8>: {:?}", &b_v8);

  let s = ron::ser::to_string(&a)?;
  println!("[exercise2] ron: {:?}", &s);
  let b_ron: Move = ron::de::from_str(&s)?;

  println!("[exercise2] b from Ron: {:?}", &b_ron);

  let mut moves: bson::Array = vec![];
  for d in 1..=1000 {
    let m = Move {
      dir: Direction::Up,
      dist: d,
    };

    moves.push(bson::to_bson(&m)?);
  }

  let mut doc = bson::Document::new();
  doc.insert("moves".to_owned(), moves);

  let mut buf = Vec::new();
  bson::encode_document(&mut buf, &doc)?;

  let doc = bson::decode_document(&mut Cursor::new(&buf[..]))?;
  for m in doc.get_array("moves")?.iter().take(3) {
    println!("[exercise3] move: {:?}", m);
  }

  let mut buf = Vec::<u8>::new();
  for x in 1..=10 {
    let cmd = KvCommand::Set(format!("key{}", x), format!("value{}", 1000 - x));
    buf.write_all(&rmp_serde::encode::to_vec(&cmd)?)?;
  }

  let mut de = rmp_serde::decode::Deserializer::new(Cursor::new(&buf[..]));

  loop {
    let pos = de.position();
    if let Ok(cmd) = KvCommand::deserialize(&mut de) {
      println!("{} {:?}", pos, cmd);
    } else {
      break;
    }
  }

  Ok(())
}
