// serde data format
// Rust types -- (impl Serialize) --> serde data model (types) -- (impl Serializer) --> String/Vec<u8>/Write...
// &str/&[u8]/Read... -- (impl Deserializer) --> serde data model (types) -- (impl Deserialize) --> Rust types

use anyhow::{Context, Result};
use resp_serde::{read_command, read_reply, write_command, write_reply};
use serde::{Deserialize, Serialize};
use std::io::BufReader;
use std::net::{TcpListener, TcpStream};
use std::str;
use std::thread::sleep;
use std::time::Duration;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "bb3")]
enum RunAs {
  Client,
  Server,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum Command {
  Ping,
}

fn main() -> Result<()> {
  match RunAs::from_args() {
    RunAs::Client => {
      let stream = TcpStream::connect("127.0.0.1:6379").context("Cannot connect")?;
      stream.set_read_timeout(Some(Duration::from_secs(10)))?;
      stream.set_write_timeout(Some(Duration::from_secs(10)))?;
      let mut reader = BufReader::new(stream);

      loop {
        let cmd = Command::Ping;
        write_command(&cmd, reader.get_mut()).context("Writing command")?;

        println!("sent PING");

        let reply: std::result::Result<String, String> = read_reply(&mut reader).context("Reading reply")?;

        match reply {
          Ok(reply) => println!("recv {}", &reply),
          Err(err) => println!("error {}", &err),
        }

        sleep(Duration::from_secs(2));
      }
    }
    RunAs::Server => {
      let listener = TcpListener::bind("127.0.0.1:6379").context("Cannot bind")?;

      for stream in listener.incoming() {
        server_loop(stream?)?;
      }
    }
  }

  Ok(())
}

fn server_loop(stream: TcpStream) -> Result<()> {
  stream.set_read_timeout(Some(Duration::from_secs(10)))?;
  stream.set_write_timeout(Some(Duration::from_secs(10)))?;
  let mut reader = BufReader::new(stream);

  loop {
    // always expect PING command here

    let cmd = read_command(&mut reader).context("Reading command")?;
    assert_eq!(Command::Ping, cmd);

    // Good PING command!
    println!("recv PING");
    let reply = "PONG".to_owned();

    write_reply(&reply, reader.get_mut()).context("Writing reply")?;

    println!("sent PONG");
  }
}
