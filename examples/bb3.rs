// Redis protocol: RESP
// TCP to 6379
// Mostly request-response, except 1. pipeline 2. Pub/Sub
// It's a serialization protocol, which supports:
//   - Simple Strings, first byte of reply "+"
//     - no CR LF allowed
//   - Errors, "-"
//   - Integer, ":"
//     - string representation of an integer
//     - within i64 range
//     - 1/0 true/false
//   - Bulk Strings, "$"
//     - represent a single binary of up to 512MB
//     - format: $ + length + CR LF + binary + CR LF (not protocol)
//     - empty string $0\r\n\r\n
//     - NULL $-1\r\n (special)
//   - Arrays, "*"
//     - format: * + length + CR LF + each element's type encoding
//     - NULL *-1\r\n
// Client commands (requests) are always Array of Bulk Strings.
// Protocols always end with \r\n (CRLF)

use anyhow::{bail, Context, Result};
use std::io::{BufRead, BufReader, Read, Write};
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

fn main() -> Result<()> {
  match RunAs::from_args() {
    RunAs::Client => {
      let stream = TcpStream::connect("127.0.0.1:6379").context("Cannot connect")?;
      stream.set_read_timeout(Some(Duration::from_secs(10)))?;
      stream.set_write_timeout(Some(Duration::from_secs(10)))?;
      let mut reader = BufReader::new(stream);

      loop {
        let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
        reader.get_mut().write_all(ping_cmd)?;
        reader.get_mut().write_all(b"\r\n")?;

        println!("sent PING");

        // read Simple String type "+"
        expect_one(&mut reader, b'+', "Received non-Simple String reply")?;

        // read "PONG"
        let mut buf = [0; 4];
        const PONG: &[u8; 4] = b"PONG";
        reader.read_exact(&mut buf)?;
        for i in 0..4 {
          if buf[i] != PONG[i] {
            bail!("Received non-PONG reply");
          }
        }

        expect_end(&mut reader, "reply protocol")?;

        println!("recv PONG");

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

    // read Array type "*"
    expect_one(&mut reader, b'*', "Received non-Array command")?;

    // read Array len
    expect_len(&mut reader, 1, "Array")?;

    // consume Bulk String type "$"
    expect_one(&mut reader, b'$', "Command Array does not include Bulk String")?; // this doesn't work??

    expect_len(&mut reader, 4, "Bulk String")?;

    // read "PING"
    const PING: &[u8; 4] = b"PING";
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    for i in 0..4 {
      if buf[i] != PING[i] {
        bail!("Received non-PING command");
      }
    }

    expect_end(&mut reader, "Bulk String")?;

    expect_end(&mut reader, "command protocol")?;

    // Good PING command!
    println!("recv PING");

    let pong_reply = b"+PONG";
    reader.get_mut().write(pong_reply)?;
    reader.get_mut().write(b"\r\n")?;

    println!("sent PONG");
  }
}

fn expect_one(reader: &mut impl Read, expect: u8, error_msg: &'static str) -> Result<()> {
  let mut buf = [0; 1];
  reader.read_exact(&mut buf)?;
  if buf[0] != expect {
    bail!(error_msg);
  }
  Ok(())
}

fn expect_len(reader: &mut impl BufRead, expect: i64, target: &'static str) -> Result<()> {
  let mut buf = Vec::new();
  reader.read_until(b'\r', &mut buf)?;
  let (_, numbers) = buf.split_last().context(format!("Invalid length of {}", target))?;
  let len = str::from_utf8(numbers)?
    .parse::<i64>()
    .context(format!("Failed to parse {} length", target))?;

  if len != expect {
    bail!(format!("{} is not of length {}", target, expect));
  }

  // consume trailing \n
  let mut buf = [0; 1];
  reader.read_exact(&mut buf)?;
  if buf[0] != b'\n' {
    bail!(format!("Length of {} has invalid ending", target));
  }

  Ok(())
}

fn expect_end(reader: &mut impl Read, target: &'static str) -> Result<()> {
  let mut buf = [0; 2];
  reader.read_exact(&mut buf)?;
  if buf[0] != b'\r' || buf[1] != b'\n' {
    bail!(format!("Invalid ending of {}", target));
  }
  Ok(())
}
