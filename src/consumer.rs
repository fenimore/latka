// TODO: have main return Result and pass up errors instead of unwrapping
//#![allow(dead_code)]
//#![allow(unused_imports)]
//#![allow(non_snake_case)]
//#![allow(unused_variables)]
extern crate byteorder;

use std::env;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream};

use byteorder::{WriteBytesExt, NetworkEndian};
use getopts::Options;


static USAGE: &str = "Stream consumer.";
const MESSAGE_PREFIX: u8 = 42;


fn main() {
    let mut opts = Options::new();
    opts.optopt("t", "topic", "the stream topic (not implemented)", "topic");
    opts.optopt("o", "offset", "the offset to read stream from", "off");
    opts.optopt("p", "port", "broker host port (assume host is localhost)", "port");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(_) => {println!("{}", USAGE); return},
    };
    let port: u16 = match matches.opt_str("p") {
        Some(s) => s.parse().expect("Couldn't parse Port"),
        None => 7070,
    };
    let mut offset: u64 = match matches.opt_str("o") {
        Some(s) => s.parse().expect("Couldn't parse offset"),
        None => 0,
    };

    let mut stream  = match TcpStream::connect(("127.0.0.1", port)) {
        Ok(s) => s,
        Err(e) => panic!(e),
    };
    stream.write(&[MESSAGE_PREFIX]).unwrap();

    let mut buffer = vec![];
    buffer.write_u64::<NetworkEndian>(offset).unwrap();
    let num_written = stream.write(buffer.as_slice()).unwrap();
    if num_written != 8 {
        panic!("Can't communicate with broker");
    }

    let mut reader = BufReader::new(stream);
    let stdout = io::stdout();
    let mut writer = stdout.lock();

    let mut line = String::new(); // may also use with_capacity if you can guess
    loop {
        let num_read = match reader.read_line(&mut line) {
            Ok(n) => n,
            Err(e) =>{
                writeln!(writer, "{} {:?}", offset, e).unwrap();
                break
            },
        };
        if num_read == 0 {
            break
        }

        let message = format!("{}: {}", offset, line);
        write!(writer, "{}", message).unwrap();

        offset += line.len() as u64;
        line.clear(); // clear to reuse the buffer
    }
}
