// TODO: have main return Result and pass up errors instead of unwrapping
//#![allow(dead_code)]
//#![allow(unused_imports)]
//#![allow(non_snake_case)]
//#![allow(unused_variables)]
extern crate byteorder;

use std::{env, io, thread};
use std::time::Duration;
use std::io::{BufRead, Write, Error, ErrorKind};
use std::net::{TcpStream};

use bufstream::BufStream;
use byteorder::{WriteBytesExt, NetworkEndian};
use getopts::Options;


static USAGE: &str = "Stream consumer.";
const MESSAGE_PREFIX: u8 = 42;


fn handshake(stream: &mut BufStream<TcpStream>, offset: u64) -> io::Result<()> {
    stream.write(&[MESSAGE_PREFIX])?;

    let mut big_endian_buffer = vec![];
    big_endian_buffer.write_u64::<NetworkEndian>(offset)?;

    let num_written = stream.write(big_endian_buffer.as_slice())?;
    if num_written != 8 {
        return Err(Error::new(
            ErrorKind::ConnectionAborted, "Can't communicate with Broker"
        ));
    }
    Ok(())
}


fn main() -> io::Result<()>{
    let mut opts = Options::new();
    opts.optopt("t", "topic", "the stream topic (not implemented)", "topic");
    opts.optopt("o", "offset", "the offset to read stream from", "off");
    opts.optopt("p", "port", "broker host port (assume host is localhost)", "port");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(_) => {println!("{}", USAGE); return Ok(())},
    };
    let port: u16 = match matches.opt_str("p") {
        Some(s) => s.parse().expect("Couldn't parse Port"),
        None => 7070,
    };
    let mut offset: u64 = match matches.opt_str("o") {
        Some(s) => s.parse().expect("Couldn't parse offset"),
        None => 0,
    };

    let tcp_stream  = TcpStream::connect(("127.0.0.1", port))?;
    let mut stream = BufStream::new(tcp_stream);
    handshake(&mut stream, offset)?;

    let stdout = io::stdout();
    let mut writer = stdout.lock();

    stream.flush()?;

    let mut line = String::new();
    loop {
        let num_read = match stream.read_line(&mut line) {
            Ok(n) => n,
            Err(e) => {
                writeln!(writer, "{} {:?}", offset, e)?;
                break
            }
        };
        match num_read {
            0 => break,
            2 => {
                // Check for hearbeat KEEP alive signal
                // this is to communicate to the broker when
                // the consumer drops off if the consumer
                // has read to the end of the queue and is
                // waiting for more messages
                // The keep alive single is a null byte
                if line.as_bytes()[0] == 0 {
                    line.clear();
                    // reached the end of the queue, wait
                    // 100 milliseconds until next poll
                    thread::sleep(Duration::from_millis(100));
                    continue
                }
            },
            _ => {;},
        }

        let message = format!("{}: {}", offset, line);
        write!(writer, "{}", message).unwrap();

        offset += line.len() as u64;
        line.clear(); // clear to reuse the buffer
    }
    Ok(())
}
