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


fn send_offset(stream: &mut BufStream<TcpStream>, offset: u64) -> io::Result<()> {
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
    tcp_stream.set_nonblocking(true)?;
    let mut stream = BufStream::new(tcp_stream);
    stream.write(&[MESSAGE_PREFIX])?;

    let stdout = io::stdout();
    let mut writer = stdout.lock();

    'outer: loop {
        send_offset(&mut stream, offset)?;
        stream.flush()?;
        //writeln!(stream, "\0")?;

        let mut line = String::new();
        'inner: loop {
            let num_read = match stream.read_line(&mut line) {
                Ok(n) => n,
                Err(ref error) if error.kind() == ErrorKind::WouldBlock => {
                    // we've arrive at the end of the message queue
                    // the broker will block until out next nullbyte
                    // and then look for new messages
                    // so the consumer will sleep here
                    stream.flush()?;
                    thread::sleep(Duration::from_millis(1000));
                    continue 'outer;
                },
                Err(e) => {
                    writeln!(writer, "{} {:?}", offset, e)?;
                    break 'outer;
                }
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
    Ok(())
}
