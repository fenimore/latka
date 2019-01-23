// Simple  storage:  Kafka  has  a  very  simple  storage  layout.  Each
//     partition of a topic corresponds to a logical log. Physically, a log
//     is  implemented  as  a  set  of  segment  files  of  approximately  the
//     same size (e.g., 1GB). Every time a producer publishes a message
//     to  a  partition,  the  broker  simply  appends  the  message  to  the  last
//     segment file. For better performance, we flush the segment files to
//     disk  only  after  a  configurable  number  of  messages  have  been
//     published  or  a  certain  amount  of  time  has  elapsed.  A  message  is
//     only exposed to the consumers after it is flushed
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(non_snake_case)]
#![allow(unused_variables)]
#![feature(bufreader_buffer)]
use std::env;
use std::result::Result;
use std::convert::AsRef;
use std::io::{Seek, SeekFrom, BufReader, BufWriter};
use std::io::{Write, Lines, Read, BufRead, Error};
use std::path::Path;
use std::fs::{OpenOptions, File};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use getopts::Options;

static USAGE: &str = "Streaming Broker.";
static SEGMENT_PATH: &str = "topic.txt";

const CONSUMER_MESSAGE_PREFIX: u8 = 42;
const PRODUCER_MESSAGE_PREFIX: u8 = 78;


// TODO: handle creating the file (or segment) by the Broker
// and have the path passed to handle_producer
fn handle_producer(stream: TcpStream, global_offset: Arc<Mutex<u32>>) -> Result<(), Error> {
    println!("Handle Producer");

    let f: File = OpenOptions::new().append(true).read(false).open(SEGMENT_PATH)?;
    let mut producer = BufWriter::new(f);

    let mut reader = BufReader::new(stream);

    let mut buffer = String::new();
    loop {
        let n = reader.read_line(&mut buffer)?;
        if n == 0 {
            break
        } else {
            let mut pointer = global_offset.lock().unwrap();
            *pointer += buffer.len() as u32;
        }

        write!(producer, "{}", buffer).unwrap();

        buffer.clear();
    }
    Ok(())
}    // producer flushes when it drops out of scope

fn handle_consumer(mut stream: TcpStream, global_offset: Arc<Mutex<u32>>) ->  Result<(), Error> {
    let offset = match stream.read_u32::<BigEndian>() {
        Ok(o) => {
            let global_offset = global_offset.lock().unwrap();
            if *global_offset < o {
                return Ok(()); // TODO: custom error
            }
            o
        },
        Err(e) => {
            return Err(e);
        },
    };
    println!("Consumer at offset: {:?}", offset);

    let mut writer = BufWriter::new(stream);
    let (offset, mut reader): (u64, BufReader<File>) = {
        let f = OpenOptions::new().write(false).read(true).open("topic.txt")?;
        let mut reader = BufReader::new(f);
        let off = reader.seek(SeekFrom::Start(offset as u64))?;
        (off, reader)
    };

    // TODO: use syscall `sendfile` to copy directly from file to socket
    let mut buffer = String::new();
    loop {
        let n = reader.read_line(&mut buffer)?;
        if n == 0 {
            break
        }

        write!(writer, "{}", buffer).unwrap();
        buffer.clear();
    }
    Ok(())
}



fn main() -> Result<(), Error>{
    let mut opts = Options::new();
    opts.optopt("p", "port", "broker port", "port");
    opts.optflag("t", "truncate", "truncate the topic");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => return Ok(()),
    };
    if matches.opt_present("t") {
        let _  = OpenOptions::new().create(true).write(true).truncate(true).open("topic.txt")?;
    };
    let port: u16 = match matches.opt_str("p") {
        Some(s) => s.parse().expect("Couldn't parse Port"),
        None => 7070,
    };

    println!("Broker listening on  127.0.0.1:{}", port);
    let listener = TcpListener::bind(("127.0.0.1", port))?;

    let global_offset = Arc::new(Mutex::new(0_u32));

    // accept connections and process them serially
    for incoming in listener.incoming() {
        let mut stream = match incoming {
            Ok(inc) => inc,
            Err(_) => continue,
        };
        let mut message_type = [0; 1];
        let _ = stream.read(&mut message_type).unwrap();
        match message_type[0] {
            CONSUMER_MESSAGE_PREFIX => {
                let global_offset = global_offset.clone();
                thread::spawn(|| {
                    match handle_consumer(stream, global_offset) {
                        Ok(_) => {;},
                        Err(e) => println!("ERROR: {:?}", e),
                    };
                });
            },
            PRODUCER_MESSAGE_PREFIX => {
                let global_offset = global_offset.clone();
                thread::spawn(|| {
                    match handle_producer(stream, global_offset) {
                        Ok(_) => {;},
                        Err(e) => println!("ERROR: {:?}", e),
                    };
                });
            },
            _ => println!("Unrecognizable Message Prefix {}", message_type[0]),
        }
    };
    return Ok(());
}
