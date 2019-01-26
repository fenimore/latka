// Because  of  limitations  in  existing  systems,  we  developed  a  new
//     messaging-based  log  aggregator  Kafka.  We  first  introduce  the
//     basic concepts in Kafka. A stream of messages of a particular type
//     is defined by a topic. A producer
//     can publish messages to a topic.
//     The  published  messages  are  then  stored  at  a  set  of  servers  called
//     brokers. A consumer can subscribe to one or more topics from the
//     brokers,  and  consume  the  subscribed  messages  by  pulling  data
//     from the brokers.

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
use std::io;
use std::fs;
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
static TOPIC: &str = "topic";
static SEGMENT_PATH: &str = "topic.txt";

const SEGMENT_SIZE: u32 = 1024;
const CONSUMER_MESSAGE_PREFIX: u8 = 42;
const PRODUCER_MESSAGE_PREFIX: u8 = 78;



struct Partition {
    topic_name: String,
    number: u32,
    path: String,
    largest_offset: Arc<Mutex<u32>>,
    active_segment: Arc<Mutex<u32>>,
}

// a partition is a logical log, physically divided into segments
impl Partition {
    fn new(name: String, num: u32) -> Partition {
        let partition_path = format!("{}/{}-{}", name, name, num);
        Partition {
            topic_name: name,
            number: num,
            path: partition_path,
            latest_segment: Arc::new(Mutex::new(0_u32)),
            largest_offset: Arc::new(Mutex::new(0_u32)),
        }
    }

    fn create_directory(&self) -> io::Result<()> {
        fs::create_dir(self.topic_name.as_str())?;
        fs::create_dir(self.path.as_str())?;
        self.get_latest_segment()?;

        Ok(())
    }

    fn get_latest_segment(&self) -> io::Result<File> {
        let seg_name = {
            let n = self.latest_segment.lock().unwrap();
            format!("{:0>10}.log", *n)
        };

        let seg_path = Path::new(self.path.as_str()).join(seg_name.as_str());
        let segment = OpenOptions::new().create(true).append(true).open(seg_path)?;
        Ok(segment)
    }
}



fn get_segment(topic: &String, name: String) -> io::Result<File>{
    //let name = format!("{:0>21}", n);
    let log_path = Path::new(topic).join(name.as_str());
    let log = OpenOptions::new().create(true).append(true).open(log_path)?;

    Ok(log)
}



// TODO: handle creating the file (or segment) by the Broker
// and have the path passed to handle_producer
fn handle_producer(
    topic: String, stream: TcpStream, offset: Arc<Mutex<u32>>, last_seg: Arc<Mutex<u32>>
) -> Result<(), Error> {

    println!("Handle Producer");
    let mut reader = BufReader::new(stream);

    'outer: loop {
        let seg_name = {
            let n = last_seg.lock().unwrap();
            format!("{:0>21}.log", *n)
        };
        let segment_file = get_segment(&topic, seg_name)?;
        let mut segment = BufWriter::new(segment_file);

        let mut buffer = String::new();
        'inner: loop {
            let n = reader.read_line(&mut buffer)?;
            if n == 0 {
                break 'outer;
            } else {
                let mut off = offset.lock().unwrap();
                *off += buffer.len() as u32;
            }
            write!(segment, "{}", buffer).unwrap();
            buffer.clear();

            let mut seg = last_seg.lock().unwrap();
            let off = offset.lock().unwrap();
            if ((*seg + 1) * SEGMENT_SIZE) < *off {
                // increment latest segment
                // close current segment and continue with next segment
                *seg += 1;
                continue 'outer;
            }

        }
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

    // TODO: use sysc)all `sendfile` to copy directly from file to socket
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

fn find_latest_segment(topic: String) -> io::Result<u32> {
    let mut largest_offset: u32 = 0;
    for entry in fs::read_dir(topic)? {
        let file = entry?;
        let path = file.path();
        let as_path = path.as_path();
        let stem = as_path.file_stem().unwrap();
        let str_stem = stem.to_str().unwrap();
        let parsed = str_stem.parse::<u32>().unwrap();

        largest_offset = if parsed > largest_offset {
            parsed
        } else {
            largest_offset
        }
    }
    Ok(largest_offset)
}

fn main() -> Result<(), Error>{
    let mut opts = Options::new();
    opts.optopt("p", "port", "broker port", "port");
    opts.optopt("t", "topic", "topic name", "topic");
    opts.optflag("c", "create", "create topic");
    opts.optflag("h", "help", "print usage");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(e) => return Ok(()),
    };
    if matches.opt_present("h") {
        println!("Yup");
        return Ok(())
    }
    let topic = match matches.opt_str("t") {
        Some(s) => s,
        None => String::from("topic"),
    };
    if matches.opt_present("c") {
        // uhhh
        fs::create_dir(&topic)?;
        let init_seg = format!("{:0>21}.log", 0);
        let _ = get_segment(&topic, init_seg)?;
    };
    let port: u16 = match matches.opt_str("p") {
        Some(s) => s.parse().expect("Couldn't parse Port"),
        None => 7070,
    };

    println!("Broker listening on  127.0.0.1:{}", port);
    let listener = TcpListener::bind(("127.0.0.1", port))?;

    let latest = find_latest_segment(topic.clone())?;
    let latest_segment = Arc::new(Mutex::new(latest));
    let topic_offset = Arc::new(Mutex::new(0_u32));

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
                let topic_offset = topic_offset.clone();
                thread::spawn(|| {
                    match handle_consumer(stream, topic_offset) {
                        Ok(_) => {;},
                        Err(e) => println!("ERROR: {:?}", e),
                    };
                });
            },
            PRODUCER_MESSAGE_PREFIX => {
                let topic_offset = topic_offset.clone();
                let latest_segment = latest_segment.clone();
                let top = topic.clone();
                thread::spawn(|| {
                    match handle_producer(top, stream, topic_offset, latest_segment) {
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
