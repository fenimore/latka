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
//#![allow(dead_code)]
//#![allow(unused_imports)]
//#![allow(non_snake_case)]
//#![allow(unused_variables)]
//#![feature(bufreader_buffer)]
use std::{io, fs, thread, env};
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom, BufReader, BufWriter,Write, Read, BufRead, Error};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use byteorder::{ReadBytesExt, NetworkEndian};
use getopts::Options;

static USAGE: &str = "
broker message queue
Usage:
  broker
  broker [--topic=dirname] [--port=number] [--create]
  broker [-t dirname] [-p number] [-c]

Options:
  -h --help     Show this screen.
  -t --topic    Specify which topic (directory of segments)
  -p --port     Serve on port
  -c --create   Create topic if it doesn't exist
";


const SEGMENT_SIZE: u64 = 32;
const CONSUMER_MESSAGE_PREFIX: u8 = 42;
const PRODUCER_MESSAGE_PREFIX: u8 = 78;

type Offset = u64;

fn log_filename(topic: &String, base_offset: Offset) -> String {
    format!("{}/{:0>20}.log", topic, base_offset)
}

fn get_segment(path: String) -> io::Result<File>{
    let log = OpenOptions::new().create(true).append(true).open(path)?;

    Ok(log)
}

fn sorted_segments(topic: &String) -> io::Result<Vec<Offset>> {
    let mut segments: Vec<Offset> = fs::read_dir(topic)?.map(|entry| {
        let path = entry.unwrap().path();
        let stem = path.as_path().file_stem().unwrap();
        let str_stem = stem.to_str().unwrap();
        str_stem.parse::<Offset>().unwrap()
    }).collect();
    segments.sort_unstable();
    Ok(segments)
}


fn scan_topic(topic: String) -> io::Result<(Offset, usize)> {
    let mut largest_base_offset: Offset = 0;
    let mut segment_count: usize = 0;
    for entry in fs::read_dir(topic)? {
        let file = entry?;
        let path = file.path();
        let as_path = path.as_path();
        let stem = as_path.file_stem().unwrap();
        let str_stem = stem.to_str().unwrap();
        let parsed = str_stem.parse::<Offset>().unwrap();

        largest_base_offset = if parsed > largest_base_offset {
            parsed
        } else {
            largest_base_offset
        };
        segment_count += 1;
    }
    Ok((largest_base_offset, segment_count))
}


fn handle_producer(
    topic: String,
    stream: TcpStream,
    offset: Arc<Mutex<Offset>>,
    last_seg: Arc<Mutex<Offset>>,
    seg_count: Arc<Mutex<usize>>,
) -> Result<(), Error> {
    let mut reader = BufReader::new(stream);

    'outer: loop {
        let (seg_name, curr_seg): (String, u64) = {
            let n = last_seg.lock().unwrap();
            (log_filename(&topic, *n), *n)
        };
        let segment_file = get_segment(seg_name)?;
        let mut segment = BufWriter::new(segment_file);

        let mut buffer = String::new();
        'inner: loop {
            let n = reader.read_line(&mut buffer)?;
            if n == 0 {
                break 'outer;
            }
            write!(segment, "{}", buffer)?; // does write error if not all bytes are written?
            buffer.clear();

            // update segment if it is "filled"
            let mut off = offset.lock().unwrap();
            *off += n as u64;
            let mut last_seg = last_seg.lock().unwrap();
            if *last_seg > curr_seg {
                println!("Potential misalignment of offsets here");
                continue 'outer
            }
            let mut segment_count = seg_count.lock().unwrap();
            if (*segment_count as u64 * SEGMENT_SIZE) < *off {
                *segment_count += 1;
                *last_seg = *off;
                continue 'outer;
            }

        }
    }
    Ok(())
}

fn handle_consumer(mut stream: TcpStream, topic: String, _global_offset: Arc<Mutex<Offset>>) ->  Result<Offset, Error> {
    let mut offset: Offset = stream.read_u64::<NetworkEndian>()?;
    println!("Feeding Consumer at Offset: {:?}", offset);
    let mut writer = BufWriter::new(stream);
    'infinite: loop{
        // TODO: communicate with consumer to handle disconnection
        // maybe blocking of a BufSream read?
        writer.flush()?;
        // TODO: configure when to flush underlying buffer
        // ATM the consumer is flushed data every segment
        let sorted_segments = sorted_segments(&topic)?;
        let mut peekable_segments = sorted_segments.iter().peekable();

        'outer: loop {
            let seg_base_offset = match peekable_segments.next() {
                Some(o) => o,
                None => break 'outer,
            };
            if offset < *seg_base_offset {
                break 'outer;  // seg_base_offsets will only get bigger
            }
            match peekable_segments.peek() {
                Some(n) => {
                    if offset >= **n { continue 'outer }  // already consumed this segment
                },
                None => {;}, // reached final segment, so process this segment
            }
            let mut reader: BufReader<File> = {
                let seg_path = log_filename(&topic, *seg_base_offset);
                let f = OpenOptions::new().write(false).read(true).open(seg_path)?;
                let mut reader = BufReader::new(f);
                let relative_offset = offset - *seg_base_offset;
                let _ = reader.seek(SeekFrom::Start(relative_offset))?;
                reader
            };
            let mut buffer = String::new();
            // TODO: use sysc)all `sendfile` to copy directly from file to socket

            'inner: loop {
                let n = reader.read_line(&mut buffer)?;
                if n == 0 {
                    continue 'outer;
                }
                match write!(writer, "{}", buffer){
                    Ok(_) => {;},
                    Err(_) => break 'infinite,
                };
                buffer.clear();
                offset += n as Offset;
            }
        }
        // TODO: handle if global_offset is now greater than consumed offset
    }
    Ok(offset) // this is an error? :thikning:
}


fn main() -> Result<(), Error>{
    let mut opts = Options::new();
    opts.optopt("p", "port", "broker port", "port");
    opts.optopt("t", "topic", "topic name", "topic");
    opts.optflag("r", "remove", "remove topic (for recreating)");
    opts.optflag("c", "create", "create topic");
    opts.optflag("h", "help", "print usage");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(_) => return Ok(()),
    };
    if matches.opt_present("h") {
        println!("{}", USAGE);
        return Ok(())
    }
    let topic = match matches.opt_str("t") {
        Some(s) => s,
        None => String::from("topic"),
    };
    if matches.opt_present("c") {
        // uhhh
        fs::create_dir(&topic)?;

        let init_seg = log_filename(&topic, 0);
        let _ = get_segment(init_seg)?;
    };
    if matches.opt_present("r") {
        fs::remove_dir_all(&topic)?;
        fs::create_dir(&topic)?;
        let init_seg = log_filename(&topic, 0);
        let _ = get_segment(init_seg)?;
    };
    let port: u16 = match matches.opt_str("p") {
        Some(s) => s.parse().expect("Couldn't parse Port"),
        None => 7070,
    };

    println!("Broker listening on  127.0.0.1:{}", port);
    let listener = TcpListener::bind(("127.0.0.1", port))?;


    let (largest_base_offset, count) = scan_topic(topic.clone())?;
    // TODO: put into struct
    let size_of_last_file = {
        let last_seg_name = log_filename(&topic, largest_base_offset);
        let meta = fs::metadata(last_seg_name)?;
        meta.len()
    };
    let largest_offset = Arc::new(Mutex::new(largest_base_offset + size_of_last_file));
    let last_segment = Arc::new(Mutex::new(largest_base_offset));
    let segment_count = Arc::new(Mutex::new(count));

    for incoming in listener.incoming() {
        let mut stream = match incoming {
            Ok(inc) => inc,
            Err(_) => continue,
        };
        let mut message_type = [0; 1];
        let _ = stream.read(&mut message_type).unwrap();
        match message_type[0] {
            CONSUMER_MESSAGE_PREFIX => {
                let largest_offset = largest_offset.clone();
                let t = topic.clone();
                thread::spawn(|| {
                    match handle_consumer(stream, t, largest_offset) {
                        Ok(n) => {println!("Served Consumer to offset {}", n);},
                        Err(e) => println!("ERROR: {:?}", e),
                    };
                });
            },
            PRODUCER_MESSAGE_PREFIX => {
                let largest_offset = largest_offset.clone();
                let last_segment = last_segment.clone();
                let segment_count = segment_count.clone();
                let top = topic.clone();
                thread::spawn(|| {
                    match handle_producer(top, stream, largest_offset, last_segment, segment_count) {
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
