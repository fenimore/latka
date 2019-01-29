// TODO: better loggin
// TODO: mutex unwrapping?
// TODO: partition folders
#![allow(dead_code)]
#![allow(unused_imports)]
//#![allow(non_snake_case)]
#![allow(unused_variables)]
//#![feature(bufreader_buffer)]
use std::{io, fs, thread, env};
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom, BufReader, BufWriter,Write, Read, BufRead, Error};
use std::io::ErrorKind::ConnectionReset;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

use bufstream::BufStream;
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
  -t --topic    Specify which topic [default topic]
  -p --port     Serve on port [default 7070]
  -c --create   Create topic if it doesn't exist
";


const SEGMENT_SIZE: u64 = 32;
const CONSUMER_MESSAGE_PREFIX: u8 = 42;
const PRODUCER_MESSAGE_PREFIX: u8 = 78;

type Offset = u64;


// segment_offset: LinkedList<Offset>,
// let mut offsets: LinkedList<Offset> = LinkedList::new();
// offsets.extend(sorted_segments);

struct Partition {
    largest_offset: Mutex<Offset>,
    latest_segment: Mutex<Offset>,
    segments_count: Mutex<usize>,
    topic: String,
    partition: u32,
}

impl Partition {
    fn new(topic: String, part: u32) -> io::Result<Partition> {
        let sorted_segments = crawl_sorted_segments(&topic)?;
        let (largest_base_offset, count) = match sorted_segments.last() {
            None => (0, sorted_segments.len()),
            Some(seg) => (*seg, sorted_segments.len()),
        };
        let last_segment_name = format!("{}/{}/{:0>20}.log", topic, part, largest_base_offset);
        let count = if count == 0 {
            OpenOptions::new().write(true).create_new(true).open(&last_segment_name)?;
            1
        } else {
            count
        };
        let size_of_last_file: Offset = fs::metadata(last_segment_name)?.len();
        Ok(Partition {
            partition: part,
            topic: topic,
            segments_count: Mutex::new(count),
            largest_offset: Mutex::new(largest_base_offset + size_of_last_file),
            latest_segment: Mutex::new(largest_base_offset),
        })
    }

    fn log_filename(&self, base_offset: Offset) -> String {
        format!("{}/{}/{:0>20}.log", self.topic, self.partition, base_offset)
    }

    fn open_consumer_segment_at_offset(&self, base_offset: Offset) -> io::Result<File> {
        let log = OpenOptions::new().
            write(false).
            read(true).
            open(self.log_filename(base_offset))?;

        Ok(log)
    }

    fn open_latest_segment_for_appending(&self) -> io::Result<(File, Offset)>{
        let base_offset: Offset = {
            let n = self.latest_segment.lock().unwrap();
            *n
        };
        let log = OpenOptions::new().
            create(true).
            append(true).
            open(self.log_filename(base_offset))?;

        Ok((log, base_offset))
    }


}


fn crawl_sorted_segments(topic: &String) -> io::Result<Vec<Offset>> {
    let mut segments: Vec<Offset> = fs::read_dir(topic)?.map(
        |entry| {
            let path = entry.unwrap().path();
            let stem = path.as_path().file_stem().unwrap();
            let str_stem = stem.to_str().unwrap();
            str_stem.parse::<Offset>().unwrap()
        }
    ).collect();
    segments.sort_unstable();
    Ok(segments)
}



fn handle_producer(stream: TcpStream, partition: Arc<Partition>) -> Result<(), Error> {
    let mut reader = BufReader::new(stream);

    'outer: loop {
        let (segment_file, curr_seg_base_offset) = partition.open_latest_segment_for_appending()?;

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
            let mut off = partition.largest_offset.lock().unwrap();
            *off += n as u64;
            let mut last_seg = partition.latest_segment.lock().unwrap();
            if *last_seg > curr_seg_base_offset {
                // Data consistency guarantees hold as long as you are producing
                // to one partition and consuming from one partition.
                // All guarantees are off if you are reading from the same
                // partition using two consumers or writing to the same partition
                // using two producers~ https://sookocheff.com/post/kafka/kafka-in-a-nutshell/
                continue 'outer
            }
            let mut segment_count = partition.segments_count.lock().unwrap();
            if (*segment_count as u64 * SEGMENT_SIZE) < *off {
                *segment_count += 1;
                *last_seg = *off;
                continue 'outer;
            }
        }
    }
    Ok(())
}

fn handle_consumer(tcp_stream: TcpStream, partition: Arc<Partition>) ->  Result<Offset, Error> {
    let mut stream = BufStream::new(tcp_stream);
    let mut offset: Offset = stream.read_u64::<NetworkEndian>()?;
    println!("Feeding Consumer at Offset: {:?}", offset);
    'infinite: loop{
        // flush remaining messages before sending heartbeat
        stream.flush()?;
        // keep alive NULLBYTE
        // This heartbeat message informs the broker
        // when the connection is dropped if the consumer
        // is waiting for more messages.
        writeln!(stream, "\0")?;

        let segment_offsets = crawl_sorted_segments(&partition.topic)?;
        let mut peekable_segments = segment_offsets.iter().peekable();

        'outer: loop {
            let seg_base_offset = match peekable_segments.next() {
                Some(o) => *o,
                None => break 'outer,
            };
            if offset < seg_base_offset {break 'outer} // peekable segments are sorted small->big

            if let Some(n) = peekable_segments.peek() {
                if offset >= **n {
                    continue 'outer  // already consumed this segment
                }
            }

            let mut reader: BufReader<File> = {
                let file = partition.open_consumer_segment_at_offset(seg_base_offset)?;
                let mut reader = BufReader::new(file);
                let relative_offset = offset - seg_base_offset;
                let _ = reader.seek(SeekFrom::Start(relative_offset))?;
                reader
            };

            // NOTE: the BufStream is flushed every segment
            stream.flush()?;
            let mut buffer = String::new();
            'inner: loop {
                let n = reader.read_line(&mut buffer)?;
                if n == 0 {
                    continue 'outer;
                }
                // TODO: use syscall `sendfile` to copy directly from file to socket
                match write!(stream, "{}", buffer){
                    Ok(_) => {;},
                    Err(_) => break 'infinite,
                };
                buffer.clear();
                offset += n as Offset;
            }
        }
    }
    Ok(offset)
}


fn main() -> Result<(), Error> {
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
        fs::create_dir(&topic)?;
    };
    if matches.opt_present("r") {
        fs::remove_dir_all(&topic)?;
        fs::create_dir(&topic)?;
    };
    let port: u16 = match matches.opt_str("p") {
        Some(s) => s.parse().expect("Couldn't parse Port"),
        None => 7070,
    };

    println!("Broker listening on  127.0.0.1:{}", port);
    let listener = TcpListener::bind(("127.0.0.1", port))?;



    let partition = Arc::new(
        Partition::new(topic.clone(), 0)?
    );

    for incoming in listener.incoming() {
        let mut stream = match incoming {
            Ok(inc) => inc,
            Err(_) => continue,
        };
        let mut message_type = [0; 1];
        let _ = stream.read(&mut message_type).unwrap();
        match message_type[0] {
            CONSUMER_MESSAGE_PREFIX => {
                let partition = Arc::clone(&partition);
                thread::spawn(|| {
                    match handle_consumer(stream, partition) {
                        Ok(n) => println!("SUCCESS: Consumer stopped consuming at offset {}", n),
                        Err(ref e) if e.kind() == ConnectionReset => println!("Consumer dropped off"),
                        Err(e) => println!("ERROR CON: {:?}", e),
                    };
                });
            },
            PRODUCER_MESSAGE_PREFIX => {
                let partition = Arc::clone(&partition);
                thread::spawn(move || {
                    match handle_producer(stream, partition) {
                        Ok(_) => println!("SUCCESS: Producer finished."),
                        Err(e) => println!("ERROR PRO: {:?}", e),
                    };
                });
            },
            _ => println!("Unrecognizable Message Prefix {}", message_type[0]),
        }
    };
    return Ok(());
}
