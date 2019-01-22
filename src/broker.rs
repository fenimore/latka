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
use std::io::{Seek, SeekFrom, BufReader, BufWriter};
use std::io::{Write, Lines, Read, BufRead};
use std::path::Path;
use std::fs::{OpenOptions, File};
use std::net::{TcpListener, TcpStream};
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};


const CONSUMER: u8 = 42;
const PRODUCER: u8 = 78;



fn handle_producer(stream: TcpStream) {
    println!("Handle Producer");

    // TODO: handle creating the file (or segment) by the Broker
    // and have the path passed to handle_producer
    let f: File = OpenOptions::new().create(true).append(true)
        .read(false).open("topic.txt")
        .expect("open write file descriptor");
    let mut producer = BufWriter::new(f);

    let mut stream_reader = BufReader::new(stream).lines();

    loop {
        let next_line = stream_reader.next();
        let result   = match next_line {
            Some(res) => res,
            None => break,
        };
        let line = match result {
            Ok(l) => l,
            Err(e) =>{println!("{:?}", e); break},
        };
        producer.write(line.as_bytes()).unwrap();
        producer.write(&['\n' as u8]).unwrap();
        // I could concat() these two &[u8] slices, but it's
        // a buffered reader, so it's just extra overhead
        if producer.buffer().len() > 1024 {
            // Flush the internal Buffer if it gets too large
            producer.flush().ok();
        }
    }
    // producer flushes when it drops out of scope
}


// TODO: have it return error
fn handle_consumer(mut stream: TcpStream) {
    println!("Handle Consumer!");
    let offset = match stream.read_u32::<BigEndian>() {
        Ok(o) => o,
        Err(e) => {
            println!("Couldn't read offset!");
            return
        },
    };
    println!("Consumer at offset: {:?}", offset);

    let (offset, mut consumer): (u64, Lines<BufReader<File>>) = {
        let f = OpenOptions::new().write(false)
            .read(true).open("topic.txt")
            .expect("Error opening consumer file");
        let mut reader = BufReader::new(f);
        let off = reader.seek(SeekFrom::Start(offset as u64)).expect("Couldn't seek to offset");
        (off, reader.lines())
    };

    // todo: use syscall `sendfile` to copy directly from file to socket
    loop {
        let line = match consumer.next() {
            Some(x) => x,
            None => { break }
        }.unwrap();
        stream.write(line.as_bytes()).unwrap();
        stream.write(&['\n' as u8]).unwrap();
    }
}



fn main() {
    println!("Starting broker on 7070");

    {
        // Set up topic log for development
        let _ = OpenOptions::new().write(true).truncate(true).open("topic.txt").expect("truncate");

    }

    // TODO: parameterize the Port
    let listener = match TcpListener::bind("127.0.0.1:7070") {
        Ok(lst) => lst,
        Err(e) => panic!("Couldn't bind server"),
    };

    // accept connections and process them serially
    for incoming in listener.incoming() {
        let mut stream = match incoming {
            Ok(inc) => inc,
            Err(_) => continue,
        };
        let mut message_type = [0; 1];
        let _ = stream.read(&mut message_type).unwrap();
        match message_type[0] {
            CONSUMER => handle_consumer(stream),
            PRODUCER => handle_producer(stream),
            _ => println!("Nope"),
        }
    }
}
