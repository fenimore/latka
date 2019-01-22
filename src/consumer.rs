#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(non_snake_case)]
#![allow(unused_variables)]
use std::io::{Seek, SeekFrom, BufRead, BufReader, BufWriter};
use std::io::{Write, Lines};
use std::path::Path;
use std::fs::{OpenOptions, File};
use std::net::{TcpListener, TcpStream};

extern crate byteorder;

use std::io::Cursor;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};


const MESSAGE_PREFIX: u8 = 42;


// struct Consumer {
//     offset: u64,
//     reader: Lines<BufReader<File>>,
// }

// impl Consumer {
//     fn consume(&mut self) -> String {
//         let line = match self.reader.next() {
//             Some(res) => res,
//             None => Ok(String::from("")),
//         }.unwrap();

//         self.offset = line.len() as u64 + self.offset + 1;  // add one for newline
//         line
//     }
// }


fn main() {
    let mut stream  = TcpStream::connect("127.0.0.1:7070").unwrap();
    stream.write(&[42]).unwrap();

    let desired_offset: u32 = 4;
    let mut buffer = vec![];
    buffer.write_u32::<BigEndian>(desired_offset).unwrap();
    stream.write(buffer.as_slice()).unwrap();

    let mut reader = BufReader::new(stream).lines();

    // TODO: keep track of offset

    let buf = String::from("");
    loop {
        //buf.clear();
        let line = match reader.next() {
            Some(l) => l.unwrap(),
            None => break,
        };
        println!("{:?}", line);
    }
}
