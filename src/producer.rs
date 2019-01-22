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


const MESSAGE_PREFIX: u8 = 78;

fn main() {
    let mut stream  = TcpStream::connect("127.0.0.1:7070").unwrap();
    stream.write(&[MESSAGE_PREFIX]).unwrap();

    let mut counter = 0;
    loop {
        let buf = String::from("YELLOWSUBMARINE\n");
        stream.write(buf.as_bytes()).unwrap();
        counter = counter + 1;
        if counter > 10 {
            return;
        }
    }
}
