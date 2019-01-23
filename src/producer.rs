#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(non_snake_case)]
#![allow(unused_variables)]
extern crate byteorder;

use std::env;
use std::io;
use std::io::{Seek, SeekFrom, BufRead, BufReader, BufWriter};
use std::io::{Write, Lines, Read};
use std::path::Path;
use std::fs::{OpenOptions, File};
use std::net::{TcpListener, TcpStream};

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use getopts::Options;



static USAGE: &str = "Streaming Producer from stdin.";
const MESSAGE_PREFIX: u8 = 78;


fn main() {
    let mut opts = Options::new();
    opts.optopt("p", "port", "broker port", "port");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(_) => {println!("{}", USAGE); return},
    };
    let port: u16 = match matches.opt_str("p") {
        Some(s) => s.parse().expect("Couldn't parse Port"),
        None => 7070,
    };

    // So each message is newline separated
    // and a producer ends streaming once it closes the connection
    let stream  = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let mut writer = BufWriter::new(stream);
    writer.write(&[MESSAGE_PREFIX]).unwrap();
    writer.flush().unwrap();

    let stdin = io::stdin();

    let mut input = String::new();
    loop {
        match stdin.read_line(&mut input) {
            Ok(n) => {if n == 0 {break}}
            Err(_) => break,
        }
        write!(writer, "{}", input).unwrap();
        input.clear();
    }
}
