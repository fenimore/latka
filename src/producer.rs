extern crate byteorder;

use std::{env, io};
use std::io::{Write, BufWriter};
use std::net::{TcpStream};
use std::{thread, time};

use getopts::Options;



static USAGE: &str = "Streaming Producer from stdin.";
const MESSAGE_PREFIX: u8 = 78;


fn main() {
    let mut opts = Options::new();
    opts.optopt("s", "sleep", "sleep for testing", "sleep, milliseconds");
    opts.optopt("p", "port", "broker port", "port");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(_) => {println!("{}", USAGE); return},
    };
    let sleep: u64 = match matches.opt_str("s") {
        Some(p) => p.parse().expect("Couldn't parse pause"),
        None => 100,
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

        let pause = time::Duration::from_millis(sleep);
        thread::sleep(pause);
    }
}
