extern crate byteorder;

use std::{env, io};
use std::io::{Write, BufWriter};
use std::net::{TcpStream};
use std::{thread, time};

use getopts::Options;



static USAGE: &str = "
Streaming message queue producer from stdin

Usage:
    producer
    producer [--sleep=number] [--port=number]
    producer [-s number] [-p number]

Options:
    -h --help     Show this screen.
    -p --port     Connect to broker on port [default 7070]
    -s --sleep    Milliseconds pause between writing to topic [default 100]
";

const MESSAGE_PREFIX: u8 = 78;


fn main() -> io::Result<()> {
    let mut opts = Options::new();
    opts.optopt("s", "sleep", "sleep for testing", "sleep, milliseconds");
    opts.optopt("p", "port", "broker port", "port");
    let args: Vec<_> = env::args().collect();
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(_) => {println!("{}", USAGE); return Ok(())},
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
    // TODO: handle unable to connect with more helpful message
    let stream  = TcpStream::connect(("127.0.0.1", port))?;
    let mut writer = BufWriter::new(stream);
    writer.write(&[MESSAGE_PREFIX])?;
    writer.flush()?;

    let stdin = io::stdin();

    let mut input = String::new();
    loop {
        match stdin.read_line(&mut input) {
            Ok(n) => {if n == 0 {break}}
            Err(_) => break,
        }
        write!(writer, "{}", input)?;
        input.clear();

        if sleep == 0 {
            continue
        }
        let pause = time::Duration::from_millis(sleep);
        thread::sleep(pause);
    }
    Ok(())
}
