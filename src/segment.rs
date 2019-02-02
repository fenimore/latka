#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_must_use)]
//#![allow(non_snake_case)]
#![allow(unused_variables)]
//#![feature(bufreader_buffer)]
use std::{io, fs, thread, env};
use std::cmp::{Ord, Ordering, PartialOrd, PartialEq};
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom, BufReader, BufWriter,Write, Read, BufRead, Error};
use std::io::ErrorKind::ConnectionReset;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::collections::BinaryHeap;



pub type Offset = u64;


pub enum Client { // enum for opening files
    Consumer,
    Producer,
}



#[derive(Debug)]
pub struct Segment {
    base_offset: Offset,
    filename: String,
    file: Option<File>,
}

impl Segment {
    pub fn new(partition_path: String, offset: u64) -> io::Result<Segment> {
        let filename = format!("{}/{:0>20}.log", partition_path, offset);
        Ok(Segment {
            base_offset: offset,
            filename: filename,
            file: None,
        })
    }
    pub fn open(&mut self, client: Client) ->  io::Result<()> {
        match client {
            Client::Consumer => {
                drop(OpenOptions::new().create(true).write(true).open(&self.filename)?); // touch
                let reader = OpenOptions::new().read(true).open(&self.filename)?;
                self.file = Some(reader);
                return Ok(());
            },
            Client::Producer => {
                let writer = OpenOptions::new().create(true).append(true).open(&self.filename)?;
                self.file = Some(writer);
                return Ok(());
            }
        }
    }

    pub fn close(&mut self) {
        let file = self.file.take();
        drop(file)
    }

    pub fn len(&self) -> u64 {
        if let Ok(attr) = fs::metadata(&self.filename) {
            return attr.len();
        }
        return 0
    }
}

impl Write for Segment {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(w) = &mut self.file {
            return w.write(buf)
        }
        Ok(0)
    }
    fn flush(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.file {
            return w.flush();
        }
        Ok(())
    }
}

impl Read for Segment {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(r) = &mut self.file {
            return r.read(buf);
        }
        Ok(0)
    }
}

impl Seek for Segment {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        if let Some(r) = &mut self.file {
            return r.seek(offset);
        }
        Ok(0)
    }
}


impl Eq for Segment { }

impl PartialEq for Segment {
    fn eq(&self, other: &Self) -> bool {
        self.base_offset == other.base_offset
    }
}

impl Ord for Segment {
    fn cmp(&self, other: &Self) -> Ordering {
        return self.base_offset.cmp(&other.base_offset).reverse()
    }
}

impl PartialOrd for Segment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}





#[cfg(test)]
extern crate speculate;

#[cfg(test)]
mod tests {
    use speculate::speculate;
    use std::{io, fs};
    use std::fs::{create_dir, remove_dir_all, remove_file};
    use std::io::{BufReader, BufWriter, Write, Read, BufRead, SeekFrom, Seek};
    use super::{Segment, Client};

    speculate! {
        const DATA: &[u8] = b"WOMBIESTWOODBINE";
        const SEGMENTPATH: &str = "tmp/";

        before {
            create_dir("tmp/");
        }

        after {
            remove_dir_all("tmp/");
        }

        test "new segment" {
            let segment = Segment::new(String::from("tmp/"), 0).expect("Cant open segment");
            assert_eq!(segment.base_offset, 0);
        }

        describe "consumer" {
            test "consumer can seek" {
                let mut segment = Segment::new(String::from(SEGMENTPATH), 0).expect("can't open segment");

                segment.open(Client::Producer).expect(" open write file");
                let n = segment.write(DATA).expect("writing eight bytes");
                segment.close();

                segment.open(Client::Consumer).expect("open read file");
                segment.seek(SeekFrom::Start(4)).expect("seek");
                let mut buf = [0; 4];
                let n = segment.read(&mut buf).expect("reading 4 bytes");
                assert_eq!(n, 4);
                assert_eq!(&buf, b"IEST");
            }
            test "consumer can read" {
                let mut segment = Segment::new(String::from(SEGMENTPATH), 0).expect("Can't open segment");
                segment.open(Client::Producer).expect(" open write file");
                let n = segment.write(DATA).expect("writing eight bytes");
                segment.close();

                segment.open(Client::Consumer).expect("open read file");
                let mut buf = [0; 8];
                let n = segment.read(&mut buf).expect("writing eight bytes");

                assert_eq!(n, 8);
                assert_eq!(&buf, b"WOMBIEST");
            }

            test "consumer can't write" {
                let mut segment = Segment::new(String::from(SEGMENTPATH), 0).expect("Cant open segment");
                segment.open(Client::Consumer).expect("open write file");
                let result = segment.write(DATA);
                assert!(result.is_err(), "consumer shouldn't write");
            }

        }
        describe "producer" {
            test "producer writes" {
                let mut segment = Segment::new(String::from(SEGMENTPATH), 0).expect("Cant open segment");
                segment.open(Client::Producer).expect("open write file");
                let n = segment.write(DATA).expect("writing eight bytes");
                assert_eq!(n, 16);
            }

            test "producer can't read" {
                let mut segment = Segment::new(String::from(SEGMENTPATH), 0).expect("Cant open segment");
                segment.open(Client::Producer).expect("open write file");
                segment.write(DATA).expect("write to file");

                let mut buf = [0; 8];
                let result = segment.read(&mut buf);
                assert!(result.is_err(), "producer shouldn't read");
            }
        }
    }
}
