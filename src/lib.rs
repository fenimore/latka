#![allow(dead_code)]
#![allow(unused_imports)]
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



struct Segment {
    base_offset: Offset,
    filename: String,
    file: Option<File>,
}

impl Segment {
    fn new(partition_path: String, offset: u64) -> io::Result<Segment> {
        let filename = format!("{}/{:0>20}.log", partition_path, offset);
        Ok(Segment {
            base_offset: offset,
            filename: filename,
            file: None,
        })
    }
    fn open(&mut self, client: Client) ->  io::Result<()> {
        match client {
            Client::Consumer => {
                OpenOptions::new().create(true).open(&self.filename)?;
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

    fn close(&mut self) {
        let file = self.file.take();
        drop(file)
    }

    fn len(&self) -> u64 {
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
        self.base_offset.cmp(&other.base_offset)
    }
}

impl PartialOrd for Segment {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}



struct Partition  {
    path: String,
    topic: String,
    partition: u32,
    segments: BinaryHeap<Segment>,
}

impl Partition {
    fn new(topic: String, part: u32) -> io::Result<Partition> {
        fs::create_dir(format!("{}/{}", topic, part))?;
        Ok(Partition {
            path: format!("{}/{}", &topic, &part),
            topic: topic,
            partition: part,
            segments: BinaryHeap::new(),
        })
    }
    fn fill_segments(&mut self) -> io::Result<()> {
        if !self.segments.is_empty() {
            self.segments.clear();
        }

        for entry in fs::read_dir(&self.path)? {
            let entry_path = entry.unwrap().path();
            let path = entry_path.as_path();
            let stem = path.file_stem().unwrap();
            let str_stem = stem.to_str().unwrap();
            let offset = str_stem.parse::<Offset>().unwrap();
            if let Ok(seg) = Segment::new(self.path.clone(), offset) {
                self.segments.push(seg);
            }
        }

        Ok(())
    }
}




#[cfg(test)]
mod tests {
    use std::fs::{create_dir, remove_dir_all, remove_file};
    use std::io::{BufReader, BufWriter, Write, Read, BufRead, Cursor};
    use super::*;

    // Partition
    #[test]
    fn test_new_partition() {
        let partition = Partition::new(String::from("topic"), 0).unwrap();//.expect("create partition dir");
        assert_eq!(partition.partition, 0);
        assert_eq!(partition.topic, "topic");
    }

    #[test]
    fn test_fill_segments() {
        let partition = Partition::new(String::from("topic"), 0);
        assert_eq!(partition.partition, 0);
        assert_eq!(partition.topic, "topic");
    }


    // Segment
    #[test]
    fn test_new_segment() {
        let segment = Segment::new(String::from("."), 0).expect("Cant open segment");
        assert_eq!(segment.base_offset, 0);
        fs::remove_file(segment.filename).expect(" remove file");
    }

    #[test]
    fn test_segment_consumer_seeks() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");

        segment.open(Client::Producer).expect("open write file");
        let bytes = String::from("wombiest");
        let n = segment.write(bytes.as_bytes()).expect("writing eight bytes");
        segment.close();

        segment.open(Client::Consumer).expect(" open read file");
        segment.seek(SeekFrom::Start(4)).expect(" seek");
        let mut buf = [0; 4];
        let n = segment.read(&mut buf).expect("writing eight bytes");
        assert_eq!(n, 4);
        assert_eq!(&buf, b"iest");
        fs::remove_file(segment.filename).expect("remove file");
    }

    #[test]
    fn test_segment_consumer_reads() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");

        segment.open(Client::Producer).expect(" open write file");
        let bytes = String::from("wombiest");
        let n = segment.write(bytes.as_bytes()).expect("writing eight bytes");
        segment.close();

        segment.open(Client::Consumer).expect("open read file");
        let mut buf = [0; 8];
        let n = segment.read(&mut buf).expect("writing eight bytes");

        assert_eq!(n, 8);
        assert_eq!(&buf, b"wombiest");

        fs::remove_file(segment.filename).expect(" remove file");
    }

    #[test]
    fn test_segment_producer_writes() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");
        segment.open(Client::Producer).expect("open write file");
        let bytes = String::from("wombiest");
        let n = segment.write(bytes.as_bytes()).expect("writing eight bytes");
        assert_eq!(n, 8);
        fs::remove_file(segment.filename).expect("remove file");
    }

    #[test]
    fn test_segment_fails_when_producer_reads() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");
        segment.open(Client::Producer).expect("open write file");
        let bytes = String::from("wombiest");
        segment.write(bytes.as_bytes()).expect("write to file");

        let mut buf = [0; 8];
        let result = segment.read(&mut buf);
        assert!(result.is_err(), "producer shouldn't read");
    }

    #[test]
    fn test_segment_fails_when_consumer_write() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");
        segment.open(Client::Consumer).expect("open write file");
        let bytes = String::from("wombiest");
        let result = segment.write(bytes.as_bytes());
        assert!(result.is_err(), "consumer shouldn't write");
    }

}
