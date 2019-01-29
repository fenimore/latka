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
use std::collections::LinkedList;

pub type Offset = u64;


fn sorted_segments(topic: &String) -> io::Result<Vec<Offset>> {
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


struct Partition  {
    topic: String,
    partition: u32,
    segment_offsets: LinkedList<Offset>,
}


struct Segment {
    base_offset: Offset,
    filename: String,
    writer: Option<File>,
    reader: Option<File>,
}

impl Segment {
    fn new(partition_path: String, offset: u64) -> io::Result<Segment> {
        let filename = format!("{}/{:0>20}.log", partition_path, offset);
        Ok(Segment {
            base_offset: offset,
            filename: filename,
            writer: None,
            reader: None,
        })
    }
    fn producer(&mut self) -> io::Result<()> {
        let writer: File = OpenOptions::new().create(true).append(true).open(&self.filename)?;
        self.writer = Some(writer);
        Ok(())
    }
    fn consumer(&mut self) -> io::Result<()> {
        let reader: File = OpenOptions::new().create(true).write(true).read(true).open(&self.filename)?;
        self.reader = Some(reader);
        Ok(())
    }

}

impl Write for Segment {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(w) = &mut self.writer{
            return w.write(buf)
        }
        Ok(0)
    }
    fn flush(&mut self) -> io::Result<()> {
        if let Some(w) = &mut self.writer {
            return w.flush();
        }
        Ok(())
    }
}
impl Read for Segment {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if let Some(r) = &mut self.reader {
            return r.read(buf);
        }
        Ok(0)
    }
}
impl Seek for Segment {
    fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {
        if let Some(r) = &mut self.reader {
            return r.seek(offset);
        }
        Ok(0)
    }
}



#[cfg(test)]
mod tests {
    use std::fs::{create_dir, remove_dir_all, remove_file};
    use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
    use std::io::{BufReader, BufWriter, Write, Read, BufRead, Cursor};
    use super::*;

    #[test]
    fn test_new_segment() {
        let segment = Segment::new(String::from("."), 0).expect("Cant open segment");
        assert_eq!(segment.base_offset, 0);
        fs::remove_file(segment.filename).expect("couldn't remove file");
    }

    #[test]
    fn test_segment_consumer_seeks() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");

        segment.producer().expect("couldn't open write file");
        let bytes = String::from("wombiest");
        let n = segment.write(bytes.as_bytes()).expect("writing eight bytes");
        drop(segment.writer.take());

        segment.consumer().expect("couldn't open read file");
        segment.seek(SeekFrom::Start(4)).expect("couldn't seek");
        let mut buf = [0; 4];
        let n = segment.read(&mut buf).expect("writing eight bytes");
        assert_eq!(n, 4);
        assert_eq!(&buf, b"iest");
        fs::remove_file(segment.filename).expect("couldn't remove file");
    }

    #[test]
    fn test_segment_consumer_reads() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");

        segment.producer().expect("couldn't open write file");
        let bytes = String::from("wombiest");
        let n = segment.write(bytes.as_bytes()).expect("writing eight bytes");
        drop(segment.writer.take());

        segment.consumer().expect("couldn't open read file");
        let mut buf = [0; 8];
        let n = segment.read(&mut buf).expect("writing eight bytes");
        assert_eq!(n, 8);
        assert_eq!(&buf, b"wombiest");
        fs::remove_file(segment.filename).expect("couldn't remove file");
    }

    #[test]
    fn test_segment_producer_writes() {
        let mut segment = Segment::new(String::from("."), 0).expect("Cant open segment");
        segment.producer().expect("couldn't open write file");
        let bytes = String::from("wombiest");
        let n = segment.write(bytes.as_bytes()).expect("writing eight bytes");
        assert_eq!(n, 8);
        fs::remove_file(segment.filename).expect("couldn't remove file");
    }
}
