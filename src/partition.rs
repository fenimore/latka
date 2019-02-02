#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_must_use)]
#![allow(unused_variables)]
use std::{io, fs, thread, env};
use std::cmp::{Ord, Ordering, PartialOrd, PartialEq};
use std::fs::{OpenOptions, File};
use std::io::{Seek, SeekFrom, BufReader, BufWriter,Write, Read, BufRead, Error};
use std::io::ErrorKind::ConnectionReset;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::collections::{LinkedList, BinaryHeap};


use crate::segment::{Segment, Offset, Client};


pub struct Partition  {
    path: String,
    topic: String,
    partition: u32,
    segments: BinaryHeap<Segment>,
}

impl Partition {
    pub fn new(topic: String, part: u32) -> io::Result<Partition> {
        fs::create_dir_all(format!("{}/{}", topic, part))?;
        Ok(Partition {
            path: format!("{}/{}", &topic, &part),
            topic: topic,
            partition: part,
            segments: BinaryHeap::new(),
        })
    }
    pub fn fill_segments(&mut self) -> io::Result<()> {
        if !self.segments.is_empty() {
            self.segments.clear();
        };

        for entry in fs::read_dir(&self.path)? {
            let path = entry.unwrap().path();
            let stem = path.as_path().file_stem().unwrap();
            let str_stem = stem.to_str().unwrap();
            self.segments.push(
                Segment::new(
                    path.to_string_lossy().to_string(),
                    str_stem.parse::<Offset>().unwrap()
                ).unwrap()
            )
        }

        Ok(())
        //     let mut segments: Vec<(String, u64)> = fs::read_dir(&self.path)?.map(|entry| {
        //         let path = entry.unwrap().path();
        //         let stem = path.as_path().file_stem().unwrap();
        //         let str_stem = stem.to_str().unwrap();
        //         (String::from(path.to_string_lossy()), str_stem.parse::<Offset>().unwrap())
        //     }).collect();

        // segments.sort_unstable();

        // for (path, off) in segments {
        //     if let Ok(seg) = Segment::new(path, off) {
        //         self.segments.push(seg);
        //     }
        // }
    }


}


// impl Read for Partition {
//     fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {

//         let segment: Segment = match self.segments.pop_front() {
//             Some(seg) => seg,
//             None => return Ok(0),
//         }
//         segment.
//         if let Some(r) = &mut self.file {
//             return r.read(buf);
//         }
//         Ok(0)
//     }
// }

// impl Seek for Partition {
//     fn seek(&mut self, offset: SeekFrom) -> io::Result<u64> {


//         if let Some(r) = &mut self.file {
//             return r.seek(offset);
//         }
//         Ok(0)
//     }
// }



#[cfg(test)]
extern crate speculate;

#[cfg(test)]
mod tests {
    use speculate::speculate;
    use std::fs::{create_dir, remove_dir_all, remove_file};
    use std::io::{BufReader, BufWriter, Write, Read, BufRead, Cursor};
    use super::*;

    speculate! {
        after {
            remove_dir_all("tmp/");
        }

        test "new partition" {
            let partition = Partition::new(String::from("tmp"), 0).unwrap();
            assert_eq!(partition.partition, 0);
            assert_eq!(partition.topic, "tmp");
        }

        describe "fill segments" {
            test "fill segments" {
                let mut partition = Partition::new(String::from("tmp"), 0).unwrap();
                {
                    Segment::new(partition.path.clone(), 0).expect("new segment")
                        .open(Client::Consumer).expect("open segment");
                    Segment::new(partition.path.clone(), 1).expect("new segment")
                        .open(Client::Consumer).expect("open segment");
                }

                partition.fill_segments().expect("fill segments");

                assert_eq!(partition.segments.len(), 2);
                if let Some(segment) = partition.segments.pop() {
                    assert_eq!(
                        Segment::new(String::from("tmp/0000000000000000000000.log"), 0).unwrap(), segment)
                } else {
                    assert!(false)
                }
            }
        }
    }
}
