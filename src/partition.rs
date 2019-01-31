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
        }

        for entry in fs::read_dir(&self.path)? {
            let entry_path = entry.unwrap().path();
            let path = entry_path.as_path();
            let stem = path.file_stem().unwrap();
            let str_stem = stem.to_str().unwrap();
            let offset = str_stem.parse::<Offset>().unwrap();
            println!("{}", offset);
            if let Ok(seg) = Segment::new(self.path.clone(), offset) {
                self.segments.push(seg);
            }
        }
        Ok(())
    }
}



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
            //remove_dir_all("tmp/");
        }

        test "new partition" {
            let partition = Partition::new(String::from("tmp"), 0).unwrap();//.expect("create partition dir");
            assert_eq!(partition.partition, 0);
            assert_eq!(partition.topic, "tmp");
        }

        describe "segments" {
            test "fill segments" {
                let mut partition = Partition::new(String::from("tmp"), 0).unwrap();
                {
                    Segment::new(partition.path.clone(), 5).expect("new segment")
                        .open(Client::Consumer).expect("open segment");
                    Segment::new(partition.path.clone(), 6).expect("new segment")
                        .open(Client::Consumer).expect("open segment");
                }

                partition.fill_segments().expect("fill segments");

                assert_eq!(partition.segments.len(), 2);
            }
        }
    }
}
