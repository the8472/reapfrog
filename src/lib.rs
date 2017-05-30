//   reapfrog
//   Copyright (C) 2017 The 8472
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

extern crate libc;

use std::collections::VecDeque;
use std::fs::File;
use std::fs::Metadata;
use std::io::Read;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::path::PathBuf;

struct Prefetch {
    p: PathBuf,
    f: File,
    read_pos: u64,
    prefetch_pos: u64,
    to_drop: u64,
    length: u64
}

impl Prefetch {
    fn new(f: File, p: PathBuf) -> Self {
        let len = f.metadata().unwrap().len();
        unsafe {
            libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }
        Prefetch{f, read_pos: 0, length: len, p, to_drop: 0, prefetch_pos: 0}
    }
}

pub struct MultiFileReadahead<Src> {
    source: Src,
    open: VecDeque<Prefetch>,
    dropbehind: bool,
    budget: u64,
}


pub struct Reader<'a, T: 'a> {
    owner: &'a mut MultiFileReadahead<T>
}

impl<'a, T> Reader<'a, T> where T: Iterator<Item=PathBuf> {

    pub fn metadata(&self) -> Metadata {
        self.owner.open[0].f.metadata().unwrap()
    }

    pub fn path(&self) -> &Path {
        &self.owner.open[0].p
    }

}

impl<'a, T> Read for &'a mut Reader<'a, T>
    where T: Iterator<Item=PathBuf>
{
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, std::io::Error> {
        let result = {
            let drop = self.owner.dropbehind;
            let ref mut fetch = self.owner.open[0];
            let result = fetch.f.read(buf);
            match result {
                Ok(bytes) => {
                    fetch.read_pos += bytes as u64;
                    if drop {
                        fetch.to_drop += bytes as u64;
                        if fetch.to_drop >= 512*1024 {
                            unsafe  {
                                let drop_offset = fetch.read_pos - fetch.to_drop;
                                libc::posix_fadvise(fetch.f.as_raw_fd(), drop_offset as i64, fetch.to_drop as i64, libc::POSIX_FADV_DONTNEED);
                            }
                            fetch.to_drop = 0;
                        }

                    }

                },
                _ => {}
            }
            result
        };
        self.owner.advance();
        result
    }
}


impl<Src: Iterator<Item=PathBuf>> MultiFileReadahead<Src>  {

    pub fn new(src: Src) -> Self {
        MultiFileReadahead {source: src, open: VecDeque::new(), dropbehind: false, budget: 8*1024*1024}
    }

    pub fn dropbehind(&mut self, v : bool) {
        self.dropbehind = v;
    }

    fn advance(&mut self) {

        let consumed : u64 = self.open.iter().map(|o| o.prefetch_pos.saturating_sub(o.read_pos)).sum::<u64>();

        let mut budget = self.budget - consumed;

        // hysteresis: let the loop expend the budget to ~100% if possible, then don't loop until we fall to 50%
        if budget < consumed {
            return
        }

        for i in 0.. {
            if budget < 64 * 1024 { break; }

            if i == self.open.len() {
                if !self.add_file() {
                    break
                }
            }
            if i > 512 { break }

            let ref mut p = self.open[i];

            // round down
            let internal_budget = (budget >> 16) << 16;


            let mut old_offset = std::cmp::max(p.read_pos, p.prefetch_pos);

            // round up
            let blk = 64*1024;
            old_offset = (old_offset + blk - 1) & !(blk - 1);
            //old_offset = (old_offset >> 16) << 16;

            if old_offset >= p.length {
                continue;
            }

            let prefetch_length = std::cmp::min(p.length - old_offset, internal_budget);

            unsafe {
                libc::posix_fadvise(p.f.as_raw_fd(), old_offset as i64, prefetch_length as i64, libc::POSIX_FADV_WILLNEED);
            }

            budget -= prefetch_length;
            p.prefetch_pos = old_offset + prefetch_length;
        }
    }

    fn add_file(&mut self) -> bool {
        match self.source.next() {
            None => return false,
            Some(p) => {
                let f = File::open(&p).unwrap();
                let prefetch = Prefetch::new(f, p);
                self.open.push_back(prefetch);
                return true
            }
        }
    }

    pub fn next(&mut self) -> Option<Reader<Src>> {
        // discard most recent file
        match self.open.pop_front() {
            None => {},
            Some(p) => {
                if p.to_drop > 0 {
                    unsafe {
                        libc::posix_fadvise(p.f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
                    }
                }
            }
        }
        if self.open.is_empty() && !self.add_file() {
             return None;
        };
        Some(Reader{owner: self})
    }
}
