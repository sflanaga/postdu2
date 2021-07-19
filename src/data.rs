#![allow(dead_code)]
#![allow(unused_imports)]

use std::{
    cmp::{max, min},
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap},
    fmt::Display,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Context, Result};
use csv::{ByteRecord, StringRecord};
use humantime::FormattedDuration;
use url::Url;

use crate::cli::CliCfg;

pub fn dur_to_str(dur: Duration) -> String {
    const NS: u128 = 1_000_000_000;
    const HOUR: u128 = NS * 3600;
    const DAY: u128 = HOUR * 24;
    const YEAR: u128 = DAY * 365 + 5 * HOUR;
    let x = dur.as_nanos();
    if x < DAY {
        let h = x / HOUR;
        format!("{}h", h)
    } else if x < YEAR {
        let d = x / DAY;
        let h = (x - (d * DAY)) / HOUR;
        format!("{}d{}h", d, h)
    } else {
        let y = x / YEAR;
        let d = (x - y * YEAR) / DAY;
        format!("{}Y{}d", y, d)
    }
}

#[derive(Debug)]
pub struct FileStat {
    pub file_type: char,
    pub size: u64,
    pub mod_time: u64,
}

#[derive(Debug)]
pub struct FileInfo {
    pub path: PathBuf,
    pub stat: FileStat,
}

pub fn uri_to_path(path: &str) -> PathBuf {
    match url::Url::parse(path) {
        Err(_e) => PathBuf::from(path),
        Ok(u) => PathBuf::from(u.path()),
    }
}
/*

    D|hdfs://EBDASTAGING/ |0|1507759824337
    D|hdfs://EBDASTAGING/EAS|0|1564701319129
    D|hdfs://EBDASTAGING/EBDASTAGING|0|1616181309449
    D|hdfs://EBDASTAGING/amshbase|0|1503884506994
    D|hdfs://EBDASTAGING/app-logs|0|1625785704911
    D|hdfs://EBDASTAGING/apps|0|1608248644465
    D|hdfs://EBDASTAGING/ats|0|1466105295849
    D|hdfs://EBDASTAGING/atsv2|0|1606597041148
    D|hdfs://EBDASTAGING/benchmarks|0|1614883289947
    D|hdfs://EBDASTAGING/deleteme|0|1606249988415

*/

impl FileInfo {
    pub fn new(raw_rec: StringRecord) -> Result<Self> {
        if raw_rec.len() != 4 {
            return Err(anyhow!("field count is wrong at {}", raw_rec.len()));
        }
        Ok(FileInfo {
            path: PathBuf::from(Url::parse(&raw_rec[1])?.path()),
            stat: FileStat {
                file_type: {
                    // if raw_rec[0].len() != 1 {
                    //     return Err(anyhow!("error in reading file type, got LEN != 1\"{}\"", &raw_rec[0]));
                    // }
                    // let b = raw_rec[0].as_bytes()[0];
                    // b as char

                    let c = raw_rec[0].chars().next().context("record with no character type")?;
                    match c {
                        'f' | 'F' => 'F', // this stuff is easier than to uppercase - yikes
                        'd' | 'D' => 'D',
                        's' | 'S' => 'S',
                        _ => return Err(anyhow!("error in record where file type is not known but is {}", c)),
                    }
                },
                size: raw_rec[2].parse::<u64>().with_context(|| format!("unable parse number for size: \"{}\"", &raw_rec[2]))?,
                mod_time: raw_rec[3].parse::<u64>().context("unable to parse mod time")?,
                // size: lexical::parse::<u64, _>(&raw_rec[2]).with_context(||format!("unable parse number for size: \"{}\"", &raw_rec[2]))?,
                // mod_time: lexical::parse::<u64, _>(&raw_rec[3]).with_context(||format!("unable parse number for size: \"{}\"", &raw_rec[2]))?,
                // size: 0,
                // mod_time: 0,
            },
        })
    }
    pub fn is_dir(self: &Self) -> bool {
        self.stat.file_type == 'D'
    }
    pub fn is_file(self: &Self) -> bool {
        self.stat.file_type == 'F'
    }
    pub fn is_sym(self: &Self) -> bool {
        self.stat.file_type == 'S'
    }
}

pub struct _DirStat {
    pub entry_cnt: u64,
    pub size: u64,
    pub old: u64,
    pub new: u64,
}

const GREEK_SUFFIXES: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

pub fn greek(v: f64) -> String {
    let mut number = v;
    let mut multi = 0;

    while number >= 1000.0 && multi < GREEK_SUFFIXES.len() - 1 {
        multi += 1;
        number /= 1024.0;
    }

    let mut s = format!("{}", number);
    s.truncate(4);
    if s.ends_with('.') {
        s.pop();
    }
    if s.len() < 4 {
        s.push(' ');
    }

    return format!("{:<5}{}", s, GREEK_SUFFIXES[multi]);
}

pub struct DirStat {
    pub direct: _DirStat,
    pub recurse: _DirStat,
}

impl DirStat {
    pub fn empty() -> Self {
        DirStat {
            direct: _DirStat {
                entry_cnt: 0,
                size: 0,
                old: 0,
                new: 0,
            },
            recurse: _DirStat {
                entry_cnt: 0,
                size: 0,
                old: 0,
                new: 0,
            },
        }
    }
    pub fn new(stat: &FileStat) -> Self {
        DirStat {
            direct: _DirStat {
                entry_cnt: 1,
                size: stat.size,
                old: stat.mod_time,
                new: stat.mod_time,
            },
            recurse: _DirStat {
                entry_cnt: 1,
                size: stat.size,
                old: stat.mod_time,
                new: stat.mod_time,
            },
        }
    }
    pub fn merge(self: &mut Self, stat: &FileStat, direct: bool) {
        self.recurse.entry_cnt += 1;
        self.recurse.size += stat.size;
        self.recurse.old = min(self.recurse.old, stat.mod_time);
        self.recurse.new = max(self.recurse.new, stat.mod_time);

        if direct {
            self.direct.entry_cnt += 1;
            self.direct.size += stat.size;
            self.direct.old = min(self.direct.old, stat.mod_time);
            self.direct.new = max(self.direct.new, stat.mod_time);
        }
    }
    pub fn merge_file_stat(self: &mut Self, stat: &FileStat, direct: bool) {
        self.recurse.entry_cnt += 1;
        self.recurse.size += stat.size;
        self.recurse.old = min(self.recurse.old, stat.mod_time);
        self.recurse.new = max(self.recurse.new, stat.mod_time);

        if direct {
            self.direct.entry_cnt += 1;
            self.direct.size += stat.size;
            self.direct.old = min(self.direct.old, stat.mod_time);
            self.direct.new = max(self.direct.new, stat.mod_time);
        }
    }
}

// #[derive(Eq, Debug)]
// pub struct TimeRange {
//     pub old: u64,
//     pub new: u64,
// }

#[derive(Eq, Debug)]
struct TrackedPath {
    size: u64,
    path: PathBuf,
    old: u64,
    new: u64,
}

impl Ord for TrackedPath {
    fn cmp(&self, other: &TrackedPath) -> std::cmp::Ordering {
        self.size.cmp(&other.size).reverse()
    }
}

impl PartialOrd for TrackedPath {
    fn partial_cmp(&self, other: &TrackedPath) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TrackedPath {
    fn eq(&self, other: &TrackedPath) -> bool {
        self.size == other.size
    }
}

fn track_top_n(heap: &mut BinaryHeap<TrackedPath>, p: &PathBuf, s: u64, limit: usize, old: u64, new: u64) {
    if limit > 0 {
        if heap.len() < limit {
            heap.push(TrackedPath {
                size: s,
                path: p.clone(),
                old,
                new,
            });
            return;
        } else if heap.peek().expect("internal error: cannot peek when the size is greater than 0!?").size < s {
            heap.pop();
            heap.push(TrackedPath {
                size: s,
                path: p.clone(),
                old,
                new,
            });
            return;
        }
    }
}

fn to_sort_vec(heap: &BinaryHeap<TrackedPath>) -> Vec<TrackedPath> {
    let mut v = Vec::with_capacity(heap.len());
    for i in heap {
        v.push(TrackedPath {
            path: i.path.clone(),
            size: i.size,
            old: i.old,
            new: i.new,
        });
    }
    v.sort();
    v
}

pub fn get_age(now: SystemTime, time_ms: u64) -> String {
    let now = now.duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let then = Duration::from_millis(time_ms);
    let age = now - then;
    dur_to_str(age)
}

pub fn get_age_delta(old: u64, new: u64) -> String {
    let old = Duration::from_millis(old);
    let new = Duration::from_millis(new);
    let delta = new - old;
    dur_to_str(delta)
}

pub struct Tracking {
    dtree: HashMap<PathBuf, DirStat>,
    direct_dir_size: BinaryHeap<TrackedPath>,
    direct_dir_count: BinaryHeap<TrackedPath>,
    root: PathBuf,
}

impl Tracking {
    pub fn new() -> Tracking {
        Tracking {
            dtree: HashMap::new(),
            direct_dir_count: BinaryHeap::new(),
            direct_dir_size: BinaryHeap::new(),
            root: PathBuf::from("/"),
        }
    }

    pub fn process_entry(self: &mut Self, fi: FileInfo) -> Result<()> {

        if fi.is_dir() {
            if self.dtree.contains_key(&fi.path) {
                eprintln!("dup path as D {}", fi.path.to_string_lossy());
            } else {
                // if fi.path == self.root {
                //     return Err(anyhow!("What root?  {:?}", fi));
                // }
                self.dtree.insert(fi.path, DirStat::new(&fi.stat));
                //self.dtree.insert(fi.path, DirStat::new(&fi.stat));
            }
        } else if fi.is_file() || fi.is_sym() {
            let mut direct_parent = true;
            let mut p_path = fi.path.as_path();
            loop {
                if let Some(_p_path) = p_path.parent() {
                    p_path = _p_path;
                    // println!("{}", p_path.to_string_lossy());

                    match self.dtree.get_mut(p_path) {
                        Some(stat) => {
                            stat.merge(&fi.stat, direct_parent);
                        },
                        None => {
                            let p_path_buf = p_path.to_path_buf();
                            eprintln!("parent not found for (adding with empty stats):  {} from: {}", p_path_buf.to_str().unwrap(), fi.path.to_str().unwrap());
                            //self.dtree.insert(p_path.to_path_buf(), DirStat::empty());
                        },
                    }
                    direct_parent = false;
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn dump(self: &Self) {
        for (p, ds) in &self.dtree {
            println!("{}:  {}  {}", &p.to_string_lossy(), ds.recurse.entry_cnt, ds.recurse.size);
        }
    }

    pub fn walk_and_heap(self: &Self, cli: &CliCfg) {
        let mut top_size: BinaryHeap<TrackedPath> = BinaryHeap::new();
        let mut top_cnt: BinaryHeap<TrackedPath> = BinaryHeap::new();
        let mut top_size_recur: BinaryHeap<TrackedPath> = BinaryHeap::new();
        let mut top_cnt_recur: BinaryHeap<TrackedPath> = BinaryHeap::new();
        let limit = cli.top_n;
        for (path, stat) in &self.dtree {
            track_top_n(&mut top_size, path, stat.direct.size, limit, stat.recurse.old, stat.recurse.new);
            track_top_n(&mut top_cnt, path, stat.direct.entry_cnt, limit, stat.recurse.old, stat.recurse.new);
            track_top_n(&mut &mut top_size_recur, path, stat.recurse.size, limit, stat.recurse.old, stat.recurse.new);
            track_top_n(&mut &mut top_cnt_recur, path, stat.recurse.entry_cnt, limit, stat.recurse.old, stat.recurse.new);
        }

        fn print_tp_size(now: SystemTime, tp: &TrackedPath) {
            println!(
                "{} {}  age:[{}-{} D: {}]",
                greek(tp.size as f64),
                tp.path.to_string_lossy(),
                get_age(now, tp.old),
                get_age(now, tp.new),
                get_age_delta(tp.old, tp.new)
            );
        }
        fn print_tp_cnt(now: SystemTime, tp: &TrackedPath) {
            println!("{:8} {}  age:[{}-{} D: {}]", tp.size, tp.path.to_string_lossy(), get_age(now, tp.old), get_age(now, tp.new), get_age_delta(tp.old, tp.new));
        }

        let now = SystemTime::now();

        type print_type = for<'r> fn(SystemTime, &'r TrackedPath);

        for rep in [
            ("\nTop directories based on file sizes directly in them", print_tp_size as print_type, &top_size),
            ("\nTop directories based on entry counts directly in them", print_tp_cnt as print_type, &top_cnt),
            ("\nTop directories based on file sizes recursively in them", print_tp_size as print_type, &top_size_recur),
            ("\nTop directories based on file counts recursively in them", print_tp_cnt as print_type, &top_cnt_recur),
        ] {
            println!("{}", rep.0);
            for tp in to_sort_vec(rep.2) {
                rep.1(now, &tp);
            }
        }

        println!("\nTop directories based on file directly in them");
        for tp in to_sort_vec(&top_size) {
            print_tp_size(now, &tp);
        }

        println!("\nTop directories based on entry count directly in them");
        for tp in to_sort_vec(&top_cnt) {
            print_tp_cnt(now, &tp)
        }
    }
}
