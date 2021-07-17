use std::{cmp::{max, min}, collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap}, fmt::Display, path::PathBuf, time::{Duration, SystemTime}};

use anyhow::{anyhow, Context, Result};
use csv::{ByteRecord, StringRecord};
use humantime::FormattedDuration;

use crate::cli::CliCfg;

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
        Err(e) => PathBuf::from(path),
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
            path: PathBuf::from(&raw_rec[1]),
            stat: FileStat {
                file_type: {
                    if raw_rec[0].len() != 1 {
                        return Err(anyhow!("error in reading file type, got LEN != 1\"{}\"", &raw_rec[0]));
                    }
                    let b = raw_rec[0].as_bytes()[0];
                    b as char

                    // let c = raw_rec[0]
                    //     .chars()
                    //     .next()
                    //     .context("record with no character type")?;
                    // match c {
                    //     'f' | 'F' => 'F', // this stuff is easier than to uppercase - yikes
                    //     'd' | 'D' => 'D',
                    //     's' | 'S' => 'S',
                    //     _ => {
                    //         return Err(anyhow!(
                    //             "error in record where file type is not known but is {}",
                    //             c
                    //         ))
                    //     }
                    // }
                },
                size: raw_rec[2].parse::<u64>().with_context(||format!("unable parse number for size: \"{}\"", &raw_rec[2]))?,
                mod_time: raw_rec[3]
                    .parse::<u64>()
                    .context("unable to parse mod time")?,
                // size: lexical::parse::<u64, _>(&raw_rec[2]).with_context(||format!("unable parse number for size: \"{}\"", &raw_rec[2]))?,
                // mod_time: lexical::parse::<u64, _>(&raw_rec[3]).with_context(||format!("unable parse number for size: \"{}\"", &raw_rec[2]))?,
                // size: 0,
                // mod_time: 0,
            },
        })
    }
    pub fn isDir(self: &Self) -> bool {
        self.stat.file_type == 'D'
    }
    pub fn isFile(self: &Self) -> bool {
        self.stat.file_type == 'F'
    }
    pub fn isSym(self: &Self) -> bool {
        self.stat.file_type == 'S'
    }
}

pub struct _DirStat {
    pub entry_cnt: u64,
    pub size: u64,
    pub oldest: u64,
    pub youngest: u64,
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
    if s.len() < 4 { s.push(' '); }

    return format!("{:<5}{}", s, GREEK_SUFFIXES[multi]);
}



pub struct DirStat {
    pub direct: _DirStat,
    pub recurse: _DirStat,
}

impl DirStat {
    pub fn new(stat: &FileStat) -> Self {
        DirStat {
            direct: _DirStat {
                entry_cnt: 1,
                size: stat.size,
                oldest: stat.mod_time,
                youngest: stat.mod_time,
            },
            recurse: _DirStat {
                entry_cnt: 1,
                size: stat.size,
                oldest: stat.mod_time,
                youngest: stat.mod_time,
            },
        }
    }
    pub fn merge(self: &mut Self, stat: &FileStat, direct: bool) {
        self.recurse.entry_cnt += 1;
        self.recurse.size += stat.size;
        self.recurse.oldest = min(self.recurse.oldest, stat.mod_time);
        self.recurse.youngest = max(self.recurse.oldest, stat.mod_time);

        if direct {
            self.direct.entry_cnt += 1;
            self.direct.size += stat.size;
            self.direct.oldest = min(self.direct.oldest, stat.mod_time);
            self.direct.youngest = max(self.direct.oldest, stat.mod_time);
        }
    }
    pub fn mergeFileStat(self: &mut Self, stat: &FileStat, direct: bool) {
        self.recurse.entry_cnt += 1;
        self.recurse.size += stat.size;
        self.recurse.oldest = min(self.recurse.oldest, stat.mod_time);
        self.recurse.youngest = max(self.recurse.oldest, stat.mod_time);

        if direct {
            self.direct.entry_cnt += 1;
            self.direct.size += stat.size;
            self.direct.oldest = min(self.direct.oldest, stat.mod_time);
            self.direct.youngest = max(self.direct.oldest, stat.mod_time);
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
            heap.push(TrackedPath { size: s, path: p.clone(),  old, new});
            return;
        } else if heap.peek().expect("internal error: cannot peek when the size is greater than 0!?").size < s {
            heap.pop();
            heap.push(TrackedPath { size: s, path: p.clone(), old, new});
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



pub struct Tracking {
    dtree: HashMap<PathBuf, DirStat>,
    direct_dir_size: BinaryHeap<TrackedPath>,
    direct_dir_count: BinaryHeap<TrackedPath>,
}

impl Tracking {
    pub fn new() -> Tracking {
        Tracking {
            dtree: HashMap::new(),
            direct_dir_count: BinaryHeap::new(),
            direct_dir_size: BinaryHeap::new(),
        }
    }

    pub fn processEntry(self: &mut Self, fi: FileInfo) -> Result<()> {
        if fi.isDir() {
            if self.dtree.contains_key(&fi.path) {
                eprintln!("duplicate path - better error handling?  {}", fi.path.to_string_lossy());               
            } else {
                self.dtree.insert(fi.path, DirStat::new(&fi.stat));
            }
        } else if fi.isFile() || fi.isSym() {
            if let Some(p_path) = fi.path.parent() {
                match self.dtree.get_mut(p_path) {
                    Some(stat) => {
                        stat.merge(&fi.stat, true);
                    },
                    None => eprintln!("parent not found for {}", fi.path.to_string_lossy()),
                }
            }
            let path = fi.path;
        }
        Ok(())
    }

    pub fn dump(self: &Self) {
        for (p,ds) in &self.dtree {
            println!("{}:  {}  {}", &p.to_string_lossy(), ds.recurse.entry_cnt, ds.recurse.size);
        }
    }

    pub fn walk_and_heap(self: &Self, cli: &CliCfg) {
        let mut top_size: BinaryHeap<TrackedPath> = BinaryHeap::new();
        let mut top_cnt: BinaryHeap<TrackedPath> = BinaryHeap::new();
        let limit = cli.top_n;
        for (path,stat) in &self.dtree {
            track_top_n(&mut top_size, path, stat.direct.size, limit, stat.direct.oldest, stat.direct.youngest);
            track_top_n(&mut top_cnt, path, stat.direct.entry_cnt, limit, stat.direct.oldest, stat.direct.youngest);
        }

        let now = SystemTime::now();
        println!("\nTop directories based on file directly in them");
        for tp in to_sort_vec(&top_size) {
            println!("{} {}  age range: {}  {}", greek(tp.size as f64), tp.path.to_string_lossy(), get_age(now, tp.old), get_age(now, tp.new));
        }

        println!("\nTop directories based on entry count directly in them");
        for tp in to_sort_vec(&top_cnt) {
            println!("{:8} {}", tp.size, tp.path.to_string_lossy());
        }

    }

}

pub fn dur_to_str(dur: Duration) -> String {
    const NS: u128 = 1_000_000_000;
    const HOUR: u128 = NS * 3600;
    const DAY: u128 = HOUR * 24;
    const YEAR: u128 = DAY * 365 + 5*HOUR;
    let x = dur.as_nanos();
    if x < DAY {
        let h = x/HOUR;
        format!("{}h", h)
    } else if x < YEAR {
        let d = x/DAY;
        let h = (x - (d*DAY))/HOUR;
        format!("{}d{}h", d, h)
    } else {
        let y = x/YEAR;
        let d = (x-y*YEAR)/DAY;
        format!("{}Y{}d", y, d)
    }
}



