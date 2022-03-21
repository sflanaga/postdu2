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
    pub user: String,
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
        if raw_rec.len() != 5 {
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
            user: raw_rec[4].to_string(),

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



//         type PrintType = for<'r> fn(SystemTime, &'r Tracked<std::path::Display>);


#[derive(Debug)]
struct Tracked<T> {
    size: u64,
    track: T,
    old: u64,
    new: u64,
}

impl<T> Eq for Tracked<T> {}

impl<T> Ord for Tracked<T> {
    fn cmp(&self, other: &Tracked<T>) -> std::cmp::Ordering {
        self.size.cmp(&other.size).reverse()
    }
}

impl<T> PartialOrd for Tracked<T> {
    fn partial_cmp(&self, other: &Tracked<T>) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Tracked<T> {
    fn eq(&self, other: &Tracked<T>) -> bool {
        self.size == other.size
    }
}

fn track_top_n<T: Clone>(heap: &mut BinaryHeap<Tracked<T>>, p: &T, s: u64, limit: usize, old: u64, new: u64) {
    if limit > 0 {
        if heap.len() < limit {
            heap.push(Tracked {
                size: s,
                track: p.clone(),
                old,
                new,
            });
            return;
        } else if heap.peek().expect("internal error: cannot peek when the size is greater than 0!?").size < s {
            heap.pop();
            heap.push(Tracked {
                size: s,
                track: p.clone(),
                old,
                new,
            });
            return;
        }
    }
}


fn to_sort_vec<T: Clone>(heap: &BinaryHeap<Tracked<T>>) -> Vec<Tracked<T>> {
    let mut v = Vec::with_capacity(heap.len());
    for i in heap {
        v.push(Tracked {
            track: i.track.clone(),
            size: i.size,
            old: i.old,
            new: i.new,
        });
    }
    v.sort();
    v
}

fn to_sort_vec_name_cnt(m: &HashMap<String,(u64,u64)>) -> Vec<(String,u64)> {
    let mut v = m.iter()
    .map(|(n,t)| (n.clone(),t.1))
    .collect::<Vec<(String,u64)>>();
    v.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap());
    v
}

fn to_sort_vec_name_size(m: &HashMap<String,(u64,u64)>) -> Vec<(String,u64)> {
    let mut v = m.iter()
    .map(|(n,t)| (n.clone(),t.0))
    .collect::<Vec<(String,u64)>>();
    v.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap());
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
    root: PathBuf,
    largest_file: BinaryHeap<Tracked<PathBuf>>,
    largest_user: HashMap<String,(u64, u64)>,
    largest_time: BTreeMap<u64, u64>,
    num_entries: u64,
    total_file_space: u64,
    parent_not_found: u64,
    parent_filled_in_later: u64,
}

fn print_tp_size(now: SystemTime, tp: &Tracked<PathBuf>) {
    println!(
        "{} {}  age:[{}-{} D: {}]",
        greek(tp.size as f64),
        tp.track.display(),
        get_age(now, tp.old),
        get_age(now, tp.new),
        get_age_delta(tp.old, tp.new)
    );
}


impl Tracking {
    pub fn new() -> Tracking {
        Tracking {
            dtree: HashMap::new(),
            root: PathBuf::from("/"),
            num_entries: 0,
            total_file_space: 0,
            largest_file: BinaryHeap::new(),
            largest_user: HashMap::new(),
            largest_time: BTreeMap::new(),
            parent_not_found: 0,
            parent_filled_in_later: 0,
        }
    }

    pub fn process_entry(self: &mut Self, fi: FileInfo, cli: &CliCfg) -> Result<()> {

        self.total_file_space += fi.stat.size;
        self.num_entries += 1;

        if let Some(user_entry) = self.largest_user.get_mut(&fi.user) {
            user_entry.0 += fi.stat.size;
            user_entry.1 += 1;
        } else {
            self.largest_user.insert(fi.user.clone(), (fi.stat.size,1));
        }

        if fi.is_dir() {

            if let Some(entry) = self.dtree.get_mut(&fi.path) {
                if entry.direct.old == 0 {
                    self.parent_filled_in_later += 1;
                } else {
                    self.parent_filled_in_later += 1;
                    eprintln!("weird - not a fillin path {} with old: {}", fi.path.to_string_lossy(), entry.direct.old);
                }
                entry.merge(&fi.stat, false);  
            } else {
                self.dtree.insert(fi.path, DirStat::new(&fi.stat));
            }
        } else if fi.is_file() || fi.is_sym() {
            let mut direct_parent = true;
            const WEEK_BUCKET_JAVA_MS: u64 = 1000 * 3600 * 24 * 7;
            let week_bucket = (fi.stat.mod_time / (WEEK_BUCKET_JAVA_MS)) * WEEK_BUCKET_JAVA_MS;
            if let Some(entry) = self.largest_time.get_mut(&week_bucket) {
                *entry += fi.stat.size;
            } else {
                self.largest_time.insert(week_bucket, fi.stat.size);
            }

            track_top_n(&mut self.largest_file, &fi.path, fi.stat.size, cli.top_n, fi.stat.mod_time, 0);
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
                            self.parent_not_found += 1;
                            eprintln!("parent not found for (adding with empty stats):  {} from: {}", p_path_buf.to_str().unwrap(), fi.path.to_str().unwrap());
                            self.dtree.insert(p_path.to_path_buf(), DirStat::empty());
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
        let mut top_size: BinaryHeap<Tracked<PathBuf>> = BinaryHeap::new();
        let mut top_cnt: BinaryHeap<Tracked<PathBuf>> = BinaryHeap::new();
        let mut top_size_recur: BinaryHeap<Tracked<PathBuf>> = BinaryHeap::new();
        let mut top_cnt_recur: BinaryHeap<Tracked<PathBuf>> = BinaryHeap::new();
        let limit = cli.top_n;
        for (path, stat) in &self.dtree {
            track_top_n(&mut top_size, path, stat.direct.size, limit, stat.direct.old, stat.direct.new);
            track_top_n(&mut top_cnt, path, stat.direct.entry_cnt, limit, stat.direct.old, stat.direct.new);
            track_top_n(&mut &mut top_size_recur, path, stat.recurse.size, limit, stat.recurse.old, stat.recurse.new);
            track_top_n(&mut &mut top_cnt_recur, path, stat.recurse.entry_cnt, limit, stat.recurse.old, stat.recurse.new);
        }

        fn print_tp_cnt(now: SystemTime, tp: &Tracked<PathBuf>) {
            println!("{:8} {}  age:[{}-{} D: {}]", tp.size, tp.track.to_string_lossy(), get_age(now, tp.old), get_age(now, tp.new), get_age_delta(tp.old, tp.new));
        }

        fn print_tp_file(now: SystemTime, tp: &Tracked<PathBuf>) {
            println!("{} {}  age:[{}]", greek(tp.size as f64), tp.track.to_string_lossy(), get_age(now, tp.old));
        }

        let now = SystemTime::now();

        type PrintTypePath = for<'r> fn(SystemTime, &'r Tracked<PathBuf>);

        println!("Processed {} entry Total space: {}", self.num_entries, greek(self.total_file_space as f64));
        println!("Parent not found in time: {}  Parent filled in later {}", self.parent_not_found, self.parent_filled_in_later);

        println!("\nTop usage by user ID");
        for (n, a) in to_sort_vec_name_size(&self.largest_user).iter().take(cli.top_n) {
            println!("{} {}", greek(*a as f64), &n);
        }

        println!("\nTop usage by user file count");
        for (n, a) in to_sort_vec_name_cnt(&self.largest_user).iter().take(cli.top_n) {
            println!("{:8} {}", a, &n);
        }

        for rep in [
            ("\nTop directories based on file sizes directly in them", print_tp_size as PrintTypePath, &top_size),
            ("\nTop directories based on file sizes recursively in them", print_tp_size as PrintTypePath, &top_size_recur),
            ("\nTop directories based on entry counts directly in them", print_tp_cnt as PrintTypePath, &top_cnt),
            ("\nTop directories based on file counts recursively in them", print_tp_cnt as PrintTypePath, &top_cnt_recur),
            ("\nLargest files", print_tp_file as PrintTypePath, &self.largest_file),
        ] {
            println!("{}", rep.0);
            for tp in to_sort_vec(rep.2) {
                rep.1(now, &tp);
            }
        }

        fn largest_time_to_sort_vec(time_size: &BTreeMap<u64,u64>) -> Vec<(u64,u64)> {
            let mut v = time_size.iter().map(|(t,s)| (*t,*s)).collect::<Vec<(u64,u64)>>();
            v.sort_by(|a,b| b.0.partial_cmp(&a.0).unwrap());
            v
        }
        
        println!("\nTop timeframes based on size (time->size)");
        for tf in largest_time_to_sort_vec(&self.largest_time) {
            println!("{} {}", get_age(now, tf.0), greek(tf.1 as f64));
        }
        

        // println!("\nTop directories based on file directly in them");
        // for tp in to_sort_vec(&top_size) {
        //     print_tp_size(now, &tp);
        // }

        // println!("\nTop directories based on entry count directly in them");
        // for tp in to_sort_vec(&top_cnt) {
        //     print_tp_cnt(now, &tp)

        // }
    }
}
