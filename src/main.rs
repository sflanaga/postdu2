use std::{collections::BTreeMap, fs::File, io::BufReader, path::PathBuf, str::FromStr, time::{Duration, Instant}};

use crate::{cli::CliCfg, data::{FileInfo, Tracking, dur_to_str, uri_to_path}};
use csv::{StringRecord, StringRecordsIter};
use structopt::StructOpt;

mod cli;
mod data;

use anyhow::{Context, Error, Result};

fn main() -> Result<()> {


    // println!("{}", dur_to_str(Duration::from_secs(1800)));
    // println!("{}", dur_to_str(Duration::from_secs(3600*4)));
    // println!("{}", dur_to_str(Duration::from_secs(3600*48)));

    // println!("{:?}", uri_to_path("/EBDASTAGING/amshbase"));
    // println!("{:?}", uri_to_path("hdfs://EBDASTAGING/amshbase"));
    // std::process::exit(0);

    println!("Hello   there!");
    let cfg: CliCfg = CliCfg::from_args();

    let mut bld = csv::ReaderBuilder::new();

    let csv_vbld = bld
        .delimiter(b'|')
        .has_headers(false)
        //.flexible(true)
        //.quote(b'"')
        //.escape(Some(b'\\'))
        //.comment(Some(b'#'))
        ;

    let file = File::open(&cfg.file)?;
    let mut buf = BufReader::with_capacity(1024*256, &file);
    let mut rdr = csv_vbld.from_reader(buf);

    let mut map: BTreeMap<String, FileInfo> = BTreeMap::new();

    let path_del = '/';

    let mut data = Tracking::new();

    let mut line_count = 0u64;
    let mut sum_fields = 0usize;

    let start = Instant::now();

    let (send_rec, recv_rec): (crossbeam_channel::Sender<Option<StringRecord>>, crossbeam_channel::Receiver<Option<StringRecord>>) =
        crossbeam_channel::bounded(10000);

    for rec in rdr.records() {
        line_count+=1;
        if line_count % 100_000 == 0 {
            println!("at line {}", line_count);
        }
        if line_count > 1_000_000 {
            break;
        }
        match rec {

            Ok(rec) => {
                sum_fields += rec.len();

                match FileInfo::new(rec) {
                    Ok(fi) => { 
                        data.processEntry(fi)?;
                    },
                    Err(e) => println!("error on line {} due to {}", line_count, e),
                }
            }
            Err(e) => eprintln!("error reading record: {} ", line_count),
        }
    }
    println!("{:?}", start.elapsed());
    std::process::exit(0);

    // data.dump();

    data.walk_and_heap(&cfg);
    for i in [1, 2, 3] {
        println!("{}", i);
        // std::thread::sleep(Duration::from_secs(5));
    }



    Ok(())
}

