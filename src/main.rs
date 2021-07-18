#![allow(dead_code)]
#![allow(unused_imports)]
use std::{
    collections::{self, BTreeMap},
    fs::File,
    io::BufReader,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use crate::{
    cli::CliCfg,
    data::{dur_to_str, uri_to_path, FileInfo, Tracking},
};
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
        .flexible(true)
        .quote(b'"')
        .escape(Some(b'\\'))
        .comment(Some(b'#'));

    let file = File::open(&cfg.file)?;
    let buf = BufReader::with_capacity(1024 * 256, &file);
    let mut rdr = csv_vbld.from_reader(buf);

    let mut line_count = 0u64;

    let start = Instant::now();

    let (send_sr, recv_sr): (
        crossbeam_channel::Sender<Option<(u64, StringRecord)>>,
        crossbeam_channel::Receiver<Option<(u64, StringRecord)>>,
    ) = crossbeam_channel::bounded(cfg.parser_qsize);

    let (send_fi, recv_fi): (
        crossbeam_channel::Sender<Option<(u64, FileInfo)>>,
        crossbeam_channel::Receiver<Option<(u64, FileInfo)>>,
    ) = crossbeam_channel::bounded(cfg.data_qsize);

    let mut ct = vec![];

    for i in 0..cfg.num_rec_threads {
        let recv_sr = recv_sr.clone();
        let send_fi = send_fi.clone();

        ct.push(std::thread::spawn(move || loop {
            match recv_sr.recv() {
                Ok(sr) => match sr {
                    Some((line, sr)) => match FileInfo::new(sr) {
                        Ok(fi) => {
                            send_fi
                                .send(Some((line, fi)))
                                .expect("Unable to send string record");
                        }
                        Err(e) => println!("error csv rec line {} due to {}", line, e),
                    },
                    None => return,
                },
                Err(e) => eprintln!("cannot recv due to {}", e),
            }
        }));
    }

    let cfg_c = cfg.clone();
    let data_thread = std::thread::spawn(move || {
        let mut data = Tracking::new();
        loop {
            match recv_fi.recv() {
                Ok(msg) => match msg {
                    Some((line, fi)) => match data.process_entry(fi) {
                        Err(e) => println!("error on line {} due to {}", line, e),
                        _ => {}
                    },
                    None => break,
                },
                Err(e) => panic!("data thread cannot recv a FileInfo record: {}", e),
            }
        }
        data.walk_and_heap(&cfg_c);
    });
    // });

    //})).collect::<Vec<_>>();

    for rec in rdr.records() {
        line_count += 1;
        if line_count % 1_000_000 == 0 {
            println!("at line {}", line_count);
        }
        // if line_count > 1_000_000 {
        //     break;
        // }
        match rec {
            Ok(sr) => {
                send_sr.send(Some((line_count, sr)))?;
            }
            Err(_e) => eprintln!("error reading record: {} ", line_count),
        }
    }
    dbg!("sending Nones");
    for _ in 0..cfg.num_rec_threads {
        send_sr.send(None)?;
    }
    send_fi.send(None)?;


    let _ = data_thread.join();
    println!("{:?}", start.elapsed());
    Ok(())
}
