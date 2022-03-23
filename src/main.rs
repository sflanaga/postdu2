#![allow(dead_code)]
#![allow(unused_imports)]
use std::{collections::{self, BTreeMap}, fs::File, io::{BufReader, Read}, path::PathBuf, str::FromStr, time::{Duration, Instant}};

use crate::{
    cli::CliCfg,
    data::{dur_to_str, uri_to_path, FileInfo, Tracking},
};
use csv::{StringRecord, StringRecordsIter};
use flate2::bufread::GzDecoder;
use structopt::StructOpt;

mod cli;
mod data;

use anyhow::{anyhow, Context, Error, Result};

fn main() -> Result<()> {
    // println!("{}", dur_to_str(Duration::from_secs(1800)));
    // println!("{}", dur_to_str(Duration::from_secs(3600*4)));
    // println!("{}", dur_to_str(Duration::from_secs(3600*48)));

    // println!("{:?}", uri_to_path("/EBDASTAGING/amshbase"));
    // println!("{:?}", uri_to_path("hdfs://EBDASTAGING/amshbase"));
    // std::process::exit(0);

    let cfg: CliCfg = CliCfg::from_args();

    let mut bld = csv::ReaderBuilder::new();

    let csv_vbld = bld
        .delimiter(b'|')
        .has_headers(false)
        .flexible(true)
        .quote(b'"')
        .escape(Some(b'\\'))
        .comment(Some(b'#'));

    //let file = File::open(&cfg.file)?;
    let stdin = std::io::stdin();
    let lock_stdin = stdin.lock();
    let buf = if cfg.file.is_some()  {
        let file: PathBuf = cfg.file.as_ref().unwrap().clone();
        openfile(&file)?
    } else {
        Box::new(BufReader::new(lock_stdin))  
    };
    //let buf = openfile(&cfg.file)?; // BufReader::with_capacity(1024 * 256, &file);
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
                    Some((line, fi)) => match data.process_entry(fi, &cfg_c) {
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

    let mut field_count = 0 ;
    let start_time = Instant::now();
    let mut last_time = start_time;
    let mut last_rec_count = 0u64;
    for rec in rdr.records() {
        line_count += 1;
        if cfg.ticker_interval_secs > 0 {
            let now_time = Instant::now();
            let delta_time = now_time - last_time;
            if delta_time.as_secs() > cfg.ticker_interval_secs {
                let delta_count = line_count - last_rec_count;
                if delta_count > 0 {
                    let rate = delta_count / delta_time.as_secs();
                    eprintln!("records so far line {}  rate: {} / sec", line_count, rate);
                }
                last_time = now_time;
                last_rec_count = line_count;
            }
        }

        if cfg.limit_input>0 && line_count > cfg.limit_input  {
            break;
        }

        match rec {
            Ok(sr) => {
                field_count += sr.len();
                send_sr.send(Some((line_count, sr)))?;
            }
            Err(_e) => eprintln!("error reading record: {} ", line_count),
        }
    }
    dbg!("sending Nones");
    (0..cfg.num_rec_threads).for_each(|i|send_sr.send(None).expect("send_sr - for shutdown"));
    for t in ct {
        let _ = t.join();
    }
    send_fi.send(None)?;

    let _ = data_thread.join();
    println!("{:?}", start.elapsed());
    Ok(())
}


fn openfile(path: &PathBuf) -> Result<Box<dyn Read>> {
    let ext = {
        match path.to_str().unwrap().rfind('.') {
            None => String::from(""),
            Some(i) => String::from(&path.to_str().unwrap()[i..]),
        }
    };
    let mut rdr: Box<dyn Read> = match &ext[..] {
        ".gz" | ".tgz" => {
            match File::open(&path) {
                Ok(f) => Box::new( GzDecoder::new(BufReader::new(f))),
                Err(err) => return Err(anyhow!("skipping gz file \"{}\", due to error: {}", path.display(), err)),
            }
        }
        ".zst" | ".zstd" => {
            match File::open(&path) {
                Ok(f) => {
                    match zstd::stream::read::Decoder::new(BufReader::new(f)) {
                        Ok(br) => Box::new(br),
                        Err(err) => return Err(anyhow!("skipping file \"{}\", zstd decoder error: {}", path.display(), err)),
                    }
                }
                Err(err) => return Err(anyhow!("skipping zst file \"{}\", due to error: {}", path.display(), err)),
            }
        }
        // ".bz2" | ".tbz2" | ".txz" | ".xz" | ".lz4" | ".lzma" | ".br" | ".Z" => {
        //     match DecompressionReader::new(&path) {
        //         Ok(rdr) => Box::new(rdr),
        //         Err(err) => {
        //             eprintln!("skipping general de-comp file \"{}\", due to error: {}", path.display(), err);
        //             continue;
        //         }
        //     }
        // }
        _ => {
            match File::open(&path) {
                Ok(f) => Box::new(BufReader::new(f)),
                Err(err) => return Err(anyhow!("skipping regular file \"{}\", due to error: {}", path.display(), err)),
            }
        },
    };
    return Ok(rdr);

}