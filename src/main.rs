use std::{collections::BTreeMap, fs::File, path::PathBuf, str::FromStr};

use crate::cli::CliCfg;
use csv::{StringRecord, StringRecordsIter};
use structopt::StructOpt;

mod cli;
mod data;

use anyhow::{Context, Error, Result};

fn main() -> Result<()> {
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

    let file = File::open(cfg.files)?;
    let mut rdr = csv_vbld.from_reader(&file);

    let mut map: BTreeMap<String, FileInfo> = BTreeMap::new();

    let path_del = '/';

    for rec in rdr.records() {
        match rec {
            Ok(rec) => {
                let (path,rec) = FileInfo::new(&rec)?;
                if rec.file_type == b'F' {
                    
                }
                map.insert(path, rec);
                walk
            }
            Err(e) => eprintln!("error reading record: {}", e),
        }
    }

    for i in [1, 2, 3] {
        println!("{}", i)
    }

    Ok(())
}

