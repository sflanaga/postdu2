use std::path::PathBuf;

use structopt::StructOpt;
use structopt::clap::AppSettings::*;


#[derive(StructOpt, Debug, Clone)]
#[structopt(
rename_all = "kebab-case",
global_settings(& [
    ColoredHelp, DeriveDisplayOrder]),
)]
/// Read a | delimited file of hdfs files and summarize the space results
///
pub struct CliCfg {
    #[structopt(short = "f", name = "file", parse(from_os_str))]
    /// input file
    pub file: PathBuf,

    #[structopt(short = "n", name = "top_n")]
    /// input file
    pub top_n: usize,


    #[structopt(short = "t", name = "num_rec_threads", default_value("4"))]
    /// number of threads that turn array of strings into FileInfo records
    pub num_rec_threads: usize,

    #[structopt(short = "P", name = "parser_qsize", default_value("1000"))]
    /// size of queue between csv split and parser
    pub parser_qsize: usize,

    #[structopt(short = "D", name = "data_qsize", default_value("1000"))]
    /// size of queue between parser and data processing
    pub data_qsize: usize,


}