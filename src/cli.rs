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
/// format of input from must be pipe limited and contain these fields
/// <filetype>|<path>|<size>|last modifification time
/// Note filetype must be a F (for File) or D (D for directory) or S (symbolic link)
pub struct CliCfg {
    #[structopt(short = "f", name = "file", parse(from_os_str))]
    /// input file
    pub file: Option<PathBuf>,

    #[structopt(short = "n", name = "top_n")]
    /// input file
    pub top_n: usize,

    #[structopt(short = "t", name = "num_rec_threads", default_value("3"))]
    /// number of threads that turn array of strings into FileInfo records
    pub num_rec_threads: usize,

    #[structopt(short = "P", name = "parser_qsize", default_value("1000"))]
    /// size of queue between csv split and parser
    pub parser_qsize: usize,

    #[structopt(short = "D", name = "data_qsize", default_value("1000"))]
    /// size of queue between parser and data processing
    pub data_qsize: usize,

    #[structopt(short = "L", name = "process_only_some", default_value("0"))]
    /// stops processing after X lines - used to debug things
    pub limit_input: u64,

    #[structopt(short = "i", name = "ticker_interval_secs", default_value("1"))]
    /// ticker timer in seconds - 0 means none
    pub ticker_interval_secs: u64,

    #[structopt(short = "z", name = "input_stdin_is_zstd")]
    /// ticker timer in seconds - 0 means none
    pub stdin_zstd: bool
}
