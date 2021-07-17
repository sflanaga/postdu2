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
    pub files: PathBuf,
        

}