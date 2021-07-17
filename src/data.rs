pub struct FileInfo {
    size: u64,
    mod_time: u64,
}

impl FileInfo {
    pub fn new(raw_rec: &StringRecord) -> Result<(String,Self)> {
        let p = raw_rec[0].to_string();

        Ok((p,FileInfo {
            size: raw_rec[2].parse::<u64>().context("unable parse size")?,
            mod_time: raw_rec[3]
                .parse::<u64>()
                .context("unable to parse mod time")?,
        }))
    }
}

struct _DirInfo {
    size: u64,
    max_time: u64,
    max_time: u64,
}

struct DirInfo {
    direct: _DirInfo,
    recurse: _DirInfo,
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
