use anyhow::Context;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::PathBuf;
use std::str::SplitWhitespace;

#[derive(clap::Parser)]
pub(crate) struct ReplayCmd {
    trace: PathBuf,
}

impl ReplayCmd {
    pub(crate) fn run(&self) -> anyhow::Result<()> {
        let file = File::open(&self.trace)?;

        let mut visitors = self.build_visitors();

        for line in io::BufReader::new(file).lines() {
            let line = line?;
            for v in &mut visitors {
                if let Err(e) = v.eval_line(&line) {
                    println!("ERROR: {e} for input line: {line}");
                }
            }
        }
        visitors.iter_mut().map(|v| v.flush()).collect::<anyhow::Result<()>>()?;

        Ok(())
    }
    fn build_visitors(&self) -> Vec<Box<dyn Visitor>> {
        // let mut _a = IoGasGuesser {
        //     // Assuming 7000 IOPS, reading once has a minimum latency of
        //     // 1s/7000 = 0.000142857s.
        //     // 143us is also the 99th percentile measured for NVME SSD
        //     // random read completion latency for a single 4kB block
        //     ns_per_op: 143_000,
        //     // Reading sequential at 700MB/s translates to 5.851us per 4kiB block.
        //     ns_per_4kib: 5_851,
        //     accumulator: 0,
        // };

        vec![Box::new(FoldDbOps::estimator_trace())]
        // vec![Box::new(FoldDbOps::blocks_and_receipts())]
    }
}

fn extract_key_values<'a>(
    mut tokens: SplitWhitespace<'a>,
) -> anyhow::Result<BTreeMap<&'a str, &'a str>> {
    let mut dict = BTreeMap::new();
    while let Some(key_val) = tokens.next() {
        let (key, value) =
            key_val.split_once('=').context("key-value pair delimited by `=` expected")?;
        dict.insert(key, value);
    }
    Ok(dict)
}

trait Visitor {
    // fn eval_indent(&mut self, indent: usize) -> anyhow::Result<()> {
    //     Ok(())
    // }
    fn eval_db_op(
        &mut self,
        indent: usize,
        op: &str,
        size: Option<u64>,
        key: &[u8],
        col: &str,
    ) -> anyhow::Result<()> {
        if col == "State" {
            self.eval_state_db_op(indent, op, size, key)
        } else {
            Ok(())
        }
    }
    fn eval_state_db_op(
        &mut self,
        indent: usize,
        op: &str,
        size: Option<u64>,
        key: &[u8],
    ) -> anyhow::Result<()> {
        let (_, _, _, _) = (indent, op, size, key);
        Ok(())
    }
    fn eval_storage_op(
        &mut self,
        indent: usize,
        op: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        let (_, _, _) = (indent, op, dict);
        Ok(())
    }
    /// Opening spans that are not storage or DB operations.
    fn eval_label(
        &mut self,
        indent: usize,
        label: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        let (_, _, _) = (indent, label, dict);
        Ok(())
    }
    fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// The root entry point of the visitors.
    ///
    /// This function takes a raw input line as input without any preprocessing.
    /// A visitor may choose to overwrite this function for full control but the
    /// intention is that the default implementation takes over the basic
    /// parsing and visitor implementations defined their behaviour using the
    /// other trait methods.
    fn eval_line(&mut self, line: &str) -> anyhow::Result<()> {
        if let Some(indent) = line.chars().position(|c| !c.is_whitespace()) {
            let mut tokens = line.split_whitespace();
            if let Some(keyword) = tokens.next() {
                match keyword {
                    "GET" | "SET" | "UPDATE_RC" => {
                        let col = tokens.next().context("missing column field in DB operation")?;
                        let mut key_str = tokens.next().context("missing key in DB operation")?;
                        if key_str.starts_with('"') {
                            key_str = &key_str[1..key_str.len() - 1];
                        }
                        let key = bs58::decode(key_str).into_vec()?;
                        // let key_len = key.len() - 2;
                        let dict = extract_key_values(tokens)?;
                        let size: Option<u64> = dict.get("size").map(|s| s.parse()).transpose()?;
                        self.eval_db_op(indent, keyword, size, &key, col)?;
                    }
                    "storage_read" | "storage_write" | "storage_remove" | "storage_has_key" => {
                        let op = tokens.next();
                        if op.is_none() {
                            return Ok(());
                        }

                        let dict = extract_key_values(tokens)?;
                        self.eval_storage_op(indent, keyword, &dict)?;
                    }
                    other_label => {
                        let dict = extract_key_values(tokens)?;
                        self.eval_label(indent, other_label, &dict)?;
                    }
                }
            }
        }
        Ok(())
    }
}

struct FoldDbOps {
    ops_cols: BTreeMap<String, BTreeMap<String, usize>>,
    fold_anchors: Vec<String>,
    flush_indents: Vec<usize>,
}
impl FoldDbOps {
    fn blocks_and_receipts() -> FoldDbOps {
        FoldDbOps {
            ops_cols: BTreeMap::new(),
            fold_anchors: vec!["process_receipt".to_owned(), "apply".to_owned()],
            flush_indents: vec![],
        }
    }
    fn estimator_trace() -> FoldDbOps {
        FoldDbOps {
            ops_cols: BTreeMap::new(),
            fold_anchors: vec!["measurement".to_owned()],
            flush_indents: vec![],
        }
    }
}

impl Visitor for FoldDbOps {
    fn eval_db_op(
        &mut self,
        indent: usize,
        op: &str,
        _size: Option<u64>,
        _key: &[u8],
        col: &str,
    ) -> anyhow::Result<()> {
        *self.ops_cols.entry(op.to_owned()).or_default().entry(col.to_owned()).or_default() += 1;
        self.eval_label(indent, op, &BTreeMap::new())
    }

    fn eval_storage_op(
        &mut self,
        indent: usize,
        op: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        self.eval_label(indent, op, dict)
    }

    fn eval_label(
        &mut self,
        indent: usize,
        label: &str,
        _dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        if let Some(&prev_indent) = self.flush_indents.last() {
            if prev_indent >= indent {
                self.flush()?;
                self.flush_indents.pop();
            }
        }
        if self.fold_anchors.iter().any(|anchor| *anchor == label) {
            println!("{:indent$}{label}", "");
            self.flush_indents.push(indent);
        }
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let indent = self.flush_indents.last().unwrap_or(&0) + 2;
        let ops_cols = std::mem::take(&mut self.ops_cols);
        for (op, map) in ops_cols.into_iter() {
            if !map.is_empty() {
                print!("{:indent$}{op}   ", "");
            }
            for (col, num) in map.into_iter() {
                print!("{num:8>} {col}  ");
            }
            println!();
        }
        Ok(())
    }
}

// struct IoGasGuesser {
//     ns_per_op: u64,
//     ns_per_4kib: u64,
//     accumulator: u64,
// }

// impl IoGasGuesser {
//     fn eval_line(&mut self, line: &str) -> anyhow::Result<()> {
//         let mut tokens = line.split_whitespace();

//         if let Some(keyword) = tokens.next() {
//             match keyword {
//                 "GET" => {
//                     let _col = tokens.next().unwrap();
//                     let _key = tokens.next().unwrap();
//                     // let key_len = key.len() - 2;
//                     let dict = extract_key_values(tokens)?;
//                     let size: Option<u64> = dict.get("size").map(|s| s.parse().unwrap());

//                     self.eval_get(size)
//                 }
//                 _ => {}
//             }
//         }
//         Ok(())
//     }
//     fn eval_get(&mut self, size: Option<u64>) {
//         self.accumulator += self.ns_per_op;
//         if let Some(size) = size {
//             self.accumulator += (size + 1023) / 4096 * self.ns_per_4kib;
//         } else {
//             // TODO: have a look at cost for reading non-existing keys
//         }
//     }
// }

// // #[derive(Default)]
// // struct DbOpsPerStorageOp {
// //     storage_op: Option<String>,
// //     storage_op_size: u64,
// //     indent_of_db_ops: usize,
// //     db_get_sizes: Vec<u64>,
// // }

// // impl DbOpsPerStorageOp {
// //     fn eval_line(&mut self, line: &str) -> anyhow::Result<()> {
// //         if let Some(indent) = line.chars().position(|c| !c.is_whitespace()) {
// //             let mut tokens = line.split_whitespace();

// //             if let Some(keyword) = tokens.next() {
// //                 match keyword {
// //                     "GET" => {
// //                         let _col = tokens.next().unwrap();
// //                         let key = tokens.next().unwrap();
// //                         let key_len = key.len() - 2;
// //                         let dict = extract_key_values(tokens)?;
// //                         let size: Option<u64> = dict.get("size").map(|s| s.parse().unwrap());

// //                         self.eval_get(indent, size);
// //                     }
// //                     "storage_read" | "storage_write" | "storage_remove" | "storage_has_key" => {
// //                         let _op = tokens.next().unwrap();
// //                         let dict = extract_key_values(tokens)?;
// //                         let size = if keyword == "storage_has_key" {
// //                             0
// //                         } else {
// //                             dict.get("size").context("storage operation without size")?.parse()?
// //                         };
// //                         self.eval_storage_op(indent, keyword.to_owned(), size);
// //                     }
// //                     _ => {
// //                         self.eval_indent(indent);
// //                         println!("{line}");
// //                     }
// //                 }
// //             }
// //         }
// //         Ok(())
// //     }

// //     fn eval_indent(&mut self, indent: usize) {
// //         if indent < self.indent_of_db_ops {
// //             self.flush();
// //         }
// //     }
// //     fn eval_get(&mut self, _indent: usize, size: Option<u64>) {
// //         self.db_get_sizes.push(size.unwrap_or(0));
// //     }
// //     fn eval_storage_op(&mut self, indent: usize, storage_operation: String, size: u64) {
// //         self.flush();

// //         self.storage_op = Some(storage_operation);
// //         self.storage_op_size = size;
// //         self.indent_of_db_ops = indent + 2;
// //     }
// //     fn flush(&mut self) {
// //         if let Some(op) = &self.storage_op {
// //             let size = self.storage_op_size;
// //             let list =
// //                 self.db_get_sizes.iter().map(|num| num.to_string()).collect::<Vec<_>>().join(",");
// //             let n = self.db_get_sizes.len();
// //             let sum: u64 = self.db_get_sizes.iter().sum();
// //             // println!("{op}(value={size}B): DB=({n};{sum}) [{list}]");
// //             println!("{op}(value={size}B): DB=({n} requests;{sum}B total)");
// //         }
// //         *self = Default::default();
// //     }
// // }

// #[derive(Default)]
// struct DbOpsPerTx {
//     printing: bool,

//     num_get: u64,
//     num_set: u64,
//     total_size_get: u64,
//     total_size_set: u64,

//     num_read: u64,
//     num_write: u64,
//     total_size_read: u64,
//     total_size_write: u64,

//     num_tn_shard_cache: u64,
//     num_tn_chunk_cache: u64,
//     num_tn_db: u64,

//     num_tn_shard_cache_miss: u64,
//     num_tn_shard_cache_too_large: u64,

//     indent_of_tx: usize,
// }

// #[derive(Default)]
// struct DbOpsPerCostMeasurement {
//     num_get: u64,
//     num_set: u64,
//     total_size_get: u64,
//     total_size_set: u64,

//     num_read: u64,
//     num_write: u64,
//     total_size_read: u64,
//     total_size_write: u64,

//     num_tn_shard_cache: u64,
//     num_tn_chunk_cache: u64,
//     num_tn_db: u64,

//     num_tn_shard_cache_miss: u64,
//     num_tn_shard_cache_too_large: u64,

//     indent_of_measurement: usize,
// }

// impl DbOpsPerTx {
//     fn eval_line(&mut self, line: &str, account_filter: Option<&str>) -> anyhow::Result<()> {
//         if let Some(indent) = line.chars().position(|c| !c.is_whitespace()) {
//             let mut tokens = line.split_whitespace();

//             if let Some(keyword) = tokens.next() {
//                 match keyword {
//                     "process_receipt" => {
//                         self.flush();
//                         self.printing = if let Some(filter) = account_filter {
//                             line.contains(filter)
//                         } else {
//                             true
//                         };
//                         if self.printing {
//                             println!("{line}");
//                         }
//                         self.indent_of_tx = indent;
//                     }
//                     "GET" => {
//                         let _col = tokens.next().unwrap();
//                         let _key = tokens.next().unwrap();
//                         // let key_len = key.len() - 2;
//                         let dict = extract_key_values(tokens)?;
//                         let size: Option<u64> = dict.get("size").map(|s| s.parse().unwrap_or(0));

//                         self.eval_get(indent, size);
//                     }
//                     "SET" => {
//                         let _col = tokens.next().unwrap();
//                         let _key = tokens.next().unwrap();
//                         // let key_len = key.len() - 2;
//                         let dict = extract_key_values(tokens)?;
//                         let size: Option<u64> = dict.get("size").map(|s| s.parse().unwrap_or(0));

//                         self.eval_set(indent, size);
//                     }
//                     "storage_read" | "storage_write" | "storage_remove" | "storage_has_key" => {
//                         let op = tokens.next();
//                         if op.is_none() {
//                             return Ok(());
//                         }

//                         let dict = extract_key_values(tokens)?;

//                         self.eval_storage_op(indent, keyword, &dict)?;
//                     }
//                     _ => {
//                         self.eval_indent(indent);
//                         // println!("{line}");
//                     }
//                 }
//             }
//         }
//         if line.contains("BlockInfo") {
//             // println!("{line}");
//         }
//         Ok(())
//     }

//     fn eval_indent(&mut self, indent: usize) {
//         // if indent <= self.indent_of_tx {
//         //     self.flush();
//         // }
//     }
//     fn eval_get(&mut self, _indent: usize, size: Option<u64>) {
//         self.num_get += 1;
//         self.total_size_get += size.unwrap_or(0);
//     }
//     fn eval_set(&mut self, _indent: usize, size: Option<u64>) {
//         self.num_set += 1;
//         self.total_size_set += size.unwrap_or(0);
//     }
//     fn eval_storage_op(
//         &mut self,
//         _indent: usize,
//         storage_operation: &str,
//         dict: &BTreeMap<&str, &str>,
//     ) -> anyhow::Result<()> {
//         let size = if storage_operation == "storage_has_key" {
//             0
//         } else {
//             dict.get("size").unwrap_or(&"0").parse()?
//         };
//         let mut tn_db_reads: u64 = dict
//             .get("tn_db_reads")
//             .map(|s| s.parse().unwrap())
//             .context("no tn_db_reads on storage op")?;
//         let mut tn_mem_reads: u64 = dict
//             .get("tn_mem_reads")
//             .map(|s| s.parse().unwrap())
//             .context("no tn_mem_reads on storage op")?;

//         let tn_shard_cache_hits =
//             dict.get("shard_cache_hit").map(|s| s.parse().unwrap()).unwrap_or(0);
//         let tn_shard_cache_misses =
//             dict.get("shard_cache_miss").map(|s| s.parse().unwrap()).unwrap_or(0);
//         let tn_shard_cache_too_large =
//             dict.get("shard_cache_too_large").map(|s| s.parse().unwrap()).unwrap_or(0);

//         match storage_operation {
//             "storage_read" => {
//                 self.num_read += 1;
//                 self.total_size_read += size;
//                 // We are currently counting one node too little, see
//                 // https://github.com/near/nearcore/issues/6225. But we don't
//                 // know where, could be either tn_db_reads or tn_mem_reads. But
//                 // we know that tn_db_reads = shard_cache_hits +
//                 // shard_cache_misses.
//                 if tn_db_reads < tn_shard_cache_misses + tn_shard_cache_hits {
//                     tn_db_reads += 1;
//                 } else {
//                     tn_mem_reads += 1;
//                 }
//                 debug_assert_eq!(tn_db_reads, tn_shard_cache_misses + tn_shard_cache_hits)
//             }
//             "storage_write" => {
//                 self.num_write += 1;
//                 self.total_size_write += size;
//             }
//             _ => {}
//         }

//         self.num_tn_chunk_cache += tn_mem_reads;
//         self.num_tn_shard_cache += tn_shard_cache_hits;
//         self.num_tn_db += tn_db_reads - tn_shard_cache_hits;
//         self.num_tn_shard_cache_too_large += tn_shard_cache_too_large;
//         self.num_tn_shard_cache_miss += tn_shard_cache_misses;

//         Ok(())
//     }
//     fn flush(&mut self) {
//         if self.printing {
//             let indent = self.indent_of_tx + 2;
//             println!(
//                 "{:indent$}DB GET        {:>5} requests for a total of {:>8} B",
//                 "", self.num_get, self.total_size_get
//             );
//             println!(
//                 "{:indent$}DB SET        {:>5} requests for a total of {:>8} B",
//                 "", self.num_set, self.total_size_set
//             );
//             println!(
//                 "{:indent$}STORAGE READ  {:>5} requests for a total of {:>8} B",
//                 "", self.num_read, self.total_size_read
//             );
//             println!(
//                 "{:indent$}STORAGE WRITE {:>5} requests for a total of {:>8} B",
//                 "", self.num_write, self.total_size_write
//             );
//             println!(
//                 "{:indent$}TRIE NODES    {:>4} /{:>4} /{:>4}  (chunk-cache/shard-cache/DB)",
//                 "", self.num_tn_chunk_cache, self.num_tn_shard_cache, self.num_tn_db
//             );
//             print_cache_rate(
//                 indent,
//                 "SHARD CACHE",
//                 self.num_tn_shard_cache,
//                 self.num_tn_shard_cache_miss,
//                 self.num_tn_shard_cache_too_large,
//                 "too large nodes",
//             );
//             print_cache_rate(
//                 indent,
//                 "CHUNK CACHE",
//                 self.num_tn_chunk_cache,
//                 self.num_tn_shard_cache + self.num_tn_db,
//                 self.num_tn_shard_cache,
//                 "shard cache hits",
//             );
//         }

//         *self = Default::default();
//     }
// }

// impl DbOpsPerCostMeasurement {
//     fn eval_line(&mut self, line: &str, account_filter: Option<&str>) -> anyhow::Result<()> {
//         if let Some(indent) = line.chars().position(|c| !c.is_whitespace()) {
//             let mut tokens = line.split_whitespace();

//             if let Some(keyword) = tokens.next() {
//                 match keyword {
//                     "measurement" => {
//                         self.flush();
//                         println!("{line}");
//                         self.indent_of_measurement = indent;
//                     }
//                     "GET" => {
//                         let _col = tokens.next().unwrap();
//                         let _key = tokens.next().unwrap();
//                         // let key_len = key.len() - 2;
//                         let dict = extract_key_values(tokens)?;
//                         let size: Option<u64> = dict.get("size").map(|s| s.parse().unwrap_or(0));

//                         self.eval_get(indent, size);
//                     }
//                     "SET" => {
//                         let _col = tokens.next().unwrap();
//                         let _key = tokens.next().unwrap();
//                         // let key_len = key.len() - 2;
//                         let dict = extract_key_values(tokens)?;
//                         let size: Option<u64> = dict.get("size").map(|s| s.parse().unwrap_or(0));

//                         self.eval_set(indent, size);
//                     }
//                     "apply"
//                     | "process_receipt"
//                     | "process_transaction"
//                     | "storage_read"
//                     | "storage_write"
//                     | "storage_remove"
//                     | "storage_has_key" => {
//                         let op = tokens.next();
//                         if op.is_none() {
//                             return Ok(());
//                         }

//                         let dict = extract_key_values(tokens)?;

//                         self.eval_storage_op(indent, keyword, &dict)?;
//                     }
//                     _ => {
//                         self.eval_indent(indent);
//                         // println!("{line}");
//                     }
//                 }
//             }
//         }
//         if line.contains("estimation") {
//             println!("{line}");
//         }
//         Ok(())
//     }

//     fn eval_indent(&mut self, indent: usize) {
//         // if indent <= self.indent_of_tx {
//         //     self.flush();
//         // }
//     }
//     fn eval_get(&mut self, _indent: usize, size: Option<u64>) {
//         self.num_get += 1;
//         self.total_size_get += size.unwrap_or(0);
//     }
//     fn eval_set(&mut self, _indent: usize, size: Option<u64>) {
//         self.num_set += 1;
//         self.total_size_set += size.unwrap_or(0);
//     }
//     fn eval_storage_op(
//         &mut self,
//         _indent: usize,
//         storage_operation: &str,
//         dict: &BTreeMap<&str, &str>,
//     ) -> anyhow::Result<()> {
//         let size = if storage_operation == "storage_has_key" {
//             0
//         } else {
//             dict.get("size").unwrap_or(&"0").parse()?
//         };
//         let mut tn_db_reads: u64 = dict.get("tn_db_reads").map(|s| s.parse().unwrap()).unwrap_or(0);
//         let mut tn_mem_reads: u64 =
//             dict.get("tn_mem_reads").map(|s| s.parse().unwrap()).unwrap_or(0);

//         let tn_shard_cache_hits =
//             dict.get("shard_cache_hit").map(|s| s.parse().unwrap()).unwrap_or(0);
//         let tn_shard_cache_misses =
//             dict.get("shard_cache_miss").map(|s| s.parse().unwrap()).unwrap_or(0);
//         let tn_shard_cache_too_large =
//             dict.get("shard_cache_too_large").map(|s| s.parse().unwrap()).unwrap_or(0);

//         match storage_operation {
//             "storage_read" => {
//                 self.num_read += 1;
//                 self.total_size_read += size;
//                 // We are currently counting one node too little, see
//                 // https://github.com/near/nearcore/issues/6225. But we don't
//                 // know where, could be either tn_db_reads or tn_mem_reads. But
//                 // we know that tn_db_reads = shard_cache_hits +
//                 // shard_cache_misses.
//                 if tn_db_reads < tn_shard_cache_misses + tn_shard_cache_hits {
//                     tn_db_reads += 1;
//                 } else {
//                     tn_mem_reads += 1;
//                 }
//                 debug_assert_eq!(tn_db_reads, tn_shard_cache_misses + tn_shard_cache_hits)
//             }
//             "storage_write" => {
//                 self.num_write += 1;
//                 self.total_size_write += size;
//             }
//             _ => {}
//         }

//         self.num_tn_chunk_cache += tn_mem_reads;
//         self.num_tn_shard_cache += tn_shard_cache_hits;
//         self.num_tn_db += tn_db_reads - tn_shard_cache_hits;
//         self.num_tn_shard_cache_too_large += tn_shard_cache_too_large;
//         self.num_tn_shard_cache_miss += tn_shard_cache_misses;

//         Ok(())
//     }
//     fn flush(&mut self) {
//         let indent = self.indent_of_measurement + 2;
//         println!(
//             "{:indent$}DB GET        {:>5} requests for a total of {:>8} B",
//             "", self.num_get, self.total_size_get
//         );
//         println!(
//             "{:indent$}DB SET        {:>5} requests for a total of {:>8} B",
//             "", self.num_set, self.total_size_set
//         );
//         println!(
//             "{:indent$}STORAGE READ  {:>5} requests for a total of {:>8} B",
//             "", self.num_read, self.total_size_read
//         );
//         println!(
//             "{:indent$}STORAGE WRITE {:>5} requests for a total of {:>8} B",
//             "", self.num_write, self.total_size_write
//         );
//         println!(
//             "{:indent$}TRIE NODES    {:>4} /{:>4} /{:>4}  (chunk-cache/shard-cache/DB)",
//             "", self.num_tn_chunk_cache, self.num_tn_shard_cache, self.num_tn_db
//         );
//         print_cache_rate(
//             indent,
//             "SHARD CACHE",
//             self.num_tn_shard_cache,
//             self.num_tn_shard_cache_miss,
//             self.num_tn_shard_cache_too_large,
//             "too large nodes",
//         );
//         print_cache_rate(
//             indent,
//             "CHUNK CACHE",
//             self.num_tn_chunk_cache,
//             self.num_tn_shard_cache + self.num_tn_db,
//             self.num_tn_shard_cache,
//             "shard cache hits",
//         );

//         *self = Default::default();
//     }
// }

// fn print_cache_rate(
//     indent: usize,
//     cache_name: &str,
//     hits: u64,
//     misses: u64,
//     special_misses: u64,
//     special_misses_msg: &str,
// ) {
//     let total = hits + misses;
//     if special_misses > 0 {
//         println!(
//             "{:indent$}{cache_name:<16}   {:>6.2}% hit rate, {:>6.2}% if removing {} {special_misses_msg}",
//             "",
//             hits as f64 / total as f64 * 100.0,
//             hits as f64 / (total - special_misses) as f64 * 100.0,
//             special_misses,
//         );
//     } else if total > 0 {
//         println!(
//             "{:indent$}{cache_name:<16} {:>6.2}% hit rate",
//             "",
//             hits as f64 / total as f64 * 100.0,
//         );
//     } else {
//         println!("{:indent$}{cache_name} not accessed", "");
//     }
// }
