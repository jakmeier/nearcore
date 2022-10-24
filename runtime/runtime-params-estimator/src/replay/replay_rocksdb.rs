use near_store::{DBCol, NodeStorage, Store, StoreUpdate, Temperature};
use tempfile::TempDir;

use super::Visitor;
use std::collections::HashSet;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::time::{Duration, Instant};

/// Visitor that executes all GET operations in the input trace to a RocksDB
/// instance and measures the latency for each request.
pub(super) struct StoreReplayVisitor {
    store: Store,
    /// DB latency for GETs in ns
    get_latencies: Vec<u64>,
    /// Flag whether preparation step should insert data or not.
    insert_data: bool,
    _tmp_dir: TempDir,
}

/// Prepares a store for RocksDB replay by inserting all required values.
struct FillStoreVisitor<'a> {
    store: &'a Store,
    /// Accumulate changes in a batch before writing them to speed up writes.
    update: StoreUpdate,
    /// Keep track of keys in open DB transaction to avoid overwriting values
    /// and to keep track of current TX size.
    db_tx_keys: HashSet<(Vec<u8>, DBCol)>,
}
impl FillStoreVisitor<'_> {
    const DB_WRITE_BATCH_SIZE: usize = 256;
}

impl StoreReplayVisitor {
    pub(crate) fn rocks_db(db_path: &Option<PathBuf>, insert_data: bool) -> Self {
        let config = Default::default();
        let tmp_dir = tempfile::tempdir().unwrap();

        if let Some(db_path) = db_path {
            // make copy of source DB to preserve original data
            let status = Command::new("/bin/cp")
                .arg("-r")
                .arg(db_path)
                .arg(tmp_dir.path().join("data"))
                .status()
                .expect("failed to copy source database to new temporary directory");
            assert!(
                status.success(),
                "failed to copy source database to new temporary directory: {status}"
            );
        }

        let store = NodeStorage::opener(&tmp_dir.path().join("data"), &config)
            .open()
            .unwrap()
            .get_store(Temperature::Hot);
        Self { store, get_latencies: Vec::new(), _tmp_dir: tmp_dir, insert_data }
    }
}

impl Visitor for StoreReplayVisitor {
    fn eval_db_op(
        &mut self,
        _out: &mut dyn Write,
        _indent: usize,
        op: &str,
        size: Option<u64>,
        key: &[u8],
        col: &str,
    ) -> anyhow::Result<()> {
        match op {
            "GET" => {
                let before = Instant::now();
                let value = self.store.get(DBCol::from_str(col)?, key)?;
                self.get_latencies.push(before.elapsed().as_nanos() as u64);
                assert_eq!(
                    value.map(|val| val.len() as u64),
                    size,
                    "Key {} did not have the expected value in the DB.",
                    near_o11y::pretty::Bytes(key),
                );
            }
            _ => {
                // writes aren't supported, yet
            }
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Write) -> anyhow::Result<()> {
        if self.get_latencies.is_empty() {
            writeln!(out, "no GETs measured")?;
            return Ok(());
        }

        self.get_latencies.sort_unstable();
        let min = self.get_latencies.first().unwrap();
        let max = self.get_latencies.last().unwrap();
        let total: u64 = self.get_latencies.iter().sum();
        let average = total as f64 / self.get_latencies.len() as f64;
        let median = self.get_latencies[self.get_latencies.len() / 2];

        // print a short summary
        writeln!(out, "min/avg/median/max")?;
        writeln!(
            out,
            "{:#.2?}/{:#.2?}/{:#.2?}/{:#.2?}",
            Duration::from_nanos(*min),
            Duration::from_nanos(average.round() as u64),
            Duration::from_nanos(median),
            Duration::from_nanos(*max),
        )?;

        // Print histogram with buckets buckets ranging from 1us up to 100ms.
        //
        // On choice of buckets:
        // On a local SSD, we expect most values around 10 - 40us, so there are
        // plenty of buckets in that region.
        // On persistent SSD, it could be more around 100-200 us.
        // Extra buckers towards the end are added to show infos outliers but
        // most of the time they are not shown in the output at all.
        let bucket_limits = [
            1_000, // 1us
            5_000,
            10_000,
            15_000,
            20_000,
            25_000,
            30_000,
            35_000,
            40_000,
            45_000,
            50_000,
            60_000,
            70_000,
            80_000,
            90_000,
            100_000, // 100us
            125_000,
            150_000,
            175_000,
            200_000,
            500_000,
            1_000_000, // 1ms
            5_000_000,
            20_000_000,
            100_000_000,
            500_000_000, // 500ms
            u64::MAX,
        ];

        let mut bucket_counter = 0;
        let mut bucket_sum = 0;
        let mut bucket_index = 0;
        writeln!(out, "{:>13} {:>8}  {}", "bucket", "count", "sum of request in bucket")?;
        for i in 0..self.get_latencies.len() {
            while self.get_latencies[i] > bucket_limits[bucket_index] {
                print_histo_line(
                    out,
                    bucket_index
                        .checked_sub(1)
                        .and_then(|j| bucket_limits.get(j))
                        .copied()
                        .unwrap_or(0),
                    bucket_limits[bucket_index],
                    bucket_counter,
                    bucket_sum,
                )?;
                bucket_index += 1;
                bucket_counter = 0;
                bucket_sum = 0;
            }
            bucket_counter += 1;
            bucket_sum += self.get_latencies[i];
        }

        print_histo_line(
            out,
            bucket_limits.get(bucket_index - 1).copied().unwrap_or(0),
            bucket_limits[bucket_index],
            bucket_counter,
            bucket_sum,
        )?;

        Ok(())
    }

    fn preparation_visitor(&self) -> Option<Box<dyn Visitor + '_>> {
        let update = self.store.store_update();
        self.insert_data.then(|| {
            Box::new(FillStoreVisitor {
                store: &self.store,
                update,
                db_tx_keys: Default::default(),
            }) as Box<dyn Visitor>
        })
    }
}

fn print_histo_line(
    out: &mut dyn Write,
    t0: u64,
    t1: u64,
    counter: u64,
    sum: u64,
) -> std::io::Result<()> {
    writeln!(
        out,
        "{:>#5?} - {:>#5?} {:>8} {:>#10.3?}",
        Duration::from_nanos(t0),
        Duration::from_nanos(t1),
        counter,
        Duration::from_nanos(sum),
    )
}

impl Visitor for FillStoreVisitor<'_> {
    fn eval_db_op(
        &mut self,
        _out: &mut dyn Write,
        _indent: usize,
        op: &str,
        size: Option<u64>,
        key: &[u8],
        col: &str,
    ) -> anyhow::Result<()> {
        match op {
            "GET" => {
                if let Some(size) = size {
                    // RocksDB visitor will want to read the GET and expects a
                    // value of a certain size. Ensure existence of such a value.
                    let db_col = DBCol::from_str(col)?;
                    if self.store.get(db_col, key)?.is_some() {
                        // value exists in DB, don't have to insert anything
                        // also avoids problems with RC and insert-only columns
                        // that don't allow overwriting values
                        return Ok(());
                    }
                    if !self.db_tx_keys.insert((key.to_vec(), db_col)) {
                        // key exists in transaction, don't add it again
                        return Ok(());
                    }
                    // Generate random value, only the size matters for performance.
                    // (But make it random to avoid cheap compression.)
                    let value: Vec<u8> =
                        std::iter::repeat_with(rand::random).take(size as usize).collect();
                    if db_col.is_insert_only() {
                        self.update.insert(db_col, key, &value);
                    } else if db_col.is_rc() {
                        self.update.increment_refcount(db_col, key, &value);
                    } else {
                        self.update.set(db_col, key, &value);
                    }
                    if self.db_tx_keys.len() >= Self::DB_WRITE_BATCH_SIZE {
                        self.flush_db_tx()?;
                    }
                }
            }
            _ => (),
        }
        Ok(())
    }
    fn flush(&mut self, _out: &mut dyn Write) -> anyhow::Result<()> {
        self.flush_db_tx()?;
        self.store.flush()?;
        self.store.compact()?;
        Ok(())
    }
}

impl<'a> FillStoreVisitor<'a> {
    fn flush_db_tx(&mut self) -> Result<(), anyhow::Error> {
        let new_update = self.store.store_update();
        std::mem::replace(&mut self.update, new_update).commit()?;
        self.db_tx_keys.clear();
        Ok(())
    }
}
