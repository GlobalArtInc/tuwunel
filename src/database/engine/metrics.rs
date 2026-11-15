use std::{
	ffi::{CStr, OsStr},
	fs::read_dir,
	io::ErrorKind,
	path::Path,
	sync::Arc,
};

use rocksdb::BoundColumnFamily;
use tuwunel_core::{
	Err, Error, Result, implement,
	metrics::prom::{CacheSample, RocksSample},
};

use super::Engine;
use crate::util::map_err;

const MEMTABLES: &CStr = c"rocksdb.cur-size-all-mem-tables";
const PENDING_COMPACTION: &CStr = c"rocksdb.estimate-pending-compaction-bytes";
const SST_SIZE: &CStr = c"rocksdb.total-sst-files-size";
const RUNNING_COMPACTIONS: &CStr = c"rocksdb.num-running-compactions";
const RUNNING_FLUSHES: &CStr = c"rocksdb.num-running-flushes";
const WRITE_STOPPED: &CStr = c"rocksdb.is-write-stopped";
const DELAYED_WRITE_RATE: &CStr = c"rocksdb.actual-delayed-write-rate";

/// One reading of the engine for the metrics collector.
///
/// Gauges that could not be read are `None` in the sample, and the reason for
/// each is in `failed`, so that a failing property neither zeroes a gauge nor
/// hides the others.
#[derive(Debug, Default)]
pub struct MetricsSnapshot {
	/// What was read.
	pub sample: RocksSample,

	/// Readings that failed, by name, with the error.
	pub failed: Vec<(&'static str, Error)>,
}

/// Reads the size and occupancy gauges of the engine.
///
/// Blocking: properties of every column family are summed, some of them under
/// the database mutex, and the directory of the write-ahead log is listed. Call
/// it from a thread that may block. Sizes are summed over every column family;
/// values RocksDB keeps for the whole database are read from a single one.
#[implement(Engine)]
#[must_use]
pub fn metrics_snapshot(&self) -> MetricsSnapshot {
	let mut failed = Vec::new();
	let mut sample = RocksSample::default();

	if let Some(caches) = reading(&mut failed, "block_caches", self.block_caches()) {
		sample.block_caches = caches;
	}

	if let Some((usage, capacity)) = reading(&mut failed, "row_cache", self.row_cache()) {
		sample.row_cache_usage = Some(usage);
		sample.row_cache_capacity = Some(capacity);
	}

	let cfs = self.column_handles();
	let mut sum = |name, property| reading(&mut failed, name, self.sum_property(&cfs, property));
	sample.memtable_bytes = sum("memtable_bytes", MEMTABLES);
	sample.pending_compaction_bytes = sum("pending_compaction_bytes", PENDING_COMPACTION);
	sample.sst_bytes = sum("sst_bytes", SST_SIZE);

	let mut one =
		|name, property| reading(&mut failed, name, self.global_property(&cfs, property));
	sample.running_compactions = one("running_compactions", RUNNING_COMPACTIONS);
	sample.running_flushes = one("running_flushes", RUNNING_FLUSHES);
	sample.write_stopped = one("write_stopped", WRITE_STOPPED);
	sample.delayed_write_rate_bytes = one("delayed_write_rate_bytes", DELAYED_WRITE_RATE);

	sample.wal_bytes = reading(&mut failed, "wal_bytes", self.wal_bytes());

	MetricsSnapshot { sample, failed }
}

#[implement(Engine)]
fn block_caches(&self) -> Result<Vec<CacheSample>> {
	let pools = self.ctx.col_cache.lock()?;

	pools
		.iter()
		.map(|(&pool, entry)| {
			Ok(CacheSample {
				pool,
				usage: u64::try_from(entry.cache.get_usage())?,
				pinned: u64::try_from(entry.cache.get_pinned_usage())?,
				capacity: u64::try_from(entry.cache.get_capacity())?,
			})
		})
		.collect()
}

#[implement(Engine)]
fn row_cache(&self) -> Result<(u64, u64)> {
	let cache = self.ctx.row_cache.lock()?;

	Ok((u64::try_from(cache.get_usage())?, u64::try_from(cache.get_capacity())?))
}

#[implement(Engine)]
fn column_handles(&self) -> Vec<Arc<BoundColumnFamily<'_>>> {
	self.db
		.cf_names()
		.iter()
		.filter_map(|name| self.db.cf_handle(name))
		.collect()
}

#[implement(Engine)]
fn sum_property(&self, cfs: &[Arc<BoundColumnFamily<'_>>], name: &CStr) -> Result<u64> {
	cfs.iter().try_fold(0_u64, |total, cf| {
		self.property_integer(cf, name)
			.map(|value| total.saturating_add(value))
	})
}

#[implement(Engine)]
fn global_property(&self, cfs: &[Arc<BoundColumnFamily<'_>>], name: &CStr) -> Result<u64> {
	let Some(cf) = cfs.first() else {
		return Err!("The database has no column families to read {name:?} from.");
	};

	self.property_integer(cf, name)
}

/// Counts the live SST files of the database.
///
/// Blocking: RocksDB builds the list of live files under the database mutex,
/// which is why this is not part of [`Self::metrics_snapshot`] and is meant for
/// a slow timer. Call it from a thread that may block.
#[implement(Engine)]
pub fn sst_file_count(&self) -> Result<u64> {
	let files = self.db.live_files().map_err(map_err)?;

	Ok(u64::try_from(files.len())?)
}

#[implement(Engine)]
fn wal_bytes(&self) -> Result<u64> { wal_dir_bytes(&self.ctx.server.config.database_path) }

fn wal_dir_bytes(path: &Path) -> Result<u64> {
	read_dir(path)?.try_fold(0_u64, |total, entry| {
		let entry = entry?;
		if !is_wal_name(&entry.file_name()) {
			return Ok(total);
		}

		match entry.metadata() {
			| Ok(metadata) => Ok(total.saturating_add(metadata.len())),
			| Err(e) if e.kind() == ErrorKind::NotFound => Ok(total),
			| Err(e) => Err(e.into()),
		}
	})
}

pub(super) fn is_wal_name(name: &OsStr) -> bool {
	name.to_str()
		.and_then(|name| name.strip_suffix(".log"))
		.is_some_and(|stem| !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit()))
}

fn reading<T>(
	failed: &mut Vec<(&'static str, Error)>,
	name: &'static str,
	result: Result<T>,
) -> Option<T> {
	match result {
		| Ok(value) => Some(value),
		| Err(e) => {
			failed.push((name, e));
			None
		},
	}
}
