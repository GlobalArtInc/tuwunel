//! Metrics of the RocksDB engine.
//!
//! Events of the engine are counted as they happen. Sizes and occupancy are
//! read from RocksDB by a collector and stored here as a [`RocksSample`]; a
//! reading that could not be taken is left out of the sample and the metric
//! keeps its previous value, or stays out of the exposition until the first
//! reading. Properties are summed over all column families, so no series
//! carries the name of a column family.

use super::{
	family::{DynFamily, EnumFamily},
	types::{Counter, Gauge, Metric, Seen},
	write::Writer,
};
use crate::label_set;

/// Ceiling on the block cache pools that get a series of their own.
pub const POOL_CAP: usize = 256;

label_set! {
	/// Write stall condition of a column family after a change.
	pub enum Condition("condition") {
		/// Writes proceed at full speed.
		Normal => "normal",
		/// Writes are slowed down.
		Delayed => "delayed",
		/// Writes are stopped.
		Stopped => "stopped",
	}
}

/// Occupancy of one block cache pool.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheSample {
	/// Name of the pool.
	pub pool: &'static str,

	/// Bytes held by the cache.
	pub usage: u64,

	/// Bytes of the cache pinned by readers.
	pub pinned: u64,

	/// Configured capacity in bytes.
	pub capacity: u64,
}

/// One reading of the database. A field that is `None` was unavailable and
/// leaves its metric as it was.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RocksSample {
	/// Block cache pools.
	pub block_caches: Vec<CacheSample>,

	/// Bytes held by the row cache.
	pub row_cache_usage: Option<u64>,

	/// Capacity of the row cache in bytes.
	pub row_cache_capacity: Option<u64>,

	/// Bytes held by memtables of all column families.
	pub memtable_bytes: Option<u64>,

	/// Bytes compaction has yet to rewrite, estimated, over all column
	/// families.
	pub pending_compaction_bytes: Option<u64>,

	/// Compactions running.
	pub running_compactions: Option<u64>,

	/// Flushes running.
	pub running_flushes: Option<u64>,

	/// Whether writes are stopped, as 0 or 1.
	pub write_stopped: Option<u64>,

	/// Rate limit applied to delayed writes in bytes per second, 0 when writes
	/// are not delayed.
	pub delayed_write_rate_bytes: Option<u64>,

	/// Bytes of the SST files of all column families.
	pub sst_bytes: Option<u64>,

	/// Number of live SST files.
	pub sst_files: Option<u64>,

	/// Bytes of the write-ahead log files.
	pub wal_bytes: Option<u64>,
}

/// Metrics of the RocksDB engine.
pub struct Rocksdb {
	block_cache_usage: DynFamily<Gauge>,
	block_cache_pinned: DynFamily<Gauge>,
	block_cache_capacity: DynFamily<Gauge>,
	row_cache_usage: Seen<Gauge>,
	row_cache_capacity: Seen<Gauge>,
	memtable: Seen<Gauge>,
	pending_compaction: Seen<Gauge>,
	running_compactions: Seen<Gauge>,
	running_flushes: Seen<Gauge>,
	write_stopped: Seen<Gauge>,
	delayed_write_rate: Seen<Gauge>,
	sst_bytes: Seen<Gauge>,
	sst_files: Seen<Gauge>,
	wal_bytes: Seen<Gauge>,
	compactions: Counter,
	flushes: Counter,
	background_errors: Counter,
	stalls: EnumFamily<Condition, Counter>,
}

impl Rocksdb {
	#[must_use]
	/// Creates the group with every metric at zero and no reading taken.
	pub fn new() -> Self {
		Self {
			block_cache_usage: DynFamily::new(&["pool"], POOL_CAP, Gauge::new),
			block_cache_pinned: DynFamily::new(&["pool"], POOL_CAP, Gauge::new),
			block_cache_capacity: DynFamily::new(&["pool"], POOL_CAP, Gauge::new),
			row_cache_usage: Seen::default(),
			row_cache_capacity: Seen::default(),
			memtable: Seen::default(),
			pending_compaction: Seen::default(),
			running_compactions: Seen::default(),
			running_flushes: Seen::default(),
			write_stopped: Seen::default(),
			delayed_write_rate: Seen::default(),
			sst_bytes: Seen::default(),
			sst_files: Seen::default(),
			wal_bytes: Seen::default(),
			compactions: Counter::new(),
			flushes: Counter::new(),
			background_errors: Counter::new(),
			stalls: EnumFamily::new(Counter::new),
		}
	}

	/// Stores the readings of a sample.
	pub fn record(&self, sample: &RocksSample) {
		for cache in &sample.block_caches {
			let key = [cache.pool];
			self.block_cache_usage
				.get(&key)
				.set_unsigned(cache.usage);
			self.block_cache_pinned
				.get(&key)
				.set_unsigned(cache.pinned);
			self.block_cache_capacity
				.get(&key)
				.set_unsigned(cache.capacity);
		}

		let gauges = [
			(&self.row_cache_usage, sample.row_cache_usage),
			(&self.row_cache_capacity, sample.row_cache_capacity),
			(&self.memtable, sample.memtable_bytes),
			(&self.pending_compaction, sample.pending_compaction_bytes),
			(&self.running_compactions, sample.running_compactions),
			(&self.running_flushes, sample.running_flushes),
			(&self.write_stopped, sample.write_stopped),
			(&self.delayed_write_rate, sample.delayed_write_rate_bytes),
			(&self.sst_bytes, sample.sst_bytes),
			(&self.sst_files, sample.sst_files),
			(&self.wal_bytes, sample.wal_bytes),
		];

		for (gauge, value) in gauges {
			if let Some(value) = value {
				gauge.touch().set_unsigned(value);
			}
		}
	}

	#[inline]
	/// Counts a finished compaction.
	pub fn compaction_completed(&self) { self.compactions.inc(); }

	#[inline]
	/// Counts a finished flush of a memtable.
	pub fn flush_completed(&self) { self.flushes.inc(); }

	#[inline]
	/// Counts a background error reported by the engine.
	pub fn background_error(&self) { self.background_errors.inc(); }

	#[inline]
	/// Counts a change of the write stall condition, under the new condition.
	pub fn stall_changed(&self, condition: Condition) { self.stalls.get(condition).inc(); }

	#[must_use]
	/// Returns the number of finished compactions.
	pub fn compactions(&self) -> u64 { self.compactions.get() }

	#[must_use]
	/// Returns the number of finished flushes.
	pub fn flushes(&self) -> u64 { self.flushes.get() }

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_rocksdb_block_cache_usage_bytes",
			"Bytes held by a block cache pool.",
			&self.block_cache_usage,
		);
		w.metric(
			"tuwunel_rocksdb_block_cache_pinned_bytes",
			"Bytes of a block cache pool pinned by readers.",
			&self.block_cache_pinned,
		);
		w.metric(
			"tuwunel_rocksdb_block_cache_capacity_bytes",
			"Configured capacity of a block cache pool.",
			&self.block_cache_capacity,
		);

		let gauges = [
			(
				"tuwunel_rocksdb_row_cache_usage_bytes",
				"Bytes held by the row cache.",
				&self.row_cache_usage,
			),
			(
				"tuwunel_rocksdb_row_cache_capacity_bytes",
				"Configured capacity of the row cache.",
				&self.row_cache_capacity,
			),
			(
				"tuwunel_rocksdb_memtable_bytes",
				"Bytes held by memtables, summed over column families.",
				&self.memtable,
			),
			(
				"tuwunel_rocksdb_pending_compaction_bytes",
				"Estimated bytes compaction has yet to rewrite, summed over column families.",
				&self.pending_compaction,
			),
			(
				"tuwunel_rocksdb_running_compactions",
				"Compactions running.",
				&self.running_compactions,
			),
			(
				"tuwunel_rocksdb_running_flushes",
				"Memtable flushes running.",
				&self.running_flushes,
			),
			(
				"tuwunel_rocksdb_write_stopped",
				"1 while the engine stops writes, otherwise 0.",
				&self.write_stopped,
			),
			(
				"tuwunel_rocksdb_delayed_write_rate_bytes",
				"Rate limit on delayed writes in bytes per second; 0 when writes are not \
				 delayed.",
				&self.delayed_write_rate,
			),
			(
				"tuwunel_rocksdb_sst_bytes",
				"Bytes of SST files, summed over column families.",
				&self.sst_bytes,
			),
			("tuwunel_rocksdb_sst_files", "Live SST files.", &self.sst_files),
			(
				"tuwunel_rocksdb_wal_bytes",
				"Bytes of write-ahead log files in the database directory.",
				&self.wal_bytes,
			),
		];

		for (name, help, gauge) in gauges {
			if gauge.is_seen() {
				w.metric(name, help, gauge);
			}
		}

		w.metric("tuwunel_rocksdb_compactions_total", "Compactions finished.", &self.compactions);
		w.metric("tuwunel_rocksdb_flushes_total", "Memtable flushes finished.", &self.flushes);
		w.metric(
			"tuwunel_rocksdb_background_errors_total",
			"Background errors reported by the engine.",
			&self.background_errors,
		);
		w.metric(
			"tuwunel_rocksdb_stall_changes_total",
			"Changes of the write stall condition, by the condition entered.",
			&self.stalls,
		);
	}

	pub(super) fn series(&self) -> usize {
		[
			self.block_cache_usage.series(),
			self.block_cache_pinned.series(),
			self.block_cache_capacity.series(),
			self.row_cache_usage.series(),
			self.row_cache_capacity.series(),
			self.memtable.series(),
			self.pending_compaction.series(),
			self.running_compactions.series(),
			self.running_flushes.series(),
			self.write_stopped.series(),
			self.delayed_write_rate.series(),
			self.sst_bytes.series(),
			self.sst_files.series(),
			self.wal_bytes.series(),
			self.compactions.series(),
			self.flushes.series(),
			self.background_errors.series(),
			self.stalls.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}

impl Default for Rocksdb {
	fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for Rocksdb {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Rocksdb").finish_non_exhaustive()
	}
}
