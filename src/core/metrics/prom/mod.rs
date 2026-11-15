//! Prometheus registry on relaxed atomics.
//!
//! The registry is a static structure of named groups, one file per group.
//! Metrics are counters, gauges and histograms with fixed buckets; labels come
//! from closed enums or from open sets with a ceiling on the number of series.
//! Reading takes no locks beyond short read locks on open families, and the
//! text format is Prometheus 0.0.4.

mod appservice;
mod family;
mod http;
mod inventory;
mod meta;
mod pool;
mod process;
mod rocksdb;
mod runtime;
mod storage;
mod sync;
#[cfg(test)]
mod tests;
mod types;
mod write;

pub use self::{
	appservice::{APPSERVICE_CAP, Appservice, SendResult},
	family::{DynFamily, EnumFamily, LabelSet, MAX_KEY_LEN, OTHER},
	http::{Api, Http, InFlight, Method, ROUTE_CAP, StatusClass, UNMATCHED},
	inventory::Inventory,
	meta::Meta,
	pool::Pool,
	process::{Process, ProcessSample},
	rocksdb::{CacheSample, Condition, POOL_CAP, RocksSample, Rocksdb},
	runtime::{Runtime, RuntimeSample},
	storage::{Op, PROVIDER_CAP, Storage, StoreResult},
	sync::{Fastpath, LongPoll, Pass, Reply, SinceKind, SyncMetrics},
	types::{Counter, Gauge, Histogram, Metric, Seen, Timer},
	write::{Kind, Le, Scale, Value, Writer, push_label},
};

/// Content type of the text exposition.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// All metric groups of the server.
///
/// Names, buckets and rendering of every metric are defined under this module
/// and nowhere else; the rest of the code only calls methods of the groups.
#[derive(Debug, Default)]
pub struct Registry {
	/// Build information and registry bookkeeping.
	pub meta: Meta,

	/// Requests served over HTTP.
	pub http: Http,

	/// The server process.
	pub process: Process,

	/// The async runtime.
	pub runtime: Runtime,

	/// The database worker pool.
	pub pool: Pool,

	/// Cache sizes and record counts.
	pub inventory: Inventory,

	/// The RocksDB engine.
	pub rocksdb: Rocksdb,

	/// Delivery to appservices.
	pub appservice: Appservice,

	/// Object-storage providers.
	pub storage: Storage,

	/// The sync endpoints.
	pub sync: SyncMetrics,
}

impl Registry {
	#[must_use]
	/// Creates a registry with every metric at zero.
	pub fn new() -> Self { Self::default() }

	#[must_use]
	/// Renders every metric in the Prometheus text format.
	pub fn render(&self) -> String {
		let mut w = Writer::new();
		Meta::write(&mut w);
		self.http.write(&mut w);
		self.process.write(&mut w);
		self.runtime.write(&mut w);
		self.pool.write(&mut w);
		self.inventory.write(&mut w);
		self.rocksdb.write(&mut w);
		self.appservice.write(&mut w);
		self.storage.write(&mut w);
		self.sync.write(&mut w);

		w.finish()
	}

	#[must_use]
	/// Upper bound on the number of series [`Self::render`] can produce.
	///
	/// Closed families count every value of their label set, open families
	/// count their ceiling plus `_other`, and a histogram counts its buckets,
	/// `+Inf`, sum and count.
	pub fn max_series(&self) -> usize {
		[
			Meta::series(),
			self.http.series(),
			self.process.series(),
			self.runtime.series(),
			self.pool.series(),
			self.inventory.series(),
			self.rocksdb.series(),
			self.appservice.series(),
			self.storage.series(),
			self.sync.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}
