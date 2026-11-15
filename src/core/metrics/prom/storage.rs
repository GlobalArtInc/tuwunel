use std::time::Duration;

use super::{
	family::{DynFamily, EnumFamily},
	types::{Counter, Histogram, Metric},
	write::Writer,
};
use crate::label_set;

/// Ceiling on the providers that get a series of their own.
pub const PROVIDER_CAP: usize = 16;

static SECONDS: [u64; 13] = [
	5_000_000,
	10_000_000,
	25_000_000,
	50_000_000,
	100_000_000,
	250_000_000,
	500_000_000,
	1_000_000_000,
	2_500_000_000,
	5_000_000_000,
	10_000_000_000,
	30_000_000_000,
	60_000_000_000,
];

label_set! {
	/// An operation of a storage provider.
	pub enum Op("op") {
		/// Open an object for reading.
		Get => "get",
		/// Store an object.
		Put => "put",
		/// Delete an object.
		Delete => "delete",
		/// Read the metadata of an object.
		Head => "head",
		/// Copy an object.
		Copy => "copy",
		/// Rename an object.
		Rename => "rename",
	}
}

label_set! {
	/// How an operation ended.
	pub enum StoreResult("result") {
		/// It succeeded.
		Ok => "ok",
		/// It failed.
		Error => "error",
	}
}

/// Metrics of object-storage providers.
///
/// Series are labelled by the name of the provider as configured, capped like
/// any open label; providers beyond the cap share the `_other` series.
pub struct Storage {
	operations: DynFamily<EnumFamily<(Op, StoreResult), Counter>>,
	duration: DynFamily<EnumFamily<Op, Histogram<13>>>,
}

impl Storage {
	#[must_use]
	/// Creates the group with every metric at zero.
	pub fn new() -> Self {
		Self {
			operations: DynFamily::new(&["provider"], PROVIDER_CAP, || {
				EnumFamily::new(Counter::new)
			}),
			duration: DynFamily::new(&["provider"], PROVIDER_CAP, || {
				EnumFamily::new(|| Histogram::seconds(&SECONDS))
			}),
		}
	}

	/// Counts an operation without timing it, for one that is a step of a
	/// stream.
	pub fn counted(&self, provider: &str, op: Op, ok: bool) {
		let result = if ok { StoreResult::Ok } else { StoreResult::Error };

		self.operations
			.get(&[provider])
			.get((op, result))
			.inc();
	}

	/// Counts an operation and records how long it took.
	pub fn timed(&self, provider: &str, op: Op, ok: bool, elapsed: Duration) {
		self.counted(provider, op, ok);
		self.duration
			.get(&[provider])
			.get(op)
			.observe_duration(elapsed);
	}

	#[must_use]
	/// Returns the number of operations of a provider that ended as given.
	pub fn operations(&self, provider: &str, op: Op, ok: bool) -> u64 {
		let result = if ok { StoreResult::Ok } else { StoreResult::Error };

		self.operations
			.get(&[provider])
			.get((op, result))
			.get()
	}

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_storage_operations_total",
			"Operations of a storage provider, by operation and outcome.",
			&self.operations,
		);
		w.metric(
			"tuwunel_storage_operation_seconds",
			"Time of a storage provider operation. A read is timed until the object is opened, \
			 and deletes are counted but not timed.",
			&self.duration,
		);
	}

	pub(super) fn series(&self) -> usize {
		self.operations
			.series()
			.saturating_add(self.duration.series())
	}
}

impl Default for Storage {
	fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for Storage {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Storage").finish_non_exhaustive()
	}
}
