//! Metrics of the database worker pool.

use super::{
	types::{Gauge, Metric},
	write::Writer,
};

/// Metrics of the database worker pool.
#[derive(Debug, Default)]
pub struct Pool {
	workers: Gauge,
	queued: Gauge,
}

impl Pool {
	#[must_use]
	/// Creates the group with every metric at zero.
	pub fn new() -> Self { Self::default() }

	/// Stores the size of the pool and the number of requests waiting for a
	/// worker.
	pub fn record(&self, workers: u64, queued: u64) {
		self.workers.set_unsigned(workers);
		self.queued.set_unsigned(queued);
	}

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_db_pool_workers",
			"Number of threads in the database read pool.",
			&self.workers,
		);
		w.metric(
			"tuwunel_db_pool_queued",
			"Database read requests waiting for a pool worker.",
			&self.queued,
		);
	}

	pub(super) fn series(&self) -> usize {
		self.workers
			.series()
			.saturating_add(self.queued.series())
	}
}
