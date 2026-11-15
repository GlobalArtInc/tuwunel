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

	/// Stores the number of threads the pool started and the number of
	/// requests waiting for a worker.
	pub fn record(&self, workers: u64, queued: u64) {
		self.workers.set_unsigned(workers);
		self.queued.set_unsigned(queued);
	}

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_db_pool_workers",
			"Threads started by the database read pool and not yet joined; not a count of busy \
			 or live threads.",
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
