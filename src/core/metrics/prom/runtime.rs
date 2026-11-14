//! Metrics of the async runtime.
//!
//! Only what tokio exposes without `tokio_unstable` is covered. A collector
//! samples the runtime and stores the readings here.

use super::{
	types::{Counter, Gauge, Metric},
	write::{Scale, Writer},
};

/// One sampling of the runtime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeSample {
	/// Number of worker threads.
	pub workers: u64,

	/// Number of tasks that are alive.
	pub alive_tasks: u64,

	/// Number of tasks in the global queue.
	pub global_queue_depth: u64,

	/// Time the workers have spent busy, summed over workers, in nanoseconds.
	pub busy_nanos: u64,

	/// Number of times the workers have parked, summed over workers.
	pub parks: u64,
}

/// Metrics of the async runtime.
#[derive(Debug)]
pub struct Runtime {
	workers: Gauge,
	alive_tasks: Gauge,
	global_queue_depth: Gauge,
	busy: Counter,
	parks: Counter,
}

impl Runtime {
	#[must_use]
	/// Creates the group with every metric at zero.
	pub fn new() -> Self {
		Self {
			workers: Gauge::new(),
			alive_tasks: Gauge::new(),
			global_queue_depth: Gauge::new(),
			busy: Counter::scaled(Scale::Nano),
			parks: Counter::new(),
		}
	}

	/// Stores the readings of a sample.
	pub fn record(&self, sample: &RuntimeSample) {
		self.workers.set_unsigned(sample.workers);
		self.alive_tasks.set_unsigned(sample.alive_tasks);
		self.global_queue_depth
			.set_unsigned(sample.global_queue_depth);
		self.busy.raise_to(sample.busy_nanos);
		self.parks.raise_to(sample.parks);
	}

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric("tuwunel_tokio_workers", "Number of tokio worker threads.", &self.workers);
		w.metric(
			"tuwunel_tokio_alive_tasks",
			"Number of tasks alive in the tokio runtime.",
			&self.alive_tasks,
		);
		w.metric(
			"tuwunel_tokio_global_queue_depth",
			"Number of tasks waiting in the global queue of the tokio runtime.",
			&self.global_queue_depth,
		);
		w.metric(
			"tuwunel_tokio_busy_seconds_total",
			"Time tokio workers spent running tasks, summed over workers.",
			&self.busy,
		);
		w.metric(
			"tuwunel_tokio_parks_total",
			"Number of times tokio workers parked, summed over workers.",
			&self.parks,
		);
	}

	pub(super) fn series(&self) -> usize {
		[
			self.workers.series(),
			self.alive_tasks.series(),
			self.global_queue_depth.series(),
			self.busy.series(),
			self.parks.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}

impl Default for Runtime {
	fn default() -> Self { Self::new() }
}
