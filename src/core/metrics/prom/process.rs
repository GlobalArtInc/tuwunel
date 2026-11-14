//! Metrics of the server process.
//!
//! The names follow the conventional `process_*` family so that stock
//! dashboards work. A collector fills them in from the operating system; a
//! reading the platform cannot give is left out of the exposition.

use super::{
	types::{Counter, Gauge, Metric, Seen},
	write::{Scale, Writer},
};

/// One sampling of the process. A field that is `None` was unavailable and
/// leaves its metric as it was.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProcessSample {
	/// Resident memory in bytes.
	pub resident_bytes: Option<u64>,

	/// User and system CPU time together, in microseconds.
	pub cpu_micros: Option<u64>,

	/// Number of threads.
	pub threads: Option<u64>,

	/// Number of open file descriptors.
	pub open_fds: Option<u64>,

	/// Soft limit on open file descriptors.
	pub max_fds: Option<u64>,

	/// Start of the process in seconds since the Unix epoch.
	pub start_time_seconds: Option<u64>,
}

/// Metrics of the server process.
#[derive(Debug)]
pub struct Process {
	resident: Seen<Gauge>,
	cpu: Seen<Counter>,
	threads: Seen<Gauge>,
	open_fds: Seen<Gauge>,
	max_fds: Seen<Gauge>,
	start_time: Seen<Gauge>,
}

impl Process {
	#[must_use]
	/// Creates the group with nothing sampled yet.
	pub fn new() -> Self {
		Self {
			resident: Seen::default(),
			cpu: Seen::new(Counter::scaled(Scale::Micro)),
			threads: Seen::default(),
			open_fds: Seen::default(),
			max_fds: Seen::default(),
			start_time: Seen::default(),
		}
	}

	/// Stores the readings of a sample.
	pub fn record(&self, sample: &ProcessSample) {
		let gauges = [
			(&self.resident, sample.resident_bytes),
			(&self.threads, sample.threads),
			(&self.open_fds, sample.open_fds),
			(&self.max_fds, sample.max_fds),
			(&self.start_time, sample.start_time_seconds),
		];

		for (gauge, value) in gauges {
			if let Some(value) = value {
				gauge.touch().set_unsigned(value);
			}
		}

		if let Some(micros) = sample.cpu_micros {
			self.cpu.touch().raise_to(micros);
		}
	}

	pub(super) fn write(&self, w: &mut Writer) {
		if self.resident.is_seen() {
			w.metric(
				"process_resident_memory_bytes",
				"Resident memory size in bytes.",
				&self.resident,
			);
		}

		if self.cpu.is_seen() {
			w.metric(
				"process_cpu_seconds_total",
				"Total user and system CPU time spent in seconds.",
				&self.cpu,
			);
		}

		if self.threads.is_seen() {
			w.metric("process_threads", "Number of OS threads in the process.", &self.threads);
		}

		if self.open_fds.is_seen() {
			w.metric("process_open_fds", "Number of open file descriptors.", &self.open_fds);
		}

		if self.max_fds.is_seen() {
			w.metric("process_max_fds", "Soft limit on open file descriptors.", &self.max_fds);
		}

		if self.start_time.is_seen() {
			w.metric(
				"process_start_time_seconds",
				"Start time of the process since the Unix epoch in seconds.",
				&self.start_time,
			);
		}
	}

	pub(super) fn series(&self) -> usize {
		[
			self.resident.series(),
			self.cpu.series(),
			self.threads.series(),
			self.open_fds.series(),
			self.max_fds.series(),
			self.start_time.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}

impl Default for Process {
	fn default() -> Self { Self::new() }
}
