use std::{
	sync::{Arc, atomic::Ordering},
	time::UNIX_EPOCH,
};

use tokio::{runtime::RuntimeMetrics, task::spawn_blocking};
use tuwunel_core::{
	error, implement,
	metrics::prom::{Registry, RuntimeSample},
	warn,
};

use super::{Service, process};

/// Refreshes the cheap gauges of the process, the runtime, the database pool,
/// the timeline cache and the RocksDB engine once.
///
/// Does nothing while the registry is off. A reading that cannot be taken is
/// logged and its gauge keeps the previous value.
#[implement(Service)]
pub async fn collect_once(&self) {
	let Some(registry) = self.server.metrics.prometheus() else {
		return;
	};

	self.collect_process(registry).await;
	self.collect_rocksdb(registry).await;

	if let Some(runtime) = self.server.metrics.runtime_metrics() {
		registry.runtime.record(&runtime_sample(runtime));
	}

	let pool = self.db.pool_stats();
	registry
		.pool
		.record(to_u64(pool.workers), to_u64(pool.queued));

	registry
		.inventory
		.set_last_normal_entries(to_u64(self.services.timeline.last_normal_len()));
}

#[implement(Service)]
async fn collect_process(&self, registry: &Registry) {
	let sampled = match spawn_blocking(process::sample).await {
		| Ok(sampled) => sampled,
		| Err(e) => {
			error!("The process sampling task failed: {e}");
			return;
		},
	};

	if !sampled.failed.is_empty() && !self.process_failed.swap(true, Ordering::Relaxed) {
		warn!(
			sources = ?sampled.failed,
			"Some process readings are unavailable; their gauges keep the last value or stay absent"
		);
	}

	let mut sample = sampled.sample;
	sample.start_time_seconds = self
		.server
		.started
		.duration_since(UNIX_EPOCH)
		.ok()
		.map(|since| since.as_secs());

	registry.process.record(&sample);
}

#[implement(Service)]
async fn collect_rocksdb(&self, registry: &Registry) {
	let db = Arc::clone(&self.db);
	let snapshot = match spawn_blocking(move || db.metrics_snapshot()).await {
		| Ok(snapshot) => snapshot,
		| Err(e) => {
			error!("The RocksDB sampling task failed: {e}");
			return;
		},
	};

	if !snapshot.failed.is_empty() && !self.rocksdb_failed.swap(true, Ordering::Relaxed) {
		warn!(
			readings = ?snapshot.failed,
			"Some RocksDB readings are unavailable; their gauges keep the last value or stay absent"
		);
	}

	registry.rocksdb.record(&snapshot.sample);
}

fn runtime_sample(runtime: &RuntimeMetrics) -> RuntimeSample {
	let workers = runtime.num_workers();

	RuntimeSample {
		workers: to_u64(workers),
		alive_tasks: to_u64(runtime.num_alive_tasks()),
		global_queue_depth: to_u64(runtime.global_queue_depth()),
		busy_nanos: busy_nanos(runtime, workers),
		parks: parks(runtime, workers),
	}
}

#[cfg(target_has_atomic = "64")]
fn busy_nanos(runtime: &RuntimeMetrics, workers: usize) -> u64 {
	(0..workers)
		.map(|worker| runtime.worker_total_busy_duration(worker))
		.map(|busy| u64::try_from(busy.as_nanos()).unwrap_or(u64::MAX))
		.fold(0, u64::saturating_add)
}

#[cfg(not(target_has_atomic = "64"))]
fn busy_nanos(_: &RuntimeMetrics, _: usize) -> u64 { 0 }

#[cfg(target_has_atomic = "64")]
fn parks(runtime: &RuntimeMetrics, workers: usize) -> u64 {
	(0..workers)
		.map(|worker| runtime.worker_park_count(worker))
		.fold(0, u64::saturating_add)
}

#[cfg(not(target_has_atomic = "64"))]
fn parks(_: &RuntimeMetrics, _: usize) -> u64 { 0 }

fn to_u64(value: usize) -> u64 { u64::try_from(value).unwrap_or(u64::MAX) }
