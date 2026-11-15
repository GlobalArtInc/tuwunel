//! Background collector of the Prometheus metrics.
//!
//! Scraping only reads atomics. Everything that needs a syscall or a walk
//! over the database happens here, on a timer: cheap gauges every
//! `metrics.collect_interval` seconds and counting scans every
//! `metrics.count_interval` seconds. The worker ends at once when metrics are
//! off, and a failed reading or scan is logged and leaves the previous value.

mod collect;
mod count;
mod process;
#[cfg(test)]
mod tests;

use std::{
	sync::{Arc, atomic::AtomicBool},
	time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio::time::sleep;
use tuwunel_core::{Result, Server, debug};
use tuwunel_database::{Database, Map};

use crate::{OnceServices, service::make_name};

/// Collects process, runtime, database and count gauges into the registry.
pub struct Service {
	server: Arc<Server>,
	services: Arc<OnceServices>,
	db: Arc<Database>,
	userid_password: Arc<Map>,
	roomid_shortroomid: Arc<Map>,
	userdeviceid_metadata: Arc<Map>,
	userdeviceconnid_conn: Arc<Map>,
	process_failed: AtomicBool,
	rocksdb_failed: AtomicBool,
}

#[async_trait]
impl crate::Service for Service {
	fn build(args: &crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			server: args.server.clone(),
			services: args.services.clone(),
			db: args.db.clone(),
			userid_password: args.db["userid_password"].clone(),
			roomid_shortroomid: args.db["roomid_shortroomid"].clone(),
			userdeviceid_metadata: args.db["userdeviceid_metadata"].clone(),
			userdeviceconnid_conn: args.db["userdeviceconnid_conn"].clone(),
			process_failed: AtomicBool::new(false),
			rocksdb_failed: AtomicBool::new(false),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		if !self.server.config.metrics.enable || self.server.metrics.prometheus().is_none() {
			debug!("Metrics are off; the collector does not run");
			return Ok(());
		}

		let mut counted: Option<Instant> = None;
		while self.server.is_running() {
			self.collect_once().await;

			let interval = self.server.config.metrics.count_interval;
			let due = counted.is_none_or(|at| at.elapsed() >= Duration::from_secs(interval));
			if interval > 0 && due {
				self.count_once().await;
				counted = Some(Instant::now());
			}

			let pause = self.server.config.metrics.collect_interval.max(1);

			tokio::select! {
				() = sleep(Duration::from_secs(pause)) => {},
				() = self.server.until_shutdown() => break,
			}
		}

		Ok(())
	}

	fn name(&self) -> &str { make_name(std::module_path!()) }
}
