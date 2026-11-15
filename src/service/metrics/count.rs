use std::{
	collections::HashSet,
	pin::pin,
	sync::{Arc, PoisonError},
	time::Duration,
};

use futures::{StreamExt, TryFutureExt, TryStreamExt};
use tokio::{
	task::{spawn_blocking, yield_now},
	time::timeout,
};
use tuwunel_core::{Result, err, implement, warn};
use tuwunel_database::Map;

use super::Service;

const SCAN_TIMEOUT: Duration = Duration::from_mins(1);

const QUEUE_CAP: usize = 100_000;

const YIELD_EVERY: u64 = 1000;

/// Counts users, rooms, devices and stored sliding sync connections by walking
/// their keys once, live SST files, and the events queued for each appservice
/// up to a ceiling.
///
/// An appservice that is no longer registered is set to zero queued events once
/// instead of keeping its last value.
///
/// Each count is independent. A scan that fails or runs past a minute is
/// logged and its gauge keeps the previous value. The walk holds no lock of
/// the main paths. It reads storage without filling the block cache, so it
/// evicts nothing, and it hands the thread back every thousand keys, so a long
/// column neither starves other tasks nor outlives its time limit.
#[implement(Service)]
pub async fn count_once(&self) {
	let Some(registry) = self.server.metrics.prometheus() else {
		return;
	};

	let inventory = &registry.inventory;

	if let Some(files) = guarded("sst files", self.sst_files()).await {
		registry.rocksdb.set_sst_files(files);
	}

	if let Some(users) = scan("users", &self.userid_password).await {
		inventory.set_users(users);
	}

	if let Some(rooms) = scan("rooms", &self.roomid_shortroomid).await {
		inventory.set_rooms(rooms);
	}

	if let Some(devices) = scan("devices", &self.userdeviceid_metadata).await {
		inventory.set_devices(devices);
	}

	if let Some(connections) = scan("sync connections", &self.userdeviceconnid_conn).await {
		registry
			.sync
			.set_v5_connections_stored(connections);
	}

	let sending = &self.services.sending;
	let ids: Vec<String> = self
		.services
		.appservice
		.iter_ids()
		.collect()
		.await;

	let mut queues = Vec::with_capacity(ids.len());
	for id in &ids {
		let queued = sending
			.queued_for(id, QUEUE_CAP)
			.map_ok(|events| u64::try_from(events).unwrap_or(u64::MAX));

		if let Some(events) = guarded("appservice queue", queued).await {
			queues.push((id.as_str(), events));
		}
	}

	let gone = self.unregistered_since_last_scan(&ids);

	queues.extend(gone.iter().map(|id| (id.as_str(), 0)));

	registry.appservice.set_queued(&queues);
}

#[implement(Service)]
pub(super) fn unregistered_since_last_scan(&self, ids: &[String]) -> Vec<String> {
	let current: HashSet<String> = ids.iter().cloned().collect();
	let mut previous = self
		.queued_ids
		.lock()
		.unwrap_or_else(PoisonError::into_inner);

	let gone = previous.difference(&current).cloned().collect();
	*previous = current;

	gone
}

async fn scan(what: &'static str, map: &Arc<Map>) -> Option<u64> {
	guarded(what, count_keys(map)).await
}

async fn guarded(what: &'static str, count: impl Future<Output = Result<u64>>) -> Option<u64> {
	let counted = timeout(SCAN_TIMEOUT, count)
		.await
		.map_err(|_| err!("scan exceeded {SCAN_TIMEOUT:?}"))
		.and_then(|result| result);

	counted
		.inspect_err(|e| warn!(what, "Failed to count for the metrics: {e}"))
		.ok()
}

async fn count_keys(map: &Arc<Map>) -> Result<u64> {
	let mut scan = pin!(map.raw_scan());
	let mut total = 0_u64;

	while scan.try_next().await?.is_some() {
		total = total.saturating_add(1);

		if total.is_multiple_of(YIELD_EVERY) {
			yield_now().await;
		}
	}

	Ok(total)
}

#[implement(Service)]
async fn sst_files(&self) -> Result<u64> {
	let db = Arc::clone(&self.db);

	spawn_blocking(move || db.sst_file_count()).await?
}
