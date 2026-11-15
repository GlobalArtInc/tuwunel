use std::{sync::Arc, time::Duration};

use futures::{StreamExt, TryFutureExt, TryStreamExt, future::ready};
use tokio::time::timeout;
use tuwunel_core::{Result, err, implement, warn};
use tuwunel_database::Map;

use super::Service;

const SCAN_TIMEOUT: Duration = Duration::from_mins(1);

const QUEUE_CAP: usize = 100_000;

/// Counts users, rooms and devices by walking their keys once, and the events
/// queued for each appservice up to a ceiling.
///
/// Each count is independent. A scan that fails or runs past a minute is
/// logged and its gauge keeps the previous value. The walk holds no lock of
/// the main paths; it reads through a database iterator like any other scan.
#[implement(Service)]
pub async fn count_once(&self) {
	let Some(registry) = self.server.metrics.prometheus() else {
		return;
	};

	let inventory = &registry.inventory;

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

	for id in ids {
		let queued = sending
			.queued_for(&id, QUEUE_CAP)
			.map_ok(|events| u64::try_from(events).unwrap_or(u64::MAX));

		if let Some(events) = guarded("appservice queue", queued).await {
			registry.appservice.set_queued(&id, events);
		}
	}
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
	map.raw_keys()
		.map_ok(|_| 1_u64)
		.try_fold(0_u64, |total, one| ready(Ok(total.saturating_add(one))))
		.await
}
