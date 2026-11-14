use std::time::Duration;

use tokio::time::timeout;
use tuwunel_core::{Result, config::Figment};

use super::process::{parse_stat, parse_statm};
use crate::{Service as _, test_utils::fixture};

fn sample_of(text: &str, name: &str) -> Option<f64> {
	text.lines()
		.find_map(|line| line.strip_prefix(name)?.strip_prefix(' '))
		.and_then(|value| value.parse().ok())
}

#[test]
fn proc_files_are_parsed_past_a_command_name_with_spaces() {
	let stat = "42 (tuwunel (worker) x) S 1 42 42 0 -1 4194560 100 0 0 0 7 5 0 0 20 0 9 0 12345 \
	            1000 100 18446744073709551615";

	assert_eq!(parse_stat(stat), Some((12, 9)));
	assert_eq!(parse_stat("42 (short) S 1"), None);
	assert_eq!(parse_statm("500 300 20 1 0 80 0"), Some(300));
	assert_eq!(parse_statm("500"), None);
}

#[tokio::test]
async fn one_collection_fills_the_gauges_and_one_count_the_totals() -> Result {
	let Some(fixture) = fixture(Figment::new().merge(("metrics.enable", true))).await? else {
		return Ok(());
	};

	let services = &fixture.services;
	let registry = services.server.metrics.enable_prometheus();

	services.metrics.collect_once().await;

	let text = registry.render();

	for family in [
		"tuwunel_tokio_workers",
		"tuwunel_tokio_alive_tasks",
		"tuwunel_tokio_global_queue_depth",
		"tuwunel_tokio_busy_seconds_total",
		"tuwunel_tokio_parks_total",
		"tuwunel_db_pool_workers",
		"tuwunel_db_pool_queued",
		"tuwunel_timeline_last_normal_entries",
	] {
		assert!(sample_of(&text, family).is_some(), "{family}\n{text}");
	}

	assert!(!text.contains("tuwunel_users_registered"), "{text}");
	assert!(sample_of(&text, "tuwunel_tokio_workers") > Some(0.0));
	assert!(sample_of(&text, "tuwunel_db_pool_workers") > Some(0.0));

	#[cfg(target_os = "linux")]
	for family in [
		"process_resident_memory_bytes",
		"process_cpu_seconds_total",
		"process_threads",
		"process_open_fds",
		"process_max_fds",
		"process_start_time_seconds",
	] {
		assert!(sample_of(&text, family) > Some(0.0), "{family}\n{text}");
	}

	services.metrics.count_once().await;

	let text = registry.render();

	for family in ["tuwunel_users_registered", "tuwunel_rooms_known", "tuwunel_devices"] {
		assert!(sample_of(&text, family).is_some(), "{family}\n{text}");
	}

	Ok(())
}

#[tokio::test]
async fn a_disabled_collector_returns_at_once_and_creates_nothing() -> Result {
	let Some(fixture) = fixture(Figment::new()).await? else {
		return Ok(());
	};

	let services = &fixture.services;
	let worker = services.metrics.clone().worker();

	timeout(Duration::from_secs(5), worker)
		.await
		.expect("the worker returns without waiting")?;

	services.metrics.collect_once().await;
	services.metrics.count_once().await;

	assert!(services.server.metrics.prometheus().is_none());

	Ok(())
}
