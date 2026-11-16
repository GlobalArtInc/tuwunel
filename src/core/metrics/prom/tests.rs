use std::{collections::BTreeSet, future::pending, sync::Arc, thread, time::Duration};

use futures::FutureExt;
use regex::Regex;

use super::{
	APPSERVICE_CAP, Api, Build, BuildPath, CacheSample, Condition, Counter, DynFamily,
	EnumFamily, Fastpath, Gauge, Histogram, JournalKind, Lookup, MAX_KEY_LEN, Metric, Mismatch,
	OTHER, Op, PROVIDER_CAP, Pass, Phase, Registry, Reply, RocksSample, Scale, Section,
	SelectPath, SelectReason, SendResult, SinceKind, Skipped, V5Path, V5Phase, Verdict,
	WaitStage, WakeReason, WatchPart, Writer,
};
use crate::label_set;

label_set! {
	enum Kind("kind") { Appservice => "appservice", Push => "push", Federation => "federation" }
}

label_set! {
	enum Outcome("result") { Ok => "ok", Error => "error" }
}

static SECONDS: [u64; 3] = [5_000_000, 250_000_000, 1_000_000_000];
static BYTES: [u64; 2] = [1_024, 65_536];

const ALLOWED_LABELS: &[&str] = &[
	"route",
	"method",
	"status",
	"api",
	"since",
	"outcome",
	"fastpath",
	"path",
	"pass",
	"phase",
	"part",
	"section",
	"appservice",
	"kind",
	"how",
	"verdict",
	"reason",
	"stage",
	"result",
	"pool",
	"provider",
	"op",
	"condition",
	"version",
	"le",
	"family",
];

struct Probe {
	total: Counter,
	elapsed: Counter,
	level: Gauge,
	latency: Histogram<3>,
	size: Histogram<2>,
	pairs: EnumFamily<(Kind, Outcome), Counter>,
	routes: DynFamily<Histogram<3>>,
	hits: DynFamily<EnumFamily<Outcome, Counter>>,
}

impl Probe {
	fn new(cap: usize) -> Self {
		Self {
			total: Counter::new(),
			elapsed: Counter::scaled(Scale::Micro),
			level: Gauge::new(),
			latency: Histogram::seconds(&SECONDS),
			size: Histogram::new(&BYTES),
			pairs: EnumFamily::new(Counter::new),
			routes: DynFamily::new(&["route", "method"], cap, || Histogram::seconds(&SECONDS)),
			hits: DynFamily::new(&["appservice"], cap, || EnumFamily::new(Counter::new)),
		}
	}

	fn render(&self) -> String {
		let mut w = Writer::new();
		w.metric("tuwunel_probe_total", "Plain counter.", &self.total);
		w.metric("tuwunel_probe_seconds_total", "Scaled counter.", &self.elapsed);
		w.metric("tuwunel_probe_level", "Gauge with a back\\slash\nand a newline.", &self.level);
		w.metric("tuwunel_probe_latency_seconds", "Latency.", &self.latency);
		w.metric("tuwunel_probe_size_bytes", "Size.", &self.size);
		w.metric("tuwunel_probe_pairs_total", "Closed pair.", &self.pairs);
		w.metric("tuwunel_probe_route_seconds", "Open pair.", &self.routes);
		w.metric("tuwunel_probe_hits_total", "Open then closed.", &self.hits);

		w.finish()
	}

	fn series(&self) -> usize {
		[
			self.total.series(),
			self.elapsed.series(),
			self.level.series(),
			self.latency.series(),
			self.size.series(),
			self.pairs.series(),
			self.routes.series(),
			self.hits.series(),
		]
		.into_iter()
		.sum()
	}
}

#[derive(Default)]
struct Parsed {
	samples: usize,
	label_names: BTreeSet<String>,
}

fn parse(text: &str) -> Parsed {
	let name = "[a-zA-Z_:][a-zA-Z0-9_:]*";
	let help = Regex::new(&format!("^# HELP ({name}) .+$")).unwrap();
	let kind = Regex::new(&format!("^# TYPE ({name}) (counter|gauge|histogram)$")).unwrap();
	let sample =
		Regex::new(&format!(r"^({name})(?:\{{(.*)\}})? (-?[0-9]+(?:\.[0-9]+)?)$")).unwrap();
	let label = Regex::new(r#"([a-z_][a-z0-9_]*)="((?:[^"\\]|\\.)*)""#).unwrap();

	let mut parsed = Parsed::default();
	let mut pending_help: Option<String> = None;
	let mut family: Option<(String, String)> = None;
	let mut buckets: Vec<(String, f64, u64)> = Vec::new();

	let finish = |buckets: &mut Vec<(String, f64, u64)>, count: Option<(String, u64)>| {
		if let Some((group, count)) = count {
			let own: Vec<_> = buckets
				.iter()
				.filter(|(g, ..)| *g == group)
				.collect();
			assert!(!own.is_empty(), "histogram series without buckets: {group}");
			assert!(
				own.windows(2)
					.all(|w| w[0].1 < w[1].1 && w[0].2 <= w[1].2)
			);
			let last = own.last().unwrap();
			assert!(last.1.is_infinite(), "last bucket must be +Inf");
			assert_eq!(last.2, count, "+Inf must equal _count for {group}");
			buckets.retain(|(g, ..)| *g != group);
		}
	};

	for line in text.lines() {
		if let Some(caps) = help.captures(line) {
			assert!(pending_help.is_none(), "HELP without TYPE");
			pending_help = Some(caps[1].to_owned());
		} else if let Some(caps) = kind.captures(line) {
			assert_eq!(pending_help.take().as_deref(), Some(&caps[1]), "TYPE must follow HELP");
			assert!(buckets.is_empty(), "unfinished histogram series");
			family = Some((caps[1].to_owned(), caps[2].to_owned()));
		} else if let Some(caps) = sample.captures(line) {
			let (family, kind) = family.as_ref().expect("sample before TYPE");
			let metric = &caps[1];
			let body = caps.get(2).map_or("", |m| m.as_str());

			let rebuilt: Vec<_> = label
				.captures_iter(body)
				.map(|c| {
					parsed.label_names.insert(c[1].to_owned());
					c[0].to_owned()
				})
				.collect();
			assert_eq!(rebuilt.join(","), body, "malformed label body: {line}");

			parsed.samples = parsed.samples.saturating_add(1);
			if kind != "histogram" {
				assert_eq!(metric, family, "sample outside its family: {line}");
				continue;
			}

			let own: Vec<_> = label
				.captures_iter(body)
				.filter(|c| &c[1] != "le")
				.map(|c| c[0].to_owned())
				.collect();
			let group = own.join(",");
			let value: u64 = caps[3].parse().unwrap_or(0);
			if metric == format!("{family}_bucket") {
				let le = label
					.captures_iter(body)
					.find(|c| &c[1] == "le")
					.expect("bucket without le");
				let bound = if &le[2] == "+Inf" {
					f64::INFINITY
				} else {
					le[2].parse().unwrap()
				};
				buckets.push((group, bound, value));
			} else if metric == format!("{family}_count") {
				finish(&mut buckets, Some((group, value)));
			} else {
				assert_eq!(metric, format!("{family}_sum"), "unexpected series: {line}");
			}
		} else {
			panic!("line is neither HELP, TYPE nor a sample: {line:?}");
		}
	}

	assert!(pending_help.is_none(), "HELP without TYPE at the end");
	assert!(buckets.is_empty(), "histogram series without _count");

	parsed
}

#[test]
fn text_format_is_well_formed() {
	let probe = Probe::new(8);
	probe.total.add(3);
	probe.elapsed.add(1_500_000);
	probe.level.set(-7);
	probe.latency.observe(5_000_000);
	probe.latency.observe(5_000_001);
	probe.latency.observe(2_500_000_000);
	probe.size.observe(1_024);
	probe.size.observe(1_000_000);
	probe
		.pairs
		.get((Kind::Push, Outcome::Error))
		.inc();
	probe
		.pairs
		.get((Kind::Federation, Outcome::Ok))
		.add(2);
	probe
		.routes
		.get(&["/rooms/{room_id}/x", "GET"])
		.observe(1);
	probe
		.routes
		.get(&["/rooms/{room_id}/x", "PUT"])
		.observe(1);
	probe.hits.get(&["bridge"]).get(Outcome::Ok).inc();

	let text = probe.render();
	let parsed = parse(&text);

	assert!(parsed.samples > 0);
	assert!(text.contains("tuwunel_probe_total 3\n"));
	assert!(text.contains("tuwunel_probe_seconds_total 1.5\n"));
	assert!(text.contains("tuwunel_probe_level -7\n"));
	assert!(
		text.contains("# HELP tuwunel_probe_level Gauge with a back\\\\slash\\nand a newline.\n")
	);
	assert!(text.contains("tuwunel_probe_latency_seconds_bucket{le=\"0.005\"} 1\n"));
	assert!(text.contains("tuwunel_probe_latency_seconds_bucket{le=\"0.25\"} 2\n"));
	assert!(text.contains("tuwunel_probe_latency_seconds_bucket{le=\"1\"} 2\n"));
	assert!(text.contains("tuwunel_probe_latency_seconds_bucket{le=\"+Inf\"} 3\n"));
	assert!(text.contains("tuwunel_probe_latency_seconds_sum 2.510000001\n"));
	assert!(text.contains("tuwunel_probe_latency_seconds_count 3\n"));
	assert!(text.contains("tuwunel_probe_size_bytes_bucket{le=\"1024\"} 1\n"));
	assert!(text.contains("tuwunel_probe_size_bytes_sum 1001024\n"));
	assert!(text.contains("tuwunel_probe_pairs_total{kind=\"push\",result=\"error\"} 1\n"));
	assert!(text.contains("tuwunel_probe_pairs_total{kind=\"federation\",result=\"ok\"} 2\n"));
	assert!(text.contains("tuwunel_probe_pairs_total{kind=\"appservice\",result=\"ok\"} 0\n"));
	assert!(text.contains(
		"tuwunel_probe_route_seconds_bucket{route=\"/rooms/{room_id}/x\",method=\"PUT\",le=\"0.\
		 005\"} 1\n"
	));
	assert!(text.contains("tuwunel_probe_hits_total{appservice=\"bridge\",result=\"ok\"} 1\n"));
}

#[test]
fn label_values_are_escaped() {
	let probe = Probe::new(8);
	probe
		.hits
		.get(&["a\\b\"c\nd"])
		.get(Outcome::Ok)
		.inc();

	let text = probe.render();
	parse(&text);

	assert!(text.contains(r#"appservice="a\\b\"c\nd""#), "{text}");
	assert!(!text.contains("c\nd"));
}

#[test]
fn open_family_is_capped_with_overflow() {
	let probe = Probe::new(8);
	for index in 0..10_000 {
		probe
			.hits
			.get(&[&format!("as{index}")])
			.get(Outcome::Ok)
			.inc();
	}

	assert_eq!(probe.hits.len(), 8);
	assert_eq!(probe.hits.overflowed(), 9_992);

	let known = probe.hits.get(&["as0"]);
	assert_eq!(known.get(Outcome::Ok).get(), 1);
	assert_eq!(probe.hits.overflowed(), 9_992, "a known key must not overflow");
	assert!(Arc::ptr_eq(&known, &probe.hits.get(&["as0"])));

	let text = probe.render();
	parse(&text);

	let rows = text
		.lines()
		.filter(|line| {
			line.starts_with("tuwunel_probe_hits_total{") && line.contains("result=\"ok\"")
		})
		.count();
	assert_eq!(rows, 8 + 1);
	assert!(
		text.contains("tuwunel_probe_hits_total{appservice=\"_other\",result=\"ok\"} 9992\n")
	);
	assert!(text.contains(
		"tuwunel_metrics_series_overflow_total{family=\"tuwunel_probe_hits_total\"} 9992\n"
	));
}

#[test]
fn malformed_keys_go_to_other() {
	let probe = Probe::new(8);
	let long = "x".repeat(MAX_KEY_LEN + 1);

	probe.routes.get(&["/only-one-part"]).observe(1);
	probe.routes.get(&["a\u{1f}b", "GET"]).observe(1);
	probe.routes.get(&[&long, "GET"]).observe(1);
	probe.routes.get(&["/a", "GET"]).observe(1);

	assert_eq!(probe.routes.len(), 1);
	assert_eq!(probe.routes.overflowed(), 3);

	let text = probe.render();
	parse(&text);
	assert!(text.contains(&format!(
		"tuwunel_probe_route_seconds_count{{route=\"{OTHER}\",method=\"{OTHER}\"}} 3\n"
	)));
}

#[test]
fn the_other_label_value_is_reserved() {
	let probe = Probe::new(8);

	probe.hits.get(&[OTHER]).get(Outcome::Ok).inc();

	assert_eq!(probe.hits.len(), 0);
	assert_eq!(probe.hits.overflowed(), 1);

	let text = probe.render();
	parse(&text);
	assert_eq!(
		text.matches("tuwunel_probe_hits_total{appservice=\"_other\",result=\"ok\"}")
			.count(),
		1
	);
}

#[test]
fn concurrent_inserts_respect_the_cap() {
	let family = DynFamily::new(&["appservice"], 8, Counter::new);

	thread::scope(|scope| {
		for worker in 0..4_usize {
			let family = &family;
			scope.spawn(move || {
				for index in 0..2_000_usize {
					family
						.get(&[&format!(
							"as{}",
							index
								.saturating_add(worker)
								.checked_rem(500)
								.unwrap_or(0)
						)])
						.inc();
				}
			});
		}
	});

	assert_eq!(family.len(), 8);

	let mut w = Writer::new();
	w.metric("tuwunel_probe_total", "Total.", &family);
	let text = w.finish();
	let total: u64 = text
		.lines()
		.filter(|line| line.starts_with("tuwunel_probe_total{"))
		.filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
		.sum();

	assert_eq!(total, 8_000);
}

#[test]
fn series_bound_covers_what_is_rendered() {
	let probe = Probe::new(8);
	for index in 0..100 {
		probe
			.routes
			.get(&[&format!("/r{index}"), "GET"])
			.observe(1);
		probe
			.hits
			.get(&[&format!("as{index}")])
			.get(Outcome::Error)
			.inc();
	}

	let text = probe.render();
	let parsed = parse(&text);

	assert!(parsed.samples <= probe.series());
	assert_eq!(probe.latency.series(), 3 + 3);
	assert_eq!(probe.routes.series(), (8 + 1) * 6 + 1);
}

#[test]
fn registry_stays_within_the_series_ceiling() {
	let registry = Registry::new();
	assert!(registry.max_series() <= 25_000);

	let text = registry.render();
	let parsed = parse(&text);

	assert!(text.contains("tuwunel_build_info{version=\""));
	assert!(
		parsed
			.label_names
			.iter()
			.all(|name| ALLOWED_LABELS.contains(&name.as_str())),
		"label names outside the allowlist: {:?}",
		parsed.label_names
	);
}

#[test]
fn probe_labels_stay_within_the_allowlist() {
	let probe = Probe::new(8);
	probe
		.pairs
		.get((Kind::Appservice, Outcome::Ok))
		.inc();
	probe.routes.get(&["/a", "GET"]).observe(1);
	probe.hits.get(&["x"]).get(Outcome::Ok).inc();

	let parsed = parse(&probe.render());

	assert!(
		parsed
			.label_names
			.iter()
			.all(|name| ALLOWED_LABELS.contains(&name.as_str())),
		"label names outside the allowlist: {:?}",
		parsed.label_names
	);
}

#[test]
fn rocksdb_readings_are_exposed_once_taken_and_events_always() {
	let registry = Registry::new();
	let text = registry.render();

	assert!(!text.contains("tuwunel_rocksdb_sst_bytes"));
	assert!(text.contains("tuwunel_rocksdb_flushes_total 0\n"));

	registry.rocksdb.flush_completed();
	registry.rocksdb.compaction_completed();
	registry.rocksdb.stall_changed(Condition::Stopped);
	registry.rocksdb.record(&RocksSample {
		sst_bytes: Some(4096),
		block_caches: vec![CacheSample {
			pool: "Shared",
			usage: 7,
			pinned: 3,
			capacity: 100,
		}],
		..RocksSample::default()
	});

	let text = registry.render();
	let parsed = parse(&text);

	assert!(text.contains("tuwunel_rocksdb_sst_bytes 4096\n"), "{text}");
	assert!(text.contains("tuwunel_rocksdb_flushes_total 1\n"), "{text}");
	assert!(text.contains("tuwunel_rocksdb_compactions_total 1\n"), "{text}");
	assert!(text.contains("tuwunel_rocksdb_stall_changes_total{condition=\"stopped\"} 1\n"));
	assert!(text.contains("tuwunel_rocksdb_block_cache_usage_bytes{pool=\"Shared\"} 7\n"));
	assert!(!text.contains("tuwunel_rocksdb_wal_bytes "), "{text}");
	assert!(parsed.samples <= registry.max_series());

	registry.rocksdb.record(&RocksSample::default());
	assert!(
		registry
			.render()
			.contains("tuwunel_rocksdb_sst_bytes 4096\n")
	);
}

#[test]
fn appservice_delivery_is_counted_per_appservice_and_capped() {
	let registry = Registry::new();
	let group = &registry.appservice;

	group.transaction("bridge", SendResult::Ok, Duration::from_millis(30));
	group.transaction("bridge", SendResult::Timeout, Duration::from_secs(40));
	group.ping("bridge", SendResult::BadResponse);
	group.set_queued(&[("bridge", 7)]);

	for index in 0..APPSERVICE_CAP.saturating_add(10) {
		group.ping(&format!("as{index}"), SendResult::Ok);
	}

	assert_eq!(group.transactions("bridge", SendResult::Ok), 1);
	assert_eq!(group.transactions("bridge", SendResult::Network), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_appservice_transactions_total{appservice=\"bridge\",result=\"ok\"} 1\n",
		"tuwunel_appservice_transactions_total{appservice=\"bridge\",result=\"timeout\"} 1\n",
		"tuwunel_appservice_transaction_seconds_bucket{appservice=\"bridge\",le=\"0.05\"} 1\n",
		"tuwunel_appservice_transaction_seconds_count{appservice=\"bridge\"} 2\n",
		"tuwunel_appservice_queued_events{appservice=\"bridge\"} 7\n",
		"tuwunel_appservice_ping_total{appservice=\"bridge\",result=\"bad_response\"} 1\n",
		"tuwunel_appservice_ping_total{appservice=\"_other\",result=\"ok\"} 11\n",
		"tuwunel_metrics_series_overflow_total{family=\"tuwunel_appservice_ping_total\"} 11\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn storage_operations_are_counted_per_provider_and_op() {
	let registry = Registry::new();
	let group = &registry.storage;

	group.timed("media", Op::Get, true, Duration::from_millis(30));
	group.timed("media", Op::Get, false, Duration::from_secs(1));
	group.counted("media", Op::Delete, true);

	assert_eq!(group.operations("media", Op::Get, true), 1);
	assert_eq!(group.operations("media", Op::Get, false), 1);
	assert_eq!(group.operations("media", Op::Put, true), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_storage_operations_total{provider=\"media\",op=\"get\",result=\"ok\"} 1\n",
		"tuwunel_storage_operations_total{provider=\"media\",op=\"get\",result=\"error\"} 1\n",
		"tuwunel_storage_operations_total{provider=\"media\",op=\"delete\",result=\"ok\"} 1\n",
		"tuwunel_storage_operation_seconds_count{provider=\"media\",op=\"get\"} 2\n",
		"tuwunel_storage_operation_seconds_count{provider=\"media\",op=\"delete\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	for index in 0..PROVIDER_CAP.saturating_add(4) {
		group.counted(&format!("p{index}"), Op::Head, true);
	}

	let text = registry.render();

	assert!(
		text.contains(
			"tuwunel_metrics_series_overflow_total{family=\"tuwunel_storage_operations_total\"} \
			 5\n"
		),
		"{text}"
	);
	assert!(parse(&text).samples <= registry.max_series());
	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_is_counted_per_verdict_and_a_long_poll_leaves_with_its_guard() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_rooms(Fastpath::Quiet, 3);
	group.v3_rooms(Fastpath::BusyTimeline, 1);
	group.v3_rooms_per_sync(4);
	group.v3_answered(SinceKind::Initial, Reply::Data);
	group.v3_answered(SinceKind::Incremental, Reply::Empty);
	drop(group.v3_build(SinceKind::Incremental));
	group.fastpath_mismatch();

	assert_eq!(group.v3_rooms_counted(Fastpath::Quiet), 3);
	assert_eq!(group.v3_rooms_counted(Fastpath::Off), 0);
	assert_eq!(group.v3_answers(SinceKind::Initial, Reply::Data), 1);
	assert_eq!(group.v3_answers(SinceKind::Initial, Reply::Empty), 0);
	assert_eq!(group.v3_builds(SinceKind::Incremental), 1);
	assert_eq!(group.v3_builds(SinceKind::Initial), 0);

	let mut waiting = Box::pin(async {
		let _poll = group.long_poll(Api::V3);

		pending::<()>().await;
	});

	assert!(waiting.as_mut().now_or_never().is_none());
	assert_eq!(group.long_polling(Api::V3), 1);
	assert_eq!(group.long_polling(Api::V5), 0);

	drop(waiting);

	assert_eq!(group.long_polling(Api::V3), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_rooms_total{fastpath=\"quiet\"} 3\n",
		"tuwunel_sync_v3_rooms_total{fastpath=\"busy_timeline\"} 1\n",
		"tuwunel_sync_v3_rooms_total{fastpath=\"off\"} 0\n",
		"tuwunel_sync_v3_responses_total{since=\"initial\",outcome=\"data\"} 1\n",
		"tuwunel_sync_v3_responses_total{since=\"incremental\",outcome=\"empty\"} 1\n",
		"tuwunel_sync_v3_build_seconds_count{since=\"incremental\"} 1\n",
		"tuwunel_sync_v3_rooms_per_sync_bucket{le=\"2\"} 0\n",
		"tuwunel_sync_v3_rooms_per_sync_bucket{le=\"5\"} 1\n",
		"tuwunel_sync_fastpath_mismatches_total 1\n",
		"tuwunel_sync_long_polls{api=\"v3\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v5_is_counted_per_pass_and_stored_readings_wait_for_a_value() {
	let registry = Registry::new();
	let group = &registry.sync;
	let text = registry.render();

	assert!(!text.contains("tuwunel_sync_v5_store_bytes"), "{text}");
	assert!(!text.contains("tuwunel_sync_v5_connections_stored"), "{text}");
	assert!(text.contains("tuwunel_sync_v5_connections_loaded 0\n"), "{text}");

	drop(group.v5_pass(Pass::First));
	drop(group.v5_pass(Pass::Woken));
	drop(group.v5_pass(Pass::Woken));
	group.v5_unknown_pos();
	group.v5_state(5_000);
	group.v5_response_rooms(0);
	group.v5_response_rooms(30);
	group.set_v5_connections_loaded(7);
	group.set_v5_connections_stored(9);
	group.set_v5_store_bytes(4_096);

	assert_eq!(group.v5_passes(Pass::First), 1);
	assert_eq!(group.v5_passes(Pass::Woken), 2);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v5_pass_seconds_count{pass=\"first\"} 1\n",
		"tuwunel_sync_v5_pass_seconds_count{pass=\"woken\"} 2\n",
		"tuwunel_sync_v5_unknown_pos_total 1\n",
		"tuwunel_sync_v5_connection_state_bytes_bucket{le=\"4096\"} 0\n",
		"tuwunel_sync_v5_connection_state_bytes_bucket{le=\"16384\"} 1\n",
		"tuwunel_sync_v5_response_rooms_bucket{le=\"0\"} 1\n",
		"tuwunel_sync_v5_response_rooms_bucket{le=\"50\"} 2\n",
		"tuwunel_sync_v5_connections_loaded 7\n",
		"tuwunel_sync_v5_connections_stored 9\n",
		"tuwunel_sync_v5_store_bytes 4096\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v5_phases_are_timed_per_phase() {
	let registry = Registry::new();
	let group = &registry.sync;

	drop(group.v5_phase(V5Phase::Watch));
	drop(group.v5_phase(V5Phase::Select));
	drop(group.v5_phase(V5Phase::Collect));
	drop(group.v5_phase(V5Phase::Extensions));
	drop(group.v5_phase(V5Phase::Extensions));
	drop(group.v5_phase(V5Phase::Profiles));
	drop(group.v5_phase(V5Phase::Store));
	drop(group.v5_phase(V5Phase::Store));

	assert_eq!(group.v5_phases(V5Phase::Watch), 1);
	assert_eq!(group.v5_phases(V5Phase::Store), 2);
	assert_eq!(group.v5_phases(V5Phase::Collect), 1);
	assert_eq!(group.v5_phases(V5Phase::Extensions), 2);
	assert_eq!(group.v5_phases(V5Phase::Profiles), 1);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v5_phase_seconds_count{phase=\"watch\"} 1\n",
		"tuwunel_sync_v5_phase_seconds_count{phase=\"collect\"} 1\n",
		"tuwunel_sync_v5_phase_seconds_count{phase=\"extensions\"} 2\n",
		"tuwunel_sync_v5_phase_seconds_count{phase=\"profiles\"} 1\n",
		"tuwunel_sync_v5_phase_seconds_count{phase=\"store\"} 2\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v5_requests_and_fresh_payloads_are_counted() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v5_request(V5Path::Quiet);
	group.v5_request(V5Path::Pass);
	group.v5_request(V5Path::Pass);
	group.v5_fresh_payloads(0);
	group.v5_fresh_payloads(7);

	assert_eq!(group.v5_requests_total(V5Path::Quiet), 1);
	assert_eq!(group.v5_requests_total(V5Path::Pass), 2);
	assert_eq!(group.v5_fresh_payload_passes(), 2);

	group.v5_wait_verified(WaitStage::Gate, Verdict::Quiet);

	assert_eq!(group.v5_wait_verified_total(WaitStage::Gate, Verdict::Quiet), 1);
	assert_eq!(group.v5_wait_verified_total(WaitStage::Woken, Verdict::Quiet), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v5_requests_total{path=\"quiet\"} 1\n",
		"tuwunel_sync_v5_requests_total{path=\"pass\"} 2\n",
		"tuwunel_sync_v5_fresh_payloads_per_pass_bucket{le=\"0\"} 1\n",
		"tuwunel_sync_v5_fresh_payloads_per_pass_bucket{le=\"10\"} 2\n",
		"tuwunel_sync_v5_fresh_payloads_per_pass_count 2\n",
		"tuwunel_sync_v5_wait_verify_total{stage=\"gate\",verdict=\"quiet\"} 1\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_phases_are_timed_per_part() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_phase(Phase::Joined, Duration::from_millis(30));
	group.v3_phase(Phase::E2ee, Duration::from_millis(1));
	group.v3_phase(Phase::E2ee, Duration::from_millis(2));

	assert_eq!(group.v3_phases(Phase::Joined), 1);
	assert_eq!(group.v3_phases(Phase::E2ee), 2);
	assert_eq!(group.v3_phases(Phase::Left), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_phase_seconds_count{phase=\"joined\"} 1\n",
		"tuwunel_sync_v3_phase_seconds_count{phase=\"e2ee\"} 2\n",
		"tuwunel_sync_v3_phase_seconds_bucket{phase=\"joined\",le=\"0.05\"} 1\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_watch_is_timed_per_part() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_watch(WatchPart::Register, Duration::from_millis(3));
	group.v3_watch(WatchPart::Release, Duration::from_secs(2));

	assert_eq!(group.v3_watches(WatchPart::Register), 1);
	assert_eq!(group.v3_watches(WatchPart::Release), 1);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_watch_seconds_count{part=\"register\"} 1\n",
		"tuwunel_sync_v3_watch_seconds_bucket{part=\"release\",le=\"2.5\"} 1\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_watchers_are_counted_per_sync() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_watchers_per_sync(60);

	assert_eq!(group.v3_watcher_syncs(), 1);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_watchers_per_sync_bucket{le=\"50\"} 0\n",
		"tuwunel_sync_v3_watchers_per_sync_bucket{le=\"100\"} 1\n",
		"tuwunel_sync_v3_watchers_per_sync_count 1\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_builds_are_counted_per_outcome() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_build_outcome(Build::Empty);
	group.v3_build_outcome(Build::Empty);
	group.v3_build_outcome(Build::Data);
	group.v3_build_outcome(Build::PresenceOnly);

	assert_eq!(group.v3_outcomes(Build::Empty), 2);
	assert_eq!(group.v3_outcomes(Build::Data), 1);
	assert_eq!(group.v3_outcomes(Build::PresenceOnly), 1);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_builds_total{outcome=\"data\"} 1\n",
		"tuwunel_sync_v3_builds_total{outcome=\"empty\"} 2\n",
		"tuwunel_sync_v3_builds_total{outcome=\"presence_only\"} 1\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_sections_with_data_are_counted_per_section() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_section(Section::Rooms);
	group.v3_section(Section::Presence);
	group.v3_section(Section::Rooms);

	assert_eq!(group.v3_sections_counted(Section::Rooms), 2);
	assert_eq!(group.v3_sections_counted(Section::Users), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_response_sections_total{section=\"rooms\"} 2\n",
		"tuwunel_sync_v3_response_sections_total{section=\"presence\"} 1\n",
		"tuwunel_sync_v3_response_sections_total{section=\"users\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_left_rooms_are_counted_per_build() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_left_rooms_per_sync(0);
	group.v3_left_rooms_per_sync(12);

	assert_eq!(group.v3_left_syncs(), 2);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_left_rooms_per_sync_bucket{le=\"0\"} 1\n",
		"tuwunel_sync_v3_left_rooms_per_sync_bucket{le=\"10\"} 1\n",
		"tuwunel_sync_v3_left_rooms_per_sync_bucket{le=\"20\"} 2\n",
		"tuwunel_sync_v3_left_rooms_per_sync_count 2\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_journal_is_counted_per_kind_and_its_size_is_a_gauge() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.set_journal_entries(40);
	group.set_journal_entries(25);
	group.journal_evicted(15);
	group.journal_evicted(3);
	group.journal_recorded(JournalKind::Membership);
	group.journal_recorded(JournalKind::Membership);
	group.journal_recorded(JournalKind::ToDevice);
	group.journal_miss(JournalKind::PrivateRead);
	group.journal_miss(JournalKind::DeviceLists);

	assert_eq!(group.journal_held(), 25);
	assert_eq!(group.journal_evicted_total(), 18);
	assert_eq!(group.journal_recorded_total(JournalKind::Membership), 2);
	assert_eq!(group.journal_recorded_total(JournalKind::ToDevice), 1);
	assert_eq!(group.journal_recorded_total(JournalKind::Timeline), 0);
	assert_eq!(group.journal_missed(JournalKind::PrivateRead), 1);
	assert_eq!(group.journal_missed(JournalKind::DeviceLists), 1);
	assert_eq!(group.journal_missed(JournalKind::Timeline), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_journal_entries 25\n",
		"tuwunel_sync_journal_evictions_total 18\n",
		"tuwunel_sync_journal_recorded_total{kind=\"membership\"} 2\n",
		"tuwunel_sync_journal_recorded_total{kind=\"to_device\"} 1\n",
		"tuwunel_sync_journal_recorded_total{kind=\"timeline\"} 0\n",
		"tuwunel_sync_journal_misses_total{kind=\"private_read\"} 1\n",
		"tuwunel_sync_journal_misses_total{kind=\"device_lists\"} 1\n",
		"tuwunel_sync_journal_misses_total{kind=\"timeline\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_journal_waits_count_the_changes_they_pass_and_their_lookups_by_way() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.wait_scanned(40);
	group.wait_scanned(2);
	group.wait_lookup(Lookup::Room);
	group.wait_lookup(Lookup::Room);
	group.wait_lookup(Lookup::Set);

	assert_eq!(group.wait_scanned_total(), 42);
	assert_eq!(group.wait_lookups_total(Lookup::Room), 2);
	assert_eq!(group.wait_lookups_total(Lookup::Set), 1);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_wait_scanned_total 42\n",
		"tuwunel_sync_v3_wait_lookups_total{how=\"room\"} 2\n",
		"tuwunel_sync_v3_wait_lookups_total{how=\"set\"} 1\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_journal_wait_verdicts_are_counted_per_verdict() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.wait_verified(Verdict::Miss);
	group.wait_verified(Verdict::Miss);
	group.wait_verified(Verdict::Quiet);

	assert_eq!(group.wait_verified_total(Verdict::Miss), 2);
	assert_eq!(group.wait_verified_total(Verdict::Quiet), 1);
	assert_eq!(group.wait_verified_total(Verdict::Both), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_wait_verify_total{verdict=\"miss\"} 2\n",
		"tuwunel_sync_v3_wait_verify_total{verdict=\"quiet\"} 1\n",
		"tuwunel_sync_v3_wait_verify_total{verdict=\"both\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_journal_wait_wakes_are_counted_per_reason() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.wait_woken(WakeReason::Journal);
	group.wait_woken(WakeReason::Timeout);

	assert_eq!(group.wait_woken_total(WakeReason::Journal), 1);
	assert_eq!(group.wait_woken_total(WakeReason::Timeout), 1);
	assert_eq!(group.wait_woken_total(WakeReason::Horizon), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_wait_wakes_total{reason=\"journal\"} 1\n",
		"tuwunel_sync_v3_wait_wakes_total{reason=\"timeout\"} 1\n",
		"tuwunel_sync_v3_wait_wakes_total{reason=\"horizon\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v5_journal_waits_are_counted_apart_from_the_v3_ones() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v5_wait_woken(WakeReason::Journal);
	group.v5_wait_woken(WakeReason::Journal);
	group.v5_wait_woken(WakeReason::Horizon);
	group.v5_wait_verified(WaitStage::Woken, Verdict::Miss);
	group.v5_wait_verified(WaitStage::Timeout, Verdict::Miss);
	group.v5_wait_verified(WaitStage::Timeout, Verdict::Quiet);
	group.v5_wait_verified(WaitStage::Timeout, Verdict::Quiet);

	assert_eq!(group.v5_wait_woken_total(WakeReason::Journal), 2);
	assert_eq!(group.v5_wait_woken_total(WakeReason::Horizon), 1);
	assert_eq!(group.v5_wait_woken_total(WakeReason::Timeout), 0);
	assert_eq!(group.v5_wait_verified_total(WaitStage::Woken, Verdict::Miss), 1);
	assert_eq!(group.v5_wait_verified_total(WaitStage::Timeout, Verdict::Miss), 1);
	assert_eq!(group.v5_wait_verified_total(WaitStage::Timeout, Verdict::Quiet), 2);
	assert_eq!(group.v5_wait_verified_total(WaitStage::Woken, Verdict::Quiet), 0);
	assert_eq!(group.wait_woken_total(WakeReason::Journal), 0);
	assert_eq!(group.wait_verified_total(Verdict::Miss), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v5_wait_wakes_total{reason=\"journal\"} 2\n",
		"tuwunel_sync_v5_wait_wakes_total{reason=\"horizon\"} 1\n",
		"tuwunel_sync_v5_wait_wakes_total{reason=\"timeout\"} 0\n",
		"tuwunel_sync_v5_wait_verify_total{stage=\"woken\",verdict=\"miss\"} 1\n",
		"tuwunel_sync_v5_wait_verify_total{stage=\"timeout\",verdict=\"miss\"} 1\n",
		"tuwunel_sync_v5_wait_verify_total{stage=\"timeout\",verdict=\"quiet\"} 2\n",
		"tuwunel_sync_v5_wait_verify_total{stage=\"woken\",verdict=\"quiet\"} 0\n",
		"tuwunel_sync_v3_wait_wakes_total{reason=\"journal\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v3_build_paths_and_candidates_are_counted() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v3_build_path(BuildPath::Scan);
	group.v3_build_path(BuildPath::Candidates);
	group.v3_build_path(BuildPath::Scan);
	group.v3_candidates_per_sync(3);
	group.v3_candidates_per_sync(0);
	group.candidates_miss();

	assert_eq!(group.v3_build_paths_counted(BuildPath::Scan), 2);
	assert_eq!(group.v3_build_paths_counted(BuildPath::Candidates), 1);
	assert_eq!(group.v3_candidate_syncs(), 2);
	assert_eq!(group.candidates_missed(), 1);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v3_build_paths_total{path=\"scan\"} 2\n",
		"tuwunel_sync_v3_build_paths_total{path=\"candidates\"} 1\n",
		"tuwunel_sync_v3_candidates_per_sync_bucket{le=\"0\"} 1\n",
		"tuwunel_sync_v3_candidates_per_sync_bucket{le=\"5\"} 2\n",
		"tuwunel_sync_v3_candidates_per_sync_count 2\n",
		"tuwunel_sync_candidates_misses_total 1\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}

#[test]
fn sync_v5_selection_is_counted_by_path_and_reason() {
	let registry = Registry::new();
	let group = &registry.sync;

	group.v5_selected(SelectPath::Candidates, SelectReason::None);
	group.v5_selected(SelectPath::Candidates, SelectReason::None);
	group.v5_selected(SelectPath::Full, SelectReason::AccountData);
	group.v5_candidates(0);
	group.v5_candidates(3);
	group.v5_candidates_mismatch(Mismatch::Count);
	group.v5_candidates_skip(Skipped::Explained);
	group.v5_candidates_skip(Skipped::Explained);

	assert_eq!(group.v5_candidates_skipped_total(Skipped::Explained), 2);
	assert_eq!(group.v5_candidates_skipped_total(Skipped::Journal), 0);
	assert_eq!(group.v5_selected_total(SelectPath::Candidates, SelectReason::None), 2);
	assert_eq!(group.v5_selected_total(SelectPath::Full, SelectReason::AccountData), 1);
	assert_eq!(group.v5_selected_total(SelectPath::Full, SelectReason::Horizon), 0);
	assert_eq!(group.v5_candidate_passes(), 2);
	assert_eq!(group.v5_candidates_mismatched_total(Mismatch::Count), 1);
	assert_eq!(group.v5_candidates_mismatched_total(Mismatch::Window), 0);

	let text = registry.render();
	let parsed = parse(&text);

	for line in [
		"tuwunel_sync_v5_select_paths_total{path=\"candidates\",reason=\"none\"} 2\n",
		"tuwunel_sync_v5_select_paths_total{path=\"full\",reason=\"account_data\"} 1\n",
		"tuwunel_sync_v5_select_paths_total{path=\"full\",reason=\"no_index\"} 0\n",
		"tuwunel_sync_v5_candidates_per_pass_bucket{le=\"0\"} 1\n",
		"tuwunel_sync_v5_candidates_per_pass_bucket{le=\"5\"} 2\n",
		"tuwunel_sync_v5_candidates_per_pass_count 2\n",
		"tuwunel_sync_v5_candidates_mismatch_total{part=\"count\"} 1\n",
		"tuwunel_sync_v5_candidates_mismatch_total{part=\"window\"} 0\n",
		"tuwunel_sync_v5_candidates_verify_skipped_total{reason=\"explained\"} 2\n",
		"tuwunel_sync_v5_candidates_verify_skipped_total{reason=\"journal\"} 0\n",
	] {
		assert!(text.contains(line), "{line}\n{text}");
	}

	assert!(parsed.samples <= registry.max_series());
}
