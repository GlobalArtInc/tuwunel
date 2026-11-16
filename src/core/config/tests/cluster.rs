use std::{collections::BTreeMap, fmt::Write, net::SocketAddr, path::Path};

use super::{TempFile, assert_verdict, config_from_toml, default_config};
use crate::config::{check::check, example_config};

const MIB: u64 = 1024 * 1024;

type Changes<'a> = &'a [(&'a str, Option<&'a str>)];
type Row<'a> = (Changes<'a>, bool, &'a str);

fn document(secret: &Path, changes: Changes<'_>) -> String {
	let mut keys: BTreeMap<&str, String> = BTreeMap::from([
		("enable", "true".to_owned()),
		("node_id", "0".to_owned()),
		("advertise", "\"http://tuwunel-0.peers.svc:8009\"".to_owned()),
		("secret_file", format!("{secret:?}")),
		("raft_path", "\"/var/lib/tuwunel/raft\"".to_owned()),
		("snapshot_path", "\"/var/lib/tuwunel/snapshots\"".to_owned()),
	]);

	for (key, value) in changes {
		match value {
			| Some(value) => keys.insert(key, (*value).to_owned()),
			| None => keys.remove(key),
		};
	}

	let mut toml = String::from("[global.cluster]\n");
	for (key, value) in &keys {
		writeln!(toml, "{key} = {value}").expect("a string accepts writes");
	}

	toml
}

#[test]
fn cluster_is_off_by_default_with_documented_values() {
	let default = default_config();
	let cluster = &default.cluster;

	assert!(!cluster.enable, "the cluster is off by default");
	assert_eq!(cluster.node_id, None, "no node identity by default");
	assert!(!cluster.node_id_from_hostname, "no host name identity by default");
	assert_eq!(
		cluster.listen,
		"0.0.0.0:8009".parse::<SocketAddr>().unwrap(),
		"cluster listener address"
	);
	assert!(cluster.advertise.is_none(), "no advertised address by default");
	assert!(cluster.secret_file.is_none(), "no secret file by default");
	assert!(cluster.raft_path.is_none(), "no log path by default");
	assert!(cluster.snapshot_path.is_none(), "no snapshot path by default");
	assert!(!cluster.initialize, "a cluster is never formed by default");
	assert_eq!(cluster.heartbeat_ms, 250, "heartbeat");
	assert_eq!(
		(cluster.election_timeout_ms.min, cluster.election_timeout_ms.max),
		(1500, 3000),
		"election timeout"
	);
	assert_eq!(cluster.lease_ms, 1000, "lease");
	assert_eq!(cluster.commit_timeout_ms, 10_000, "commit timeout");
	assert_eq!(cluster.snapshot_log_bytes, 256 * MIB, "snapshot interval");
	assert_eq!(cluster.keep_log_bytes, 1024 * MIB, "kept log");
	assert_eq!(cluster.snapshot_rate_mb, 50, "snapshot rate");
	assert_eq!(cluster.capture_queue_bytes, 256 * MIB, "capture queue");
	assert!(!cluster.allow_faults, "faults are not allowed by default");
	check(&default).expect("the default cluster section should pass the check");

	let example = example_config().expect("pristine configuration rendered");
	for line in [
		"#[global.cluster]\n",
		"#enable = false\n",
		"#node_id =\n",
		"#node_id_from_hostname = false\n",
		"#listen = \"0.0.0.0:8009\"\n",
		"#advertise =\n",
		"#secret_file =\n",
		"#raft_path =\n",
		"#snapshot_path =\n",
		"#initialize = false\n",
		"#heartbeat_ms = 250\n",
		"#election_timeout_ms = [1500, 3000]\n",
		"#lease_ms = 1000\n",
		"#commit_timeout_ms = 10000\n",
		"#snapshot_log_bytes = \"256 MiB\"\n",
		"#keep_log_bytes = \"1 GiB\"\n",
		"#snapshot_rate_mb = 50\n",
		"#capture_queue_bytes = \"256 MiB\"\n",
		"#allow_faults = false\n",
	] {
		assert!(example.contains(line), "tuwunel-example.toml lacks {line:?}");
	}
}

#[test]
fn cluster_sizes_and_timeouts_parse_and_a_bad_timeout_range_does_not() {
	let config = config_from_toml(
		"[global.cluster]\nelection_timeout_ms = [900, 1800]\nsnapshot_log_bytes = \"1 \
		 MiB\"\nkeep_log_bytes = 2048\n",
	)
	.expect("the config should parse");

	assert_eq!(
		(config.cluster.election_timeout_ms.min, config.cluster.election_timeout_ms.max),
		(900, 1800),
		"range"
	);
	assert_eq!(config.cluster.snapshot_log_bytes, MIB, "size with a suffix");
	assert_eq!(config.cluster.keep_log_bytes, 2048, "size in bytes");
	assert_eq!(config.cluster.heartbeat_ms, 250, "omitted keys keep their defaults");

	for range in ["[1500]", "[1, 2, 3]", "[]", "1500"] {
		let toml = format!("[global.cluster]\nelection_timeout_ms = {range}\n");

		assert!(config_from_toml(&toml).is_err(), "{range} is not a pair of bounds");
	}
}

#[test]
fn an_enabled_cluster_is_validated() {
	let secret = TempFile::new("tuwunel-cluster-secret", "shared-secret\n");
	let empty = TempFile::new("tuwunel-cluster-empty-secret", " \n");

	let secret_file = &secret.0;
	let empty_file = &empty.0;
	let empty_path = format!("{empty_file:?}");

	let rows: [Row<'_>; 44] = [
		(&[], true, ""),
		(&[("node_id", None)], false, "cluster.node_id"),
		(&[("node_id", None), ("node_id_from_hostname", Some("true"))], true, ""),
		(
			&[("node_id_from_hostname", Some("true"))],
			false,
			"cluster.node_id_from_hostname",
		),
		(&[("advertise", None)], false, "cluster.advertise"),
		(&[("advertise", Some("\"not a url\""))], false, "cluster.advertise"),
		(&[("advertise", Some("\"ftp://tuwunel-0:8009\""))], false, "cluster.advertise"),
		(&[("advertise", Some("\"http:///\""))], false, "cluster.advertise"),
		(&[("advertise", Some("\"tuwunel-0:8009\""))], false, "cluster.advertise"),
		(&[("advertise", Some("\"https://tuwunel-0:8009\""))], true, ""),
		(&[("advertise", Some("\"http://[::1]:8009\""))], true, ""),
		(&[("secret_file", None)], false, "cluster.secret_file"),
		(
			&[("secret_file", Some("\"/nonexistent/secret\""))],
			false,
			"cluster.secret_file",
		),
		(&[("secret_file", Some(empty_path.as_str()))], false, "cluster.secret_file"),
		(&[("raft_path", None)], false, "cluster.raft_path"),
		(&[("raft_path", Some("\"\""))], false, "cluster.raft_path"),
		(&[("snapshot_path", None)], false, "cluster.snapshot_path"),
		(&[("snapshot_path", Some("\"\""))], false, "cluster.snapshot_path"),
		(&[("heartbeat_ms", Some("0"))], false, "cluster.heartbeat_ms"),
		(&[("lease_ms", Some("0"))], false, "cluster.lease_ms"),
		(&[("commit_timeout_ms", Some("0"))], false, "cluster.commit_timeout_ms"),
		(&[("snapshot_log_bytes", Some("0"))], false, "cluster.snapshot_log_bytes"),
		(&[("keep_log_bytes", Some("0"))], false, "cluster.keep_log_bytes"),
		(&[("snapshot_rate_mb", Some("0"))], false, "cluster.snapshot_rate_mb"),
		(&[("capture_queue_bytes", Some("0"))], false, "cluster.capture_queue_bytes"),
		(
			&[("election_timeout_ms", Some("[3000, 1500]"))],
			false,
			"cluster.election_timeout_ms",
		),
		(
			&[("election_timeout_ms", Some("[1500, 1500]"))],
			false,
			"cluster.election_timeout_ms",
		),
		(
			&[("election_timeout_ms", Some("[750, 1000]")), ("lease_ms", Some("500"))],
			true,
			"",
		),
		(
			&[("election_timeout_ms", Some("[749, 1000]"))],
			false,
			"cluster.election_timeout_ms",
		),
		(&[("heartbeat_ms", Some("500"))], true, ""),
		(&[("heartbeat_ms", Some("501"))], false, "cluster.election_timeout_ms"),
		(
			&[
				("heartbeat_ms", Some("6148914691236517206")),
				("election_timeout_ms", Some("[1, 2]")),
			],
			false,
			"cluster.election_timeout_ms",
		),
		(&[("initialize", Some("true"))], true, ""),
		(&[("allow_faults", Some("true"))], true, ""),
		(&[("listen", Some("\"127.0.0.1:9009\""))], true, ""),
		(&[("snapshot_log_bytes", Some("\"512 MiB\""))], true, ""),
		(&[("lease_ms", Some("1499"))], true, ""),
		(&[("lease_ms", Some("1500"))], false, "cluster.lease_ms"),
		(&[("lease_ms", Some("251"))], true, ""),
		(&[("lease_ms", Some("250"))], false, "cluster.lease_ms"),
		(&[("commit_timeout_ms", Some("1"))], true, ""),
		(&[("snapshot_rate_mb", Some("1"))], true, ""),
		(&[("capture_queue_bytes", Some("1"))], true, ""),
		(&[("keep_log_bytes", Some("1"))], true, ""),
	];

	for (changes, valid, field) in rows {
		let toml = document(secret_file, changes);
		let config = config_from_toml(&toml).expect("the config should parse");

		assert_verdict(&toml, check(&config), valid, field);
	}
}

#[test]
fn a_disabled_cluster_is_not_checked() {
	let config = config_from_toml(
		"[global.cluster]\nenable = false\nnode_id_from_hostname = true\nnode_id = 1\nadvertise \
		 = \"nonsense\"\nsecret_file = \"/nonexistent\"\nheartbeat_ms = 0\nelection_timeout_ms \
		 = [5, 1]\n",
	)
	.expect("the config should parse");

	check(&config).expect("nothing of a disabled cluster is checked");
}

#[test]
fn node_identity_comes_from_the_key_or_from_the_host_name() {
	let by_host = config_from_toml("[global.cluster]\nnode_id_from_hostname = true\n")
		.expect("the config should parse");

	for (hostname, expected) in [
		("tuwunel-cluster-2", Some(2)),
		("tuwunel-0", Some(0)),
		("tuwunel-cluster-12.tuwunel-peers.default.svc", Some(12)),
		("tuwunel-007", Some(7)),
		("tuwunel", None),
		("tuwunel-", None),
		("tuwunel-x", None),
		("tuwunel-+1", None),
		("tuwunel-1-b", None),
		("tuwunel-18446744073709551616", None),
		("", None),
	] {
		let resolved = by_host.cluster.resolve_node_id(hostname);

		assert_eq!(resolved.as_ref().ok().copied(), expected, "{hostname:?}: {resolved:?}");
	}

	let by_key =
		config_from_toml("[global.cluster]\nnode_id = 100\n").expect("the config should parse");

	assert_eq!(
		by_key
			.cluster
			.resolve_node_id("tuwunel-2")
			.expect("an explicit identity"),
		100,
		"the key does not depend on the host name"
	);

	let both = config_from_toml("[global.cluster]\nnode_id = 1\nnode_id_from_hostname = true\n")
		.expect("the config should parse");

	assert!(both.cluster.resolve_node_id("tuwunel-2").is_err(), "both keys are refused");
	assert!(
		default_config()
			.cluster
			.resolve_node_id("tuwunel-2")
			.is_err(),
		"neither key is refused"
	);
}
