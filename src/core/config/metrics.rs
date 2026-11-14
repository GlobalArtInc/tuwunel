//! Configures the Prometheus metrics endpoint.
//!
//! The endpoint has its own listener, separate from the client and federation
//! listeners, and is off by default. A background collector samples process,
//! runtime and database state at fixed intervals.

use std::{
	net::{IpAddr, Ipv4Addr},
	path::PathBuf,
};

use derive_more::Debug;
use serde::Deserialize;
use tuwunel_macros::config_example_generator;

use crate::redacted_debug;

/// Prometheus metrics
///
/// Serves server metrics in the Prometheus text format at `GET /metrics` on a
/// listener of its own, apart from the client and federation listeners. It is
/// off by default; when off, nothing is recorded and no port is opened. Every
/// setting except the intervals and the token is read once at startup, so
/// restart the server after changing `enable`, `address` or `port`.
///
/// Metric labels never carry user, room, device or server identifiers, and the
/// number of series is bounded, so the output stays small however much traffic
/// the server sees.
///
/// Tips:
/// - Do not publish this port through a reverse proxy or an ingress. Point the
///   scraper at it directly, and set `bearer_token` or `bearer_token_file`
///   when the port can be reached from anywhere you do not trust.
/// - `address` defaults to loopback. Use `0.0.0.0` or `::` to scrape from
///   another host or from outside a container.
/// - Startup refuses `port` when it matches a port that `http.listen` already
///   uses on the same address, and fails if the port is taken.
/// - `bearer_token` and `bearer_token_file` are mutually exclusive.
#[expect(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
#[derive(Clone, Debug, Deserialize)]
#[config_example_generator(
	filename = "tuwunel-example.toml",
	section = "global.metrics",
	group = "true"
)]
pub struct Metrics {
	/// Serves metrics on a separate listener.
	///
	/// Opens the listener at `address` and `port`, records request and
	/// runtime measurements, and starts the background collector. The setting
	/// is read once at startup and requires a restart to change.
	#[serde(default)]
	pub enable: bool,

	/// Sets the IP address the metrics listener binds to.
	///
	/// Requires a restart to change. The startup check does not treat `::` and
	/// `0.0.0.0` as overlapping, so a clash between them on a dual-stack host
	/// shows up as a bind error instead.
	///
	/// default: "127.0.0.1"
	#[serde(default = "default_address")]
	pub address: IpAddr,

	/// Sets the TCP port of the metrics listener.
	///
	/// Startup fails when the port is already in use or equals a port of
	/// `http.listen` on the same address. Requires a restart to change.
	///
	/// default: 9149
	#[serde(default = "default_port")]
	pub port: u16,

	/// Requires scrapers to send this token in an `Authorization: Bearer`
	/// header.
	///
	/// Without a token or a token file the endpoint answers anyone who can
	/// reach it. Requests with a missing or wrong token get `401`. The value
	/// must not be empty, and it cannot be combined with `bearer_token_file`.
	///
	/// reloadable: yes
	///
	/// display: sensitive
	#[debug("{}", redacted_debug!(bearer_token))]
	pub bearer_token: Option<String>,

	/// Reads the bearer token from a file.
	///
	/// The file is read again at most once a second, when a scrape finds the
	/// cached token older than that, so rotating the token needs no restart
	/// and takes effect within a second. A read that takes longer than two
	/// seconds is abandoned and answered with `500`. Surrounding whitespace is
	/// ignored. With the endpoint enabled, startup and reload refuse a file
	/// that cannot be read or is empty. It cannot be combined with
	/// `bearer_token`.
	///
	/// reloadable: yes
	/// example: "/etc/tuwunel/.metrics_token"
	pub bearer_token_file: Option<PathBuf>,

	/// Sets the seconds between samples of cheap gauges.
	///
	/// Covers process, async runtime, database pool, cache and RocksDB
	/// properties. Must be at least 1.
	///
	/// reloadable: yes
	/// default: 15
	#[serde(default = "default_collect_interval")]
	pub collect_interval: u64,

	/// Sets the seconds between counting scans, or 0 to turn them off.
	///
	/// A scan walks database keys to count users, rooms, devices, stored
	/// sliding sync connections and queued appservice events. Those gauges
	/// stay at their last value while scans are off.
	///
	/// reloadable: yes
	/// default: 300
	#[serde(default = "default_count_interval")]
	pub count_interval: u64,
}

impl Default for Metrics {
	fn default() -> Self {
		Self {
			enable: Default::default(),
			address: default_address(),
			port: default_port(),
			bearer_token: Default::default(),
			bearer_token_file: Default::default(),
			collect_interval: default_collect_interval(),
			count_interval: default_count_interval(),
		}
	}
}

fn default_address() -> IpAddr { IpAddr::V4(Ipv4Addr::LOCALHOST) }

fn default_port() -> u16 { 9149 }

fn default_collect_interval() -> u64 { 15 }

fn default_count_interval() -> u64 { 300 }
