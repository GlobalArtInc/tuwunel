//! Configures client-facing protocol behavior.
//!
//! Sync settings bound long-polling timeouts. Room-summary controls select
//! sync calculations.

use serde::Deserialize;
use tuwunel_macros::config_example_generator;

use super::true_fn;

/// Client API
///
/// Groups settings for how the server answers client requests.
/// `[global.client.sync]` bounds sync long polling and turns room hero
/// calculation on or off.
#[expect(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
#[derive(Clone, Copy, Debug, Deserialize, Default)]
#[config_example_generator(
	filename = "tuwunel-example.toml",
	section = "global.client",
	group = "true",
	ignore = "sync"
)]
pub struct Client {
	/// Settings for client sync.
	///
	/// display: section
	#[serde(default)]
	pub sync: Sync,
}

/// Client sync long polling
///
/// Bounds how long a client's sync request may wait for new events before
/// the server answers anyway, and whether sync computes room heroes.
///
/// Tips:
/// - Sliding sync honors only `timeout_max`; `timeout_min` and
///   `timeout_default` apply to legacy `/sync` and to `/events`.
/// - Keep `timeout_max` (milliseconds) below `request_timeout` in
///   `[global.http]` (seconds) and below the read timeout of any reverse
///   proxy in front of the server, or long polls fail instead of returning.
#[expect(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
#[derive(Clone, Copy, Debug, Deserialize)]
#[config_example_generator(
	filename = "tuwunel-example.toml",
	section = "global.client.sync",
	group = "true"
)]
pub struct Sync {
	/// Sets the minimum requested timeout for long-polling sync.
	///
	/// Smaller client requests are clamped up to this value.
	///
	/// reloadable: yes
	/// default: 5000
	/// legacy: client_sync_timeout_min
	#[serde(default = "default_timeout_min")]
	pub timeout_min: u64,

	/// Sets the long-polling sync timeout when the client supplies none in its
	/// query string.
	///
	/// reloadable: yes
	/// default: 30000
	/// legacy: client_sync_timeout_default
	#[serde(default = "default_timeout_default")]
	pub timeout_default: u64,

	/// Sets the maximum requested timeout for long-polling sync.
	///
	/// Larger client requests are clamped down to this value.
	///
	/// reloadable: yes
	/// default: 90000
	/// legacy: client_sync_timeout_max
	#[serde(default = "default_timeout_max")]
	pub timeout_max: u64,

	/// Calculates room heroes in sliding and legacy client sync.
	///
	/// The Matrix specification requires this calculation, so clients may
	/// malfunction when it is disabled. Disabling is intended for custom
	/// software minimizing unused resources; otherwise the savings are
	/// negligible.
	///
	/// reloadable: yes
	/// legacy: calculate_heroes
	#[serde(default = "true_fn")]
	pub calculate_heroes: bool,

	/// Answers a quiet room in legacy sync from a cheap probe instead of
	/// assembling its entry.
	///
	/// The probe covers the room's timeline, typing, read receipts, private
	/// read markers, room account data, notification counts and device-list
	/// changes. A room with a change in any of them is assembled in full as
	/// usual, so the response is the same. `fastpath_verify` takes precedence.
	///
	/// reloadable: yes
	#[serde(default)]
	pub fastpath: bool,

	/// Runs the quiet-room probe in legacy sync but always answers with the
	/// fully assembled entry.
	///
	/// A quiet verdict that the assembled entry contradicts is logged as an
	/// error and counted, so the probe can be checked against real traffic
	/// without any risk to clients. Takes precedence over `fastpath`.
	///
	/// reloadable: yes
	#[serde(default)]
	pub fastpath_verify: bool,

	/// Joined-room count from which a legacy sync that has only presence to
	/// report keeps waiting instead of answering.
	///
	/// Every presence update moves the global sync position, so an account in
	/// thousands of rooms is otherwise answered at once and rebuilt in full
	/// again and again, for presence of other users alone. With a value above
	/// 0, an incremental sync of an account with at least this many joined
	/// rooms whose response would hold nothing but presence continues to wait,
	/// as after an empty build, until a real change arrives or the timeout
	/// ends. The cost is that presence of other users does not reach such an
	/// account until something else in its rooms changes; presence of a
	/// request that ends this way is dropped, not delivered later. 0 disables
	/// the mode and leaves every response as it was; a value of 1000 is a
	/// reasonable start. The value is read on each sync request, so a
	/// configuration reload takes effect for the next request.
	///
	/// reloadable: yes
	/// default: 0
	#[serde(default)]
	pub presence_only_min_rooms: usize,

	/// Number of changes the legacy sync journal holds in memory.
	///
	/// The journal records each room that changes in any of the streams the
	/// `fastpath` probe reads. For now it only checks the probe: a room the
	/// probe finds changed that the journal holds no change for is logged and
	/// counted in `tuwunel_sync_journal_misses_total`, and no response depends
	/// on it. The oldest changes are dropped past this number, and a request
	/// older than what is left is not checked. 0 turns the journal off.
	///
	/// The journal is written whether or not `fastpath` is on. Each write takes
	/// a lock and clones a room ID, and 100000 changes take roughly 10 to 15 MB
	/// of memory (an estimate). One change of an account's keys is recorded
	/// once for every room the account is in, so it takes that many changes.
	///
	/// It is read once at startup, so changing it needs a restart.
	///
	/// default: 100000
	#[serde(default = "default_journal_capacity")]
	pub journal_capacity: usize,

	/// Joined-room count above which an initial legacy sync caps the timeline
	/// of every room.
	///
	/// An initial sync is one without `since` or with a stale one, see
	/// `initial_large_account_stale_gap`. An account with thousands of rooms
	/// cannot be answered in reasonable time with the full timeline the client
	/// asks for. Past this count the timeline limit is lowered to
	/// `initial_large_account_timeline_limit`; a room with more events is
	/// marked `limited` and carries a `prev_batch` token, so the client loads
	/// the earlier history when it opens the room. Only the first response of
	/// a request is capped, and syncs from a recent `since` are not affected.
	/// 0 disables the cap.
	///
	/// reloadable: yes
	/// default: 1000
	#[serde(default = "default_initial_large_account_rooms")]
	pub initial_large_account_rooms: usize,

	/// Highest timeline limit of a room in an initial legacy sync of an
	/// account above `initial_large_account_rooms`.
	///
	/// A smaller limit requested by the client is kept. A value of 0 is
	/// treated as 1, because a room with no events would have no `prev_batch`
	/// to load its history from.
	///
	/// reloadable: yes
	/// default: 1
	#[serde(default = "default_initial_large_account_timeline_limit")]
	pub initial_large_account_timeline_limit: usize,

	/// Number of changes by which a numeric `since` token may trail the
	/// server's current counter before legacy sync treats it as stale.
	///
	/// A client that keeps a token from long ago would otherwise be sent every
	/// change of every room of its account. The first response to a stale
	/// token gets the same timeline cap as an initial sync, for accounts above
	/// `initial_large_account_rooms`. 0 disables the check.
	///
	/// reloadable: yes
	/// default: 100000
	#[serde(default = "default_initial_large_account_stale_gap")]
	pub initial_large_account_stale_gap: u64,
}

impl Default for Sync {
	fn default() -> Self {
		Self {
			timeout_min: default_timeout_min(),
			timeout_default: default_timeout_default(),
			timeout_max: default_timeout_max(),
			calculate_heroes: true_fn(),
			fastpath: false,
			fastpath_verify: false,
			presence_only_min_rooms: 0,
			journal_capacity: default_journal_capacity(),
			initial_large_account_rooms: default_initial_large_account_rooms(),
			initial_large_account_timeline_limit: default_initial_large_account_timeline_limit(),
			initial_large_account_stale_gap: default_initial_large_account_stale_gap(),
		}
	}
}

fn default_timeout_min() -> u64 { 5000 }

fn default_journal_capacity() -> usize { 100_000 }

fn default_timeout_default() -> u64 { 30000 }

fn default_timeout_max() -> u64 { 90000 }

fn default_initial_large_account_rooms() -> usize { 1000 }

fn default_initial_large_account_timeline_limit() -> usize { 1 }

fn default_initial_large_account_stale_gap() -> u64 { 100_000 }
