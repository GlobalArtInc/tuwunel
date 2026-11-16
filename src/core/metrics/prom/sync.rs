use std::time::Duration;

use super::{
	family::EnumFamily,
	http::Api,
	types::{Counter, Gauge, Histogram, Metric, Seen, Timer},
	write::Writer,
};
use crate::label_set;

static SECONDS: [u64; 15] = [
	1_000_000,
	2_500_000,
	5_000_000,
	10_000_000,
	25_000_000,
	50_000_000,
	100_000_000,
	250_000_000,
	500_000_000,
	1_000_000_000,
	2_500_000_000,
	5_000_000_000,
	10_000_000_000,
	30_000_000_000,
	60_000_000_000,
];

static ROOMS: [u64; 15] =
	[0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000];

static WATCHERS: [u64; 10] = [20, 50, 100, 500, 1_000, 5_000, 10_000, 20_000, 50_000, 100_000];

static STATE_BYTES: [u64; 7] = [1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20, 1 << 22];

label_set! {
	/// Whether a sync request starts a stream or continues one.
	pub enum SinceKind("since") {
		/// The request carried no `since` token.
		Initial => "initial",
		/// The request carried a `since` token.
		Incremental => "incremental",
	}
}

label_set! {
	/// How a sync request was answered.
	pub enum Reply("outcome") {
		/// The response carries changes, or the request asked for full state.
		Data => "data",
		/// The wait for activity ended without changes.
		Empty => "empty",
		/// The `since` token came from another server and was answered with a
		/// fresh position.
		Foreign => "foreign",
	}
}

label_set! {
	/// What the fast path decided about one joined room of a sync build.
	pub enum Fastpath("fastpath") {
		/// The fast path did not run for the room.
		Off => "off",
		/// The room had no changes and was skipped.
		Quiet => "quiet",
		/// A new event was in the timeline.
		BusyTimeline => "busy_timeline",
		/// The room had no cached count of its newest event.
		BusyNoTimelineCount => "busy_no_timeline_count",
		/// Typing changed.
		BusyTyping => "busy_typing",
		/// The user's private read marker changed.
		BusyPrivateRead => "busy_private_read",
		/// Account data of the room changed.
		BusyAccountData => "busy_account_data",
		/// Read receipts changed.
		BusyReceipts => "busy_receipts",
		/// Notification counts changed.
		BusyNotifications => "busy_notifications",
		/// A check failed to read its data.
		BusyError => "busy_error",
	}
}

label_set! {
	/// Which pass of a sliding sync request ran.
	pub enum Pass("pass") {
		/// The first pass of the request, when nothing woke it before.
		First => "first",
		/// A pass that follows a wake: the watchers firing, the journal naming a
		/// change, or the wake of the gate that held the request before its first
		/// pass.
		Woken => "woken",
	}
}

label_set! {
	/// A part of a sliding sync pass, timed from its start to its end, not
	/// counting the wait for activity.
	pub enum V5Phase("phase") {
		/// Registering the watchers before the wait.
		Watch => "watch",
		/// Choosing the rooms and lists of the pass, with their membership.
		Select => "select",
		/// Collecting the ranges of the window rooms: the room payloads, receipts and
		/// room account data.
		Collect => "collect",
		/// Collecting the extensions other than the profiles.
		Extensions => "extensions",
		/// Collecting the profiles extension.
		Profiles => "profiles",
		/// Storing the connection state the response leaves behind.
		Store => "store",
	}
}

label_set! {
	/// How a sliding sync request was answered.
	pub enum V5Path("path") {
		/// By the gate, with no pass and no connection saved.
		Quiet => "quiet",
		/// After a pass, with the connection saved.
		Pass => "pass",
	}
}

label_set! {
	/// Where a sliding sync pass took the rooms of its lists from.
	pub enum SelectPath("path") {
		/// From the index of the connection, revised with the rooms the change
		/// journal names.
		Candidates => "candidates",
		/// From every room of the account.
		Full => "full",
	}
}

label_set! {
	/// Why a sliding sync pass took its rooms from every room of the account.
	pub enum SelectReason("reason") {
		/// The pass took the rooms from the index.
		None => "none",
		/// The switches are off or the journal is.
		Off => "off",
		/// The connection had no index yet.
		NoIndex => "no_index",
		/// The request repeated an earlier position.
		Replay => "replay",
		/// The lists or their filters changed since the index was built.
		Filters => "filters",
		/// A list filters by space, whose children no change names.
		Spaces => "spaces",
		/// The index was older than ten minutes.
		Refresh => "refresh",
		/// The journal no longer covers the position of the index.
		Horizon => "horizon",
		/// The journal names more rooms than the index is revised for.
		Overflow => "overflow",
		/// The global account data of the user changed.
		AccountData => "account_data",
		/// The index could not be revised because a read failed.
		Error => "error",
	}
}

label_set! {
	/// Which part of a sliding sync answer differed between the index and the full
	/// pass.
	pub enum Mismatch("part") {
		/// The rooms of the window, or what the window holds of one.
		Window => "window",
		/// The count of a list.
		Count => "count",
		/// A room outside the rooms the index collects the ranges of, for which the
		/// full pass collected a payload, receipts or account data.
		Range => "range",
	}
}

label_set! {
	/// A part of a v3 sync build, timed by the time spent polling it.
	pub enum Phase("phase") {
		/// Joined rooms.
		Joined => "joined",
		/// Left rooms.
		Left => "left",
		/// Invited and knocked rooms.
		Invited => "invited",
		/// Presence updates.
		Presence => "presence",
		/// Global account data.
		AccountData => "account_data",
		/// Device keys, to-device events and one-time key counts.
		E2ee => "e2ee",
		/// Device lists of users who left and user profiles.
		Profiles => "profiles",
	}
}

label_set! {
	/// Which half of the watcher lifecycle of a v3 sync request was timed.
	pub enum WatchPart("part") {
		/// Registering the watchers before the wait.
		Register => "register",
		/// Releasing the watchers once the wait ended.
		Release => "release",
	}
}

label_set! {
	/// Whether a v3 sync build had changes to send.
	pub enum Build("outcome") {
		/// The build had changes.
		Data => "data",
		/// The build had no changes.
		Empty => "empty",
		/// The build carried only presence and the account is above
		/// `presence_only_min_rooms`, so the request kept waiting.
		PresenceOnly => "presence_only",
	}
}

label_set! {
	/// A section of a v3 sync response that carries data.
	pub enum Section("section") {
		/// Joined, left, invited or knocked rooms.
		Rooms => "rooms",
		/// Presence updates.
		Presence => "presence",
		/// Global account data.
		AccountData => "account_data",
		/// Device list changes and departures.
		DeviceLists => "device_lists",
		/// To-device events.
		ToDevice => "to_device",
		/// User profiles.
		Users => "users",
	}
}

label_set! {
	/// Where a v3 build took the joined rooms to look at from.
	pub enum BuildPath("path") {
		/// Only the rooms the change journal and typing named.
		Candidates => "candidates",
		/// Every room the account has joined.
		Scan => "scan",
	}
}

label_set! {
	/// A kind of change the sync journal records, and the kind of a write the
	/// fast path found that the journal failed to account for.
	pub enum JournalKind("kind") {
		/// A new event was in the timeline.
		Timeline => "timeline",
		/// Typing changed.
		Typing => "typing",
		/// The user's private read marker changed.
		PrivateRead => "private_read",
		/// Account data of the room changed.
		AccountData => "account_data",
		/// Read receipts changed.
		Receipts => "receipts",
		/// Notification counts changed.
		Notifications => "notifications",
		/// A user's device keys changed in the room.
		DeviceLists => "device_lists",
		/// A local user's membership in a room changed.
		Membership => "membership",
		/// A user's profile changed.
		Profile => "profile",
		/// Account data outside any room changed.
		GlobalAccountData => "global_account_data",
		/// A user's own device keys changed.
		OwnKeys => "own_keys",
		/// A user's one-time key counts changed.
		OneTimeKeys => "one_time_keys",
		/// A to-device event arrived for a device.
		ToDevice => "to_device",
	}
}

label_set! {
	/// How a wait on the sync journal found out whether the user is in a room.
	pub enum Lookup("how") {
		/// One room was looked up on its own.
		Room => "room",
		/// The list of every room the user is in was loaded.
		Set => "set",
	}
}

label_set! {
	/// The verdict of one shadow comparison of a wait on the sync journal with
	/// the database watchers.
	pub enum Verdict("verdict") {
		/// The watchers and the journal both saw a change.
		Both => "both",
		/// Only the watchers saw a change, so the journal would have missed it.
		Miss => "miss",
		/// The watchers fired, and the journal holds no change for the user.
		WatcherSpurious => "watcher_spurious",
		/// The journal saw a change, and the watchers did not fire.
		JournalExtra => "journal_extra",
		/// Neither the watchers nor the journal saw a change.
		Quiet => "quiet",
	}
}

label_set! {
	/// The point of a sliding sync request at which the shadow comparison of a
	/// wait on the sync journal was made.
	pub enum WaitStage("stage") {
		/// After the first pass of a request the gate would have let wait.
		Gate => "gate",
		/// After the pass that followed a wake of the database watchers.
		Woken => "woken",
		/// After the wait ran out of time.
		Timeout => "timeout",
	}
}

label_set! {
	/// Why a long poll that waits through the sync journal ended.
	pub enum WakeReason("reason") {
		/// A change in the journal woke the poll.
		Journal => "journal",
		/// The journal dropped changes after the point the poll started from.
		Horizon => "horizon",
		/// The timeout of the request ran out.
		Timeout => "timeout",
		/// The server is stopping.
		Shutdown => "shutdown",
	}
}

label_set! {
	/// Why the comparison of the index of a sliding sync connection with the full
	/// pass did not judge every difference it found.
	pub enum Skipped("reason") {
		/// The journal could not say what was written after the pass, so no
		/// difference was judged.
		Journal => "journal",
		/// The journal names a write after the pass for a room or a count that
		/// differs, so the difference was set aside.
		Explained => "explained",
	}
}

/// Metrics of the sync endpoints.
pub struct SyncMetrics {
	v3_build: EnumFamily<SinceKind, Histogram<15>>,
	v3_responses: EnumFamily<(SinceKind, Reply), Counter>,
	v3_rooms: EnumFamily<Fastpath, Counter>,
	v3_rooms_per_sync: Histogram<15>,
	v3_phase: EnumFamily<Phase, Histogram<15>>,
	v3_watch: EnumFamily<WatchPart, Histogram<15>>,
	v3_watchers_per_sync: Histogram<10>,
	v3_build_outcomes: EnumFamily<Build, Counter>,
	v3_sections: EnumFamily<Section, Counter>,
	v3_left_rooms_per_sync: Histogram<15>,
	v3_build_paths: EnumFamily<BuildPath, Counter>,
	v3_candidates_per_sync: Histogram<15>,
	fastpath_mismatches: Counter,
	candidates_misses: Counter,
	journal_entries: Gauge,
	journal_evictions: Counter,
	journal_records: EnumFamily<JournalKind, Counter>,
	journal_misses: EnumFamily<JournalKind, Counter>,
	wait_scanned: Counter,
	wait_lookups: EnumFamily<Lookup, Counter>,
	wait_verify: EnumFamily<Verdict, Counter>,
	wait_wakes: EnumFamily<WakeReason, Counter>,
	long_polls: EnumFamily<Api, Gauge>,
	v5_wait_verify: EnumFamily<(WaitStage, Verdict), Counter>,
	v5_wait_wakes: EnumFamily<WakeReason, Counter>,
	v5_pass: EnumFamily<Pass, Histogram<15>>,
	v5_phase: EnumFamily<V5Phase, Histogram<15>>,
	v5_requests: EnumFamily<V5Path, Counter>,
	v5_fresh_payloads: Histogram<15>,
	v5_ranges_per_pass: Histogram<15>,
	v5_select_paths: EnumFamily<(SelectPath, SelectReason), Counter>,
	v5_candidates_per_pass: Histogram<15>,
	v5_candidates_mismatches: EnumFamily<Mismatch, Counter>,
	v5_candidates_skipped: EnumFamily<Skipped, Counter>,
	v5_connections_loaded: Gauge,
	v5_connections_stored: Seen<Gauge>,
	v5_state_bytes: Histogram<7>,
	v5_store_bytes: Seen<Gauge>,
	v5_response_rooms: Histogram<15>,
	v5_unknown_pos: Counter,
}

/// A request waiting for activity, counted in the long-poll gauge until the
/// guard is dropped, whether the wait ended or its future was cancelled.
#[must_use = "the request leaves the long-poll gauge when the guard is dropped"]
pub struct LongPoll<'a> {
	gauge: &'a Gauge,
}

impl SyncMetrics {
	#[must_use]
	/// Creates the group with every metric at zero.
	pub fn new() -> Self {
		Self {
			v3_build: EnumFamily::new(|| Histogram::seconds(&SECONDS)),
			v3_responses: EnumFamily::new(Counter::new),
			v3_rooms: EnumFamily::new(Counter::new),
			v3_rooms_per_sync: Histogram::new(&ROOMS),
			v3_phase: EnumFamily::new(|| Histogram::seconds(&SECONDS)),
			v3_watch: EnumFamily::new(|| Histogram::seconds(&SECONDS)),
			v3_watchers_per_sync: Histogram::new(&WATCHERS),
			v3_build_outcomes: EnumFamily::new(Counter::new),
			v3_sections: EnumFamily::new(Counter::new),
			v3_left_rooms_per_sync: Histogram::new(&ROOMS),
			v3_build_paths: EnumFamily::new(Counter::new),
			v3_candidates_per_sync: Histogram::new(&ROOMS),
			fastpath_mismatches: Counter::new(),
			candidates_misses: Counter::new(),
			journal_entries: Gauge::new(),
			journal_evictions: Counter::new(),
			journal_records: EnumFamily::new(Counter::new),
			journal_misses: EnumFamily::new(Counter::new),
			wait_scanned: Counter::new(),
			wait_lookups: EnumFamily::new(Counter::new),
			wait_verify: EnumFamily::new(Counter::new),
			wait_wakes: EnumFamily::new(Counter::new),
			long_polls: EnumFamily::new(Gauge::new),
			v5_wait_verify: EnumFamily::new(Counter::new),
			v5_wait_wakes: EnumFamily::new(Counter::new),
			v5_pass: EnumFamily::new(|| Histogram::seconds(&SECONDS)),
			v5_phase: EnumFamily::new(|| Histogram::seconds(&SECONDS)),
			v5_requests: EnumFamily::new(Counter::new),
			v5_fresh_payloads: Histogram::new(&ROOMS),
			v5_ranges_per_pass: Histogram::new(&ROOMS),
			v5_select_paths: EnumFamily::new(Counter::new),
			v5_candidates_per_pass: Histogram::new(&ROOMS),
			v5_candidates_mismatches: EnumFamily::new(Counter::new),
			v5_candidates_skipped: EnumFamily::new(Counter::new),
			v5_connections_loaded: Gauge::new(),
			v5_connections_stored: Seen::new(Gauge::new()),
			v5_state_bytes: Histogram::new(&STATE_BYTES),
			v5_store_bytes: Seen::new(Gauge::new()),
			v5_response_rooms: Histogram::new(&ROOMS),
			v5_unknown_pos: Counter::new(),
		}
	}

	#[inline]
	/// Starts timing the computation of a v3 response; the time is recorded
	/// when the timer is dropped.
	pub fn v3_build(&self, since: SinceKind) -> Timer<'_, 15> { self.v3_build.get(since).start() }

	#[inline]
	/// Counts one v3 response.
	pub fn v3_answered(&self, since: SinceKind, reply: Reply) {
		self.v3_responses.get((since, reply)).inc();
	}

	#[inline]
	/// Counts `rooms` joined rooms that got the same fast path verdict.
	pub fn v3_rooms(&self, fastpath: Fastpath, rooms: u64) {
		self.v3_rooms.get(fastpath).add(rooms);
	}

	#[inline]
	/// Records how many joined rooms one v3 build looked at.
	pub fn v3_rooms_per_sync(&self, rooms: u64) { self.v3_rooms_per_sync.observe(rooms); }

	#[inline]
	/// Records the time one part of a v3 build spent polling.
	pub fn v3_phase(&self, phase: Phase, busy: Duration) {
		self.v3_phase.get(phase).observe_duration(busy);
	}

	#[inline]
	/// Records the time one half of the watcher lifecycle of a v3 request took.
	pub fn v3_watch(&self, part: WatchPart, elapsed: Duration) {
		self.v3_watch.get(part).observe_duration(elapsed);
	}

	#[inline]
	/// Records how many watchers one v3 request registered.
	pub fn v3_watchers_per_sync(&self, watchers: usize) {
		self.v3_watchers_per_sync
			.observe(u64::try_from(watchers).unwrap_or(u64::MAX));
	}

	#[inline]
	/// Counts one v3 build by whether it had changes to send.
	pub fn v3_build_outcome(&self, build: Build) { self.v3_build_outcomes.get(build).inc(); }

	#[inline]
	/// Counts one section that carries data in a v3 response.
	pub fn v3_section(&self, section: Section) { self.v3_sections.get(section).inc(); }

	#[inline]
	/// Records how many left rooms one v3 build walked, before the room filter.
	pub fn v3_left_rooms_per_sync(&self, rooms: u64) {
		self.v3_left_rooms_per_sync.observe(rooms);
	}

	#[inline]
	/// Counts one v3 build by where it took its joined rooms from.
	pub fn v3_build_path(&self, path: BuildPath) { self.v3_build_paths.get(path).inc(); }

	#[inline]
	/// Records how many candidate rooms one v3 build took its joined rooms
	/// from.
	pub fn v3_candidates_per_sync(&self, rooms: u64) {
		self.v3_candidates_per_sync.observe(rooms);
	}

	#[inline]
	/// Counts a room where the verified fast path disagreed with the full
	/// computation.
	pub fn fastpath_mismatch(&self) { self.fastpath_mismatches.inc(); }

	#[inline]
	/// Counts a room that a full walk answered with changes and that the
	/// candidates did not hold.
	pub fn candidates_miss(&self) { self.candidates_misses.inc(); }

	/// Stores the number of changes the sync journal holds.
	pub fn set_journal_entries(&self, entries: u64) {
		self.journal_entries.set_unsigned(entries);
	}

	#[inline]
	/// Counts changes the sync journal dropped to stay within its capacity.
	pub fn journal_evicted(&self, entries: u64) { self.journal_evictions.add(entries); }

	#[inline]
	/// Counts one change of `kind` stored in the sync journal.
	pub fn journal_recorded(&self, kind: JournalKind) { self.journal_records.get(kind).inc(); }

	#[inline]
	/// Counts a room where the fast path found a change that the sync journal
	/// did not hold.
	pub fn journal_miss(&self, kind: JournalKind) { self.journal_misses.get(kind).inc(); }

	#[inline]
	/// Counts `entries` changes of the sync journal that a wait passed over.
	pub fn wait_scanned(&self, entries: u64) { self.wait_scanned.add(entries); }

	#[inline]
	/// Counts one way a wait on the sync journal found out whether the user is in
	/// a room.
	pub fn wait_lookup(&self, how: Lookup) { self.wait_lookups.get(how).inc(); }

	#[inline]
	/// Counts the verdict of one shadow comparison of a wait on the sync journal
	/// with the database watchers.
	pub fn wait_verified(&self, verdict: Verdict) { self.wait_verify.get(verdict).inc(); }

	#[inline]
	/// Counts why a long poll that waits through the sync journal ended.
	pub fn wait_woken(&self, reason: WakeReason) { self.wait_wakes.get(reason).inc(); }

	#[inline]
	/// Counts the verdict of one shadow comparison of a sliding sync wait on the
	/// sync journal with the database watchers, at a stage of the request.
	pub fn v5_wait_verified(&self, stage: WaitStage, verdict: Verdict) {
		self.v5_wait_verify.get((stage, verdict)).inc();
	}

	#[inline]
	/// Counts why a sliding sync long poll that waits through the sync journal
	/// ended.
	pub fn v5_wait_woken(&self, reason: WakeReason) { self.v5_wait_wakes.get(reason).inc(); }

	#[inline]
	/// Counts a request as waiting for activity until the guard is dropped.
	pub fn long_poll(&self, api: Api) -> LongPoll<'_> {
		let gauge = self.long_polls.get(api);
		gauge.inc();

		LongPoll { gauge }
	}

	#[inline]
	/// Starts timing one pass of a sliding sync request; the time is recorded
	/// when the timer is dropped.
	pub fn v5_pass(&self, pass: Pass) -> Timer<'_, 15> { self.v5_pass.get(pass).start() }

	#[inline]
	/// Starts timing one phase of a sliding sync pass; the time is recorded
	/// when the timer is dropped.
	pub fn v5_phase(&self, phase: V5Phase) -> Timer<'_, 15> { self.v5_phase.get(phase).start() }

	#[inline]
	/// Counts a sliding sync request as answered by the gate or after a pass.
	pub fn v5_request(&self, path: V5Path) { self.v5_requests.get(path).inc(); }

	#[inline]
	/// Records how many rooms one sliding sync pass built a payload for anew.
	pub fn v5_fresh_payloads(&self, rooms: u64) { self.v5_fresh_payloads.observe(rooms); }

	#[inline]
	/// Records how many rooms of its window one sliding sync pass collected the
	/// ranges of.
	pub fn v5_ranges(&self, rooms: u64) { self.v5_ranges_per_pass.observe(rooms); }

	#[inline]
	/// Counts a sliding sync pass by where it took its rooms from and, for the full
	/// walk, why.
	pub fn v5_selected(&self, path: SelectPath, reason: SelectReason) {
		self.v5_select_paths.get((path, reason)).inc();
	}

	#[inline]
	/// Records how many rooms the journal had one sliding sync pass revise in the
	/// index of its connection.
	pub fn v5_candidates(&self, rooms: u64) { self.v5_candidates_per_pass.observe(rooms); }

	#[inline]
	/// Counts a difference between the index of a sliding sync connection and the
	/// full pass.
	pub fn v5_candidates_mismatch(&self, part: Mismatch) {
		self.v5_candidates_mismatches.get(part).inc();
	}

	#[inline]
	/// Counts a comparison of the index of a sliding sync connection with the full
	/// pass that did not judge every difference it found.
	pub fn v5_candidates_skip(&self, reason: Skipped) {
		self.v5_candidates_skipped.get(reason).inc();
	}

	#[inline]
	/// Counts a sliding sync request answered with an unknown position.
	pub fn v5_unknown_pos(&self) { self.v5_unknown_pos.inc(); }

	#[inline]
	/// Records the size of a sliding sync connection as it was stored.
	pub fn v5_state(&self, bytes: u64) { self.v5_state_bytes.observe(bytes); }

	#[inline]
	/// Records how many rooms one sliding sync response carries.
	pub fn v5_response_rooms(&self, rooms: u64) { self.v5_response_rooms.observe(rooms); }

	/// Stores the number of sliding sync connections held in memory.
	pub fn set_v5_connections_loaded(&self, connections: u64) {
		self.v5_connections_loaded
			.set_unsigned(connections);
	}

	/// Stores the number of sliding sync connections kept in the database.
	pub fn set_v5_connections_stored(&self, connections: u64) {
		self.v5_connections_stored
			.publish(|gauge| gauge.set_unsigned(connections));
	}

	/// Stores the bytes the database holds for sliding sync connections.
	pub fn set_v5_store_bytes(&self, bytes: u64) {
		self.v5_store_bytes
			.publish(|gauge| gauge.set_unsigned(bytes));
	}

	#[must_use]
	/// Returns the number of rooms counted for a verdict.
	pub fn v3_rooms_counted(&self, fastpath: Fastpath) -> u64 {
		self.v3_rooms.get(fastpath).get()
	}

	#[must_use]
	/// Returns the number of v3 responses counted for a kind and a reply.
	pub fn v3_answers(&self, since: SinceKind, reply: Reply) -> u64 {
		self.v3_responses.get((since, reply)).get()
	}

	#[must_use]
	/// Returns the number of v3 builds timed for a kind.
	pub fn v3_builds(&self, since: SinceKind) -> u64 { self.v3_build.get(since).count() }

	#[must_use]
	/// Returns the number of busy time observations recorded for a part of a v3
	/// build.
	pub fn v3_phases(&self, phase: Phase) -> u64 { self.v3_phase.get(phase).count() }

	#[must_use]
	/// Returns the number of watcher lifecycle times recorded for a half of it.
	pub fn v3_watches(&self, part: WatchPart) -> u64 { self.v3_watch.get(part).count() }

	#[must_use]
	/// Returns the number of v3 requests whose watchers were counted.
	pub fn v3_watcher_syncs(&self) -> u64 { self.v3_watchers_per_sync.count() }

	#[must_use]
	/// Returns the number of v3 builds counted for an outcome.
	pub fn v3_outcomes(&self, build: Build) -> u64 { self.v3_build_outcomes.get(build).get() }

	#[must_use]
	/// Returns the number of sections with data counted.
	pub fn v3_sections_counted(&self, section: Section) -> u64 {
		self.v3_sections.get(section).get()
	}

	#[must_use]
	/// Returns the number of v3 builds whose left rooms were counted.
	pub fn v3_left_syncs(&self) -> u64 { self.v3_left_rooms_per_sync.count() }

	#[must_use]
	/// Returns the number of v3 builds counted for a path.
	pub fn v3_build_paths_counted(&self, path: BuildPath) -> u64 {
		self.v3_build_paths.get(path).get()
	}

	#[must_use]
	/// Returns the number of v3 builds whose candidate rooms were counted.
	pub fn v3_candidate_syncs(&self) -> u64 { self.v3_candidates_per_sync.count() }

	#[must_use]
	/// Returns the number of fast path mismatches counted.
	pub fn fastpath_mismatches(&self) -> u64 { self.fastpath_mismatches.get() }

	#[must_use]
	/// Returns the number of candidate misses counted.
	pub fn candidates_missed(&self) -> u64 { self.candidates_misses.get() }

	#[must_use]
	/// Returns the number of changes the sync journal holds.
	pub fn journal_held(&self) -> i64 { self.journal_entries.get() }

	#[must_use]
	/// Returns the number of changes the sync journal dropped.
	pub fn journal_evicted_total(&self) -> u64 { self.journal_evictions.get() }

	#[must_use]
	/// Returns the number of changes stored in the sync journal for a kind.
	pub fn journal_recorded_total(&self, kind: JournalKind) -> u64 {
		self.journal_records.get(kind).get()
	}

	#[must_use]
	/// Returns the number of journal misses counted for a kind.
	pub fn journal_missed(&self, kind: JournalKind) -> u64 { self.journal_misses.get(kind).get() }

	#[must_use]
	/// Returns the number of journal changes waits have passed over.
	pub fn wait_scanned_total(&self) -> u64 { self.wait_scanned.get() }

	#[must_use]
	/// Returns the number of membership lookups waits have made in a way.
	pub fn wait_lookups_total(&self, how: Lookup) -> u64 { self.wait_lookups.get(how).get() }

	#[must_use]
	/// Returns the number of shadow verdicts counted for a verdict.
	pub fn wait_verified_total(&self, verdict: Verdict) -> u64 {
		self.wait_verify.get(verdict).get()
	}

	#[must_use]
	/// Returns the number of long polls that ended for a reason.
	pub fn wait_woken_total(&self, reason: WakeReason) -> u64 {
		self.wait_wakes.get(reason).get()
	}

	#[must_use]
	/// Returns the number of shadow verdicts of sliding sync waits counted for a
	/// stage and a verdict.
	pub fn v5_wait_verified_total(&self, stage: WaitStage, verdict: Verdict) -> u64 {
		self.v5_wait_verify.get((stage, verdict)).get()
	}

	#[must_use]
	/// Returns the number of sliding sync long polls that ended for a reason.
	pub fn v5_wait_woken_total(&self, reason: WakeReason) -> u64 {
		self.v5_wait_wakes.get(reason).get()
	}

	#[must_use]
	/// Returns the number of requests waiting for activity on an API.
	pub fn long_polling(&self, api: Api) -> i64 { self.long_polls.get(api).get() }

	#[must_use]
	/// Returns the number of sliding sync passes timed.
	pub fn v5_passes(&self, pass: Pass) -> u64 { self.v5_pass.get(pass).count() }

	#[must_use]
	/// Returns the number of sliding sync phases timed for a phase.
	pub fn v5_phases(&self, phase: V5Phase) -> u64 { self.v5_phase.get(phase).count() }

	#[must_use]
	/// Returns the number of sliding sync requests answered by a path.
	pub fn v5_requests_total(&self, path: V5Path) -> u64 { self.v5_requests.get(path).get() }

	#[must_use]
	/// Returns the number of sliding sync passes whose rebuilt rooms were counted.
	pub fn v5_fresh_payload_passes(&self) -> u64 { self.v5_fresh_payloads.count() }

	#[must_use]
	/// Returns the number of sliding sync passes whose collected rooms were counted.
	pub fn v5_range_passes(&self) -> u64 { self.v5_ranges_per_pass.count() }

	#[must_use]
	/// Returns the number of sliding sync passes counted for a path and a reason.
	pub fn v5_selected_total(&self, path: SelectPath, reason: SelectReason) -> u64 {
		self.v5_select_paths.get((path, reason)).get()
	}

	#[must_use]
	/// Returns the number of sliding sync passes whose revised rooms were counted.
	pub fn v5_candidate_passes(&self) -> u64 { self.v5_candidates_per_pass.count() }

	#[must_use]
	/// Returns the number of index mismatches counted for a part.
	pub fn v5_candidates_mismatched_total(&self, part: Mismatch) -> u64 {
		self.v5_candidates_mismatches.get(part).get()
	}

	#[must_use]
	/// Returns the number of index comparisons that skipped differences for a
	/// reason.
	pub fn v5_candidates_skipped_total(&self, reason: Skipped) -> u64 {
		self.v5_candidates_skipped.get(reason).get()
	}

	#[must_use]
	/// Returns the number of unknown position answers counted.
	pub fn v5_unknown_positions(&self) -> u64 { self.v5_unknown_pos.get() }

	#[must_use]
	/// Returns the number of connection sizes recorded.
	pub fn v5_states(&self) -> u64 { self.v5_state_bytes.count() }

	#[must_use]
	/// Returns the number of sliding sync responses recorded by room count.
	pub fn v5_responses(&self) -> u64 { self.v5_response_rooms.count() }

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_sync_v3_build_seconds",
			"Time to compute one v3 sync response, not counting the wait for activity.",
			&self.v3_build,
		);
		w.metric(
			"tuwunel_sync_v3_responses_total",
			"v3 sync responses by kind of request and how they were answered.",
			&self.v3_responses,
		);
		w.metric(
			"tuwunel_sync_v3_rooms_total",
			"Joined rooms handled by v3 sync builds, by the verdict of the fast path.",
			&self.v3_rooms,
		);
		w.metric(
			"tuwunel_sync_v3_rooms_per_sync",
			"Joined rooms handled by one v3 sync build.",
			&self.v3_rooms_per_sync,
		);
		w.metric(
			"tuwunel_sync_v3_phase_seconds",
			"Time one part of a v3 sync build spent polling, by part; the time is the sum of \
			 the polls of that part, not the wall time, so parts run in one task do not \
			 overlap. The e2ee and profiles parts record each of their futures, so they add \
			 more than one observation per build.",
			&self.v3_phase,
		);
		w.metric(
			"tuwunel_sync_v3_watch_seconds",
			"Time to register the watchers of one v3 sync request, and to release them once the \
			 wait ended, by part; a request stopped by shutdown releases nothing.",
			&self.v3_watch,
		);
		w.metric(
			"tuwunel_sync_v3_watchers_per_sync",
			"Watchers registered by one v3 sync request.",
			&self.v3_watchers_per_sync,
		);
		w.metric(
			"tuwunel_sync_v3_builds_total",
			"v3 sync builds by outcome: data when the build had changes to send, empty when it \
			 had none, presence_only when it carried only presence and the request kept \
			 waiting; a build repeated within one request counts again.",
			&self.v3_build_outcomes,
		);
		w.metric(
			"tuwunel_sync_v3_response_sections_total",
			"Sections with data, counted in the v3 sync responses that carry data; one response \
			 can count several sections.",
			&self.v3_sections,
		);
		w.metric(
			"tuwunel_sync_v3_left_rooms_per_sync",
			"Left rooms one v3 sync build walked, before the room filter.",
			&self.v3_left_rooms_per_sync,
		);
		w.metric(
			"tuwunel_sync_v3_build_paths_total",
			"v3 sync builds by where they took their joined rooms from: candidates when only \
			 the rooms named by the change journal and typing were looked at, scan when every \
			 joined room was.",
			&self.v3_build_paths,
		);
		w.metric(
			"tuwunel_sync_v3_candidates_per_sync",
			"Candidate rooms one v3 sync build took its joined rooms from, after the room \
			 filter and before the membership check.",
			&self.v3_candidates_per_sync,
		);
		w.metric(
			"tuwunel_sync_fastpath_mismatches_total",
			"Rooms where the verified sync fast path disagreed with the full computation.",
			&self.fastpath_mismatches,
		);
		w.metric(
			"tuwunel_sync_candidates_misses_total",
			"Rooms a full v3 walk answered with changes that the candidate rooms did not hold; \
			 any count above zero means a change the journal and typing do not name.",
			&self.candidates_misses,
		);
		self.write_journal(w);
		w.metric(
			"tuwunel_sync_long_polls",
			"Sync requests waiting for activity, by API version.",
			&self.long_polls,
		);
		w.metric(
			"tuwunel_sync_v5_pass_seconds",
			"Time of one pass of a sliding sync request, not counting the wait for activity; \
			 first is the opening pass, woken one after a wake of the watchers, journal or gate.",
			&self.v5_pass,
		);
		w.metric(
			"tuwunel_sync_v5_phase_seconds",
			"Time one part of a sliding sync pass spent running, by phase: watch registers the \
			 watchers, select picks the rooms and lists, collect gathers the ranges of the \
			 window rooms, extensions gathers the extensions other than the profiles, profiles \
			 gathers the profiles extension, store saves the connection; collect and extensions \
			 run side by side, so their times overlap; not counting the wait for activity.",
			&self.v5_phase,
		);
		w.metric(
			"tuwunel_sync_v5_requests_total",
			"Sliding sync requests by how they were answered: quiet by the gate with no pass \
			 and no connection saved, pass after a pass.",
			&self.v5_requests,
		);
		w.metric(
			"tuwunel_sync_v5_fresh_payloads_per_pass",
			"Rooms one sliding sync pass built a payload for anew, counted for the passes that \
			 collect ranges.",
			&self.v5_fresh_payloads,
		);
		w.metric(
			"tuwunel_sync_v5_connections_loaded",
			"Sliding sync connections held in memory.",
			&self.v5_connections_loaded,
		);

		if self.v5_connections_stored.is_seen() {
			w.metric(
				"tuwunel_sync_v5_connections_stored",
				"Sliding sync connections kept in the database, counted by a periodic scan.",
				&self.v5_connections_stored,
			);
		}

		w.metric(
			"tuwunel_sync_v5_connection_state_bytes",
			"Size of a sliding sync connection as stored after a response; the empty state \
			 stored when a client resets its connection is not counted.",
			&self.v5_state_bytes,
		);

		if self.v5_store_bytes.is_seen() {
			w.metric(
				"tuwunel_sync_v5_store_bytes",
				"Bytes of table files the sliding sync connection column occupies, without \
				 memtables and including superseded versions of rows until compaction.",
				&self.v5_store_bytes,
			);
		}

		w.metric(
			"tuwunel_sync_v5_response_rooms",
			"Rooms in one sliding sync response.",
			&self.v5_response_rooms,
		);
		w.metric(
			"tuwunel_sync_v5_unknown_pos_total",
			"Sliding sync requests answered with an unknown position.",
			&self.v5_unknown_pos,
		);
	}

	fn write_candidates(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_sync_v5_select_paths_total",
			"Sliding sync passes by where they took their rooms from: candidates for the index \
			 of the connection revised with the journal, full for every room of the account, \
			 with the reason for the full walk.",
			&self.v5_select_paths,
		);
		w.metric(
			"tuwunel_sync_v5_ranges_per_pass",
			"Rooms of its window one sliding sync pass collected the ranges of, counted for the \
			 passes that collect ranges: every room of the window, or the rooms the pass owes \
			 when it takes its rooms from the index.",
			&self.v5_ranges_per_pass,
		);
		w.metric(
			"tuwunel_sync_v5_candidates_per_pass",
			"Rooms the journal had one sliding sync pass revise in the index of its connection, \
			 after leaving out the rooms the account has nothing to do with.",
			&self.v5_candidates_per_pass,
		);
		w.metric(
			"tuwunel_sync_v5_candidates_mismatch_total",
			"Differences between the index of a sliding sync connection and the full pass, by \
			 part, that no write after the pass explains; any count above zero means a change \
			 the journal does not name, or a room the index judges otherwise than the full pass \
			 does.",
			&self.v5_candidates_mismatches,
		);
		w.metric(
			"tuwunel_sync_v5_candidates_verify_skipped_total",
			"Comparisons of the index of a sliding sync connection with the full pass that did \
			 not judge every difference, by reason: journal when it could not say what was \
			 written after the pass, explained when a write after the pass accounts for a \
			 difference and it was set aside; neither is a mismatch.",
			&self.v5_candidates_skipped,
		);
	}

	fn write_journal(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_sync_journal_entries",
			"Changes the sync journal holds in memory, zero when the journal is off.",
			&self.journal_entries,
		);
		w.metric(
			"tuwunel_sync_journal_evictions_total",
			"Changes the sync journal dropped to stay within its capacity.",
			&self.journal_evictions,
		);
		w.metric(
			"tuwunel_sync_journal_recorded_total",
			"Changes stored in the sync journal, by kind, one per room of a write.",
			&self.journal_records,
		);
		w.metric(
			"tuwunel_sync_journal_misses_total",
			"Rooms where the fast path found a change that the sync journal did not hold, by \
			 verdict; any count above zero means a write the journal does not record.",
			&self.journal_misses,
		);
		w.metric(
			"tuwunel_sync_v3_wait_scanned_total",
			"Changes of the sync journal that waits on it passed over.",
			&self.wait_scanned,
		);
		w.metric(
			"tuwunel_sync_v3_wait_lookups_total",
			"Lookups of a user's membership by waits on the sync journal, by how: room for one \
			 room looked up on its own, set for the list of every room the user is in loaded.",
			&self.wait_lookups,
		);
		w.metric(
			"tuwunel_sync_v3_wait_verify_total",
			"Verdicts of the shadow comparison of the sync journal with the database watchers, \
			 by verdict; miss means the journal would have let a long poll sleep through a \
			 change.",
			&self.wait_verify,
		);
		w.metric(
			"tuwunel_sync_v3_wait_wakes_total",
			"Why a long poll that waits through the sync journal ended, by reason.",
			&self.wait_wakes,
		);
		w.metric(
			"tuwunel_sync_v5_wait_verify_total",
			"Verdicts of the shadow comparison of the sync journal with the database watchers \
			 for sliding sync, by stage and verdict; miss means the journal would have let a \
			 long poll sleep through a change.",
			&self.v5_wait_verify,
		);
		w.metric(
			"tuwunel_sync_v5_wait_wakes_total",
			"Why a sliding sync long poll that waits through the sync journal ended, by reason.",
			&self.v5_wait_wakes,
		);
		self.write_candidates(w);
	}

	pub(super) fn series(&self) -> usize {
		[
			self.v3_build.series(),
			self.v3_responses.series(),
			self.v3_rooms.series(),
			self.v3_rooms_per_sync.series(),
			self.v3_phase.series(),
			self.v3_watch.series(),
			self.v3_watchers_per_sync.series(),
			self.v3_build_outcomes.series(),
			self.v3_sections.series(),
			self.v3_left_rooms_per_sync.series(),
			self.v3_build_paths.series(),
			self.v3_candidates_per_sync.series(),
			self.fastpath_mismatches.series(),
			self.candidates_misses.series(),
			self.journal_entries.series(),
			self.journal_evictions.series(),
			self.journal_records.series(),
			self.journal_misses.series(),
			self.wait_scanned.series(),
			self.wait_lookups.series(),
			self.wait_verify.series(),
			self.wait_wakes.series(),
			self.long_polls.series(),
			self.v5_wait_verify.series(),
			self.v5_wait_wakes.series(),
			self.v5_pass.series(),
			self.v5_phase.series(),
			self.v5_requests.series(),
			self.v5_fresh_payloads.series(),
			self.v5_ranges_per_pass.series(),
			self.v5_select_paths.series(),
			self.v5_candidates_per_pass.series(),
			self.v5_candidates_mismatches.series(),
			self.v5_candidates_skipped.series(),
			self.v5_connections_loaded.series(),
			self.v5_connections_stored.series(),
			self.v5_state_bytes.series(),
			self.v5_store_bytes.series(),
			self.v5_response_rooms.series(),
			self.v5_unknown_pos.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}

impl Default for SyncMetrics {
	fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for SyncMetrics {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SyncMetrics")
			.finish_non_exhaustive()
	}
}

impl Drop for LongPoll<'_> {
	fn drop(&mut self) { self.gauge.dec(); }
}
