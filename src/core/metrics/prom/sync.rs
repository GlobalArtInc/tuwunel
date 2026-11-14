use super::{
	family::EnumFamily,
	http::Api,
	types::{Counter, Gauge, Histogram, Metric, Timer},
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

/// Metrics of the sync endpoints.
pub struct SyncMetrics {
	v3_build: EnumFamily<SinceKind, Histogram<15>>,
	v3_responses: EnumFamily<(SinceKind, Reply), Counter>,
	v3_rooms: EnumFamily<Fastpath, Counter>,
	v3_rooms_per_sync: Histogram<15>,
	fastpath_mismatches: Counter,
	long_polls: EnumFamily<Api, Gauge>,
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
			fastpath_mismatches: Counter::new(),
			long_polls: EnumFamily::new(Gauge::new),
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
	/// Counts a room where the verified fast path disagreed with the full
	/// computation.
	pub fn fastpath_mismatch(&self) { self.fastpath_mismatches.inc(); }

	#[inline]
	/// Counts a request as waiting for activity until the guard is dropped.
	pub fn long_poll(&self, api: Api) -> LongPoll<'_> {
		let gauge = self.long_polls.get(api);
		gauge.inc();

		LongPoll { gauge }
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
	/// Returns the number of fast path mismatches counted.
	pub fn fastpath_mismatches(&self) -> u64 { self.fastpath_mismatches.get() }

	#[must_use]
	/// Returns the number of requests waiting for activity on an API.
	pub fn long_polling(&self, api: Api) -> i64 { self.long_polls.get(api).get() }

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
			"tuwunel_sync_fastpath_mismatches_total",
			"Rooms where the verified sync fast path disagreed with the full computation.",
			&self.fastpath_mismatches,
		);
		w.metric(
			"tuwunel_sync_long_polls",
			"Sync requests waiting for activity, by API version.",
			&self.long_polls,
		);
	}

	pub(super) fn series(&self) -> usize {
		[
			self.v3_build.series(),
			self.v3_responses.series(),
			self.v3_rooms.series(),
			self.v3_rooms_per_sync.series(),
			self.fastpath_mismatches.series(),
			self.long_polls.series(),
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
