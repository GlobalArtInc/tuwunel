use std::{
	collections::{BTreeMap, HashSet},
	sync::{Arc, OnceLock},
};

use futures::{StreamExt, future::join3, pin_mut};
use ruma::{
	DeviceId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::client::{filter::FilterDefinition, sync::sync_events::v3::JoinedRoom},
};
use tuwunel_core::{
	Error, Result, debug, error,
	metrics::prom::{Fastpath, JournalKind, SyncMetrics},
};
use tuwunel_service::Services;

use super::{
	super::StateAfter,
	load_room,
	notifications::{GateInputs, compute_gates, count_futures, in_window, quiet_counts_reported},
};

type Loaded = (JoinedRoom, HashSet<OwnedUserId>, HashSet<OwnedUserId>);
type Outcome = (JoinedRoom, HashSet<OwnedUserId>, HashSet<OwnedUserId>, Verdict);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in super::super) enum Busy {
	Timeline,
	NoTimelineCount,
	Typing,
	PrivateRead,
	AccountData,
	Receipts,
	Notifications,
	Error,
}

impl Busy {
	pub(in super::super) const fn journal_kind(self) -> Option<JournalKind> {
		match self {
			| Self::Timeline => Some(JournalKind::Timeline),
			| Self::Typing => Some(JournalKind::Typing),
			| Self::PrivateRead => Some(JournalKind::PrivateRead),
			| Self::AccountData => Some(JournalKind::AccountData),
			| Self::Receipts => Some(JournalKind::Receipts),
			| Self::Notifications => Some(JournalKind::Notifications),
			| Self::NoTimelineCount | Self::Error => None,
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) enum Verdict {
	Off,
	Quiet,
	Busy(Busy),
}

impl Verdict {
	pub(in super::super) const fn label(self) -> Fastpath {
		match self {
			| Self::Off => Fastpath::Off,
			| Self::Quiet => Fastpath::Quiet,
			| Self::Busy(Busy::Timeline) => Fastpath::BusyTimeline,
			| Self::Busy(Busy::NoTimelineCount) => Fastpath::BusyNoTimelineCount,
			| Self::Busy(Busy::Typing) => Fastpath::BusyTyping,
			| Self::Busy(Busy::PrivateRead) => Fastpath::BusyPrivateRead,
			| Self::Busy(Busy::AccountData) => Fastpath::BusyAccountData,
			| Self::Busy(Busy::Receipts) => Fastpath::BusyReceipts,
			| Self::Busy(Busy::Notifications) => Fastpath::BusyNotifications,
			| Self::Busy(Busy::Error) => Fastpath::BusyError,
		}
	}
}

#[derive(Default)]
pub(in super::super) struct Tally {
	off: usize,
	quiet: usize,
	busy: BTreeMap<Busy, usize>,
}

#[derive(Clone, Copy)]
struct Round<'a> {
	services: &'a Services,
	sender_user: &'a UserId,
	sender_device: Option<&'a DeviceId>,
	room_id: &'a RoomId,
	since: u64,
	next_batch: u64,
	full_state: bool,
	state_after: StateAfter,
	filter: &'a FilterDefinition,
	changed: &'a Changed,
}

/// The rooms the sync journal holds a change for, asked once and shared by
/// every probe that uses it.
///
/// It answers for the user and `since` of the first probe to ask, so it is
/// shared only by probes of one build.
#[derive(Default)]
pub(in super::super) struct Changed(OnceLock<Option<HashSet<OwnedRoomId>>>);

impl Changed {
	fn rooms(
		&self,
		services: &Services,
		user: &UserId,
		since: u64,
	) -> Option<&HashSet<OwnedRoomId>> {
		self.rooms_with(|| services.sync.journal.changed_rooms(user, since))
	}

	pub(in super::super) fn rooms_with(
		&self,
		ask: impl FnOnce() -> Option<HashSet<OwnedRoomId>>,
	) -> Option<&HashSet<OwnedRoomId>> {
		self.0.get_or_init(ask).as_ref()
	}
}

impl Tally {
	pub(in super::super) fn record(&mut self, verdict: Verdict) {
		match verdict {
			| Verdict::Off => self.off = self.off.saturating_add(1),
			| Verdict::Quiet => self.quiet = self.quiet.saturating_add(1),
			| Verdict::Busy(busy) => {
				let count = self.busy.entry(busy).or_default();

				*count = count.saturating_add(1);
			},
		}
	}

	pub(in super::super) fn publish(&self, metrics: &SyncMetrics) {
		let busy = self
			.busy
			.values()
			.fold(0_usize, |total, rooms| total.saturating_add(*rooms));
		let rooms = self
			.off
			.saturating_add(self.quiet)
			.saturating_add(busy);

		metrics.v3_rooms_per_sync(to_u64(rooms));
		add(metrics, Verdict::Off, self.off);
		add(metrics, Verdict::Quiet, self.quiet);

		for (busy, rooms) in &self.busy {
			add(metrics, Verdict::Busy(*busy), *rooms);
		}
	}

	pub(in super::super) fn log(&self, since: u64, next_batch: u64) {
		if self.quiet == 0 && self.busy.is_empty() {
			return;
		}

		debug!(
			target: "tuwunel_api_client::sync::v3::fastpath",
			since,
			next_batch,
			off = self.off,
			quiet = self.quiet,
			busy = ?self.busy,
			"sync fastpath summary"
		);
	}
}

#[expect(clippy::too_many_arguments)]
pub(in super::super) async fn load_room_or_skip(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	room_id: OwnedRoomId,
	since: u64,
	next_batch: u64,
	full_state: bool,
	state_after: StateAfter,
	filter: &FilterDefinition,
	changed: Arc<Changed>,
) -> Result<Outcome> {
	let sync = services.config.client.sync;
	let probing = (sync.fastpath || sync.fastpath_verify) && since != 0 && !full_state;

	let round = Round {
		services,
		sender_user,
		sender_device,
		room_id: &room_id,
		since,
		next_batch,
		full_state,
		state_after,
		filter,
		changed: &changed,
	};

	if !probing {
		return round.load().await.map(with_verdict(Verdict::Off));
	}

	match round.probe().await {
		| Err(busy) => round
			.load()
			.await
			.map(with_verdict(Verdict::Busy(busy))),
		| Ok(dlu) if sync.fastpath_verify => round.verify(dlu).await,
		| Ok(dlu) => Ok((JoinedRoom::default(), dlu, HashSet::new(), Verdict::Quiet)),
	}
}

impl Round<'_> {
	async fn load(self) -> Result<Loaded> {
		load_room(
			self.services,
			self.sender_user,
			self.sender_device,
			self.room_id.to_owned(),
			self.since,
			self.next_batch,
			self.full_state,
			self.state_after,
			self.filter,
		)
		.await
	}

	async fn verify(self, dlu: HashSet<OwnedUserId>) -> Result<Outcome> {
		let Self {
			services,
			sender_user,
			room_id,
			since,
			next_batch,
			..
		} = self;

		let loaded = self.load().await;
		let diff = match &loaded {
			| Ok(loaded) => mismatch(loaded, &dlu),
			| Err(error) => {
				debug!(
					target: "tuwunel_api_client::sync::v3::fastpath",
					%room_id,
					%sender_user,
					since,
					next_batch,
					%error,
					"sync fastpath verification skipped, load_room failed"
				);

				None
			},
		};

		let Some(diff) = diff else {
			return loaded.map(with_verdict(Verdict::Quiet));
		};

		match self.probe().await {
			| Err(busy) => {
				debug!(
					target: "tuwunel_api_client::sync::v3::fastpath",
					%room_id,
					%sender_user,
					since,
					next_batch,
					?busy,
					%diff,
					"sync fastpath probe raced with a write"
				);

				loaded.map(with_verdict(Verdict::Busy(busy)))
			},
			| Ok(_) => {
				error!(
					target: "tuwunel_api_client::sync::v3::fastpath",
					%room_id,
					%sender_user,
					since,
					next_batch,
					%diff,
					"sync fastpath mismatch"
				);

				services.sync.note_fastpath_mismatch();

				loaded.map(with_verdict(Verdict::Quiet))
			},
		}
	}

	async fn probe(self) -> Result<HashSet<OwnedUserId>, Busy> {
		self.checks().await.inspect_err(|busy| {
			if let Some(kind) = busy.journal_kind() {
				self.audit(kind);
			}
		})?;

		let updated = self.device_list_updates().await;

		if !updated.is_empty() {
			self.audit(JournalKind::DeviceLists);
		}

		Ok(updated)
	}

	async fn checks(self) -> Result<(), Busy> {
		self.check_timeline().await?;
		self.check_typing().await?;
		self.check_private_read().await?;
		self.check_account_data().await?;
		self.check_receipts().await?;
		self.check_notifications().await
	}

	fn audit(self, kind: JournalKind) {
		let Self {
			services,
			sender_user,
			room_id,
			since,
			changed,
			..
		} = self;

		let rooms = changed.rooms(services, sender_user, since);

		if !services
			.sync
			.journal
			.audit(kind, room_id, sender_user, since, rooms)
		{
			return;
		}

		debug!(
			target: "tuwunel_api_client::sync::v3::fastpath",
			%room_id,
			%sender_user,
			since,
			?kind,
			"sync journal holds no change for a room the probe found changed"
		);
	}

	async fn check_timeline(self) -> Result<(), Busy> {
		let Self { services, room_id, since, .. } = self;

		let count = services
			.timeline
			.last_normal_count(room_id)
			.await
			.map_err(|error| probe_failed(room_id, &error))?;

		match count {
			| Some(count) if count <= since => Ok(()),
			| Some(_) => Err(Busy::Timeline),
			| None => Err(Busy::NoTimelineCount),
		}
	}

	async fn check_typing(self) -> Result<(), Busy> {
		let Self { services, room_id, since, .. } = self;

		let count = services
			.typing
			.last_typing_update(room_id)
			.await
			.map_err(|error| probe_failed(room_id, &error))?;

		(count <= since).then_some(()).ok_or(Busy::Typing)
	}

	async fn check_private_read(self) -> Result<(), Busy> {
		let Self {
			services, sender_user, room_id, since, ..
		} = self;

		let count = services
			.read_receipt
			.last_privateread_update(sender_user, room_id)
			.await;

		(count <= since)
			.then_some(())
			.ok_or(Busy::PrivateRead)
	}

	async fn check_account_data(self) -> Result<(), Busy> {
		let Self {
			services, sender_user, room_id, since, ..
		} = self;

		let changes =
			services
				.account_data
				.changes_since(Some(room_id), sender_user, since, None);

		pin_mut!(changes);
		changes
			.next()
			.await
			.is_none()
			.then_some(())
			.ok_or(Busy::AccountData)
	}

	async fn check_receipts(self) -> Result<(), Busy> {
		let Self { services, room_id, since, next_batch, .. } = self;

		let receipts = services
			.read_receipt
			.readreceipts_since(room_id, since, Some(next_batch));

		pin_mut!(receipts);
		receipts
			.next()
			.await
			.is_none()
			.then_some(())
			.ok_or(Busy::Receipts)
	}

	async fn check_notifications(self) -> Result<(), Busy> {
		let Self {
			services,
			sender_user,
			room_id,
			since,
			next_batch,
			filter,
			..
		} = self;

		let last_notification_read = services
			.pusher
			.last_notification_read(sender_user, room_id)
			.await
			.ok();

		let thread_last_reads = services
			.pusher
			.thread_last_notification_reads(sender_user, room_id)
			.await;

		let in_window = in_window(since, next_batch);
		let gates = compute_gates(
			GateInputs {
				last_notification_read,
				thread_last_reads: &thread_last_reads,
				quiet_round: true,
				timeline_changed: false,
				since,
			},
			in_window,
		);

		if !gates.send_notification_counts {
			return Ok(());
		}

		let (notification_count, highlight_count, thread_counts) =
			count_futures(services, sender_user, room_id, true);

		let (notification_count, highlight_count, thread_counts) =
			join3(notification_count, highlight_count, thread_counts).await;

		let admitted = |root: &OwnedEventId| {
			thread_last_reads
				.get(root)
				.copied()
				.is_some_and(in_window)
		};

		let reported = quiet_counts_reported(
			notification_count,
			highlight_count,
			thread_counts.as_ref(),
			gates.send_notification_count_filter,
			filter.room.timeline.unread_thread_notifications,
			admitted,
		);

		(!reported)
			.then_some(())
			.ok_or(Busy::Notifications)
	}

	async fn device_list_updates(self) -> HashSet<OwnedUserId> {
		let Self { services, room_id, since, next_batch, .. } = self;

		services
			.users
			.room_keys_changed(room_id, since, Some(next_batch))
			.map(|(user_id, _)| user_id)
			.map(ToOwned::to_owned)
			.collect()
			.await
	}
}

fn add(metrics: &SyncMetrics, verdict: Verdict, rooms: usize) {
	if rooms > 0 {
		metrics.v3_rooms(verdict.label(), to_u64(rooms));
	}
}

fn to_u64(rooms: usize) -> u64 { u64::try_from(rooms).unwrap_or(u64::MAX) }

fn with_verdict(verdict: Verdict) -> impl FnOnce(Loaded) -> Outcome {
	move |(room, dlu, leu)| (room, dlu, leu, verdict)
}

fn probe_failed(room_id: &RoomId, error: &Error) -> Busy {
	debug!(
		target: "tuwunel_api_client::sync::v3::fastpath",
		%room_id,
		%error,
		"sync fastpath probe failed"
	);

	Busy::Error
}

fn mismatch(loaded: &Loaded, dlu: &HashSet<OwnedUserId>) -> Option<String> {
	let (room, slow_dlu, leu) = loaded;

	(!(room.is_empty() && slow_dlu == dlu && leu.is_empty()))
		.then(|| describe(room, slow_dlu, leu, dlu))
}

fn describe(
	room: &JoinedRoom,
	slow_dlu: &HashSet<OwnedUserId>,
	leu: &HashSet<OwnedUserId>,
	dlu: &HashSet<OwnedUserId>,
) -> String {
	let only_slow = slow_dlu.difference(dlu).collect::<Vec<_>>();
	let only_fast = dlu.difference(slow_dlu).collect::<Vec<_>>();

	format!(
		"timeline={} state_empty={} ephemeral={} account_data={} limited={} prev_batch={} \
		 unread_notifications={:?} threads={} summary_empty={} dlu_only_slow={only_slow:?} \
		 dlu_only_fast={only_fast:?} leu={}",
		room.timeline.events.len(),
		room.state.is_empty(),
		room.ephemeral.events.len(),
		room.account_data.events.len(),
		room.timeline.limited,
		room.timeline.prev_batch.is_some(),
		room.unread_notifications,
		room.unread_thread_notifications.len(),
		room.summary.is_empty(),
		leu.len(),
	)
}
