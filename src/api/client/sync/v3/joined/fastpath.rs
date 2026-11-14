use std::collections::{BTreeMap, HashSet};

use futures::{StreamExt, future::join3, pin_mut};
use ruma::{
	DeviceId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	api::client::{filter::FilterDefinition, sync::sync_events::v3::JoinedRoom},
};
use tuwunel_core::{Error, Result, debug, error};
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in super::super) enum Verdict {
	Off,
	Quiet,
	Busy(Busy),
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
		let loaded = self.load().await;
		let Some(diff) = mismatch(&loaded, &dlu) else {
			return loaded.map(with_verdict(Verdict::Quiet));
		};

		let Self {
			services,
			sender_user,
			room_id,
			since,
			next_batch,
			..
		} = self;

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
		self.check_timeline().await?;
		self.check_typing().await?;
		self.check_private_read().await?;
		self.check_account_data().await?;
		self.check_receipts().await?;
		self.check_notifications().await?;

		Ok(self.device_list_updates().await)
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

fn mismatch(loaded: &Result<Loaded>, dlu: &HashSet<OwnedUserId>) -> Option<String> {
	match loaded {
		| Err(error) => Some(format!("load_room failed: {error}")),
		| Ok((room, slow_dlu, leu)) if room.is_empty() && slow_dlu == dlu && leu.is_empty() =>
			None,
		| Ok((room, slow_dlu, leu)) => Some(describe(room, slow_dlu, leu, dlu)),
	}
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
