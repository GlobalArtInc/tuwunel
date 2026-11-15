use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::Arc,
};

use axum::extract::State;
use futures::{
	FutureExt, StreamExt, TryFutureExt,
	future::{join, join3, join4, join5},
};
use ruma::{
	DeviceId, OwnedRoomId, OwnedUserId, UserId,
	api::client::{
		filter::FilterDefinition,
		sync::{
			sync_events,
			sync_events::{
				DeviceLists,
				v3::{
					Filter, GlobalAccountData, InviteState, InvitedRoom, JoinedRoom, KnockState,
					KnockedRoom, Presence, Response, Rooms, ToDevice,
				},
			},
		},
	},
	events::{
		AnyGlobalAccountDataEvent, AnyRawAccountDataEvent,
		presence::{PresenceEvent, PresenceEventContent},
	},
	serde::Raw,
};
use tokio::time::timeout_at;
use tuwunel_core::{
	Result, at, debug,
	debug::INFO_SPAN_LEVEL,
	error::inspect_log,
	extract_variant,
	metrics::prom::{Api, Build, Phase, Registry, Reply, Section, SinceKind},
	trace,
	utils::{
		BoolExt, IterStream, ReadyExt, TryFutureExtExt,
		option::OptionExt,
		stream::{BroadbandExt, Tools},
	},
};
use tuwunel_matrix::event::Matches;
use tuwunel_service::{Services, presence::Ping, users::InviteFilter};

use super::{
	super::{
		invite_permitted, long_poll, profiles::collect as collect_profiles, share_encrypted_room,
	},
	Since, StateAfter, capped_timeline_limit,
	joined::fastpath::{Changed, Tally, load_room_or_skip},
	left::collect_rooms as collect_left_rooms,
	log_future_since,
	phase::timed,
	stale_since,
};
use crate::{
	ClientIp, Ruma, is_empty_account_data_event,
	utils::{normalize_profile_fields, sync_deadline},
};

struct Params<'a> {
	services: &'a Services,
	sender_user: &'a UserId,
	sender_device: Option<&'a DeviceId>,
	since: Option<u64>,
	next_batch: u64,
	full_state: bool,
	state_after: StateAfter,
	filter: &'a FilterDefinition,
}

type PresenceUpdates = HashMap<OwnedUserId, PresenceEventContent>;

/// # `GET /_matrix/client/r0/sync`
///
/// Synchronize the client's state with the latest state on the server.
///
/// - This endpoint takes a `since` parameter which should be the `next_batch`
///   value from a previous request for incremental syncs.
///
/// Calling this endpoint without a `since` parameter returns:
/// - Some of the most recent events of each timeline
/// - Notification counts for each room
/// - Joined and invited member counts, heroes
/// - All state events
///
/// Calling this endpoint with a `since` parameter from a previous `next_batch`
/// returns: For joined rooms:
/// - Some of the most recent events of each timeline that happened after since
/// - If user joined the room after since: All state events (unless lazy loading
///   is activated) and all device list updates in that room
/// - If the user was already in the room: A list of all events that are in the
///   state now, but were not in the state at `since`
/// - If the state we send contains a member event: Joined and invited member
///   counts, heroes
/// - Device list updates that happened after `since`
/// - If there are events in the timeline we send or the user send updated his
///   read mark: Notification counts
/// - EDUs that are active now (read receipts, typing updates, presence)
/// - TODO: Allow multiple sync streams to support Pantalaimon
///
/// For invited rooms:
/// - If the user was invited after `since`: A subset of the state of the room
///   at the point of the invite
///
/// For left rooms:
/// - If the user left after `since`: `prev_batch` token, empty state (TODO:
///   subset of the state at the point of the leave)
#[tracing::instrument(
	target = "tuwunel_api_client::sync::v3",
	name = "sync",
	level = "debug",
	skip_all,
	fields(
		user_id = %body.sender_user(),
		device_id = %body.sender_device.as_deref().map_or("<no device>", |x| x.as_str()),
    )
)]
pub(crate) async fn sync_events_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<sync_events::v3::Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();

	let filter = body
		.body
		.filter
		.as_ref()
		.map_async(async |filter| match filter {
			| Filter::FilterDefinition(filter) => filter.clone(),
			| Filter::FilterId(filter_id) => services
				.users
				.get_filter(sender_user, filter_id)
				.await
				.unwrap_or_default(),
		});

	let filter = filter.map(Option::unwrap_or_default);
	let full_state = body.body.full_state;
	let set_presence = &body.body.set_presence;
	let state_after =
		StateAfter::from((body.body.use_state_after, body.body.use_state_after_unstable));

	let ping = Ping {
		device_id: body.sender_device.as_deref(),
		client_ip: Some(client),
		new_state: Some(set_presence),
		appservice: body.appservice_info.as_ref(),
	};

	let ping_presence = services
		.presence
		.maybe_ping_presence(sender_user, ping)
		.inspect_err(inspect_log)
		.ok();

	// Record user as actively syncing for push suppression heuristic.
	let note_sync = services
		.presence
		.note_sync(sender_user, body.appservice_info.as_ref());

	let (filter, ..) = join3(filter, ping_presence, note_sync).await;

	services
		.profile
		.check_requested_fields(&filter.profile_fields.ids)?;

	let filter = normalize_profile_fields(filter);

	let since = match Since::from(body.body.since.as_deref()) {
		| Since::Foreign if full_state => Since::Initial,
		| since => since,
	};

	let registry = services.server.metrics.prometheus();
	let kind = match since {
		| Since::Initial => SinceKind::Initial,
		| Since::Count(_) | Since::Foreign => SinceKind::Incremental,
	};

	let answered = |reply| {
		if let Some(registry) = registry {
			registry.sync.v3_answered(kind, reply);
		}
	};

	let mut since = match since {
		| Since::Initial => None,
		| Since::Count(since) => Some(since),
		| Since::Foreign => {
			let next_batch = services.globals.wait_pending().await?;

			debug!(
				target: "tuwunel_api_client::sync::v3",
				user_id = %sender_user,
				device_id = ?sender_device,
				next_batch,
				"foreign since token"
			);

			let response =
				build_foreign_response(&services, sender_user, sender_device, next_batch).await;

			answered(Reply::Foreign);

			return Ok(response);
		},
	};

	let stale_gap = services
		.config
		.client
		.sync
		.initial_large_account_stale_gap;

	let current_count = services.globals.current_count();
	let mut capped_filter =
		if since.is_none_or(|since| stale_since(current_count, since, stale_gap)) {
			initial_capped_filter(&services, sender_user, &filter).await
		} else {
			None
		};

	let mut presence_only_account: Option<bool> = None;

	let stop_at = sync_deadline(&services, body.body.timeout);

	loop {
		let watch_rooms = services
			.state_cache
			.rooms_joined(sender_user)
			.chain(services.state_cache.rooms_invited(sender_user));

		let watchers = services
			.sync
			.watch(sender_user, sender_device, watch_rooms)
			.await;

		let next_batch = services.globals.wait_pending().await?;
		if since.is_some_and(|since| since > next_batch) {
			log_future_since(since, next_batch);
			since = Some(next_batch);
		}

		if since.is_none_or(|since| since < next_batch) || full_state {
			let capped = capped_filter.take();
			let response = build_timed(registry, kind, Params {
				services: &services,
				sender_user,
				sender_device,
				since,
				next_batch,
				full_state,
				state_after,
				filter: capped.as_ref().unwrap_or(&filter),
			})
			.await?;

			let empty = response.presence.is_empty() && none_apart_from_presence(&response);

			let held = since.is_some()
				&& !full_state
				&& held_presence_only(
					&services,
					sender_user,
					&response,
					&mut presence_only_account,
				)
				.await;

			count_build(registry, empty, held);

			if (!empty && !held) || full_state {
				answered(Reply::Data);
				count_sections(registry, &response);

				return Ok(response);
			}
		}

		// Wait for activity
		let timed_out = {
			let _long_poll = long_poll(&services, Api::V3);

			timeout_at(stop_at, watchers).await.is_err()
		};

		if timed_out || services.server.is_stopping() {
			let response =
				build_empty_response(&services, sender_user, sender_device, next_batch).await;

			answered(Reply::Empty);

			trace!(target: "tuwunel_api_client::sync::v3", ?since, next_batch, "empty response");
			return Ok(response);
		}

		trace!(target: "tuwunel_api_client::sync::v3",
			?since,
			last_batch = ?next_batch,
			count = ?services.globals.pending_count(),
			stop_at = ?stop_at,
			"notified by watcher"
		);

		since = Some(next_batch);
	}
}

async fn build_timed(
	registry: Option<&Registry>,
	kind: SinceKind,
	params: Params<'_>,
) -> Result<Response> {
	let _timer = registry.map(|registry| registry.sync.v3_build(kind));

	build_sync_events(params).await
}

fn none_apart_from_presence(response: &Response) -> bool {
	response.rooms.is_empty()
		&& response.account_data.is_empty()
		&& response.device_lists.is_empty()
		&& response.to_device.is_empty()
		&& response.users.is_empty()
}

pub(super) fn presence_only(response: &Response) -> bool {
	!response.presence.is_empty() && none_apart_from_presence(response)
}

async fn held_presence_only(
	services: &Services,
	user_id: &UserId,
	response: &Response,
	known: &mut Option<bool>,
) -> bool {
	let min_rooms = services
		.config
		.client
		.sync
		.presence_only_min_rooms;

	if min_rooms == 0 || !presence_only(response) {
		return false;
	}

	if let Some(held) = *known {
		return held;
	}

	let held = joined_at_least(services, user_id, min_rooms).await;

	*known = Some(held);

	held
}

fn count_build(registry: Option<&Registry>, empty: bool, held: bool) {
	let Some(registry) = registry else {
		return;
	};

	let outcome = if empty {
		Build::Empty
	} else if held {
		Build::PresenceOnly
	} else {
		Build::Data
	};

	registry.sync.v3_build_outcome(outcome);
}

async fn joined_at_least(services: &Services, user_id: &UserId, min: usize) -> bool {
	services
		.state_cache
		.rooms_joined(user_id)
		.take(min)
		.count()
		.await
		>= min
}

fn count_sections(registry: Option<&Registry>, response: &Response) {
	let Some(registry) = registry else {
		return;
	};

	let rooms = &response.rooms;
	let sections = [
		(
			Section::Rooms,
			!rooms.join.is_empty()
				|| !rooms.leave.is_empty()
				|| !rooms.invite.is_empty()
				|| !rooms.knock.is_empty(),
		),
		(Section::Presence, !response.presence.is_empty()),
		(Section::AccountData, !response.account_data.is_empty()),
		(Section::DeviceLists, !response.device_lists.is_empty()),
		(Section::ToDevice, !response.to_device.is_empty()),
		(Section::Users, !response.users.is_empty()),
	];

	for (section, present) in sections {
		if present {
			registry.sync.v3_section(section);
		}
	}
}

async fn initial_capped_filter(
	services: &Services,
	sender_user: &UserId,
	filter: &FilterDefinition,
) -> Option<FilterDefinition> {
	let sync = &services.config.client.sync;
	let threshold = sync.initial_large_account_rooms;
	if threshold == 0 {
		return None;
	}

	let rooms = services
		.state_cache
		.rooms_joined(sender_user)
		.take(threshold.saturating_add(1))
		.count()
		.await;

	let limit = capped_timeline_limit(
		rooms,
		threshold,
		sync.initial_large_account_timeline_limit,
		filter.room.timeline.limit,
	)?;

	debug!(
		target: "tuwunel_api_client::sync::v3",
		user_id = %sender_user,
		rooms,
		%limit,
		"capping the timeline of an initial or stale sync"
	);

	let mut filter = filter.clone();
	filter.room.timeline.limit = Some(limit);

	Some(filter)
}

async fn build_foreign_response(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	next_batch: u64,
) -> Response {
	let to_device_events = sender_device.map_async(|sender_device| {
		services
			.users
			.get_to_device_events(sender_user, sender_device, None, Some(next_batch))
			.map(at!(1))
			.collect::<Vec<_>>()
	});

	let (mut response, to_device_events) = join(
		build_empty_response(services, sender_user, sender_device, next_batch),
		to_device_events,
	)
	.await;

	response.to_device = ToDevice {
		events: to_device_events.unwrap_or_default(),
	};

	response
}

async fn build_empty_response(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	next_batch: u64,
) -> Response {
	let device_one_time_keys_count = sender_device.map_async(|sender_device| {
		services
			.users
			.count_one_time_keys(sender_user, sender_device)
	});

	let device_unused_fallback_key_types = sender_device.map_async(|sender_device| {
		services
			.users
			.unused_fallback_key_algorithms(sender_user, sender_device)
			.collect::<Vec<_>>()
	});

	let (device_one_time_keys_count, device_unused_fallback_key_types) =
		join(device_one_time_keys_count, device_unused_fallback_key_types).await;

	Response {
		device_one_time_keys_count: device_one_time_keys_count.unwrap_or_default(),
		device_unused_fallback_key_types,
		..Response::new(next_batch.to_string())
	}
}

#[tracing::instrument(
	target = "tuwunel_api_client::sync::v3",
	name = "build",
	level = INFO_SPAN_LEVEL,
	skip_all,
	fields(
		?since,
		%next_batch,
		count = ?services.globals.pending_count(),
    )
)]
#[expect(clippy::too_many_lines)]
async fn build_sync_events(
	Params {
		services,
		sender_user,
		sender_device,
		since,
		next_batch,
		full_state,
		state_after,
		filter,
	}: Params<'_>,
) -> Result<Response> {
	let profile_since = since;
	let since = since.unwrap_or(0);
	let registry = services.server.metrics.prometheus();

	// MSC4155: a stored invite whose sender the recipient ignores or blocks is
	// withheld here, and relaxing the configuration re-exposes it.
	let invite_filter = services.users.invite_filter(sender_user).await;

	let joined_rooms = timed(
		registry,
		Phase::Joined,
		collect_joined_rooms(
			services,
			sender_user,
			sender_device,
			since,
			next_batch,
			full_state,
			state_after,
			filter,
		),
	);

	let left_rooms = timed(
		registry,
		Phase::Left,
		collect_left_rooms(
			services,
			sender_user,
			since,
			next_batch,
			full_state,
			state_after,
			filter,
		),
	);

	let invited_rooms = timed(
		registry,
		Phase::Invited,
		collect_invited_rooms(services, sender_user, since, next_batch, filter, &invite_filter),
	);

	let knocked_rooms = timed(
		registry,
		Phase::Invited,
		collect_knocked_rooms(services, sender_user, since, next_batch, filter),
	);

	let presence_updates = services
		.config
		.presence
		.allow_local
		.then_async(|| {
			timed(
				registry,
				Phase::Presence,
				process_presence_updates(services, since, next_batch, sender_user, filter),
			)
		});

	let account_data = timed(
		registry,
		Phase::AccountData,
		collect_global_account_data(services, sender_user, since, next_batch),
	);

	let keys_changed = timed(
		registry,
		Phase::E2ee,
		services
			.users
			.keys_changed(sender_user, since, Some(next_batch))
			.map(ToOwned::to_owned)
			.collect::<HashSet<_>>(),
	);

	let to_device_events = sender_device.map_async(|sender_device| {
		timed(
			registry,
			Phase::E2ee,
			services
				.users
				.get_to_device_events(sender_user, sender_device, Some(since), Some(next_batch))
				.map(at!(1))
				.collect::<Vec<_>>(),
		)
	});

	let device_one_time_keys_count = sender_device.map_async(|sender_device| {
		timed(
			registry,
			Phase::E2ee,
			services
				.users
				.count_one_time_keys(sender_user, sender_device),
		)
	});

	let device_unused_fallback_key_types = sender_device.map_async(|sender_device| {
		timed(
			registry,
			Phase::E2ee,
			services
				.users
				.unused_fallback_key_algorithms(sender_user, sender_device)
				.collect::<Vec<_>>(),
		)
	});

	// Remove all to-device events the device received *last time*
	let remove_to_device_events = sender_device.map_async(|sender_device| {
		timed(
			registry,
			Phase::E2ee,
			services
				.users
				.remove_to_device_events(sender_user, sender_device, since),
		)
	});

	let (
		account_data,
		keys_changed,
		presence_updates,
		(_, to_device_events, device_one_time_keys_count, device_unused_fallback_key_types),
		(
			(joined_rooms, mut device_list_updates, left_encrypted_users),
			left_rooms,
			invited_rooms,
			knocked_rooms,
		),
	) = join5(
		account_data,
		keys_changed,
		presence_updates,
		join4(
			remove_to_device_events,
			to_device_events,
			device_one_time_keys_count,
			device_unused_fallback_key_types,
		),
		join4(joined_rooms, left_rooms, invited_rooms, knocked_rooms),
	)
	.boxed() // size firewall
	.await;

	device_list_updates.extend(keys_changed);

	let rooms = Rooms {
		leave: left_rooms,
		join: joined_rooms,
		invite: invited_rooms,
		knock: knocked_rooms,
	};

	// The profile reads and the left-device fan-out both pend on the pool.
	let (device_list_left, users) = join(
		timed(
			registry,
			Phase::Profiles,
			collect_device_list_left(services, sender_user, left_encrypted_users),
		),
		timed(
			registry,
			Phase::Profiles,
			collect_profiles(services, sender_user, profile_since, next_batch, filter, &rooms),
		),
	)
	.await;

	let presence_events = build_presence_events(presence_updates);
	let device_lists = DeviceLists {
		left: device_list_left,
		changed: device_list_updates.into_iter().collect(),
	};

	let to_device = ToDevice {
		events: to_device_events.unwrap_or_default(),
	};

	Ok(Response {
		account_data: GlobalAccountData { events: account_data },
		device_lists,
		device_one_time_keys_count: device_one_time_keys_count.unwrap_or_default(),
		device_unused_fallback_key_types,
		next_batch: next_batch.to_string(),
		presence: Presence { events: presence_events },
		rooms,
		to_device,
		users: users?,
	})
}

#[expect(clippy::too_many_arguments)]
fn collect_joined_rooms<'a>(
	services: &'a Services,
	sender_user: &'a UserId,
	sender_device: Option<&'a DeviceId>,
	since: u64,
	next_batch: u64,
	full_state: bool,
	state_after: StateAfter,
	filter: &'a FilterDefinition,
) -> impl Future<
	Output = (BTreeMap<OwnedRoomId, JoinedRoom>, HashSet<OwnedUserId>, HashSet<OwnedUserId>),
> + Send
+ 'a {
	let changed = Arc::new(Changed::default());

	services
		.state_cache
		.rooms_joined(sender_user)
		.ready_filter(|&room_id| filter.room.matches(room_id))
		.map(ToOwned::to_owned)
		.broad_filter_map(move |room_id| {
			load_room_or_skip(
				services,
				sender_user,
				sender_device,
				room_id.clone(),
				since,
				next_batch,
				full_state,
				state_after,
				filter,
				Arc::clone(&changed),
			)
			.map_ok(move |(joined_room, dlu, jeu, verdict)| {
				(room_id, joined_room, dlu, jeu, verdict)
			})
			.ok()
		})
		.ready_fold(
			(BTreeMap::new(), HashSet::new(), HashSet::new(), Tally::default()),
			|(mut joined_rooms, mut device_list_updates, mut left_encrypted_users, mut tally),
			 (room_id, joined_room, dlu, leu, verdict)| {
				device_list_updates.extend(dlu);
				left_encrypted_users.extend(leu);
				tally.record(verdict);
				if !joined_room.is_empty() {
					joined_rooms.insert(room_id, joined_room);
				}

				(joined_rooms, device_list_updates, left_encrypted_users, tally)
			},
		)
		.map(move |(joined_rooms, device_list_updates, left_encrypted_users, tally)| {
			tally.log(since, next_batch);

			if let Some(registry) = services.server.metrics.prometheus() {
				tally.publish(&registry.sync);
			}

			(joined_rooms, device_list_updates, left_encrypted_users)
		})
}

async fn collect_invited_rooms<'a>(
	services: &'a Services,
	sender_user: &'a UserId,
	since: u64,
	next_batch: u64,
	filter: &'a FilterDefinition,
	invite_filter: &'a InviteFilter,
) -> BTreeMap<OwnedRoomId, InvitedRoom> {
	services
		.state_cache
		.rooms_invited_state(sender_user)
		.ready_filter(|(room_id, _)| filter.room.matches(room_id))
		.ready_filter(|(room_id, invite_state)| {
			invite_permitted(sender_user, room_id, invite_filter, invite_state)
		})
		.fold_default(async |mut invited_rooms: BTreeMap<_, _>, (room_id, invite_state)| {
			let invite_count = services
				.state_cache
				.get_invite_count(&room_id, sender_user)
				.await
				.ok();

			// Invited before last sync
			if Some(since) >= invite_count || Some(next_batch) < invite_count {
				return invited_rooms;
			}

			let invited_room = InvitedRoom {
				invite_state: InviteState { events: invite_state },
			};

			invited_rooms.insert(room_id, invited_room);
			invited_rooms
		})
		.await
}

async fn collect_knocked_rooms<'a>(
	services: &'a Services,
	sender_user: &'a UserId,
	since: u64,
	next_batch: u64,
	filter: &'a FilterDefinition,
) -> BTreeMap<OwnedRoomId, KnockedRoom> {
	services
		.state_cache
		.rooms_knocked_state(sender_user)
		.ready_filter(|(room_id, _)| filter.room.matches(room_id))
		.fold_default(async |mut knocked_rooms: BTreeMap<_, _>, (room_id, knock_state)| {
			let knock_count = services
				.state_cache
				.get_knock_count(&room_id, sender_user)
				.await
				.ok();

			// Knocked before last sync; or after the cutoff for this sync
			if Some(since) >= knock_count || Some(next_batch) < knock_count {
				return knocked_rooms;
			}

			let knocked_room = KnockedRoom {
				knock_state: KnockState { events: knock_state },
			};

			knocked_rooms.insert(room_id, knocked_room);
			knocked_rooms
		})
		.await
}

fn collect_global_account_data<'a>(
	services: &'a Services,
	sender_user: &'a UserId,
	since: u64,
	next_batch: u64,
) -> impl Future<Output = Vec<Raw<AnyGlobalAccountDataEvent>>> + Send + 'a {
	services
		.account_data
		.changes_since(None, sender_user, since, Some(next_batch))
		.ready_filter_map(|e| extract_variant!(e, AnyRawAccountDataEvent::Global))
		.ready_filter(move |e| since != 0 || !is_empty_account_data_event(e))
		.collect()
}

fn collect_device_list_left<'a>(
	services: &'a Services,
	sender_user: &'a UserId,
	left_encrypted_users: HashSet<OwnedUserId>,
) -> impl Future<Output = Vec<OwnedUserId>> + Send + 'a {
	left_encrypted_users
		.into_iter()
		.stream()
		.broad_filter_map(async |user_id: OwnedUserId| {
			share_encrypted_room(services, sender_user, &user_id)
				.await
				.is_false()
				.then_some(user_id)
		})
		.collect()
}

fn build_presence_events(presence_updates: Option<PresenceUpdates>) -> Vec<Raw<PresenceEvent>> {
	presence_updates
		.into_iter()
		.flat_map(IntoIterator::into_iter)
		.map(|(sender, content)| PresenceEvent { content, sender })
		.map(|ref event| Raw::new(event))
		.filter_map(Result::ok)
		.collect()
}

#[tracing::instrument(
	target = "tuwunel_api_client::sync::v3",
	name = "presence",
	level = "debug",
	skip_all
)]
async fn process_presence_updates(
	services: &Services,
	since: u64,
	next_batch: u64,
	syncing_user: &UserId,
	filter: &FilterDefinition,
) -> PresenceUpdates {
	services
		.presence
		.presence_since(since, Some(next_batch))
		.ready_filter(|(user_id, ..)| filter.presence.matches(user_id))
		.filter(|(user_id, ..)| {
			services
				.state_cache
				.user_sees_user(syncing_user, user_id)
		})
		.filter_map(|(user_id, _, presence_bytes)| {
			services
				.presence
				.from_json_bytes_to_event(presence_bytes, user_id)
				.map_ok(move |event| (user_id, event))
				.ok()
		})
		.map(|(user_id, event)| (user_id.to_owned(), event.content))
		.collect()
		.await
}
