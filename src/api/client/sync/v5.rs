mod extensions;
mod filter;
mod index;
mod journal;
mod range;
mod rooms;
mod selector;

#[cfg(test)]
mod tests;

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt::Debug,
	iter::{once, repeat},
	mem::take,
	sync::Arc,
};

use axum::extract::{Extension, State};
use futures::{TryFutureExt, future::join};
use ruma::{
	DeviceId, OwnedRoomId, RoomId, UserId,
	api::client::sync::sync_events::v5::{ListId, Request, Response, response},
	events::{AnyStrippedStateEvent, StateEventType, room::member::MembershipState},
	serde::Raw,
};
use serde::Deserialize;
use tokio::sync::Notify;
use tuwunel_core::{
	Err, Result, debug,
	debug::INFO_SPAN_LEVEL,
	debug_warn,
	error::inspect_log,
	metrics::prom::{Api, Pass, Registry, V5Path, V5Phase},
	smallvec::SmallVec,
	trace,
	utils::{
		TryFutureExtExt,
		result::{FlatOk, NotFound},
		time::millis,
	},
	warn,
};
use tuwunel_service::{
	Services,
	presence::Ping,
	rooms::state_cache::StrippedRoomState,
	sync::{Connection, ConnectionKey, ConnectionVal, into_connection_key},
};

use self::{
	extensions::{
		acknowledge_to_device, apply_profiles, apply_ranges, check_to_device,
		handle as handle_extensions, to_device_pending,
	},
	index::select,
	journal::{
		Arrival, Checked, Ended, Gated, Wait, gate, list_counts, verify_pass, verify_timeout,
		wait_for_change,
	},
	range::collect as collect_ranges,
};
use super::{joined_since, long_poll, share_encrypted_room};
use crate::{ClientIp, Ruma, utils::poll_deadline};

#[derive(Copy, Clone)]
struct SyncInfo<'a> {
	services: &'a Services,
	sender_user: &'a UserId,
	sender_device: Option<&'a DeviceId>,
	previous_connection_pos: Option<u64>,

	/// The rooms the user calls direct, read once for the whole pass.
	///
	/// Both the list filters and every room payload answer `is_dm` from this.
	direct_rooms: &'a BTreeSet<OwnedRoomId>,
}

#[derive(Clone, Debug)]
struct WindowRoom {
	room_id: OwnedRoomId,
	membership: Option<MembershipState>,
	forgotten: bool,
	lists: ListIds,
	event_count: u64,
	payload_count: u64,
}

impl WindowRoom {
	#[inline]
	fn payload_is_fresh(&self, roomsince: u64) -> bool {
		roomsince == 0 || self.payload_count > roomsince
	}
}

type Window = BTreeMap<OwnedRoomId, WindowRoom>;
type ResponseLists = BTreeMap<ListId, response::List>;
type ListIds = SmallVec<[ListId; 1]>;

/// `POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync`
/// ([MSC4186])
///
/// A simplified version of sliding sync ([MSC3575]).
///
/// Get all new events in a sliding window of rooms since the last sync or a
/// given point in time.
///
/// [MSC3575]: https://github.com/matrix-org/matrix-spec-proposals/pull/3575
/// [MSC4186]: https://github.com/matrix-org/matrix-spec-proposals/pull/4186
#[tracing::instrument(
	name = "sync",
	level = INFO_SPAN_LEVEL,
	skip_all,
	fields(
		user_id = %body.sender_user().localpart(),
		device_id = %body.sender_device.as_deref().map_or("<no device>", |x| x.as_str()),
		conn_id = ?body.body.conn_id.clone().unwrap_or_default(),
		since = ?body.body.pos.clone().unwrap_or_default(),
	)
)]
#[expect(clippy::too_many_lines)]
pub(crate) async fn sync_events_v5_route(
	Extension(interrupted): Extension<Arc<Notify>>,
	ClientIp(client): ClientIp,
	State(ref services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	let sender_user = body.sender_user();
	let sender_device = body.sender_device.as_deref();
	let request = &body.body;
	let registry = services.server.metrics.prometheus();

	if let Some(fields) = request.extensions.profiles.fields.as_deref() {
		services.profile.check_requested_fields(fields)?;
	}

	check_to_device(&request.extensions.to_device)?;

	let since = request
		.pos
		.as_ref()
		.and_then(|string| string.parse().ok())
		.unwrap_or(0);

	let timeout = poll_timeout(request, services.config.client.sync.timeout_max);

	let conn_key = into_connection_key(sender_user, sender_device, request.conn_id.as_deref());
	let conn_val = services
		.sync
		.load_or_init_connection(&conn_key)
		.await?;

	let conn = conn_val.lock();
	let ping = Ping {
		device_id: sender_device,
		client_ip: Some(client),
		new_state: Some(&request.set_presence),
		appservice: body.appservice_info.as_ref(),
	};

	let ping_presence = services
		.presence
		.maybe_ping_presence(sender_user, ping)
		.inspect_err(inspect_log)
		.ok();

	let (mut conn, _) = join(conn, ping_presence).await;

	if since != 0 {
		check_profile_selection(services, request, &conn)?;
	}

	if since != 0 && conn.next_batch == 0 {
		unknown_position(services);

		return Err!(Request(UnknownPos(warn!("Connection lost; restarting sync stream."))));
	}

	if since == 0 {
		*conn = Connection::default();
		services
			.sync
			.store_connection(&conn_key, &conn_val, &conn)
			.await?;

		debug_warn!(?conn_key, "Client cleared cache and reloaded.");
	}

	let advancing = since == conn.next_batch;
	let retarding = since != 0 && since <= conn.globalsince;
	if !advancing && !retarding {
		unknown_position(services);

		return Err!(Request(UnknownPos(warn!(
			"Requesting unknown or invalid stream position."
		))));
	}

	debug_assert!(
		advancing || retarding,
		"Request should either be advancing or replaying the since token."
	);

	// Update parameters regardless of replay or advance
	conn.globalsince = since;
	let (config_changed, settings_changed) = conn.update_cache_tracked(request);
	let stop_at = poll_deadline(timeout);

	let arrival = Arrival {
		since,
		advancing,
		config_changed,
		settings_changed,
		timeout,
	};
	let passage = match gate(
		services,
		(sender_user, sender_device),
		request,
		&conn,
		arrival,
		(&interrupted, stop_at),
	)
	.await?
	{
		| Gated::Quiet(response) => return Ok(*response),
		| Gated::Pass(passage) => passage,
	};

	conn.next_batch = services.globals.wait_pending().await?;
	conn.globalsince = since.min(conn.next_batch);

	if !passage.acknowledged {
		acknowledge_to_device(services, sender_user, sender_device, &conn).await?;
	}

	let caught_up = conn.globalsince == conn.next_batch;

	// Caught-up connections run no pass, so config changes, owed profiles or to-device messages
	// after `since` need a reserved position.
	let needs_pass = config_changed
		|| conn.own_profile_owed()
		|| caught_up && to_device_pending(services, sender_user, sender_device, &conn).await?;

	if config_change_needs_position(needs_pass, advancing, caught_up, since) {
		// The permit publishes the reserved position when it retires on drop.
		drop(services.globals.next_count());
	}

	conn.update_rooms_prologue(retarding.then_some(since));

	let mut response = Response {
		txn_id: request.txn_id.clone(),
		lists: Default::default(),
		pos: Default::default(),
		rooms: Default::default(),
		extensions: Default::default(),
	};

	let mut verify_after = passage.checked.then_some((Checked::Gate, since));

	for woken in once(passage.woken).chain(repeat(true)) {
		debug_assert!(
			conn.globalsince <= conn.next_batch,
			"since should not be greater than next_batch."
		);

		let wait = Wait::current(services);
		if wait != Wait::Verify {
			verify_after = None;
		}

		let pass = registry.map(|registry| {
			registry
				.sync
				.v5_pass(if woken { Pass::Woken } else { Pass::First })
		});

		let window;
		let watch_phase = registry
			.filter(|_| wait.registers())
			.map(|registry| registry.sync.v5_phase(V5Phase::Watch));

		let watchers = if wait.registers() {
			Some(
				services
					.sync
					.watch(
						sender_user,
						sender_device,
						services.state_cache.rooms_joined(sender_user),
					)
					.await,
			)
		} else {
			None
		};

		drop(watch_phase);

		conn.next_batch = services.globals.wait_pending().await?;
		let next_batch = conn.next_batch;
		let direct_rooms = services
			.account_data
			.direct_rooms(sender_user)
			.await;

		let sync_info = SyncInfo {
			services,
			sender_user,
			sender_device,
			previous_connection_pos: since.ne(&0).then_some(since),
			direct_rooms: &direct_rooms,
		};

		let select_phase = registry.map(|registry| registry.sync.v5_phase(V5Phase::Select));
		(window, response.lists) = select(&mut conn, sync_info, !advancing).await;
		drop(select_phase);
		conn.last_counts = Some(list_counts(&response.lists));

		let ran = conn.globalsince < conn.next_batch;
		let answers = if ran {
			let ranges =
				phased(registry, V5Phase::Collect, collect_ranges(sync_info, &conn, &window));
			let extensions = phased(
				registry,
				V5Phase::Extensions,
				handle_extensions(sync_info, &conn, &window),
			);

			let (mut ranges, extensions) = join(ranges, extensions).await;

			let mut extensions = extensions?;

			phased(
				registry,
				V5Phase::Profiles,
				apply_profiles(sync_info, &conn, &window, &ranges, &mut extensions),
			)
			.await?;

			apply_ranges(&conn, &window, &mut ranges, &mut extensions);
			conn.update_rooms_epilogue(since, ranges.room_updates());
			conn.update_profiles_epilogue();
			conn.ranges_owed = ranges.failed() > 0;

			if let Some(registry) = registry {
				registry
					.sync
					.v5_fresh_payloads(u64::try_from(ranges.fresh()).unwrap_or(u64::MAX));
			}

			response.rooms = ranges.into_payloads();
			let keys_answer = woken || extensions.keys_changed();

			response.extensions = extensions.into_response(&response.rooms);

			!is_empty_response(&mut response, keys_answer)
		} else {
			false
		};

		drop(pass);

		if let Some((checked, after)) = verify_after.take() {
			verify_pass(
				services,
				sender_user,
				sender_device,
				checked,
				(after, next_batch),
				ran.then_some(&mut response),
			)
			.await;
		}

		if answers {
			return respond(services, (&conn_key, &conn_val), &conn, response).await;
		}

		let waiter = async || {
			let _long_poll = long_poll(services, Api::V5);

			tokio::select! {
				() = interrupted.notified() => Ended::Stopped,
				ended = wait_for_change(
					services,
					sender_user,
					sender_device,
					next_batch,
					stop_at,
					watchers,
				) => ended,
			}
		};

		let ended = if timeout == 0 || services.server.is_stopping() {
			Ended::Stopped
		} else {
			waiter().await
		};

		if ended == Ended::TimedOut && wait == Wait::Verify {
			verify_timeout(services, sender_user, sender_device, next_batch).await;
		}

		if ended != Ended::Woken {
			return respond(services, (&conn_key, &conn_val), &conn, response).await;
		}

		debug!(
			?timeout,
			last_since = conn.globalsince,
			last_batch = conn.next_batch,
			pend_count = ?services.globals.pending_count(),
			"notified"
		);

		conn.globalsince = conn.next_batch;
		verify_after = (wait == Wait::Verify).then_some((Checked::Woken, next_batch));
	}

	unreachable!("a long poll has no last pass")
}

/// Runs a future and records the time it took as a phase of the pass.
async fn phased<Phase>(
	registry: Option<&Registry>,
	phase: V5Phase,
	future: Phase,
) -> Phase::Output
where
	Phase: Future,
{
	let _phase = registry.map(|registry| registry.sync.v5_phase(phase));

	future.await
}

/// How long a request may long-poll, in milliseconds.
///
/// Per MSC4186 the timeout of a request without `pos` is ignored, so opening
/// a connection answers after its first pass.
fn poll_timeout(request: &Request, max: u64) -> u64 {
	request
		.timeout
		.filter(|_| request.pos.is_some())
		.and_then(millis)
		.map(|timeout| timeout.min(max))
		.unwrap_or(0)
}

fn unknown_position(services: &Services) {
	if let Some(registry) = services.server.metrics.prometheus() {
		registry.sync.v5_unknown_pos();
	}
}

fn check_profile_selection(services: &Services, request: &Request, conn: &Connection) -> Result {
	let fields = request
		.extensions
		.profiles
		.fields
		.as_deref()
		.or(conn.extensions.profiles.fields.as_deref());

	if let Some(fields) = fields {
		services.profile.check_requested_fields(fields)?;
	}

	Ok(())
}

fn config_change_needs_position(
	config_changed: bool,
	advancing: bool,
	caught_up: bool,
	since: u64,
) -> bool {
	config_changed && advancing && caught_up && since != 0
}

/// Whether a pass has nothing to answer the client with.
///
/// The key counts and fallback types are current state; they answer a pass the
/// user's watchers woke or one where the counts changed since `pos`.
/// A woken answer lets matrix-rust-sdk back up its room keys.
/// Both fields remain in the response whether or not they answer the pass.
fn is_empty_response(response: &mut Response, keys_answer: bool) -> bool {
	let key_counts = take(
		&mut response
			.extensions
			.e2ee
			.device_one_time_keys_count,
	);

	let fallback_key_types = response
		.extensions
		.e2ee
		.device_unused_fallback_key_types
		.take(); // Extensions::is_empty cannot skip the key fields

	let empty = response.extensions.is_empty()
		&& response.rooms.is_empty()
		&& (!keys_answer || (key_counts.is_empty() && fallback_key_types.is_none()));

	response
		.extensions
		.e2ee
		.device_one_time_keys_count = key_counts;

	response
		.extensions
		.e2ee
		.device_unused_fallback_key_types = fallback_key_types;

	empty
}

/// Positions a response at the connection's next batch and stores the
/// connection.
///
/// The connection is stored as this response leaves it, so the client's next
/// request resumes from the position the response carries.
async fn respond(
	services: &Services,
	(key, loaded): (&ConnectionKey, &ConnectionVal),
	conn: &Connection,
	response: Response,
) -> Result<Response> {
	let response = Response {
		pos: conn.next_batch.to_string().into(),
		..response
	};

	trace!(conn.globalsince, conn.next_batch, ?response, "Responding");

	if let Some(registry) = services.server.metrics.prometheus() {
		registry
			.sync
			.v5_response_rooms(u64::try_from(response.rooms.len()).unwrap_or(u64::MAX));
	}

	let store_phase = services
		.server
		.metrics
		.prometheus()
		.map(|registry| registry.sync.v5_phase(V5Phase::Store));

	services
		.sync
		.store_connection(key, loaded, conn)
		.await?;

	drop(store_phase);

	if let Some(registry) = services.server.metrics.prometheus() {
		registry.sync.v5_request(V5Path::Pass);
	}

	Ok(response)
}

/// The stripped state MSC4186 sends for an invited or knocked room.
///
/// For a knocked room this is the summary stored with the knock, ending with
/// the user's own knock event, so a client can present a room this server is
/// not in. An empty knock summary is omitted, since a client reading the
/// user's membership from it would take the room for an invite.
#[tracing::instrument(level = "trace", skip_all)]
async fn stripped_state(
	services: &Services,
	sender_user: &UserId,
	room_id: &RoomId,
	membership: Option<&MembershipState>,
) -> StrippedRoomState {
	let membership = membership?;
	let state_cache = &services.state_cache;
	let state = match membership {
		| MembershipState::Invite =>
			state_cache
				.invite_state(sender_user, room_id)
				.await,
		| MembershipState::Knock =>
			state_cache
				.knock_state(sender_user, room_id)
				.await,
		| _ => return None,
	};

	state
		.present()
		.inspect_err(|error| warn!(%room_id, %error, "Unreadable stripped room state"))
		.flat_ok()
		.filter(|state| !state.is_empty() || *membership == MembershipState::Invite)
}

/// The content of the stripped state event with this type and an empty state
/// key.
///
/// The first match wins; an event whose type or state key does not parse is
/// skipped.
fn stripped_content<'a, T>(
	state: &'a [Raw<AnyStrippedStateEvent>],
	kind: &StateEventType,
) -> Option<T>
where
	T: Deserialize<'a>,
{
	state
		.iter()
		.filter(|event| {
			event
				.get_field("type")
				.flat_ok()
				.is_some_and(|event_type: StateEventType| event_type == *kind)
		})
		.find(|event| {
			event
				.get_field("state_key")
				.flat_ok()
				.is_some_and(str::is_empty)
		})?
		.get_field("content")
		.flat_ok()
}

#[inline]
fn departed(membership: Option<&MembershipState>) -> bool {
	matches!(membership, Some(MembershipState::Leave | MembershipState::Ban))
}
