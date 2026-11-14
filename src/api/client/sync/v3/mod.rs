use ruma::{
	OwnedUserId, RoomId, UInt,
	api::client::sync::sync_events::v3::{State as RoomState, StateEvents},
};
use tuwunel_core::{Error, debug_error, err};
use tuwunel_matrix::pdu::PduEvent;
use tuwunel_service::rooms::{lazy_loading::Witness, short::ShortStateHash};

pub use self::state::calculate_heroes;
pub(crate) use self::sync_events::sync_events_route;

const DEFAULT_TIMELINE_LIMIT: usize = 10;

mod joined;
mod left;
mod state;
mod sync_events;

#[cfg(test)]
mod tests;

#[derive(Default)]
struct StateChanges {
	heroes: Option<Vec<OwnedUserId>>,
	joined_member_count: Option<u64>,
	invited_member_count: Option<u64>,
	state_events: Vec<PduEvent>,

	/// Witnessed members' current state, lazily loaded for display; never a
	/// membership change, so the device-list scan leaves them out.
	lazy_members: Vec<PduEvent>,
}

struct StateChangeParams<'a> {
	full_state: bool,
	state_after: StateAfter,
	since_shortstatehash: Option<ShortStateHash>,
	horizon_shortstatehash: Option<ShortStateHash>,
	after_shortstatehash: Option<ShortStateHash>,
	current_shortstatehash: ShortStateHash,
	joined_since_last_sync: bool,
	witness: Option<&'a Witness>,
	include_heroes: bool,
}

/// MSC4222 `state_after` opt-in: which room-state field the response carries.
#[derive(Clone, Copy, Debug)]
enum StateAfter {
	Off,
	Stable,
	Unstable,
}

impl StateAfter {
	fn requested(self) -> bool { !matches!(self, Self::Off) }

	fn wrap(self, events: StateEvents) -> RoomState {
		match self {
			| Self::Off => RoomState::Before(events),
			| Self::Stable => RoomState::After(events),
			| Self::Unstable => RoomState::AfterUnstable(events),
		}
	}
}

impl From<(bool, bool)> for StateAfter {
	fn from((stable, unstable): (bool, bool)) -> Self {
		// Unstable opt-in wins: such a client reads the unstable field name.
		match (stable, unstable) {
			| (_, true) => Self::Unstable,
			| (true, _) => Self::Stable,
			| _ => Self::Off,
		}
	}
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Since {
	Initial,
	Count(u64),
	Foreign,
}

impl From<Option<&str>> for Since {
	fn from(since: Option<&str>) -> Self {
		match since
			.filter(|since| !since.is_empty())
			.map(str::parse)
		{
			| None => Self::Initial,
			| Some(Ok(count)) => Self::Count(count),
			| Some(Err(_)) => Self::Foreign,
		}
	}
}

fn capped_timeline_limit(
	rooms: usize,
	threshold: usize,
	cap: usize,
	requested: Option<UInt>,
) -> Option<UInt> {
	if threshold == 0 || rooms <= threshold {
		return None;
	}

	let cap = UInt::try_from(cap.max(1)).unwrap_or(UInt::MAX);
	let requested = requested.unwrap_or_else(|| {
		UInt::try_from(DEFAULT_TIMELINE_LIMIT).expect("default timeline limit fits UInt")
	});

	(requested > cap).then_some(cap)
}

fn stale_since(current: u64, since: u64, gap: u64) -> bool {
	gap > 0 && current.saturating_sub(since) > gap
}

// Keep diagnostic macro expansions under the original tracing target.
fn log_future_since(since: Option<u64>, next_batch: u64) {
	debug_error!(?since, next_batch, "received since > next_batch, clamping");
}

fn missing_room_state(room_id: &RoomId) -> Error {
	err!(Database(error!("Room {room_id} has no state")))
}
