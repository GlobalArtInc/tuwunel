use std::{cmp::Ordering, ops::Range};

use axum::extract::State;
use futures::StreamExt;
use ruma::{OwnedRoomId, uint};
use synapse_admin_api::rooms::list_rooms::v1::{
	Request, Response, RoomDetails, RoomSortOrder, SortDirection,
};
use tuwunel_core::{
	Result,
	utils::{
		IterStream,
		stream::{BroadbandExt, ReadyExt, WidebandExt},
	},
};
use tuwunel_service::Services;

use super::{
	room_canonical_alias, room_creator, room_encryption, room_guest_access,
	room_history_visibility, room_join_rules, room_joined_local_members, room_joined_members,
	room_name, room_public, room_row, room_version, state_event_count, usize_to_uint,
};
use crate::{Ruma, require_admin};

/// # `GET /_synapse/admin/v1/rooms`
///
/// Lists rooms known to the server with search, filtering, ordering, and
/// integer-offset pagination, mirroring Synapse's List Room API.
pub(crate) async fn admin_list_rooms_route(
	State(services): State<crate::State>,
	body: Ruma<Request>,
) -> Result<Response> {
	require_admin(&services, body.sender_user()).await?;

	let search_term = body.search_term.as_deref().map(str::to_lowercase);
	let order_by = body
		.order_by
		.clone()
		.unwrap_or(RoomSortOrder::Name);

	let backward = matches!(body.dir, Some(SortDirection::Backward));

	let column = Column::of(&order_by);
	let columns = needed_columns(
		column,
		search_term.is_some(),
		body.public_rooms.is_some(),
		body.empty_rooms.is_some(),
	);

	let mut rooms: Vec<RoomDetails> = services
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.broad_then(async |room_id| probe_room(&services, room_id, &columns).await)
		.ready_filter(|room| {
			matches_search(room, search_term.as_deref())
				&& matches_public(room, body.public_rooms)
				&& matches_empty(room, body.empty_rooms)
		})
		.collect()
		.await;

	sort_rooms(&mut rooms, column);

	if backward {
		rooms.reverse();
	}

	let total_rooms = usize_to_uint(rooms.len());
	let from = body.from.unwrap_or_else(|| uint!(0));
	let limit = body.limit.unwrap_or_else(|| uint!(100));

	let offset = usize::try_from(from).unwrap_or(usize::MAX);
	let window = usize::try_from(limit).unwrap_or(usize::MAX);
	let page = page_window(rooms.len(), offset, window);

	let page_rooms: Vec<RoomDetails> = rooms[page.range]
		.iter()
		.map(|room| room.room_id.clone())
		.stream()
		.wide_then(async |room_id| room_row(&services, &room_id).await)
		.collect()
		.await;

	Ok(Response {
		rooms: page_rooms,
		offset: usize_to_uint(offset),
		total_rooms,
		next_batch: page.next_batch.map(usize_to_uint),
		prev_batch: page.prev_batch.map(usize_to_uint),
	})
}

/// The room-list columns a filter or the sort order can read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Column {
	Name,
	CanonicalAlias,
	JoinedMembers,
	JoinedLocalMembers,
	Version,
	Creator,
	Encryption,
	Federatable,
	Public,
	JoinRules,
	GuestAccess,
	HistoryVisibility,
	StateEvents,
}

impl Column {
	/// The column an `order_by` value sorts on. The deprecated `Alphabetical`
	/// and `Size` aliases resolve to their current columns.
	fn of(order_by: &RoomSortOrder) -> Self {
		match order_by {
			| RoomSortOrder::CanonicalAlias => Self::CanonicalAlias,
			| RoomSortOrder::JoinedMembers | RoomSortOrder::Size => Self::JoinedMembers,
			| RoomSortOrder::JoinedLocalMembers => Self::JoinedLocalMembers,
			| RoomSortOrder::Version => Self::Version,
			| RoomSortOrder::Creator => Self::Creator,
			| RoomSortOrder::Encryption => Self::Encryption,
			| RoomSortOrder::Federatable => Self::Federatable,
			| RoomSortOrder::Public => Self::Public,
			| RoomSortOrder::JoinRules => Self::JoinRules,
			| RoomSortOrder::GuestAccess => Self::GuestAccess,
			| RoomSortOrder::HistoryVisibility => Self::HistoryVisibility,
			| RoomSortOrder::StateEvents => Self::StateEvents,
			| _ => Self::Name,
		}
	}
}

/// The columns to read for every room before the page is cut: the sort column
/// plus those of the filters that are set.
fn needed_columns(order: Column, search: bool, public: bool, empty: bool) -> Vec<Column> {
	let wanted = [
		(search, Column::Name),
		(search, Column::CanonicalAlias),
		(public, Column::Public),
		(empty, Column::JoinedLocalMembers),
	];

	let mut columns = vec![order];
	for (_, column) in wanted.into_iter().filter(|(wanted, _)| *wanted) {
		if !columns.contains(&column) {
			columns.push(column);
		}
	}

	columns
}

/// Builds a row holding only the given columns; every other field keeps its
/// `RoomDetails::new` default, so it must not be read.
async fn probe_room(
	services: &Services,
	room_id: OwnedRoomId,
	columns: &[Column],
) -> RoomDetails {
	let mut room = RoomDetails::new(room_id);

	for &column in columns {
		load_column(services, &mut room, column).await;
	}

	room
}

async fn load_column(services: &Services, room: &mut RoomDetails, column: Column) {
	let room_id = room.room_id.as_ref();

	match column {
		| Column::Name => room.name = room_name(services, room_id).await,
		| Column::CanonicalAlias =>
			room.canonical_alias = room_canonical_alias(services, room_id).await,
		| Column::JoinedMembers =>
			room.joined_members = room_joined_members(services, room_id).await,
		| Column::JoinedLocalMembers =>
			room.joined_local_members = room_joined_local_members(services, room_id).await,
		| Column::Version => room.version = room_version(services, room_id).await,
		| Column::Creator | Column::Federatable => {
			let (creator, federatable) = room_creator(services, room_id).await;

			room.creator = creator;
			room.federatable = federatable;
		},
		| Column::Encryption => room.encryption = room_encryption(services, room_id).await,
		| Column::Public => room.public = room_public(services, room_id).await,
		| Column::JoinRules => room.join_rules = room_join_rules(services, room_id).await,
		| Column::GuestAccess => room.guest_access = room_guest_access(services, room_id).await,
		| Column::HistoryVisibility =>
			room.history_visibility = room_history_visibility(services, room_id).await,
		| Column::StateEvents => room.state_events = state_event_count(services, room_id).await,
	}
}

#[derive(Debug, Eq, PartialEq)]
struct Page {
	range: Range<usize>,
	next_batch: Option<usize>,
	prev_batch: Option<usize>,
}

fn page_window(total: usize, offset: usize, window: usize) -> Page {
	let next_offset = offset.saturating_add(window);

	Page {
		range: offset.min(total)..next_offset.min(total),
		next_batch: (next_offset < total).then_some(next_offset),
		prev_batch: (offset > 0).then(|| offset.saturating_sub(window)),
	}
}

/// Matches a room against the search term: the name and canonical alias by
/// case-insensitive substring, but the room id by exact equality.
fn matches_search(room: &RoomDetails, search_term: Option<&str>) -> bool {
	let Some(term) = search_term else {
		return true;
	};

	let name_hit = room
		.name
		.as_deref()
		.is_some_and(|name| name.to_lowercase().contains(term));

	let alias_hit = room
		.canonical_alias
		.as_ref()
		.is_some_and(|alias| alias.as_str().to_lowercase().contains(term));

	name_hit || alias_hit || room.room_id.as_str() == term
}

fn matches_public(room: &RoomDetails, public_rooms: Option<bool>) -> bool {
	public_rooms.is_none_or(|want| room.public == want)
}

fn matches_empty(room: &RoomDetails, empty_rooms: Option<bool>) -> bool {
	empty_rooms.is_none_or(|want| (room.joined_local_members == uint!(0)) == want)
}

/// Orders rooms ascending by the requested column, tiebreaking on room id for
/// deterministic pagination.
fn sort_rooms(rooms: &mut [RoomDetails], column: Column) {
	let tiebreak = |a: &RoomDetails, b: &RoomDetails, primary: Ordering| {
		primary.then_with(|| a.room_id.cmp(&b.room_id))
	};

	match column {
		| Column::CanonicalAlias =>
			rooms.sort_by(|a, b| tiebreak(a, b, a.canonical_alias.cmp(&b.canonical_alias))),
		| Column::JoinedMembers =>
			rooms.sort_by(|a, b| tiebreak(a, b, a.joined_members.cmp(&b.joined_members))),
		| Column::JoinedLocalMembers => rooms.sort_by(|a, b| {
			tiebreak(
				a,
				b,
				a.joined_local_members
					.cmp(&b.joined_local_members),
			)
		}),
		| Column::Version => rooms.sort_by(|a, b| tiebreak(a, b, a.version.cmp(&b.version))),
		| Column::Creator => rooms.sort_by(|a, b| tiebreak(a, b, a.creator.cmp(&b.creator))),
		| Column::Encryption =>
			rooms.sort_by(|a, b| tiebreak(a, b, a.encryption.cmp(&b.encryption))),
		| Column::Federatable =>
			rooms.sort_by(|a, b| tiebreak(a, b, a.federatable.cmp(&b.federatable))),
		| Column::Public => rooms.sort_by(|a, b| tiebreak(a, b, a.public.cmp(&b.public))),
		| Column::JoinRules => rooms.sort_by(|a, b| {
			tiebreak(a, b, enum_key(a.join_rules.as_ref()).cmp(&enum_key(b.join_rules.as_ref())))
		}),
		| Column::GuestAccess => rooms.sort_by(|a, b| {
			tiebreak(
				a,
				b,
				enum_key(a.guest_access.as_ref()).cmp(&enum_key(b.guest_access.as_ref())),
			)
		}),
		| Column::HistoryVisibility => rooms.sort_by(|a, b| {
			tiebreak(
				a,
				b,
				enum_key(a.history_visibility.as_ref())
					.cmp(&enum_key(b.history_visibility.as_ref())),
			)
		}),
		| Column::StateEvents =>
			rooms.sort_by(|a, b| tiebreak(a, b, a.state_events.cmp(&b.state_events))),
		| Column::Name => rooms.sort_by(|a, b| tiebreak(a, b, a.name.cmp(&b.name))),
	}
}

/// Projects an optional string enum to a comparable key.
fn enum_key<T: AsRef<str>>(value: Option<&T>) -> Option<&str> { value.map(AsRef::as_ref) }

#[cfg(test)]
mod tests {
	use ruma::{room_alias_id, room_id};
	use synapse_admin_api::rooms::list_rooms::v1::RoomSortOrder;

	use super::{
		Column, Page, RoomDetails, matches_search, needed_columns, page_window, sort_rooms,
	};

	fn room() -> RoomDetails { RoomDetails::new(room_id!("!abcdef:example.org").to_owned()) }

	#[test]
	fn search_none_matches_every_room() {
		assert!(matches_search(&room(), None));
	}

	#[test]
	fn search_matches_name_substring_case_insensitively() {
		let mut room = room();
		room.name = Some("The Lounge".to_owned());

		assert!(matches_search(&room, Some("lounge")));
		assert!(!matches_search(&room, Some("kitchen")));
	}

	#[test]
	fn search_matches_canonical_alias_substring() {
		let mut room = room();
		room.canonical_alias = Some(room_alias_id!("#lounge:example.org").to_owned());

		assert!(matches_search(&room, Some("loung")));
	}

	#[test]
	fn search_matches_room_id_only_by_exact_equality() {
		let room = room();

		assert!(matches_search(&room, Some("!abcdef:example.org")));
		// A substring of the room id must not match: the id is compared exactly.
		assert!(!matches_search(&room, Some("abcdef")));
	}

	#[test]
	fn first_page_has_next_batch_and_no_prev_batch() {
		assert_eq!(page_window(250, 0, 100), Page {
			range: 0..100,
			next_batch: Some(100),
			prev_batch: None,
		});
	}

	#[test]
	fn middle_page_has_both_batches() {
		assert_eq!(page_window(250, 100, 100), Page {
			range: 100..200,
			next_batch: Some(200),
			prev_batch: Some(0),
		});
	}

	#[test]
	fn last_partial_page_has_no_next_batch() {
		assert_eq!(page_window(250, 200, 100), Page {
			range: 200..250,
			next_batch: None,
			prev_batch: Some(100),
		});
	}

	#[test]
	fn page_ending_exactly_at_the_end_has_no_next_batch() {
		assert_eq!(page_window(250, 150, 100), Page {
			range: 150..250,
			next_batch: None,
			prev_batch: Some(50),
		});
	}

	#[test]
	fn offset_past_the_end_is_empty_without_next_batch() {
		assert_eq!(page_window(250, 300, 100), Page {
			range: 250..250,
			next_batch: None,
			prev_batch: Some(200),
		});
		assert_eq!(page_window(0, 0, 100), Page {
			range: 0..0,
			next_batch: None,
			prev_batch: None,
		});
	}

	#[test]
	fn zero_limit_is_empty_and_keeps_the_cursor() {
		assert_eq!(page_window(250, 10, 0), Page {
			range: 10..10,
			next_batch: Some(10),
			prev_batch: Some(10),
		});
	}

	#[test]
	fn huge_limit_does_not_overflow() {
		assert_eq!(page_window(250, 10, usize::MAX), Page {
			range: 10..250,
			next_batch: None,
			prev_batch: Some(0),
		});
	}

	#[test]
	fn sort_column_resolves_aliases_and_defaults_to_name() {
		assert_eq!(Column::of(&RoomSortOrder::Size), Column::JoinedMembers);
		assert_eq!(Column::of(&RoomSortOrder::JoinedMembers), Column::JoinedMembers);
		assert_eq!(Column::of(&RoomSortOrder::Name), Column::Name);
		assert_eq!(Column::of(&RoomSortOrder::Federatable), Column::Federatable);
	}

	#[test]
	fn only_the_columns_of_the_set_filters_are_read() {
		assert_eq!(needed_columns(Column::Name, false, false, false), [Column::Name]);
		assert_eq!(needed_columns(Column::Version, true, true, true), [
			Column::Version,
			Column::Name,
			Column::CanonicalAlias,
			Column::Public,
			Column::JoinedLocalMembers,
		]);
		assert_eq!(needed_columns(Column::Name, true, false, false), [
			Column::Name,
			Column::CanonicalAlias,
		]);
		assert_eq!(needed_columns(Column::Public, false, true, false), [Column::Public]);
	}

	#[test]
	fn rooms_sort_by_the_column_then_by_room_id() {
		let named = |id: &str, members: u32| {
			let mut room = RoomDetails::new(ruma::RoomId::parse(id).unwrap());
			room.joined_members = members.into();
			room
		};

		let mut rooms = vec![named("!c:x", 2), named("!a:x", 5), named("!b:x", 2)];

		sort_rooms(&mut rooms, Column::JoinedMembers);

		let ids: Vec<&str> = rooms
			.iter()
			.map(|room| room.room_id.as_str())
			.collect();
		assert_eq!(ids, ["!b:x", "!c:x", "!a:x"]);
	}
}
