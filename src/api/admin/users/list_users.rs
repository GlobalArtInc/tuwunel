use std::{
	ops::Range,
	sync::{Arc, Mutex, PoisonError},
	time::{Duration, Instant},
};

use axum::extract::State;
use futures::StreamExt;
use ruma::{MilliSecondsSinceUnixEpoch, UInt, UserId, api::Direction};
use synapse_admin_api::users::list_users::{
	v2::{self, UserMinorDetails},
	v3,
};
use tuwunel_core::{
	Result,
	utils::{
		IterStream, ReadyExt,
		math::{ruma_from_usize, usize_from_ruma},
		stream::WidebandExt,
	},
};

use crate::{Ruma, require_admin};

/// The `deactivated` query filter, whose semantics differ between v2 and v3.
#[derive(Clone, Copy, Eq, PartialEq)]
enum DeactivatedFilter {
	/// Include both active and deactivated users (v3 absent).
	Any,

	/// Only deactivated users (v3 `true`).
	Only,

	/// Exclude deactivated users (v2 absent/false, v3 `false`).
	Exclude,

	/// Include deactivated users (v2 `true`).
	Include,
}

impl DeactivatedFilter {
	fn needs_lookup(self) -> bool { matches!(self, Self::Only | Self::Exclude) }

	fn keeps(self, deactivated: bool) -> bool {
		match self {
			| Self::Any | Self::Include => true,
			| Self::Only => deactivated,
			| Self::Exclude => !deactivated,
		}
	}
}

/// Filter and pagination parameters shared by the v2 and v3 list endpoints.
struct ListParams<'a> {
	from: usize,
	limit: usize,
	name: Option<&'a str>,
	user_id: Option<&'a str>,
	admins: Option<bool>,
	locked: bool,
	deactivated: DeactivatedFilter,
	excluded_types: &'a [String],
	dir: Direction,
}

const LIST_TTL: Duration = Duration::from_secs(5);

const LIST_KEYS: usize = 8;

static LIST_CACHE: Mutex<ListCache> = Mutex::new(ListCache::new());

#[derive(Clone, Eq, PartialEq)]
struct ListKey {
	owner: usize,
	backward: bool,
	name: Option<String>,
	user_id: Option<String>,
	admins: Option<bool>,
	locked: bool,
	deactivated: DeactivatedFilter,
	excluded_types: Vec<String>,
}

struct ListCache {
	entries: Vec<(ListKey, Instant, Arc<[String]>)>,
}

impl ListKey {
	fn new(services: crate::State, params: &ListParams<'_>) -> Self {
		Self {
			owner: std::ptr::from_ref(&*services).addr(),
			backward: matches!(params.dir, Direction::Backward),
			name: params.name.map(ToOwned::to_owned),
			user_id: params.user_id.map(ToOwned::to_owned),
			admins: params.admins,
			locked: params.locked,
			deactivated: params.deactivated,
			excluded_types: params.excluded_types.to_vec(),
		}
	}
}

impl ListCache {
	const fn new() -> Self { Self { entries: Vec::new() } }

	fn get(&mut self, key: &ListKey, now: Instant) -> Option<Arc<[String]>> {
		self.entries
			.retain(|(_, stored, _)| now.saturating_duration_since(*stored) < LIST_TTL);

		self.entries
			.iter()
			.find(|(cached, ..)| cached == key)
			.map(|(.., names)| names.clone())
	}

	fn put(&mut self, key: ListKey, names: Arc<[String]>, now: Instant) {
		self.entries.retain(|(cached, ..)| *cached != key);

		if self.entries.len() >= LIST_KEYS {
			self.entries.remove(0);
		}

		self.entries.push((key, now, names));
	}
}

fn cached_names(key: &ListKey) -> Option<Arc<[String]>> {
	LIST_CACHE
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
		.get(key, Instant::now())
}

fn cache_names(key: ListKey, names: Arc<[String]>) {
	LIST_CACHE
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
		.put(key, names, Instant::now());
}

/// # `GET /_synapse/admin/v2/users`
pub(crate) async fn admin_list_users_v2_route(
	State(services): State<crate::State>,
	body: Ruma<v2::Request>,
) -> Result<v2::Response> {
	require_admin(&services, body.sender_user()).await?;

	let deactivated = match body.deactivated {
		| true => DeactivatedFilter::Include,
		| false => DeactivatedFilter::Exclude,
	};

	let params = ListParams {
		from: usize_from_ruma(body.from),
		limit: body.limit.map_or(100, usize_from_ruma),
		name: body.name.as_deref(),
		user_id: body.user_id.as_deref(),
		admins: body.admins,
		locked: body.locked,
		deactivated,
		excluded_types: &body.not_user_type,
		dir: body.dir.unwrap_or(Direction::Forward),
	};

	let (users, next_token, total) = list_users(services, &params).await;

	Ok(v2::Response { users, next_token, total })
}

/// # `GET /_synapse/admin/v3/users`
pub(crate) async fn admin_list_users_v3_route(
	State(services): State<crate::State>,
	body: Ruma<v3::Request>,
) -> Result<v3::Response> {
	require_admin(&services, body.sender_user()).await?;

	let deactivated = match body.deactivated {
		| None => DeactivatedFilter::Any,
		| Some(true) => DeactivatedFilter::Only,
		| Some(false) => DeactivatedFilter::Exclude,
	};

	let params = ListParams {
		from: usize_from_ruma(body.from),
		limit: body.limit.map_or(100, usize_from_ruma),
		name: body.name.as_deref(),
		user_id: body.user_id.as_deref(),
		admins: body.admins,
		locked: body.locked,
		deactivated,
		excluded_types: &body.not_user_type,
		dir: body.dir.unwrap_or(Direction::Forward),
	};

	let (users, next_token, total) = list_users(services, &params).await;

	Ok(v3::Response { users, next_token, total })
}

/// Returns the filtered, name-ordered and paginated user page, the `next_token`
/// (present only while the page does not reach the end of the filtered set) and
/// the filtered total. `order_by` beyond `name` is not backed by stored fields,
/// so the name ordering (reversed for `dir=b`) is the only one applied.
async fn list_users(
	services: crate::State,
	params: &ListParams<'_>,
) -> (Vec<UserMinorDetails>, Option<String>, UInt) {
	let key = ListKey::new(services, params);
	let matched = match cached_names(&key) {
		| Some(matched) => matched,
		| None => {
			let matched: Arc<[String]> = matching_names(services, params).await.into();

			cache_names(key, matched.clone());

			matched
		},
	};

	let total = ruma_from_usize(matched.len());
	let (window, next_token) = page_window(matched.len(), params.from, params.limit);

	let page: Vec<UserMinorDetails> = matched[window]
		.iter()
		.map(String::as_str)
		.stream()
		.wide_filter_map(async |name| user_minor_details(services, name).await)
		.collect()
		.await;

	(page, next_token, total)
}

async fn matching_names(services: crate::State, params: &ListParams<'_>) -> Vec<String> {
	let mut names: Vec<String> = if walks_typed_only(params.excluded_types) {
		services
			.users
			.typed()
			.ready_filter(|user_id| services.globals.user_is_local(user_id))
			.map(ToString::to_string)
			.collect()
			.await
	} else {
		services
			.users
			.stream()
			.ready_filter(|user_id| services.globals.user_is_local(user_id))
			.map(ToString::to_string)
			.collect()
			.await
	};

	order_names(&mut names, params.dir);

	names
		.iter()
		.map(String::as_str)
		.stream()
		.wide_filter_map(async |name| {
			user_matches(services, name, params)
				.await
				.then(|| name.to_owned())
		})
		.collect()
		.await
}

fn order_names(names: &mut [String], dir: Direction) {
	names.sort_unstable();

	if matches!(dir, Direction::Backward) {
		names.reverse();
	}
}

fn walks_typed_only(excluded: &[String]) -> bool { excluded.iter().any(String::is_empty) }

fn type_allowed(user_type: Option<&str>, excluded: &[String]) -> bool {
	let user_type = user_type.unwrap_or_default();

	!excluded
		.iter()
		.any(|excluded| excluded == user_type)
}

fn page_window(matched: usize, from: usize, limit: usize) -> (Range<usize>, Option<String>) {
	let start = from.min(matched);
	let end = from.saturating_add(limit).min(matched);
	let next_token = (end < matched).then(|| end.to_string());

	(start..end, next_token)
}

async fn user_matches(services: crate::State, name: &str, params: &ListParams<'_>) -> bool {
	let Ok(user_id) = UserId::parse(name) else {
		return false;
	};

	if let Some(needle) = params.user_id.filter(|_| params.name.is_none())
		&& !name.contains(needle)
	{
		return false;
	}

	if let Some(needle) = params.name
		&& !name_matches(services, &user_id, needle).await
	{
		return false;
	}

	if let Some(want_admin) = params.admins
		&& want_admin != services.admin.user_is_admin(&user_id).await
	{
		return false;
	}

	if !params.locked && services.users.is_locked(&user_id).await {
		return false;
	}

	if !params.excluded_types.is_empty() {
		let user_type = services.users.user_type(&user_id).await;

		if !type_allowed(user_type.as_deref(), params.excluded_types) {
			return false;
		}
	}

	if params.deactivated.needs_lookup() {
		let deactivated = services
			.users
			.is_deactivated(&user_id)
			.await
			.unwrap_or(false);

		return params.deactivated.keeps(deactivated);
	}

	true
}

async fn name_matches(services: crate::State, user_id: &UserId, needle: &str) -> bool {
	if user_id.localpart().contains(needle) {
		return true;
	}

	services
		.profile
		.displayname(user_id)
		.await
		.ok()
		.is_some_and(|displayname| displayname.contains(needle))
}

async fn user_minor_details(services: crate::State, name: &str) -> Option<UserMinorDetails> {
	let user_id = UserId::parse(name).ok()?;

	let displayname = services.profile.displayname(&user_id).await.ok();

	let admin = services.admin.user_is_admin(&user_id).await;

	let locked = services.users.is_locked(&user_id).await;

	let deactivated = services
		.users
		.is_deactivated(&user_id)
		.await
		.unwrap_or(false);

	let avatar_url = services
		.profile
		.avatar_url(&user_id)
		.await
		.ok()
		.map(|url| url.to_string());

	let erased = services.users.is_erased(&user_id).await;

	let user_type = services.users.user_type(&user_id).await;

	let last_seen_ts = services
		.users
		.all_devices_metadata(&user_id)
		.ready_fold(None, |max, device| max.max(device.last_seen_ts))
		.await;

	Some(UserMinorDetails {
		displayname,
		avatar_url,
		admin,
		deactivated,
		locked,
		erased,
		last_seen_ts,
		user_type,
		// tuwunel has no creation timestamp; emit a 0 sentinel (strict clients reject null).
		creation_ts: Some(MilliSecondsSinceUnixEpoch(UInt::from(0_u32))),
		..UserMinorDetails::new(name.to_owned())
	})
}

#[cfg(test)]
mod tests {
	use std::{
		sync::Arc,
		time::{Duration, Instant},
	};

	use ruma::api::Direction;

	use super::{
		DeactivatedFilter, LIST_KEYS, LIST_TTL, ListCache, ListKey, order_names, page_window,
		type_allowed, walks_typed_only,
	};

	#[test]
	fn first_page_has_next_token() {
		assert_eq!(page_window(250, 0, 100), (0..100, Some("100".to_owned())));
	}

	#[test]
	fn last_partial_page_has_no_next_token() {
		assert_eq!(page_window(250, 200, 100), (200..250, None));
	}

	#[test]
	fn page_ending_exactly_at_the_end_has_no_next_token() {
		assert_eq!(page_window(250, 150, 100), (150..250, None));
	}

	#[test]
	fn from_past_the_end_is_empty_without_next_token() {
		assert_eq!(page_window(250, 300, 100), (250..250, None));
		assert_eq!(page_window(0, 0, 100), (0..0, None));
	}

	#[test]
	fn zero_limit_keeps_the_cursor() {
		assert_eq!(page_window(250, 10, 0), (10..10, Some("10".to_owned())));
	}

	#[test]
	fn huge_limit_does_not_overflow() {
		assert_eq!(page_window(250, 10, usize::MAX), (10..250, None));
	}

	#[test]
	fn backward_order_reverses_the_name_order() {
		let names = || vec!["@b:x".to_owned(), "@c:x".to_owned(), "@a:x".to_owned()];

		let mut forward = names();
		order_names(&mut forward, Direction::Forward);
		assert_eq!(forward, ["@a:x", "@b:x", "@c:x"]);

		let mut backward = names();
		order_names(&mut backward, Direction::Backward);
		assert_eq!(backward, ["@c:x", "@b:x", "@a:x"]);

		let (window, next_token) = page_window(backward.len(), 0, 2);
		assert_eq!(&backward[window], ["@c:x", "@b:x"]);
		assert_eq!(next_token, Some("2".to_owned()));
	}

	#[test]
	fn deactivated_filter_semantics() {
		let keeps = |filter: DeactivatedFilter| {
			(filter.keeps(false), filter.keeps(true), filter.needs_lookup())
		};

		assert_eq!(keeps(DeactivatedFilter::Any), (true, true, false));
		assert_eq!(keeps(DeactivatedFilter::Include), (true, true, false));
		assert_eq!(keeps(DeactivatedFilter::Only), (false, true, true));
		assert_eq!(keeps(DeactivatedFilter::Exclude), (true, false, true));
	}

	#[test]
	fn type_filter_excludes_the_listed_types() {
		let none: &[String] = &[];
		assert!(type_allowed(None, none));
		assert!(type_allowed(Some("bot"), none));

		let untyped = [String::new()];
		assert!(!type_allowed(None, &untyped));
		assert!(type_allowed(Some("bot"), &untyped));

		let bots = ["bot".to_owned()];
		assert!(type_allowed(None, &bots));
		assert!(!type_allowed(Some("bot"), &bots));
		assert!(type_allowed(Some("x"), &bots));

		let untyped_and_bots = [String::new(), "bot".to_owned()];
		assert!(!type_allowed(None, &untyped_and_bots));
		assert!(!type_allowed(Some("bot"), &untyped_and_bots));
		assert!(type_allowed(Some("x"), &untyped_and_bots));
	}

	#[test]
	fn typed_walk_applies_only_when_untyped_users_are_excluded() {
		assert!(walks_typed_only(&[String::new()]));
		assert!(walks_typed_only(&["x".to_owned(), String::new()]));
		assert!(!walks_typed_only(&["bot".to_owned()]));
		assert!(!walks_typed_only(&[]));
	}

	fn key(name: &str) -> ListKey {
		ListKey {
			owner: 1,
			backward: false,
			name: Some(name.to_owned()),
			user_id: None,
			admins: None,
			locked: false,
			deactivated: DeactivatedFilter::Exclude,
			excluded_types: Vec::new(),
		}
	}

	fn names(name: &str) -> Arc<[String]> { Arc::from([name.to_owned()]) }

	#[test]
	fn cached_names_expire_with_the_ttl() {
		let start = Instant::now();
		let mut cache = ListCache::new();

		cache.put(key("a"), names("@a:x"), start);

		let almost = start + LIST_TTL.saturating_sub(Duration::from_millis(1));

		assert_eq!(cache.get(&key("a"), almost).as_deref(), Some(&["@a:x".to_owned()][..]));
		assert!(cache.get(&key("a"), start + LIST_TTL).is_none());
		assert!(cache.entries.is_empty());
	}

	#[test]
	fn cache_keeps_only_the_newest_keys() {
		let start = Instant::now();
		let mut cache = ListCache::new();

		for index in 0..=LIST_KEYS {
			cache.put(key(&index.to_string()), names("@a:x"), start);
		}

		assert_eq!(cache.entries.len(), LIST_KEYS);
		assert!(cache.get(&key("0"), start).is_none());
		assert!(cache.get(&key("1"), start).is_some());
		assert!(
			cache
				.get(&key(&LIST_KEYS.to_string()), start)
				.is_some()
		);
	}

	#[test]
	fn storing_a_key_again_replaces_its_entry() {
		let start = Instant::now();
		let mut cache = ListCache::new();

		cache.put(key("a"), names("@old:x"), start);
		cache.put(key("a"), names("@new:x"), start);

		assert_eq!(cache.entries.len(), 1);
		assert_eq!(cache.get(&key("a"), start).as_deref(), Some(&["@new:x".to_owned()][..]));
	}

	#[test]
	fn every_filter_separates_the_cached_list() {
		let base = key("a");
		let changed = [
			ListKey { owner: 2, ..base.clone() },
			ListKey { backward: true, ..base.clone() },
			ListKey { name: None, ..base.clone() },
			ListKey {
				user_id: Some("a".to_owned()),
				..base.clone()
			},
			ListKey { admins: Some(true), ..base.clone() },
			ListKey { locked: true, ..base.clone() },
			ListKey {
				deactivated: DeactivatedFilter::Only,
				..base.clone()
			},
			ListKey {
				excluded_types: vec![String::new()],
				..base.clone()
			},
		];

		let start = Instant::now();
		let mut cache = ListCache::new();

		cache.put(base.clone(), names("@a:x"), start);

		for other in &changed {
			assert!(*other != base);
			assert!(cache.get(other, start).is_none());
		}

		assert!(cache.get(&base, start).is_some());
	}
}
