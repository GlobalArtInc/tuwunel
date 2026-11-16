use std::time::{Duration, Instant};

use axum::http::HeaderValue;
use openraft::{
	entry::RaftEntry, impls::Vote, raft::AppendEntriesRequest, type_config::alias::EntryOf,
};
use tuwunel_database::Entry;

use super::{TOKEN, log_id, write};
use crate::{
	Node, Request, TypeConfig,
	net::{
		APPEND_LIMIT, Peer, Secret,
		client::chunks,
		limiter::Limiter,
		readiness::{Contact, Lag, entries_behind},
		validate_node, wire,
	},
};

#[test]
fn a_large_append_is_sent_as_requests_that_continue_each_other() {
	let big = |index| {
		EntryOf::<TypeConfig>::new_normal(
			log_id(1, index),
			Request(Entry { rep: vec![0; 600_000], ..write(index) }),
		)
	};

	let request = AppendEntriesRequest {
		vote: Vote::new_committed(1, 0),
		prev_log_id: Some(log_id(1, 4)),
		entries: (5..10).map(big).collect(),
		leader_commit: Some(log_id(1, 9)),
	};

	let sent: Vec<AppendEntriesRequest<TypeConfig>> = chunks(&request)
		.expect("the request splits")
		.iter()
		.map(|chunk| {
			assert!(chunk.body.len() <= APPEND_LIMIT, "a chunk fits a request");

			wire::decode(&chunk.body).expect("a chunk decodes")
		})
		.collect();

	assert_eq!(sent.len(), 5, "each entry is too big to share a chunk");

	for (position, chunk) in sent.iter().enumerate() {
		let first = chunk.entries[0].log_id();
		let previous = u64::try_from(position).expect("small");

		assert_eq!(
			chunk.prev_log_id,
			Some(log_id(1, previous.saturating_add(4))),
			"chunks chain"
		);
		assert_eq!(first, log_id(1, previous.saturating_add(5)), "chunks keep the order");
	}

	assert_eq!(sent[0].leader_commit, Some(log_id(1, 5)), "commit stops at the chunk");
	assert_eq!(sent[4].leader_commit, Some(log_id(1, 9)), "the last chunk commits all");

	let huge = AppendEntriesRequest {
		entries: vec![EntryOf::<TypeConfig>::new_normal(
			log_id(1, 5),
			Request(Entry { rep: vec![0; APPEND_LIMIT], ..write(5) }),
		)],
		..request
	};

	let error = chunks(&huge)
		.err()
		.expect("one entry beyond the limit cannot be sent");

	assert!(error.contains("cannot be split"), "{error}");
}

#[test]
fn the_secret_matches_only_its_own_bearer_token() {
	let secret = Secret::new("  correct-horse-battery  \n").expect("secret");
	let header = |text: &str| HeaderValue::from_str(text).expect("header");

	assert!(secret.accepts(Some(&secret.authorization())), "its own header");
	assert!(secret.accepts(Some(&header("bearer correct-horse-battery"))), "any case");

	for wrong in [
		"Bearer correct-horse-batterz",
		"Bearer correct-horse-battery-and-more",
		"Bearer correct-horse",
		"Basic correct-horse-battery",
		"correct-horse-battery",
		"Bearer",
	] {
		assert!(!secret.accepts(Some(&header(wrong))), "{wrong:?} is refused");
	}

	assert!(!secret.accepts(None), "no header is refused");
}

#[test]
fn a_weak_or_unsendable_secret_is_refused_and_never_printed() {
	for text in [
		"",
		"   ",
		"too-short",
		"newline-inside\nthe-middle-of-the-secret",
		"a-secret-with spaces-inside-of-it",
		"секрет-секрет-секрет-секрет",
		"ascii-secret-with-a-last-byte-\u{e9}",
	] {
		let error = Secret::new(text)
			.expect_err("the secret is refused")
			.to_string();

		let secret = text.trim();
		assert!(
			secret.is_empty() || !error.contains(secret),
			"the error repeats the secret: {error}"
		);
		assert!(!error.contains("middle") && !error.contains("секрет"), "{error}");
	}

	let secret = Secret::new(TOKEN).expect("secret");

	assert_eq!(format!("{secret:?}"), "Secret(..)", "debug output hides the secret");
	assert_eq!(format!("{:?}", secret.authorization()), "Sensitive", "so does the header");
}

#[test]
fn member_records_name_a_base_url_and_a_real_incarnation() {
	let node = |addr: &str, incarnation| Node { addr: addr.to_owned(), incarnation };

	for good in ["http://tuwunel-0.peers.ns.svc:8009", "https://10.0.0.1:8009/", "http://a"] {
		assert!(validate_node(&node(good, 1)).is_ok(), "{good} is a base URL");
	}

	for bad in [
		"",
		"tuwunel-0:8009",
		"ftp://host",
		"http://",
		"http://user:secret@host:8009",
		"http://host:8009/path",
		"http://host:8009/?q=1",
		"http://host:8009/#frag",
	] {
		assert!(validate_node(&node(bad, 1)).is_err(), "{bad:?} is refused");
	}

	assert!(
		validate_node(&node("http://host:8009", 0)).is_err(),
		"a zero incarnation is refused"
	);
}

#[test]
fn peers_are_named_the_same_way_by_both_ends() {
	let peer = Peer::new(3, 255);

	assert_eq!(peer.to_string(), "3:000000000000000000000000000000ff", "the wire form");
	assert_eq!(peer.to_string().parse::<Peer>(), Ok(peer), "it parses back");

	for bad in [
		"",
		"3",
		"3:ff",
		"x:000000000000000000000000000000ff",
		"3:zz0000000000000000000000000000ff",
	] {
		assert!(bad.parse::<Peer>().is_err(), "{bad:?} is not a peer");
	}
}

fn after(base: Instant, millis: u64) -> Instant {
	base.checked_add(Duration::from_millis(millis))
		.expect("the instant is in range")
}

#[test]
fn a_follower_is_ready_within_the_lag_limits_and_not_beyond_them() {
	let lag = |entries, behind_ms, silent_ms: Option<u64>| Lag {
		entries,
		behind_for: Duration::from_millis(behind_ms),
		silent_for: silent_ms.map(Duration::from_millis),
	};

	for (what, lag, ready) in [
		("current and in touch", lag(0, 0, Some(10)), true),
		("exactly at the entry limit", lag(1000, 100, Some(10)), true),
		("one entry past the limit", lag(1001, 100, Some(10)), false),
		("behind for exactly the limit", lag(5, 5000, Some(10)), true),
		("behind for a moment longer", lag(5, 5001, Some(10)), false),
		("at par, whatever the clock says", lag(0, 60_000, Some(10)), true),
		("silent for exactly the limit", lag(0, 0, Some(5000)), true),
		("silent for a moment longer", lag(0, 0, Some(5001)), false),
		("never heard from the leader", lag(0, 0, None), false),
	] {
		assert_eq!(lag.verdict().is_ok(), ready, "{what}: {:?}", lag.verdict());
	}
}

#[test]
fn the_entries_behind_are_counted_from_the_commit_and_the_applied_index() {
	for (commit, applied, behind) in [
		(None, None, 0),
		(None, Some(7), 0),
		(Some(0), None, 1),
		(Some(9), None, 10),
		(Some(9), Some(9), 0),
		(Some(9), Some(4), 5),
		(Some(4), Some(9), 0),
	] {
		assert_eq!(
			entries_behind(commit, applied),
			behind,
			"commit {commit:?}, applied {applied:?}"
		);
	}
}

#[test]
fn the_lag_follows_the_leader_commit_and_the_clock() {
	let base = Instant::now();
	let mut contact = Contact::new(base);

	let lag = contact.measure(None, None, after(base, 100));
	assert!(lag.silent_for.is_none(), "nothing was heard yet");
	assert_eq!(lag.entries, 0, "nothing is committed yet");

	contact.heard(Some(10), after(base, 200));

	let lag = contact.measure(None, Some(10), after(base, 1000));
	assert_eq!(lag.entries, 0, "everything the leader committed is applied");
	assert_eq!(
		lag.silent_for,
		Some(Duration::from_millis(800)),
		"silence counts from the request"
	);

	let lag = contact.measure(None, Some(4), after(base, 3000));
	assert_eq!(lag.entries, 6, "the commit of the request counts, not the log of the follower");
	assert_eq!(lag.behind_for, Duration::from_secs(2), "behind since it was last at par");

	let lag = contact.measure(Some(12), Some(4), after(base, 4000));
	assert_eq!(lag.entries, 8, "a larger commit of the Raft core counts as well");

	contact.heard(Some(3), after(base, 4500));

	let lag = contact.measure(None, Some(4), after(base, 5000));
	assert_eq!(lag.entries, 6, "the commit index of the leader never moves back");
	assert_eq!(lag.silent_for, Some(Duration::from_millis(500)), "a request ends the silence");
}

#[test]
fn a_repeating_refusal_is_logged_once_per_interval_with_the_count_held_back() {
	let base = Instant::now();
	let mut limiter = Limiter::new(Duration::from_secs(10));

	assert_eq!(limiter.take(base), Some(0), "the first one is logged");
	assert_eq!(limiter.take(after(base, 1)), None, "the next is held back");
	assert_eq!(limiter.take(after(base, 9999)), None, "up to the interval");
	assert_eq!(
		limiter.take(after(base, 10_000)),
		Some(2),
		"then one passes and reports the two"
	);
	assert_eq!(limiter.take(after(base, 10_001)), None, "and the count starts over");
	assert_eq!(limiter.take(after(base, 20_000)), Some(1), "the next interval reports one");
}
