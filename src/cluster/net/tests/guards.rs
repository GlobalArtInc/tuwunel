use std::{collections::BTreeMap, time::Duration};

use openraft::{
	impls::{EntryPayload, Membership, Vote},
	raft::{AppendEntriesRequest, VoteRequest, VoteResponse},
};
use reqwest::StatusCode;
use tokio::time::sleep;
use tuwunel_core::metrics::prom::Rejection;

use super::{
	TOKEN, WAIT, eventually, follower_append, pair, post, prepare, raw_membership, run, state,
	torn,
};
use crate::{
	Node, Request, TypeConfig,
	net::{
		APPEND, APPEND_LIMIT, PRE_VOTE, Peer, SMALL_LIMIT, TRANSFER, VOTE, validate_membership,
		wire,
	},
};

#[tokio::test(flavor = "multi_thread")]
async fn a_foreign_incarnation_is_refused_with_409_and_nothing_changes() {
	let (leader, follower) = pair().await;
	let before = state(&follower);

	let higher: VoteRequest<TypeConfig> =
		VoteRequest::new(Vote::new(before.0.leader_id.term.saturating_add(5), 0), None);
	let vote = wire::encode(&higher).expect("encodes");
	let append = wire::encode(&follower_append(&follower, EntryPayload::Blank)).expect("encodes");

	let stranger = Peer::new(0, leader.peer.incarnation ^ 1);
	let unknown = Peer::new(7, 1);
	let renamed = Peer::new(1, follower.peer.incarnation ^ 1);
	let myself = Peer::new(1, follower.peer.incarnation);

	for (from, to) in [
		(stranger, follower.peer),
		(unknown, follower.peer),
		(leader.peer, renamed),
		(myself, follower.peer),
	] {
		for (route, body) in
			[(VOTE, &vote), (PRE_VOTE, &vote), (TRANSFER, &vote), (APPEND, &append)]
		{
			let response =
				post(&follower, route, Some(TOKEN), Some(from), Some(to), body.clone()).await;

			assert_eq!(response.status(), StatusCode::CONFLICT, "{route} from {from} to {to}");
		}
	}

	for (from, to) in [(None, Some(follower.peer)), (Some(leader.peer), None)] {
		let response = post(&follower, VOTE, Some(TOKEN), from, to, vote.clone()).await;

		assert_eq!(response.status(), StatusCode::BAD_REQUEST, "a request must name both nodes");
	}

	assert_eq!(state(&follower), before, "neither the vote nor the log moved");

	let lower: VoteRequest<TypeConfig> = VoteRequest::new(Vote::new(0, 0), None);
	let control = post(
		&follower,
		VOTE,
		Some(TOKEN),
		Some(leader.peer),
		Some(follower.peer),
		wire::encode(&lower).expect("encodes"),
	)
	.await;

	assert_eq!(control.status(), StatusCode::OK, "the real leader is heard");

	let pre = post(
		&follower,
		PRE_VOTE,
		Some(TOKEN),
		Some(leader.peer),
		Some(follower.peer),
		wire::encode(&lower).expect("encodes"),
	)
	.await;

	assert_eq!(pre.status(), StatusCode::OK, "a pre-vote of the real leader is heard too");

	let answer: VoteResponse<TypeConfig> =
		wire::decode(&control.bytes().await.expect("body")).expect("decodes");

	assert!(!answer.vote_granted, "an old vote is not granted, but it was considered");

	let group = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	assert_eq!(group.rejections(Rejection::Identity), 16, "identity refusals are counted");
	assert_eq!(group.rejections(Rejection::Malformed), 2, "malformed headers are counted");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_under_another_incarnation_than_recorded_is_never_admitted() {
	let (first, second) = (prepare(0).await, prepare(1).await);
	let mut recorded = second.node.clone();
	recorded.incarnation ^= 1;

	let (leader, follower) = (run(first, false).await, run(second, false).await);

	leader
		.raft
		.initialize(Membership::from(BTreeMap::from([(0, leader.node.clone())])))
		.await
		.expect("a cluster of one forms");

	leader
		.raft
		.wait(WAIT)
		.applied_index_at_least(Some(1), "the leader committed its first entries")
		.await
		.expect("a cluster of one commits on its own");

	leader
		.raft
		.add_learner(1, recorded, false)
		.await
		.expect("the learner is recorded under the other incarnation");

	let group = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;

	eventually("the node is turned away by its identity", async || {
		group.rejections(Rejection::Identity) > 0
	})
	.await;

	sleep(Duration::from_millis(500)).await;

	let (vote, last) = state(&follower);
	assert_eq!((vote.leader_id.term, last), (0, None), "the replaced node saw nothing");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_recorded_under_another_incarnation_than_its_own_stops_answering() {
	let (leader, follower) = pair().await;
	let mut other = follower.node.clone();
	other.incarnation ^= 1;

	let recorded = EntryPayload::Membership(Membership::from(BTreeMap::from([
		(0, leader.node.clone()),
		(1, other),
	])));

	let admitted = post(
		&follower,
		APPEND,
		Some(TOKEN),
		Some(leader.peer),
		Some(follower.peer),
		wire::encode(&follower_append(&follower, recorded)).expect("encodes"),
	)
	.await;

	assert_eq!(
		admitted.status(),
		StatusCode::OK,
		"the entry arrives while the node is still named"
	);

	let lower: VoteRequest<TypeConfig> = VoteRequest::new(Vote::new(0, 0), None);
	let body = wire::encode(&lower).expect("encodes");

	eventually("the node refuses the sender it trusted a moment ago", async || {
		post(
			&follower,
			VOTE,
			Some(TOKEN),
			Some(leader.peer),
			Some(follower.peer),
			body.clone(),
		)
		.await
		.status()
			== StatusCode::CONFLICT
	})
	.await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_entries_are_refused_before_the_log() {
	let (leader, follower) = pair().await;
	let before = state(&follower);

	let bad_node =
		|node: Node| EntryPayload::Membership(Membership::from(BTreeMap::from([(1, node)])));

	for payload in [
		EntryPayload::Normal(Request(torn())),
		bad_node(Node {
			addr: "ftp://elsewhere".to_owned(),
			incarnation: 5,
		}),
		bad_node(Node {
			addr: "http://elsewhere:8009".to_owned(),
			incarnation: 0,
		}),
	] {
		let body = wire::encode(&follower_append(&follower, payload)).expect("encodes");
		let response =
			post(&follower, APPEND, Some(TOKEN), Some(leader.peer), Some(follower.peer), body)
				.await;

		assert_eq!(response.status(), StatusCode::BAD_REQUEST, "a bad entry is refused");
	}

	assert_eq!(state(&follower), before, "no entry reached the log");

	let group = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	assert_eq!(group.rejections(Rejection::Invalid), 3, "refused entries are counted");
}

#[tokio::test(flavor = "multi_thread")]
async fn memberships_that_would_stop_every_node_are_refused_before_the_log() {
	let (leader, follower) = pair().await;
	let before = state(&follower);

	let record = |incarnation| Node {
		addr: "http://elsewhere:8009".to_owned(),
		incarnation,
	};

	let nodes = BTreeMap::from([(1, record(5))]);
	let both = BTreeMap::from([(1, record(5)), (2, record(6))]);

	for (what, membership) in [
		("no voter configuration", raw_membership(&[], &nodes)),
		("an empty voter configuration", raw_membership(&[&[]], &nodes)),
		("an empty second configuration", raw_membership(&[&[1], &[]], &nodes)),
		("a voter without a record", raw_membership(&[&[1, 2]], &nodes)),
		(
			"a voter of the second configuration without a record",
			raw_membership(&[&[1], &[1, 2]], &nodes),
		),
	] {
		let body =
			wire::encode(&follower_append(&follower, EntryPayload::Membership(membership)))
				.expect("encodes");

		let response =
			post(&follower, APPEND, Some(TOKEN), Some(leader.peer), Some(follower.peer), body)
				.await;

		assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{what} is refused");
	}

	assert_eq!(state(&follower), before, "no membership reached the log");
	assert!(
		validate_membership(&raw_membership(&[&[1], &[1, 2]], &both)).is_ok(),
		"a joint membership whose voters all have records is fine"
	);

	let group = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	assert_eq!(group.rejections(Rejection::Invalid), 5, "refused memberships are counted");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_in_no_membership_hears_an_unknown_sender_only_on_append() {
	let fresh = run(prepare(2).await, false).await;
	let stranger = Peer::new(5, 1);

	let heartbeat: AppendEntriesRequest<TypeConfig> = AppendEntriesRequest {
		vote: Vote::new_committed(1, 5),
		prev_log_id: None,
		entries: Vec::new(),
		leader_commit: None,
	};

	let lower: VoteRequest<TypeConfig> = VoteRequest::new(Vote::new(0, 5), None);
	let vote = wire::encode(&lower).expect("encodes");

	let response = post(
		&fresh,
		APPEND,
		Some(TOKEN),
		Some(stranger),
		Some(fresh.peer),
		wire::encode(&heartbeat).expect("encodes"),
	)
	.await;

	assert_eq!(
		response.status(),
		StatusCode::OK,
		"a node learns its first membership by append"
	);

	for route in [VOTE, PRE_VOTE, TRANSFER] {
		let response =
			post(&fresh, route, Some(TOKEN), Some(stranger), Some(fresh.peer), vote.clone())
				.await;

		assert_eq!(response.status(), StatusCode::CONFLICT, "{route} from an unknown node");
	}

	let group = &fresh
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	assert_eq!(group.rejections(Rejection::Identity), 3, "each refusal is counted");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_token_is_refused_before_the_identity_is_looked_at() {
	let (_leader, follower) = pair().await;
	let before = state(&follower);
	let stranger = Peer::new(7, 1);
	let renamed = Peer::new(1, follower.peer.incarnation ^ 1);

	for route in [APPEND, VOTE, PRE_VOTE, TRANSFER] {
		let response = post(
			&follower,
			route,
			Some("another-shared-cluster-secret"),
			Some(stranger),
			Some(renamed),
			Vec::new(),
		)
		.await;

		assert_eq!(
			response.status(),
			StatusCode::UNAUTHORIZED,
			"{route} answers the token first"
		);
	}

	let group = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	assert_eq!(group.rejections(Rejection::Unauthorized), 4, "the refusals are counted as such");
	assert_eq!(group.rejections(Rejection::Identity), 0, "the identity was never looked at");
	assert_eq!(state(&follower), before, "nothing moved");
}

#[tokio::test(flavor = "multi_thread")]
async fn bodies_over_the_limit_are_refused_with_413() {
	let (leader, follower) = pair().await;
	let identity = (Some(leader.peer), Some(follower.peer));

	for (route, size) in [(APPEND, APPEND_LIMIT), (VOTE, SMALL_LIMIT)] {
		let body = vec![0_u8; size.saturating_add(1)];
		let response = post(&follower, route, Some(TOKEN), identity.0, identity.1, body).await;

		assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE, "{route} over its limit");
	}

	let response =
		post(&follower, VOTE, Some(TOKEN), identity.0, identity.1, vec![0xFF; 16]).await;

	assert_eq!(response.status(), StatusCode::BAD_REQUEST, "garbage is not a vote");

	let group = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	assert_eq!(group.rejections(Rejection::TooLarge), 2, "oversized bodies are counted");
}
