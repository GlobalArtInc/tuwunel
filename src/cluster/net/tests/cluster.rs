use axum::http::header::WWW_AUTHENTICATE;
use reqwest::StatusCode;
use tuwunel_core::metrics::prom::Rejection;

use super::{TOKEN, WAIT, eventually, get, pair, post, prepare, run, write};
use crate::{
	Request,
	net::{
		APPEND, LEADER, LIVE, PRE_VOTE, READY, STATUS, TRANSFER, VOTE, observe::leader_changed,
	},
};

#[tokio::test(flavor = "multi_thread")]
async fn two_nodes_elect_and_replicate_over_http() {
	let (leader, follower) = pair().await;

	let written = leader
		.raft
		.client_write(Request(write(0)))
		.await
		.expect("the follower confirms the write");

	follower
		.raft
		.wait(WAIT)
		.applied_index_at_least(Some(written.log_id.index), "the follower applies the write")
		.await
		.expect("the write reaches the follower over HTTP");

	let ours = &leader
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	let theirs = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;

	eventually("both nodes publish their role", async || {
		ours.state_value() == 3 && theirs.state_value() == 1 && theirs.apply_index() > 0
	})
	.await;

	eventually("the leader sees the follower caught up", async || {
		ours.is_enabled()
			&& leader
				.metrics
				.prometheus()
				.is_some_and(|registry| {
					registry
						.render()
						.contains("tuwunel_cluster_follower_lag_entries{node=\"1\"} 0\n")
				})
	})
	.await;

	let leader_text = leader
		.metrics
		.prometheus()
		.expect("registry")
		.render();
	let follower_text = follower
		.metrics
		.prometheus()
		.expect("registry")
		.render();

	assert!(
		!leader_text.contains("tuwunel_cluster_quorum_ack_age_milliseconds -1"),
		"a leader reports when a majority last confirmed it: {leader_text}"
	);
	assert!(
		follower_text.contains("tuwunel_cluster_quorum_ack_age_milliseconds -1\n"),
		"a follower reports no confirmation age: {follower_text}"
	);

	assert!(ours.term() > 0, "the term is published");
	assert!(ours.commit_index() > 0, "the commit index is published");
	assert_eq!(ours.rejections(Rejection::Identity), 0, "honest nodes are not refused");

	let status: serde_json::Value = get(&follower, STATUS, Some(TOKEN))
		.await
		.json()
		.await
		.expect("status is JSON");

	assert_eq!(status["role"], "follower", "the status names the role: {status}");
	assert_eq!(status["leader"], 0, "the status names the leader: {status}");
	assert_eq!(
		status["membership"]["nodes"]["1"]["incarnation"],
		format!("{:032x}", follower.peer.incarnation),
		"the status shows the incarnations: {status}"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn leadership_moves_over_http_and_the_change_is_counted() {
	let (leader, follower) = pair().await;

	let old = &leader
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;

	let new = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;

	eventually("both nodes have published the leader they know", async || {
		old.state_value() == 3 && new.state_value() == 1
	})
	.await;

	leader
		.raft
		.trigger()
		.transfer_leader(1)
		.await
		.expect("the transfer starts");

	for node in [&leader, &follower] {
		node.raft
			.wait(WAIT)
			.current_leader(1, "node 1 took over")
			.await
			.expect("the transfer request and the vote travel over HTTP");
	}

	eventually("the roles and the change are published", async || {
		old.state_value() == 1
			&& new.state_value() == 3
			&& old.leader_changes() == 1
			&& new.leader_changes() == 1
	})
	.await;
}

#[test]
fn a_leader_change_is_a_different_leader_and_not_a_new_term() {
	let mut seen = None;

	assert!(!leader_changed(&mut seen, 0), "the first leader is not a change");
	assert!(!leader_changed(&mut seen, 0), "the same leader is not a change");
	assert!(leader_changed(&mut seen, 1), "another leader is");
	assert!(!leader_changed(&mut seen, 1), "and it is a change once");
	assert!(leader_changed(&mut seen, 0), "the first one returning is a change too");
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_without_the_token_are_refused_and_the_probes_are_open() {
	let (leader, follower) = pair().await;
	let identity = (Some(leader.peer), Some(follower.peer));

	for route in [APPEND, VOTE, PRE_VOTE, TRANSFER] {
		for token in [None, Some("another-shared-cluster-secret")] {
			let response = post(&follower, route, token, identity.0, identity.1, vec![]).await;

			assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{route} with {token:?}");
			assert!(
				response.headers().contains_key(WWW_AUTHENTICATE),
				"{route} asks for a token"
			);
		}
	}

	assert_eq!(
		get(&follower, STATUS, None).await.status(),
		StatusCode::UNAUTHORIZED,
		"the status needs the token"
	);

	assert_eq!(
		get(&follower, STATUS, Some(TOKEN)).await.status(),
		StatusCode::OK,
		"the status answers the token"
	);

	let group = &follower
		.metrics
		.prometheus()
		.expect("registry")
		.cluster;
	assert_eq!(group.rejections(Rejection::Unauthorized), 9, "every refusal is counted");

	for node in [&leader, &follower] {
		assert_eq!(get(node, LIVE, None).await.status(), StatusCode::OK, "live needs no token");
	}

	eventually("the follower is ready", async || {
		get(&follower, READY, None).await.status() == StatusCode::OK
	})
	.await;

	assert_eq!(
		get(&leader, LEADER, None).await.status(),
		StatusCode::SERVICE_UNAVAILABLE,
		"a leader whose services are not running is not offered"
	);

	leader.serving.set(true);

	eventually("the leader is offered once serving", async || {
		get(&leader, LEADER, None).await.status() == StatusCode::OK
	})
	.await;

	assert_eq!(
		get(&follower, LEADER, None).await.status(),
		StatusCode::SERVICE_UNAVAILABLE,
		"a follower is never offered"
	);

	let fresh = run(prepare(2).await, false).await;

	assert_eq!(
		get(&fresh, READY, None).await.status(),
		StatusCode::SERVICE_UNAVAILABLE,
		"a node outside any membership is not ready"
	);
	assert_eq!(get(&fresh, LIVE, None).await.status(), StatusCode::OK, "but it is alive");
}
