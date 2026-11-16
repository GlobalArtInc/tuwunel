use std::{
	sync::Arc,
	time::{Duration, Instant},
};

use openraft::{
	errors::RPCError,
	impls::Vote,
	network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
	raft::AppendEntriesRequest,
};
use reqwest::StatusCode;

use super::{TOKEN, eventually, get, http, pair, prepare, run, state};
use crate::{
	TypeConfig,
	net::{FAULTS, Factory, Faults, Peer, Secret, Setup, Target},
};

#[tokio::test(flavor = "multi_thread")]
async fn faults_are_unavailable_unless_allowed_and_checked_when_they_are() {
	let (leader, follower) = pair().await;

	assert_eq!(
		get(&leader, FAULTS, Some(TOKEN)).await.status(),
		StatusCode::NOT_FOUND,
		"a node that does not allow faults has no such route"
	);

	let put = |body: &'static str, token: &'static str| {
		let url = format!("{}{FAULTS}", follower.node.addr);

		async move {
			http()
				.put(url)
				.bearer_auth(token)
				.header("content-type", "application/json")
				.body(body)
				.send()
				.await
				.expect("the node answers")
		}
	};

	assert_eq!(
		put("{}", "another-shared-cluster-secret")
			.await
			.status(),
		StatusCode::UNAUTHORIZED,
		"faults need the token"
	);

	for refused in [
		r#"{"rpc":[{"node":0,"loss_percent":101}]}"#,
		r#"{"rpc":[{"node":0,"delay_ms":999999}]}"#,
		r#"{"rpc":[{"node":0},{"node":0}]}"#,
		r#"{"sync_delay_ms":999999}"#,
		r#"{"sync_delay":5}"#,
	] {
		assert!(
			put(refused, TOKEN)
				.await
				.status()
				.is_client_error(),
			"{refused} is refused"
		);
	}

	assert_eq!(follower.faults.setup(), Setup::default(), "a refusal changes nothing");

	let accepted = put(r#"{"rpc":[{"node":0,"delay_ms":5}],"sync_delay_ms":30}"#, TOKEN).await;

	assert_eq!(accepted.status(), StatusCode::OK, "a valid setup is applied");
	assert_eq!(
		follower.sync.get(),
		Duration::from_millis(30),
		"the sync delay reaches the store"
	);
	assert!(
		follower
			.faults
			.rule(0)
			.is_some_and(|rule| rule.delay_ms == 5),
		"the rule is in force"
	);

	let cleared = http()
		.delete(format!("{}{FAULTS}", follower.node.addr))
		.bearer_auth(TOKEN)
		.send()
		.await
		.expect("the node answers");

	assert_eq!(cleared.status(), StatusCode::OK, "faults can be cleared");
	assert_eq!(follower.sync.get(), Duration::ZERO, "clearing restores the sync");
	assert!(follower.faults.rule(0).is_none(), "clearing removes the rules");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lost_rpc_never_reaches_the_peer_and_a_delayed_one_arrives_late() {
	let (caller, target) = (prepare(0).await, prepare(1).await);
	let target_node = target.node.clone();
	let secret = Secret::new(TOKEN).expect("secret");
	let faults = Faults::on(caller.store.sync_delay());
	let mut factory = Factory::<Vec<u8>>::new(
		Peer::new(0, caller.node.incarnation),
		secret,
		Arc::clone(&faults),
	)
	.expect("factory");

	let target = run(target, false).await;
	let mut client = factory.new_client(1, &target_node).await;
	let option = || RPCOption::new(Duration::from_millis(300));
	let request = || AppendEntriesRequest::<TypeConfig> {
		vote: Vote::new_committed(9, 0),
		prev_log_id: None,
		entries: Vec::new(),
		leader_commit: None,
	};
	let rule = |loss_percent, delay_ms| Setup {
		rpc: vec![Target { node: None, loss_percent, delay_ms }],
		sync_delay_ms: 0,
	};

	faults
		.apply(&rule(100, 0))
		.expect("faults are on");

	let started = Instant::now();
	let lost = client.append_entries(request(), option()).await;

	assert!(matches!(lost, Err(RPCError::Timeout(_))), "a lost RPC times out: {lost:?}");
	assert!(started.elapsed() >= Duration::from_millis(300), "it waits out its deadline");
	assert_eq!(state(&target).0.leader_id.term, 0, "the peer never saw it");

	faults
		.apply(&rule(0, 150))
		.expect("faults are on");

	let started = Instant::now();
	let delayed = client
		.append_entries(request(), option())
		.await
		.expect("the delayed RPC arrives");

	assert!(started.elapsed() >= Duration::from_millis(150), "it is held back");
	assert!(delayed.is_ok(), "and then it is an ordinary append: {delayed:?}");

	eventually("the peer records the vote of the append", async || {
		state(&target).0.leader_id.term == 9
	})
	.await;
}
