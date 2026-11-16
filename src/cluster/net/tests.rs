mod client;
mod cluster;
mod faults;
mod guards;
mod unit;

use std::{
	collections::{BTreeMap, BTreeSet},
	future::pending,
	sync::Arc,
	time::Duration,
};

use openraft::{
	Config, Raft,
	impls::Membership,
	raft::AppendEntriesRequest,
	storage::RaftStateMachine,
	type_config::{
		alias::{CommittedLeaderIdOf, EntryOf, EntryPayloadOf, LogIdOf, VoteOf},
		async_runtime::WatchReceiver,
	},
};
use reqwest::{Client as Http, Response};
use serde::Serialize;
use tokio::{net::TcpListener, time::sleep};
use tuwunel_core::metrics::Metrics;
use tuwunel_database::Entry;

use super::{
	Factory, Faults, Peer, Secret, Server, Serving, Settings, serve,
	wire::{FROM, TO},
};
use crate::{
	LogStore, Node, NodeId, SyncDelay, TypeConfig,
	tests::{TempDir, machine::Machine},
};

const TOKEN: &str = "a-shared-cluster-secret";

const WAIT: Option<Duration> = Some(Duration::from_secs(15));

struct Prepared {
	id: NodeId,
	dir: TempDir,
	store: LogStore,
	listener: TcpListener,
	node: Node,
}

struct Running<SM = Machine>
where
	SM: RaftStateMachine<TypeConfig>,
{
	node: Node,
	peer: Peer,
	raft: Raft<TypeConfig, SM>,
	sync: SyncDelay,
	serving: Serving,
	metrics: Arc<Metrics>,
	faults: Arc<Faults>,
	_dir: TempDir,
}

async fn prepare(id: NodeId) -> Prepared {
	let dir = TempDir::new();
	let store = LogStore::open(dir.path())
		.await
		.expect("store opens");

	store
		.meta()
		.bind_node(id)
		.await
		.expect("node is bound");

	let listener = TcpListener::bind("127.0.0.1:0")
		.await
		.expect("listener binds");

	let node = Node {
		addr: format!("http://{}", listener.local_addr().expect("listener address")),
		incarnation: store.meta().incarnation(),
	};

	Prepared { id, dir, store, listener, node }
}

const HEARTBEAT_MS: u64 = 40;

async fn run(prepared: Prepared, allow_faults: bool) -> Running {
	run_with(prepared, allow_faults, Machine::default(), HEARTBEAT_MS).await
}

async fn run_with<SM>(
	prepared: Prepared,
	allow_faults: bool,
	machine: SM,
	heartbeat_ms: u64,
) -> Running<SM>
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let Prepared { id, dir, store, listener, node } = prepared;

	let secret = Secret::new(TOKEN).expect("secret");
	let peer = Peer::new(id, node.incarnation);
	let sync = store.sync_delay();
	let faults = if allow_faults {
		Faults::on(sync.clone())
	} else {
		Faults::off()
	};
	let factory = Factory::<SM::SnapshotData>::new(peer, secret.clone(), Arc::clone(&faults))
		.expect("factory");

	let config = Config {
		heartbeat_interval: heartbeat_ms,
		election_timeout_min: heartbeat_ms.saturating_mul(6),
		election_timeout_max: heartbeat_ms.saturating_mul(12),
		..Config::default()
	}
	.validate()
	.expect("config");

	let raft = Raft::new(id, Arc::new(config), factory, store, machine)
		.await
		.expect("raft starts");

	let metrics = Metrics::new(None);
	metrics.enable_prometheus();

	let server = Server::new(raft.clone(), Settings {
		me: peer,
		secret,
		faults: Arc::clone(&faults),
		lease: Duration::from_secs(1),
		metrics: Some(Arc::clone(&metrics)),
	});

	let serving = server.serving();
	drop(server.observe());
	drop(tokio::spawn(serve(listener, server.router(), pending())));

	Running {
		node,
		peer,
		raft,
		sync,
		serving,
		metrics,
		faults,
		_dir: dir,
	}
}

async fn pair() -> (Running, Running) {
	let (first, second) = (prepare(0).await, prepare(1).await);
	let learner = second.node.clone();
	let (leader, follower) = (run(first, false).await, run(second, true).await);

	leader
		.raft
		.initialize(Membership::from(BTreeMap::from([(0, leader.node.clone())])))
		.await
		.expect("a cluster of one forms");

	leader
		.raft
		.wait(WAIT)
		.current_leader(0, "the only node leads")
		.await
		.expect("the only voter elects itself");

	leader
		.raft
		.wait(WAIT)
		.applied_index_at_least(Some(1), "the leader committed its first entries")
		.await
		.expect("a cluster of one commits on its own");

	leader
		.raft
		.add_learner(1, learner, true)
		.await
		.expect("the learner catches up over HTTP");

	let promoted = leader
		.raft
		.change_membership(BTreeSet::from([0, 1]), false)
		.await
		.expect("the learner becomes a voter");

	for node in [&leader, &follower] {
		node.raft
			.wait(WAIT)
			.current_leader(0, "the nodes agree on the leader")
			.await
			.expect("both nodes know the leader");

		node.raft
			.wait(WAIT)
			.applied_index_at_least(Some(promoted.log_id.index), "both nodes applied the change")
			.await
			.expect("the membership change reaches the follower over HTTP");
	}

	(leader, follower)
}

fn http() -> Http {
	Http::builder()
		.no_proxy()
		.build()
		.expect("client")
}

async fn post(
	to: &Running,
	route: &str,
	token: Option<&str>,
	from: Option<Peer>,
	whom: Option<Peer>,
	body: Vec<u8>,
) -> Response {
	let mut request = http()
		.post(format!("{}{route}", to.node.addr))
		.body(body);

	if let Some(token) = token {
		request = request.bearer_auth(token);
	}

	if let Some(from) = from {
		request = request.header(FROM, from.to_string());
	}

	if let Some(whom) = whom {
		request = request.header(TO, whom.to_string());
	}

	request.send().await.expect("the node answers")
}

async fn get(to: &Running, route: &str, token: Option<&str>) -> Response {
	let mut request = http().get(format!("{}{route}", to.node.addr));
	if let Some(token) = token {
		request = request.bearer_auth(token);
	}

	request.send().await.expect("the node answers")
}

async fn eventually<F>(what: &str, mut check: F)
where
	F: AsyncFnMut() -> bool,
{
	for _ in 0..200 {
		if check().await {
			return;
		}

		sleep(Duration::from_millis(50)).await;
	}

	panic!("{what} did not happen in time");
}

fn log_id(term: u64, index: u64) -> LogIdOf<TypeConfig> {
	LogIdOf::<TypeConfig>::new(CommittedLeaderIdOf::<TypeConfig>::new(term), index)
}

fn write(start: u64) -> Entry {
	Entry {
		version: Entry::VERSION,
		origin: 0xFEED,
		seq: start..start.saturating_add(1),
		cfs: Vec::new(),
		rep: vec![0x1, 3, b'k', b'e', b'y', 1, b'v'],
		hints: None,
	}
}

#[derive(Serialize)]
struct RawMembership {
	configs: Vec<BTreeSet<NodeId>>,
	nodes: BTreeMap<NodeId, Node>,
}

fn raw_membership(
	configs: &[&[NodeId]],
	nodes: &BTreeMap<NodeId, Node>,
) -> Membership<NodeId, Node> {
	let raw = RawMembership {
		configs: configs
			.iter()
			.map(|voters| voters.iter().copied().collect())
			.collect(),
		nodes: nodes.clone(),
	};

	postcard::from_bytes(&postcard::to_allocvec(&raw).expect("encodes"))
		.expect("a membership decodes whatever its parts say")
}

fn torn() -> Entry { Entry { seq: 5..9, ..write(0) } }

fn follower_append(
	follower: &Running,
	payload: EntryPayloadOf<TypeConfig>,
) -> AppendEntriesRequest<TypeConfig> {
	let metrics = follower.raft.metrics().borrow_watched().clone();
	let last = metrics
		.last_log_index
		.expect("the follower has a log");
	let term = metrics.vote.leader_id.term;

	AppendEntriesRequest {
		vote: metrics.vote,
		prev_log_id: Some(log_id(term, last)),
		entries: vec![EntryOf::<TypeConfig> {
			log_id: log_id(term, last.saturating_add(1)),
			payload,
		}],
		leader_commit: None,
	}
}

fn state(node: &Running) -> (VoteOf<TypeConfig>, Option<u64>) {
	let metrics = node.raft.metrics().borrow_watched().clone();

	(metrics.vote, metrics.last_log_index)
}
