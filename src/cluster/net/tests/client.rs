use std::{
	collections::BTreeMap,
	path::Path,
	sync::{Arc, Mutex},
	time::Duration,
};

use futures::TryStreamExt;
use openraft::{
	errors::RPCError,
	impls::{Membership, Vote},
	network::{RPCOption, RaftNetworkFactory, RaftNetworkV2},
	raft::VoteRequest,
};
use tokio::{
	net::{TcpListener, TcpStream},
	sync::watch,
	time::{sleep, timeout},
};
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
};
use tuwunel_database::{Database, Entry, Role};

use super::{TOKEN, WAIT, prepare, run_with};
use crate::{
	Machine as Replica, Node, Request, TypeConfig,
	net::{Factory, Faults, Peer, Secret},
	tests::TempDir,
};

const MAP: &str = "alias_roomid";

const BIG: usize = 800_000;

const SLOW_HEARTBEAT_MS: u64 = 1000;

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

async fn open(path: &Path, role: Role) -> Result<Arc<Database>> {
	let raw = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_path", path));

	let runtime = tokio::runtime::Handle::current();
	let logging = Logging {
		subscriber: Arc::new(NoSubscriber::new()),
		reload: LogLevelReloadHandles::default(),
		capture: Arc::new(State::new()),
	};

	let metrics = Metrics::new(Some(&runtime));
	let config = Config::new(&raw)?;
	let server = Server::new(config, Sources::default(), Some(&runtime), logging, metrics);
	let db = Database::open(&Arc::new(server)).await?;
	db.engine.set_role(role)?;

	Ok(db)
}

async fn rows(db: &Database) -> Result<Rows> {
	db.get(MAP)?
		.raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await
}

fn drain(db: &Database) -> Vec<Entry> {
	std::iter::from_fn(|| {
		db.engine
			.drain_captured(0)
			.expect("drain captured")
	})
	.collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_learner_catches_up_on_a_batch_larger_than_a_request_and_applies_it() -> Result {
	let dir = TempDir::new();
	let leader_db = open(&dir.path().join("leader"), Role::Leader).await?;
	let follower_db = open(&dir.path().join("follower"), Role::Follower).await?;

	let (first, second) = (prepare(0).await, prepare(1).await);
	let learner = second.node.clone();
	let leading = Replica::new(Arc::clone(&leader_db), watch::channel(0).0);
	let following = Replica::new(Arc::clone(&follower_db), watch::channel(0).0);

	let leader = run_with(first, false, leading, SLOW_HEARTBEAT_MS).await;
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

	let map = leader_db.get(MAP)?;
	for index in 0..3_u8 {
		map.insert(&[b'k', index], vec![index; BIG]);
	}

	let mut last = 0;
	for entry in drain(&leader_db) {
		last = leader
			.raft
			.client_write(Request(entry))
			.await
			.expect("the lone voter commits a write")
			.log_id
			.index;
	}

	let follower = run_with(second, false, following, SLOW_HEARTBEAT_MS).await;
	timeout(Duration::from_mins(1), leader.raft.add_learner(1, learner, true))
		.await
		.expect("the learner catches up in time")
		.expect("the learner is added");

	follower
		.raft
		.wait(WAIT)
		.applied_index_at_least(Some(last), "the learner applies the big writes")
		.await
		.expect("the batch reaches the learner in requests the node accepts");

	let (ours, theirs) = (rows(&leader_db).await?, rows(&follower_db).await?);

	assert_eq!(ours.len(), 3, "the leader holds the three writes");
	assert!(ours.iter().all(|(_, value)| value.len() == BIG), "each is a big value");
	assert!(ours == theirs, "the learner's database equals the leader's");

	Ok(())
}

fn connections(accepted: &Mutex<Vec<TcpStream>>) -> usize {
	accepted
		.lock()
		.expect("connections are kept")
		.len()
}

#[tokio::test]
async fn a_peer_whose_address_is_not_a_base_url_is_never_sent_the_secret() {
	let listener = TcpListener::bind("127.0.0.1:0")
		.await
		.expect("listener binds");

	let port = listener.local_addr().expect("address").port();
	let accepted = Arc::new(Mutex::new(Vec::new()));
	let held = Arc::clone(&accepted);

	drop(tokio::spawn(async move {
		while let Ok((connection, _)) = listener.accept().await {
			held.lock()
				.expect("connections are kept")
				.push(connection);
		}
	}));

	let secret = Secret::new(TOKEN).expect("secret");
	let mut factory =
		Factory::<Vec<u8>>::new(Peer::new(0, 1), secret, Faults::off()).expect("factory");

	let request = || VoteRequest::<TypeConfig>::new(Vote::new(1, 0), None);
	let option = || RPCOption::new(Duration::from_millis(300));

	for addr in [
		format!("http://user:secret@127.0.0.1:{port}"),
		format!("http://127.0.0.1:{port}/prefix"),
		format!("http://127.0.0.1:{port}/?redirect=elsewhere"),
		format!("ftp://127.0.0.1:{port}"),
	] {
		let mut client = factory
			.new_client(1, &Node { addr: addr.clone(), incarnation: 7 })
			.await;

		let result = client.vote(request(), option()).await;

		assert!(matches!(result, Err(RPCError::Unreachable(_))), "{addr} is refused: {result:?}");
	}

	let mut client = factory
		.new_client(1, &Node {
			addr: format!("http://127.0.0.1:{port}"),
			incarnation: 0,
		})
		.await;

	assert!(client.vote(request(), option()).await.is_err(), "a zero incarnation is refused");

	sleep(Duration::from_millis(100)).await;
	assert_eq!(connections(&accepted), 0, "no refused peer saw a connection");

	let mut client = factory
		.new_client(1, &Node {
			addr: format!("http://127.0.0.1:{port}"),
			incarnation: 7,
		})
		.await;

	let result = client.vote(request(), option()).await;

	assert!(
		matches!(result, Err(RPCError::Timeout(_))),
		"a good address is dialled: {result:?}"
	);
	assert_eq!(connections(&accepted), 1, "and the listener does see a connection");
}
