//! Helpers of the tests that run the state machine over real databases.

use std::{
	env::temp_dir,
	fs::{self, remove_dir_all},
	io,
	path::{Path, PathBuf},
	process::id,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use futures::{Stream, TryStreamExt, stream};
use openraft::{
	Config as RaftConfig, OptionalSend, RaftNetworkFactory, RaftNetworkV2, SnapshotPolicy,
	entry::RaftEntry,
	errors::{RPCError, ReplicationClosed, StreamingError, Unreachable},
	impls::EntryPayload,
	network::RPCOption,
	raft::{AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse},
	storage::{EntryResponder, IOFlushed, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine},
	type_config::{
		TypeConfigExt,
		alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, SnapshotOf, VoteOf},
	},
};
use tokio::{runtime::Handle, sync::watch};
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
};
use tuwunel_database::{Database, Entry, Role};

use crate::{
	LogStore, Machine, Node, NodeId, Recovery, Reopen, Request, SnapshotDir, SnapshotStore,
	TypeConfig,
};

pub(crate) const MAP: &str = "alias_roomid";

static NEXT: AtomicU64 = AtomicU64::new(0);

pub(crate) type Rows = Vec<(Vec<u8>, Vec<u8>)>;

/// A scratch directory removed on drop.
pub(crate) struct Dir(PathBuf);

/// Where one node keeps its files.
#[derive(Clone)]
pub(crate) struct Layout {
	pub(crate) database: PathBuf,
	pub(crate) snapshots: PathBuf,
	pub(crate) raft: PathBuf,
}

/// A node as the runtime brings it up: log store, snapshot store and database.
pub(crate) struct Started {
	pub(crate) log: LogStore,
	pub(crate) store: SnapshotStore,
	pub(crate) db: Arc<Database>,
	pub(crate) recovery: Recovery,
}

impl Dir {
	pub(crate) fn new() -> Self {
		let name = format!("tuwunel-snapshot-{}-{}", id(), NEXT.fetch_add(1, Ordering::Relaxed));

		Self(temp_dir().join(name))
	}

	pub(crate) fn at(&self, name: &str) -> PathBuf { self.0.join(name) }

	/// The files of a node whose database has a directory of its own.
	pub(crate) fn layout(&self, name: &str) -> Layout {
		let root = self.at(name);

		Layout {
			database: root.join("db"),
			snapshots: root.join("snapshots"),
			raft: root.join("raft"),
		}
	}

	/// The files of a node that began as a single server: the database is in
	/// the root of the volume, with everything else beside its files.
	pub(crate) fn legacy(&self, name: &str) -> Layout {
		let root = self.at(name);

		Layout {
			database: root.clone(),
			snapshots: root.join("snapshots"),
			raft: root.join("raft"),
		}
	}
}

impl Drop for Dir {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

pub(crate) async fn open(path: &Path) -> Result<Arc<Database>> {
	let raw = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_path", path));

	let runtime = Handle::current();
	let logging = Logging {
		subscriber: Arc::new(NoSubscriber::new()),
		reload: LogLevelReloadHandles::default(),
		capture: Arc::new(State::new()),
	};

	let metrics = Metrics::new(Some(&runtime));
	let config = Config::new(&raw)?;
	let server = Server::new(config, Sources::default(), Some(&runtime), logging, metrics);

	Database::open(&Arc::new(server)).await
}

fn reopen(path: PathBuf) -> Reopen {
	Arc::new(move || {
		let path = path.clone();

		Box::pin(async move { open(&path).await.map_err(io::Error::other) })
	})
}

/// Opens the log store and builds the snapshot store of a node, without
/// opening the database.
pub(crate) async fn parts(layout: &Layout) -> io::Result<(LogStore, SnapshotStore)> {
	let log = LogStore::open(&layout.raft).await?;
	let store = SnapshotStore::new(
		layout.database.clone(),
		layout.snapshots.clone(),
		log.meta(),
		reopen(layout.database.clone()),
	);

	Ok((log, store))
}

/// Starts a node the way the runtime does: open the log store, then the
/// database through the snapshot store.
pub(crate) async fn start(layout: &Layout) -> io::Result<Started> {
	let (log, store) = parts(layout).await?;
	let (db, recovery) = store.open_database().await?;

	Ok(Started { log, store, db, recovery })
}

/// A database that leads: it captures its writes for the state machine.
pub(crate) async fn leader(dir: &Dir, name: &str) -> Result<Arc<Database>> {
	let db = open(&dir.at(name)).await?;
	db.engine.set_role(Role::Leader)?;

	Ok(db)
}

pub(crate) fn machine(
	db: Arc<Database>,
	store: &SnapshotStore,
) -> (Machine, watch::Receiver<u64>) {
	let (sender, receiver) = watch::channel(0);

	(Machine::new(db, sender).with_snapshots(store.clone()), receiver)
}

pub(crate) fn log_id(term: u64, index: u64) -> LogIdOf<TypeConfig> {
	LogIdOf::<TypeConfig>::new(CommittedLeaderIdOf::<TypeConfig>::new(term), index)
}

pub(crate) fn normal(term: u64, index: u64, entry: Entry) -> EntryOf<TypeConfig> {
	EntryOf::<TypeConfig>::new_normal(log_id(term, index), Request(entry))
}

pub(crate) fn blank(term: u64, index: u64) -> EntryOf<TypeConfig> {
	EntryOf::<TypeConfig>::new_blank(log_id(term, index))
}

pub(crate) fn node(id: u64) -> Node {
	Node {
		addr: format!("http://node{id}:8009"),
		incarnation: u128::from(id),
	}
}

/// Starts a node, applies the entries and stops it without any ceremony.
pub(crate) async fn apply_and_stop(layout: &Layout, items: Vec<EntryOf<TypeConfig>>) -> Result {
	let Started { store, db, .. } = start(layout).await?;
	let (mut applier, _) = machine(db, &store);
	applier.apply(feed(items)).await?;

	Ok(())
}

/// Appends entries to a log store and waits until they are synced.
pub(crate) async fn append<I>(store: &mut LogStore, entries: I)
where
	I: IntoIterator<Item = EntryOf<TypeConfig>>,
{
	let entries: Vec<_> = entries.into_iter().collect();
	let (tx, rx) = TypeConfig::oneshot::<io::Result<()>>();

	store
		.append(entries, IOFlushed::signal(tx))
		.await
		.expect("entries are appended");

	rx.await
		.expect("the callback is sent")
		.expect("entries are synced");
}

/// Takes everything the leader captured, as entries of at most `max` bytes.
pub(crate) fn drain(db: &Database, max: usize) -> Vec<Entry> {
	std::iter::from_fn(|| {
		db.engine
			.drain_captured(max)
			.expect("drain captured")
	})
	.collect()
}

/// Numbers entries as the log of term `term` from index `first`.
pub(crate) fn logged(term: u64, first: u64, entries: Vec<Entry>) -> Vec<EntryOf<TypeConfig>> {
	entries
		.into_iter()
		.zip(first..)
		.map(|(entry, index)| normal(term, index, entry))
		.collect()
}

pub(crate) fn feed(
	entries: Vec<EntryOf<TypeConfig>>,
) -> impl Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + Send {
	stream::iter(entries.into_iter().map(|entry| Ok((entry, None))))
}

pub(crate) async fn rows(db: &Database) -> Result<Rows> {
	db.get(MAP)?
		.raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await
}

/// Writes a script of puts and deletes through a leader and returns what it
/// captured.
pub(crate) fn script(leader: &Database) -> Vec<Entry> {
	write_script(leader);

	drain(leader, 0)
}

/// Writes the script and leaves what the leader captured in its queue.
pub(crate) fn write_script(leader: &Database) {
	let map = leader.get(MAP).expect("map exists");
	for round in 0..6_u8 {
		map.insert(b"same", [round]);
		map.insert(&[b'k', round], [round]);
		if round % 2 == 1 {
			map.remove(&[b'k', round.saturating_sub(1)]);
		}
	}
}

/// Copies a flat or nested directory.
pub(crate) fn copy_tree(from: &Path, to: &Path) {
	fs::create_dir_all(to).expect("target directory is created");
	for entry in fs::read_dir(from).expect("source directory reads") {
		let entry = entry.expect("source entry reads");
		let target = to.join(entry.file_name());
		if entry
			.file_type()
			.expect("file type reads")
			.is_dir()
		{
			copy_tree(&entry.path(), &target);
		} else {
			fs::copy(entry.path(), &target).expect("file copies");
		}
	}
}

/// Hands a snapshot over as a transfer would: its files are copied into the
/// staging directory of the receiver.
pub(crate) async fn stage(receiver: &SnapshotStore, snapshot: &SnapshotDir) -> SnapshotDir {
	let staging = receiver
		.prepare_staging()
		.await
		.expect("staging is prepared");

	for file in snapshot.files() {
		fs::copy(snapshot.path().join(&file.name), staging.join(&file.name))
			.expect("file is staged");
	}

	SnapshotDir::scan(&staging).expect("staged snapshot scans")
}

/// The numbered snapshot directories under `root`.
pub(crate) fn snapshot_dirs(root: &Path) -> Vec<String> {
	let mut names: Vec<_> = fs::read_dir(root)
		.map(|entries| {
			entries
				.map(|entry| {
					entry
						.expect("entry reads")
						.file_name()
						.to_string_lossy()
						.into_owned()
				})
				.filter(|name| name.starts_with("snapshot-"))
				.collect()
		})
		.unwrap_or_default();

	names.sort();

	names
}

pub(crate) async fn within<F: Future>(future: F) -> F::Output {
	tokio::time::timeout(std::time::Duration::from_mins(1), future)
		.await
		.expect("finished in time")
}

/// A network no peer is reachable on.
pub(crate) struct Offline;

pub(crate) struct Nobody;

impl RaftNetworkFactory<TypeConfig> for Offline {
	type Network = Nobody;

	async fn new_client(&mut self, _target: NodeId, _node: &Node) -> Nobody { Nobody }
}

impl RaftNetworkV2<TypeConfig> for Nobody {
	type SnapshotData = SnapshotDir;

	async fn append_entries(
		&mut self,
		_rpc: AppendEntriesRequest<TypeConfig>,
		_option: RPCOption,
	) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
		Err(RPCError::Unreachable(Unreachable::from_string("offline")))
	}

	async fn vote(
		&mut self,
		_rpc: VoteRequest<TypeConfig>,
		_option: RPCOption,
	) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
		Err(RPCError::Unreachable(Unreachable::from_string("offline")))
	}

	async fn full_snapshot(
		&mut self,
		_vote: VoteOf<TypeConfig>,
		_snapshot: SnapshotOf<TypeConfig, SnapshotDir>,
		_cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
		_option: RPCOption,
	) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
		Err(StreamingError::Unreachable(Unreachable::from_string("offline")))
	}
}

/// Feeds the entries a leader committed to the state machine, numbering them
/// as the log of term one, and keeps them for a replay.
pub(crate) struct Pump {
	pub(crate) next: u64,
	pub(crate) log: Vec<EntryOf<TypeConfig>>,
}

impl Pump {
	pub(crate) fn new() -> Self { Self { next: 1, log: Vec::new() } }

	pub(crate) fn last(&self) -> u64 { self.next.saturating_sub(1) }

	pub(crate) async fn run(&mut self, machine: &mut Machine, leader: &Database) -> usize {
		let entries = drain(leader, 64 * 1024);
		let count = entries.len();
		let items = logged(1, self.next, entries);

		self.next = self
			.next
			.saturating_add(u64::try_from(count).expect("count fits"));

		self.log.extend(items.iter().cloned());
		machine
			.apply(feed(items))
			.await
			.expect("entries apply");

		count
	}

	/// Applies what the leader captured, at most `limit` writes at a time.
	pub(crate) async fn step(
		&mut self,
		machine: &mut Machine,
		leader: &Database,
		limit: usize,
	) -> usize {
		let entries: Vec<_> = std::iter::from_fn(|| {
			leader
				.engine
				.drain_captured(0)
				.expect("drain captured")
		})
		.take(limit)
		.collect();

		let count = entries.len();
		let items = logged(1, self.next, entries);

		self.next = self
			.next
			.saturating_add(u64::try_from(count).expect("count fits"));

		self.log.extend(items.iter().cloned());
		machine
			.apply(feed(items))
			.await
			.expect("entries apply");

		count
	}
}

/// The keys the single-key puts of these log entries write. Reads the record
/// layout of [`Entry`] for keys and values shorter than 128 bytes.
pub(crate) fn put_keys(items: &[EntryOf<TypeConfig>]) -> Vec<Vec<u8>> {
	let mut keys = Vec::new();
	for item in items {
		let EntryPayload::Normal(Request(entry)) = &item.payload else {
			continue;
		};

		let mut rest = entry.rep.as_slice();
		while !rest.is_empty() {
			assert_eq!(rest[0], 0x5, "the writers only put into a named column");

			let (key, after_key) = rest[3..].split_at(usize::from(rest[2]));
			let (value_len, after_len) = (usize::from(after_key[0]), &after_key[1..]);

			keys.push(key.to_vec());
			rest = &after_len[value_len..];
		}
	}

	keys
}

/// A follower that applied the script of a leader, and the snapshot it built.
/// The memtables are flushed first, so the snapshot holds table files and not
/// only a write-ahead log.
pub(crate) struct Donor {
	pub(crate) snapshot: SnapshotOf<TypeConfig, SnapshotDir>,
	pub(crate) rows: Rows,
	pub(crate) items: Vec<EntryOf<TypeConfig>>,
}

pub(crate) async fn donor(dir: &Dir, name: &str) -> Result<Donor> {
	let source = leader(dir, &format!("{name}-source")).await?;
	let items = logged(1, 1, script(&source));

	let Started { store, db, .. } = start(&dir.layout(name)).await?;
	let (mut applier, _) = machine(db, &store);
	applier.apply(feed(items.clone())).await?;

	applier
		.database()
		.expect("database")
		.engine
		.sort()?;

	let snapshot = applier
		.try_create_snapshot_builder(false)
		.await
		.expect("a planned builder")
		.build_snapshot()
		.await?;

	let rows = rows(applier.database().expect("database")).await?;

	Ok(Donor { snapshot, rows, items })
}

pub(crate) fn config() -> Arc<RaftConfig> {
	Arc::new(
		RaftConfig {
			cluster_name: "snapshots".to_owned(),
			heartbeat_interval: 50,
			election_timeout_min: 300,
			election_timeout_max: 600,
			snapshot_policy: SnapshotPolicy::Never,
			..Default::default()
		}
		.validate()
		.expect("config is valid"),
	)
}
