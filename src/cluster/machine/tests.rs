use std::{
	collections::BTreeMap,
	env::temp_dir,
	fs::remove_dir_all,
	io::{self, ErrorKind},
	path::{Path, PathBuf},
	process::id,
	sync::{
		Arc, Mutex,
		atomic::{AtomicU64, Ordering},
	},
};

use futures::{Stream, StreamExt, TryStreamExt, stream};
use openraft::{
	StoredMembership,
	entry::RaftEntry,
	impls::Membership,
	storage::{
		EntryResponder, RaftSnapshotBuilder, RaftStateMachine, SnapshotMeta, StorageHelper,
	},
	type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, StoredMembershipOf},
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

use super::{Machine, decode_marker, encode_marker};
use crate::{LogStore, Node, Request, SnapshotDir, SnapshotFile, TypeConfig};

const MAP: &str = "alias_roomid";

static NEXT: AtomicU64 = AtomicU64::new(0);

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

struct Dir(PathBuf);

impl Dir {
	fn new() -> Self {
		let name = format!("tuwunel-machine-{}-{}", id(), NEXT.fetch_add(1, Ordering::Relaxed));

		Self(temp_dir().join(name))
	}

	fn at(&self, name: &str) -> PathBuf { self.0.join(name) }
}

impl Drop for Dir {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

async fn open(path: &Path) -> Result<Arc<Database>> {
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

async fn node(dir: &Dir, name: &str, role: Role) -> Result<Arc<Database>> {
	let db = open(&dir.at(name)).await?;
	db.engine.set_role(role)?;

	Ok(db)
}

fn machine(db: &Arc<Database>) -> (Machine, watch::Receiver<u64>) {
	let (sender, receiver) = watch::channel(0);

	(Machine::new(Arc::clone(db), sender), receiver)
}

fn log_id(term: u64, index: u64) -> LogIdOf<TypeConfig> {
	LogIdOf::<TypeConfig>::new(CommittedLeaderIdOf::<TypeConfig>::new(term), index)
}

fn normal(term: u64, index: u64, entry: Entry) -> EntryOf<TypeConfig> {
	EntryOf::<TypeConfig>::new_normal(log_id(term, index), Request(entry))
}

fn blank(term: u64, index: u64) -> EntryOf<TypeConfig> {
	EntryOf::<TypeConfig>::new_blank(log_id(term, index))
}

fn members() -> Membership<u64, Node> {
	Membership::from(BTreeMap::from([
		(1, Node {
			addr: "http://one:8009".to_owned(),
			incarnation: 11,
		}),
		(2, Node {
			addr: "http://two:8009".to_owned(),
			incarnation: 22,
		}),
	]))
}

fn membership(term: u64, index: u64) -> EntryOf<TypeConfig> {
	EntryOf::<TypeConfig>::new_membership(log_id(term, index), members())
}

fn stored(term: u64, index: u64) -> StoredMembershipOf<TypeConfig> {
	StoredMembership::new(Some(log_id(term, index)), members())
}

fn drain(db: &Database) -> Vec<Entry> {
	std::iter::from_fn(|| {
		db.engine
			.drain_captured(0)
			.expect("drain captured")
	})
	.collect()
}

fn logged(term: u64, first: u64, entries: Vec<Entry>) -> Vec<EntryOf<TypeConfig>> {
	entries
		.into_iter()
		.zip(first..)
		.map(|(entry, index)| normal(term, index, entry))
		.collect()
}

fn feed(
	entries: Vec<EntryOf<TypeConfig>>,
) -> impl Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + Send {
	stream::iter(entries.into_iter().map(|entry| Ok((entry, None))))
}

async fn rows(db: &Database) -> Result<Rows> {
	let map = db.get(MAP)?;

	map.raw_stream()
		.map_ok(|(key, val)| (key.to_vec(), val.to_vec()))
		.try_collect()
		.await
}

fn position(db: &Database) -> Result<Option<u64>> {
	Ok(db
		.engine
		.applied_marker()?
		.map(|marker| decode_marker(&marker).expect("marker decodes"))
		.and_then(|(applied, _)| applied)
		.map(|applied| applied.index))
}

fn script(leader: &Database) -> Vec<Entry> {
	let map = leader.get(MAP).expect("map exists");
	for round in 0..6_u8 {
		map.insert(b"same", [round]);
		map.insert(&[b'k', round], [round]);
		if round % 2 == 1 {
			map.remove(&[b'k', round.saturating_sub(1)]);
		}
	}

	drain(leader)
}

#[tokio::test]
async fn entries_are_applied_one_by_one_in_log_order() -> Result {
	let dir = Dir::new();
	let leader = node(&dir, "leader", Role::Leader).await?;
	let follower = node(&dir, "follower", Role::Follower).await?;

	let items = logged(1, 1, script(&leader));
	let last = u64::try_from(items.len()).expect("length fits");
	assert!(last >= 12, "the script produces many entries");

	let seen = Mutex::new(Vec::new());
	let watched = Arc::clone(&follower);
	let input = feed(items).inspect(|_| {
		seen.lock()
			.expect("seen locked")
			.push(position(&watched).expect("position is read"));
	});

	let (mut applier, committed) = machine(&follower);
	applier.apply(input).await?;

	let expected: Vec<_> = std::iter::once(None)
		.chain((1..last).map(Some))
		.collect();

	assert_eq!(
		*seen.lock().expect("seen locked"),
		expected,
		"each entry is applied before the next is taken from the log"
	);
	assert_eq!(position(&follower)?, Some(last), "the marker names the last entry");
	assert_eq!(rows(&follower).await?, rows(&leader).await?, "last write wins everywhere");
	assert!(
		rows(&follower)
			.await?
			.contains(&(b"same".to_vec(), vec![5])),
		"the final value is the last one written"
	);
	assert_eq!(*committed.borrow(), 0, "a follower does not confirm sequences");

	Ok(())
}

#[tokio::test]
async fn own_entries_confirm_the_sequence_and_are_applied_only_by_another_origin() -> Result {
	let dir = Dir::new();
	let leader = node(&dir, "leader", Role::Leader).await?;
	let map = leader.get(MAP)?;
	map.insert(b"a", b"1");
	map.insert(b"b", b"2");
	map.remove(b"a");

	let entries = drain(&leader);
	let ends: Vec<_> = entries
		.iter()
		.map(|entry| entry.seq.end)
		.collect();
	let items = logged(1, 1, entries);
	let old_origin = leader.engine.origin();
	let before = leader.engine.current_sequence();
	let rows_before = rows(&leader).await?;

	let (mut applier, committed) = machine(&leader);
	applier.apply(feed(items.clone())).await?;

	assert_eq!(*committed.borrow(), ends[2], "the sequence is confirmed up to the last entry");
	assert_eq!(leader.engine.current_sequence(), before, "the leader database is not written");
	assert_eq!(leader.engine.applied_marker()?, None, "the leader keeps no marker");
	assert_eq!(rows(&leader).await?, rows_before, "nothing is applied twice");

	applier.apply(feed(items[..1].to_vec())).await?;
	assert_eq!(*committed.borrow(), ends[2], "the confirmed sequence never moves back");

	drop(applier);
	drop(leader);

	let restarted = open(&dir.at("leader")).await?;
	assert_ne!(restarted.engine.origin(), old_origin, "a new process has a new origin");

	restarted.get(MAP)?.remove(b"b");
	assert!(
		!rows(&restarted)
			.await?
			.contains(&(b"b".to_vec(), b"2".to_vec()))
	);
	restarted.engine.set_role(Role::Follower)?;

	let (mut applier, committed) = machine(&restarted);
	applier.apply(feed(items)).await?;

	assert!(
		rows(&restarted)
			.await?
			.contains(&(b"b".to_vec(), b"2".to_vec())),
		"the old entries are foreign to the new origin and are applied"
	);
	assert_eq!(position(&restarted)?, Some(3), "the marker moved");
	assert_eq!(*committed.borrow(), 0, "a follower does not confirm sequences");

	Ok(())
}

#[tokio::test]
async fn own_entries_with_an_impossible_sequence_are_refused() -> Result {
	let dir = Dir::new();
	let leader = node(&dir, "leader", Role::Leader).await?;
	leader.get(MAP)?.insert(b"a", b"1");

	let entry = drain(&leader).remove(0);
	let (mut applier, committed) = machine(&leader);

	for (name, seq) in [
		("beyond the database", entry.seq.start..entry.seq.end + 1_000),
		("reversed", entry.seq.end..entry.seq.start),
	] {
		let damaged = Entry { seq, ..entry.clone() };
		let error = applier
			.apply(feed(vec![normal(1, 1, damaged)]))
			.await
			.expect_err(name);

		assert_eq!(error.kind(), ErrorKind::InvalidData, "{name}");
		assert_eq!(*committed.borrow(), 0, "{name}: nothing is confirmed");
	}

	Ok(())
}

#[tokio::test]
async fn a_restart_between_packets_resumes_from_the_marker_and_a_replay_is_harmless() -> Result {
	let dir = Dir::new();
	let leader = node(&dir, "leader", Role::Leader).await?;
	let whole = node(&dir, "whole", Role::Follower).await?;
	let crashed = node(&dir, "crashed", Role::Follower).await?;

	let map = leader.get(MAP)?;
	map.insert(b"a", b"1");
	map.insert(b"b", b"2");
	map.remove(b"a");
	map.insert(b"a", b"3");
	let items = logged(1, 1, drain(&leader));
	assert_eq!(items.len(), 4);

	machine(&whole)
		.0
		.apply(feed(items.clone()))
		.await?;
	let expected = rows(&whole).await?;
	let marker = whole.engine.applied_marker()?;
	assert_eq!(expected, rows(&leader).await?);

	machine(&crashed)
		.0
		.apply(feed(items[..2].to_vec()))
		.await?;
	drop(crashed);

	let crashed = open(&dir.at("crashed")).await?;
	crashed.engine.set_role(Role::Follower)?;
	let (mut applier, _) = machine(&crashed);
	let (applied, _) = applier.applied_state().await?;
	assert_eq!(applied, Some(log_id(1, 2)), "the marker names the last whole packet");

	let next = usize::try_from(applied.expect("applied").index).expect("index fits");
	applier
		.apply(feed(items[next..].to_vec()))
		.await?;
	assert_eq!(rows(&crashed).await?, expected, "resuming reaches the same state");
	assert_eq!(crashed.engine.applied_marker()?, marker, "and the same marker");

	applier.apply(feed(items)).await?;
	assert_eq!(
		rows(&crashed).await?,
		expected,
		"replaying from the first packet changes nothing"
	);
	assert_eq!(crashed.engine.applied_marker()?, marker, "and ends on the same marker");

	Ok(())
}

#[tokio::test]
async fn blank_and_membership_entries_move_the_marker_of_a_follower() -> Result {
	let dir = Dir::new();
	let leader = node(&dir, "leader", Role::Leader).await?;
	let follower = node(&dir, "follower", Role::Follower).await?;
	leader.get(MAP)?.insert(b"a", b"1");
	let data = drain(&leader).remove(0);

	let (mut applier, _) = machine(&follower);
	assert_eq!(
		applier.applied_state().await?,
		(None, StoredMembership::default()),
		"nothing is applied in a new database"
	);

	let before = rows(&follower).await?;
	applier.apply(feed(vec![blank(1, 1)])).await?;
	assert_eq!(
		applier.applied_state().await?,
		(Some(log_id(1, 1)), StoredMembership::default()),
		"a blank entry moves the marker"
	);

	applier
		.apply(feed(vec![membership(1, 2)]))
		.await?;
	assert_eq!(
		applier.applied_state().await?,
		(Some(log_id(1, 2)), stored(1, 2)),
		"a membership entry moves the marker and brings the membership"
	);

	applier
		.apply(feed(vec![blank(2, 3), normal(2, 4, data)]))
		.await?;
	assert_eq!(
		applier.applied_state().await?,
		(Some(log_id(2, 4)), stored(1, 2)),
		"the membership is carried along by later entries"
	);
	assert_ne!(rows(&follower).await?, before, "the data entry was written");

	drop(applier);
	drop(follower);

	let follower = open(&dir.at("follower")).await?;
	follower.engine.set_role(Role::Follower)?;
	let (mut applier, _) = machine(&follower);
	assert_eq!(
		applier.applied_state().await?,
		(Some(log_id(2, 4)), stored(1, 2)),
		"the reopened database reports the last marker"
	);

	applier.apply(feed(vec![blank(2, 5)])).await?;
	assert_eq!(
		applier.applied_state().await?,
		(Some(log_id(2, 5)), stored(1, 2)),
		"a new state machine carries the membership of the stored marker"
	);

	Ok(())
}

#[tokio::test]
async fn only_a_follower_applies_entries_of_another_origin() -> Result {
	let dir = Dir::new();
	let source = node(&dir, "source", Role::Leader).await?;
	source.get(MAP)?.insert(b"a", b"1");
	let foreign = drain(&source).remove(0);

	let standalone = open(&dir.at("standalone")).await?;
	let leader = node(&dir, "leader", Role::Leader).await?;

	for (name, db) in [("standalone", &standalone), ("leader", &leader)] {
		let before = db.engine.current_sequence();
		let (mut applier, committed) = machine(db);

		applier
			.apply(feed(vec![normal(1, 1, foreign.clone())]))
			.await
			.expect_err(name);

		assert_eq!(db.engine.current_sequence(), before, "{name}: nothing is written");
		assert_eq!(db.engine.applied_marker()?, None, "{name}: no marker");
		assert_eq!(*committed.borrow(), 0, "{name}: nothing is confirmed");
	}

	let before = standalone.engine.current_sequence();
	let (mut applier, _) = machine(&standalone);
	applier
		.apply(feed(vec![blank(1, 1)]))
		.await
		.expect_err("a standalone database takes no marker");
	assert_eq!(standalone.engine.current_sequence(), before);

	let before = leader.engine.current_sequence();
	let (mut applier, committed) = machine(&leader);
	applier
		.apply(feed(vec![blank(1, 1), membership(1, 2)]))
		.await?;
	assert_eq!(leader.engine.current_sequence(), before, "a leader writes nothing for them");
	assert_eq!(leader.engine.applied_marker()?, None);
	assert_eq!(*committed.borrow(), 0);

	Ok(())
}

#[tokio::test]
async fn a_damaged_entry_fails_the_apply_and_keeps_the_marker() -> Result {
	let dir = Dir::new();
	let leader = node(&dir, "leader", Role::Leader).await?;
	let follower = node(&dir, "follower", Role::Follower).await?;
	let map = leader.get(MAP)?;
	map.insert(b"a", b"1");
	map.insert(b"b", b"2");

	let mut entries = drain(&leader);
	let mut damaged = entries.remove(1);
	damaged.rep.pop();

	let items = vec![normal(1, 1, entries.remove(0)), normal(1, 2, damaged), blank(1, 3)];

	let (mut applier, _) = machine(&follower);
	applier
		.apply(feed(items))
		.await
		.expect_err("the damaged entry stops the apply");

	assert_eq!(position(&follower)?, Some(1), "the marker names the last whole entry");
	assert_eq!(
		rows(&follower).await?,
		vec![(b"a".to_vec(), b"1".to_vec())],
		"only the entry before it was applied"
	);

	Ok(())
}

#[test]
fn the_marker_starts_with_its_version_and_round_trips() {
	let applied = log_id(3, 7);
	let membership = stored(2, 5);
	let marker = encode_marker(&applied, &membership).expect("marker encodes");

	assert_eq!(marker[0], 1, "version first");
	assert_eq!(decode_marker(&marker).expect("decodes"), (Some(applied), membership));
	assert_eq!(
		decode_marker(&[]).expect("an empty marker is no position"),
		(None, StoredMembership::default())
	);

	let mut future = marker.clone();
	future[0] = 2;
	let mut longer = marker.clone();
	longer.push(0);
	let shorter = &marker[..marker.len() - 1];

	for (name, damaged) in [
		("version", future.as_slice()),
		("trailing byte", longer.as_slice()),
		("short", shorter),
	] {
		let error = decode_marker(damaged).expect_err(name);
		assert_eq!(error.kind(), ErrorKind::InvalidData, "{name}");
	}
}

#[test]
fn the_marker_bytes_are_golden() {
	let golden = [
		&[1, 3, 7, 1, 2, 5, 1, 2, 1, 2, 2, 1, 15][..],
		b"http://one:8009",
		&[11, 2, 15],
		b"http://two:8009",
		&[22],
	]
	.concat();

	let marker = encode_marker(&log_id(3, 7), &stored(2, 5)).expect("marker encodes");
	assert_eq!(
		marker, golden,
		"the marker encoding changed: raise MARKER_VERSION and keep reading the old version"
	);

	assert_eq!(
		decode_marker(&golden).expect("golden marker decodes"),
		(Some(log_id(3, 7)), stored(2, 5))
	);
}

#[tokio::test]
async fn a_damaged_marker_is_an_error_and_not_an_empty_position() -> Result {
	let dir = Dir::new();
	let follower = node(&dir, "follower", Role::Follower).await?;
	follower
		.engine
		.apply_replicated(&[], &[9, 1, 2])?;

	let (mut applier, _) = machine(&follower);
	let error = applier
		.applied_state()
		.await
		.expect_err("the marker is refused");

	assert_eq!(error.kind(), ErrorKind::InvalidData);
	applier
		.apply(feed(vec![blank(1, 1)]))
		.await
		.expect_err("nothing is applied on top of an unreadable marker");

	assert_eq!(
		follower.engine.applied_marker()?,
		Some(vec![9, 1, 2]),
		"the marker is left as found"
	);

	Ok(())
}

fn scratch() -> SnapshotDir {
	SnapshotDir::new(PathBuf::from("/nowhere"), vec![SnapshotFile {
		name: "CURRENT".to_owned(),
		len: 1,
	}])
	.expect("the description is valid")
}

#[tokio::test]
async fn snapshot_methods_fail_with_a_clear_error() -> Result {
	let dir = Dir::new();
	let follower = node(&dir, "follower", Role::Follower).await?;
	let (mut applier, _) = machine(&follower);

	let errors = [
		applier
			.get_snapshot_builder()
			.await
			.build_snapshot()
			.await
			.map(drop)
			.expect_err("build"),
		applier
			.install_snapshot(&SnapshotMeta::default(), scratch())
			.await
			.expect_err("install"),
	];

	for error in errors {
		assert_eq!(error.kind(), ErrorKind::Unsupported);
		assert_eq!(error.to_string(), "snapshots are not available yet");
	}

	assert!(applier.get_current_snapshot().await?.is_none(), "there is no current snapshot");

	Ok(())
}

#[tokio::test]
async fn a_planned_snapshot_is_deferred_and_a_forced_one_fails() -> Result {
	let dir = Dir::new();
	let follower = node(&dir, "follower", Role::Follower).await?;
	let (mut applier, _) = machine(&follower);

	assert!(
		applier
			.try_create_snapshot_builder(false)
			.await
			.is_none(),
		"a planned snapshot is deferred"
	);

	let error = applier
		.try_create_snapshot_builder(true)
		.await
		.expect("a forced snapshot gets a builder")
		.build_snapshot()
		.await
		.map(drop)
		.expect_err("and the build fails");

	assert_eq!(error.kind(), ErrorKind::Unsupported);
	assert_eq!(error.to_string(), "snapshots are not available yet");

	Ok(())
}

#[tokio::test]
async fn raft_reads_its_initial_state_from_the_machine_and_a_new_log() -> Result {
	let dir = Dir::new();
	let follower = node(&dir, "follower", Role::Follower).await?;
	let (mut applier, _) = machine(&follower);
	let mut store = LogStore::open(&dir.at("raft"))
		.await
		.expect("log store opens");

	StorageHelper::new(&mut store, &mut applier)
		.with_id(1)
		.get_initial_state()
		.await
		.expect("the initial state is read");

	Ok(())
}
