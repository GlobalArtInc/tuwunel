mod machine;

use std::{
	collections::BTreeMap,
	env::temp_dir,
	fs::remove_dir_all,
	io::{self, ErrorKind},
	ops::Range,
	path::{Path, PathBuf},
	process::id,
	sync::atomic::{AtomicU64, Ordering},
	time::Duration,
};

use openraft::{
	ErrorSubject, ErrorVerb, StorageError,
	entry::RaftEntry,
	impls::{Membership, Vote},
	storage::{IOFlushed, RaftLogReader, RaftLogStorage, StorageHelper},
	testing::log::{StoreBuilder, Suite},
	type_config::{
		TypeConfigExt,
		alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, OneshotReceiverOf},
	},
};
use rocksdb::{DB, Options};
use tokio::time::{sleep, timeout};
use tuwunel_database::Entry;

use self::machine::Machine;
use crate::{LogStore, Node, Request, TypeConfig};

static NEXT: AtomicU64 = AtomicU64::new(0);

type Acknowledgement = OneshotReceiverOf<TypeConfig, io::Result<()>>;

struct TempDir(PathBuf);

impl TempDir {
	fn new() -> Self {
		let name = format!("tuwunel-cluster-{}-{}", id(), NEXT.fetch_add(1, Ordering::Relaxed));

		Self(temp_dir().join(name))
	}

	fn path(&self) -> &Path { &self.0 }
}

impl Drop for TempDir {
	fn drop(&mut self) { remove_dir_all(&self.0).ok(); }
}

struct Builder;

impl StoreBuilder<TypeConfig, LogStore, Machine, TempDir> for Builder {
	async fn build(&self) -> Result<(TempDir, LogStore, Machine), StorageError<TypeConfig>> {
		let dir = TempDir::new();
		let store = LogStore::open(dir.path())
			.await
			.map_err(|e| StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, e))?;

		Ok((dir, store, Machine::default()))
	}
}

fn log_id(term: u64, index: u64) -> LogIdOf<TypeConfig> {
	LogIdOf::<TypeConfig>::new(CommittedLeaderIdOf::<TypeConfig>::new(term), index)
}

fn blank(term: u64, index: u64) -> EntryOf<TypeConfig> {
	EntryOf::<TypeConfig>::new_blank(log_id(term, index))
}

fn replica() -> Entry {
	let key = b"secret-key";
	let mut rep = vec![0x5, 0, 10];
	rep.extend_from_slice(key);
	rep.extend([1, b'v', 0x4, 0, 10]);
	rep.extend_from_slice(key);

	Entry {
		version: Entry::VERSION,
		origin: 0xFEED,
		seq: 10..12,
		cfs: vec!["roomid_pdu".to_owned()],
		rep,
		hints: Some(vec![1, 2, 3]),
	}
}

fn raw(dir: &TempDir) -> DB {
	let mut options = Options::default();
	options.create_if_missing(false);

	DB::open_cf(&options, dir.path(), ["logs", "meta"]).expect("raw open")
}

async fn open(dir: &TempDir) -> LogStore {
	LogStore::open(dir.path())
		.await
		.expect("log store opens")
}

async fn append<I>(store: &mut LogStore, entries: I)
where
	I: IntoIterator<Item = EntryOf<TypeConfig>>,
{
	let entries: Vec<_> = entries.into_iter().collect();
	let (flushed, acknowledgement) = callback();

	store
		.append(entries, flushed)
		.await
		.expect("entries are appended");

	acknowledgement
		.await
		.expect("the callback is sent")
		.expect("entries are synced");
}

fn callback() -> (IOFlushed<TypeConfig>, Acknowledgement) {
	let (tx, rx) = TypeConfig::oneshot::<io::Result<()>>();

	(IOFlushed::signal(tx), rx)
}

async fn is_pending<F: Future>(future: F) -> bool {
	timeout(Duration::from_millis(150), future)
		.await
		.is_err()
}

async fn refused(dir: &TempDir) -> ErrorKind {
	let Err(error) = LogStore::open(dir.path()).await else {
		panic!("the store is refused");
	};

	error.kind()
}

async fn indexes(store: &mut LogStore, range: Range<u64>) -> Vec<u64> {
	store
		.get_log_reader()
		.await
		.try_get_log_entries(range)
		.await
		.expect("entries are read")
		.iter()
		.map(RaftEntry::index)
		.collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn log_storage_conformance_suite() {
	Suite::<TypeConfig, LogStore, Machine, Builder, TempDir>::test_all(Builder)
		.await
		.expect("the log store passes the storage conformance suite");
}

#[tokio::test]
async fn incarnation_is_created_once_and_survives_reopening() {
	let dir = TempDir::new();
	let first = open(&dir).await.meta().incarnation();

	assert_ne!(first, 0, "a fresh store gets a nonzero incarnation");

	for _ in 0..2 {
		let reopened = open(&dir).await.meta().incarnation();

		assert_eq!(reopened, first, "reopening keeps the incarnation");
	}

	let other = TempDir::new();
	let second = open(&other).await.meta().incarnation();

	assert_ne!(second, first, "another store gets another incarnation");
}

#[tokio::test]
async fn an_empty_directory_does_not_initialize_the_cluster() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	let mut machine = Machine::default();

	let state = store
		.get_log_state()
		.await
		.expect("log state is read");

	assert_eq!(state.last_log_id, None, "no log entry is created");
	assert_eq!(state.last_purged_log_id, None, "nothing is purged");
	assert_eq!(store.read_committed().await.expect("read"), None, "nothing is committed");
	assert_eq!(
		store
			.get_log_reader()
			.await
			.read_vote()
			.await
			.expect("vote is read"),
		None,
		"no vote is cast"
	);

	let initial = StorageHelper::new(&mut store, &mut machine)
		.with_id(1)
		.get_initial_state()
		.await
		.expect("initial state is read");

	assert!(
		initial
			.membership_state
			.effective()
			.membership()
			.voter_ids()
			.next()
			.is_none(),
		"an empty directory has no membership"
	);
}

#[tokio::test]
async fn a_directory_of_another_database_is_refused() {
	let dir = TempDir::new();
	let db = DB::open_default(dir.path()).expect("foreign database opens");
	db.put(b"key", b"value")
		.expect("foreign key is written");
	drop(db);

	let Err(error) = LogStore::open(dir.path()).await else {
		panic!("a foreign database is refused");
	};

	assert_eq!(error.kind(), ErrorKind::InvalidData, "{error}");
}

#[tokio::test]
async fn state_survives_reopening() {
	let dir = TempDir::new();
	let nodes = BTreeMap::from([(1, Node {
		addr: "http://one:8009".to_owned(),
		incarnation: u128::MAX - 7,
	})]);
	let vote = Vote::new_committed(3, 1);

	let mut store = open(&dir).await;
	append(&mut store, [
		blank(1, 1),
		EntryOf::<TypeConfig>::new_membership(log_id(1, 2), Membership::from(nodes.clone())),
		EntryOf::<TypeConfig>::new_normal(log_id(2, 3), Request(replica())),
		blank(2, 4),
		blank(2, 5),
	])
	.await;

	store
		.save_vote(&vote)
		.await
		.expect("vote is saved");
	store
		.save_committed(Some(log_id(2, 4)))
		.await
		.expect("committed is saved");
	store.purge(log_id(1, 1)).await.expect("purge");
	store
		.truncate_after(Some(log_id(2, 4)))
		.await
		.expect("truncate");
	drop(store);

	let mut store = open(&dir).await;
	let state = store.get_log_state().await.expect("log state");

	assert_eq!(state.last_purged_log_id, Some(log_id(1, 1)), "the purge position is kept");
	assert_eq!(state.last_log_id, Some(log_id(2, 4)), "the truncated tail stays gone");
	assert_eq!(
		indexes(&mut store, 0..10).await,
		[2, 3, 4],
		"purged and truncated entries are gone"
	);
	assert_eq!(
		store
			.read_committed()
			.await
			.expect("committed is read"),
		Some(log_id(2, 4)),
		"committed is kept"
	);
	assert_eq!(
		store
			.get_log_reader()
			.await
			.read_vote()
			.await
			.expect("vote is read"),
		Some(vote),
		"the vote is kept"
	);

	let entries = store
		.get_log_reader()
		.await
		.try_get_log_entries(2..4)
		.await
		.expect("entries are read");

	assert_eq!(
		entries[0].get_membership(),
		Some(Membership::from(nodes)),
		"membership with node data"
	);
	assert_eq!(
		entries[1],
		EntryOf::<TypeConfig>::new_normal(log_id(2, 3), Request(replica())),
		"the replicated write"
	);
}

#[tokio::test]
async fn append_must_continue_the_log() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	append(&mut store, [blank(1, 1), blank(1, 2)]).await;

	for (rows, what) in [
		(vec![blank(1, 4)], "a gap behind the end"),
		(vec![blank(1, 2)], "an overlap with the end"),
		(vec![blank(1, 3), blank(1, 5)], "a gap inside the batch"),
	] {
		let result = store.append(rows, IOFlushed::noop()).await;

		assert!(result.is_err(), "{what} is refused");
	}

	assert_eq!(indexes(&mut store, 0..10).await, [1, 2], "refused entries leave no trace");
}

#[tokio::test]
async fn appended_entries_are_readable_at_once_and_acknowledged_after_the_sync() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	let probe = store.probe();
	probe.hold();

	let (flushed, mut acknowledgement) = callback();
	store
		.append(vec![blank(1, 1), blank(1, 2)], flushed)
		.await
		.expect("append returns while the sync is held back");

	assert_eq!(indexes(&mut store, 0..10).await, [1, 2], "entries are readable before the sync");
	assert!(
		is_pending(&mut acknowledgement).await,
		"nothing is acknowledged before the sync"
	);
	assert_eq!(probe.syncs(), 0, "nothing is synced yet");

	probe.release_all();
	acknowledgement
		.await
		.expect("the callback is sent")
		.expect("the sync succeeds");

	assert_eq!(probe.syncs(), 1, "the acknowledgement follows the sync");
}

#[tokio::test]
async fn acknowledgements_follow_the_order_of_the_appends() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	let probe = store.probe();
	probe.hold();

	let (first, mut first_acknowledgement) = callback();
	let (second, mut second_acknowledgement) = callback();
	store
		.append(vec![blank(1, 1)], first)
		.await
		.expect("first append");
	store
		.append(vec![blank(1, 2)], second)
		.await
		.expect("second append");

	timeout(Duration::from_secs(10), async {
		while probe.waiting() == 0 {
			sleep(Duration::from_millis(5)).await;
		}
	})
	.await
	.expect("the first sync starts");

	sleep(Duration::from_millis(150)).await;

	assert_eq!(probe.waiting(), 1, "the second sync does not start before the first is done");

	probe.release(1);
	timeout(Duration::from_secs(10), &mut first_acknowledgement)
		.await
		.expect("the first is acknowledged by the first sync")
		.expect("the callback is sent")
		.expect("the sync succeeds");

	assert!(
		is_pending(&mut second_acknowledgement).await,
		"the second is not acknowledged by the first sync"
	);

	probe.release(1);
	second_acknowledgement
		.await
		.expect("the callback is sent")
		.expect("the sync succeeds");
}

#[tokio::test]
async fn a_failed_sync_is_reported_to_the_callback() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	store.probe().fail(true);

	let (flushed, acknowledgement) = callback();
	store
		.append(vec![blank(1, 1)], flushed)
		.await
		.expect("append returns before the sync fails");

	let outcome = acknowledgement
		.await
		.expect("the callback is sent");

	assert!(outcome.is_err(), "the failed sync reaches the callback: {outcome:?}");
}

#[tokio::test]
async fn votes_truncations_and_purges_wait_for_the_pending_sync() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	let probe = store.probe();
	let vote = Vote::new(2, 1);
	probe.hold();

	let (flushed, acknowledgement) = callback();
	store
		.append(vec![blank(1, 1), blank(1, 2)], flushed)
		.await
		.expect("append returns while the sync is held back");

	assert!(is_pending(store.save_vote(&vote)).await, "a vote waits");
	assert!(is_pending(store.truncate_after(Some(log_id(1, 1)))).await, "a truncation waits");
	assert!(is_pending(store.purge(log_id(1, 1))).await, "a purge waits");
	assert_eq!(indexes(&mut store, 0..10).await, [1, 2], "the waiting calls changed nothing");

	probe.release_all();
	acknowledgement
		.await
		.expect("the callback is sent")
		.expect("the sync succeeds");

	store.save_vote(&vote).await.expect("vote");
	store
		.truncate_after(Some(log_id(1, 1)))
		.await
		.expect("truncation");

	assert_eq!(indexes(&mut store, 0..10).await, [1], "the calls run once the sync is done");
}

#[tokio::test]
async fn committed_is_written_without_a_sync_and_read_back() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	let probe = store.probe();

	store
		.save_vote(&Vote::new(1, 1))
		.await
		.expect("vote is saved");

	assert_eq!(probe.syncs(), 1, "a vote is synced");

	store
		.save_committed(Some(log_id(1, 7)))
		.await
		.expect("committed is saved");

	assert_eq!(probe.syncs(), 1, "committed is not synced");

	drop((store, probe));

	assert_eq!(
		open(&dir)
			.await
			.read_committed()
			.await
			.expect("committed is read"),
		Some(log_id(1, 7)),
		"committed is read after reopening"
	);
}

#[tokio::test]
async fn a_store_with_a_broken_identity_is_refused() {
	for (what, key, value) in [
		("a version without an incarnation", "incarnation", None),
		("an unknown format version", "format", Some(vec![2_u8])),
		("a zero incarnation", "incarnation", Some(vec![0_u8; 16])),
		("a short incarnation", "incarnation", Some(vec![7_u8; 15])),
	] {
		let dir = TempDir::new();
		drop(open(&dir).await);

		let db = raw(&dir);
		let meta = db.cf_handle("meta").expect("meta family");
		match value {
			| Some(value) => db
				.put_cf(&meta, key, value)
				.expect("key is replaced"),
			| None => db.delete_cf(&meta, key).expect("key is removed"),
		}

		drop(meta);
		drop(db);

		assert_eq!(refused(&dir).await, ErrorKind::InvalidData, "{what} is refused");
	}
}

#[tokio::test]
async fn bytes_that_are_not_an_entry_are_invalid_data_and_not_a_panic() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	append(&mut store, (1..=3).map(|index| blank(1, index))).await;
	drop(store);

	let db = raw(&dir);
	let logs = db.cf_handle("logs").expect("logs family");
	db.put_cf(&logs, 2_u64.to_be_bytes(), [0xFF; 4])
		.expect("garbage replaces an entry");
	drop(logs);
	drop(db);

	let mut store = open(&dir).await;
	let mut reader = store.get_log_reader().await;

	for range in [1..4, 2..3] {
		let Err(error) = reader.try_get_log_entries(range.clone()).await else {
			panic!("garbage is not an entry: {range:?}");
		};

		assert_eq!(error.kind(), ErrorKind::InvalidData, "{error}");
	}

	assert_eq!(
		reader
			.try_get_log_entries(3..4)
			.await
			.expect("entries away from the garbage are read")
			.len(),
		1,
		"the damage stays where it is"
	);
}

#[tokio::test]
async fn a_log_emptied_by_a_purge_continues_the_purge_position() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	append(&mut store, (1..=3).map(|index| blank(1, index))).await;
	store.purge(log_id(1, 3)).await.expect("purge");

	for (first, what) in [(5, "past"), (3, "at"), (2, "behind")] {
		let result = store
			.append(vec![blank(2, first)], IOFlushed::noop())
			.await;

		assert!(result.is_err(), "an append {what} the purge position is refused");
	}

	append(&mut store, [blank(2, 4)]).await;

	assert_eq!(
		indexes(&mut store, 0..10).await,
		[4],
		"the entry behind the purge position stays"
	);
}

#[tokio::test]
async fn a_purge_past_the_end_of_the_log_empties_it_and_the_log_goes_on_after_the_position() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	append(&mut store, (1..=3).map(|index| blank(1, index))).await;

	store
		.purge(log_id(1, 9))
		.await
		.expect("a purge may go past the last entry");

	let state = store.get_log_state().await.expect("log state");
	assert_eq!(state.last_purged_log_id, Some(log_id(1, 9)));
	assert_eq!(
		state.last_log_id,
		Some(log_id(1, 9)),
		"the purge position is the end of the log"
	);
	assert!(indexes(&mut store, 0..20).await.is_empty(), "no entry is left");

	for first in [4, 9] {
		store
			.append(vec![blank(2, first)], IOFlushed::noop())
			.await
			.expect_err("an append does not reach back into the purged part");
	}

	append(&mut store, [blank(2, 10)]).await;
	drop(store);

	let mut store = open(&dir).await;
	assert_eq!(indexes(&mut store, 0..20).await, [10], "the log goes on after the position");
}

#[tokio::test]
async fn a_hole_at_the_edge_of_a_read_range_is_corruption() {
	let beginning = TempDir::new();
	let mut store = open(&beginning).await;
	append(&mut store, (1..=6).map(|index| blank(1, index))).await;
	store.purge(log_id(1, 2)).await.expect("purge");
	drop(store);

	let db = raw(&beginning);
	let logs = db.cf_handle("logs").expect("logs family");
	db.delete_cf(&logs, 3_u64.to_be_bytes())
		.expect("the first entry behind the purge position is removed");
	drop(logs);
	drop(db);

	let mut store = open(&beginning).await;
	let mut reader = store.get_log_reader().await;

	for (range, what) in
		[(0..10, "from the purge position"), (3..8, "from the start"), (3..4, "alone")]
	{
		let read = reader.try_get_log_entries(range).await;

		assert!(read.is_err(), "the hole is seen when reading {what}");
	}

	assert_eq!(
		reader
			.try_get_log_entries(4..10)
			.await
			.expect("a range behind the hole is read")
			.len(),
		3,
		"the log behind the hole is intact"
	);

	let end = TempDir::new();
	let mut store = open(&end).await;
	append(&mut store, (1..=6).map(|index| blank(1, index))).await;
	drop(store);

	let db = raw(&end);
	let logs = db.cf_handle("logs").expect("logs family");
	db.delete_cf(&logs, 5_u64.to_be_bytes())
		.expect("an entry is removed from the middle");
	drop(logs);
	drop(db);

	let mut store = open(&end).await;
	let mut reader = store.get_log_reader().await;

	assert!(reader.try_get_log_entries(3..6).await.is_err(), "the hole is seen at the end");
	assert_eq!(
		reader
			.try_get_log_entries(3..5)
			.await
			.expect("a range before the hole is read")
			.len(),
		2,
		"the log before the hole is intact"
	);
}

#[tokio::test]
async fn truncation_cannot_reach_into_the_purged_log() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	append(&mut store, [blank(1, 1), blank(1, 2), blank(1, 3), blank(1, 4)]).await;
	store.purge(log_id(1, 2)).await.expect("purge");

	assert!(
		store
			.truncate_after(Some(log_id(1, 1)))
			.await
			.is_err(),
		"before the purge position"
	);
	assert!(store.truncate_after(None).await.is_err(), "all of a purged log");

	store
		.truncate_after(Some(log_id(1, 2)))
		.await
		.expect("truncation right at the purge position");

	assert_eq!(
		indexes(&mut store, 0..10).await,
		Vec::<u64>::new(),
		"everything behind it is gone"
	);
}

#[tokio::test]
async fn an_interrupted_purge_is_completed_when_the_store_opens() {
	let dir = TempDir::new();
	let mut store = open(&dir).await;
	append(&mut store, (1..=5).map(|index| blank(1, index))).await;
	drop(store);

	let db = raw(&dir);
	let meta = db.cf_handle("meta").expect("meta family");
	let position = postcard::to_allocvec(&log_id(1, 3)).expect("position encodes");
	db.put_cf(&meta, b"purged", position)
		.expect("purge position is written without the removals");
	drop(meta);
	drop(db);

	let mut store = open(&dir).await;
	let state = store.get_log_state().await.expect("log state");

	assert_eq!(state.last_purged_log_id, Some(log_id(1, 3)), "the purge position is kept");
	assert_eq!(
		indexes(&mut store, 0..10).await,
		[4, 5],
		"entries the position covers are removed"
	);
}

#[tokio::test]
async fn a_damaged_log_is_an_error_and_not_a_short_read() {
	let gap = TempDir::new();
	let mut store = open(&gap).await;
	append(&mut store, (1..=4).map(|index| blank(1, index))).await;
	drop(store);

	let db = raw(&gap);
	let logs = db.cf_handle("logs").expect("logs family");
	db.delete_cf(&logs, 3_u64.to_be_bytes())
		.expect("an entry is removed from the middle");
	drop(logs);
	drop(db);

	let mut store = open(&gap).await;
	let read = store
		.get_log_reader()
		.await
		.try_get_log_entries(0..10)
		.await;

	assert!(read.is_err(), "a gap inside the range is not skipped over");
	assert_eq!(indexes(&mut store, 0..3).await, [1, 2], "a range before the gap is intact");

	let misplaced = TempDir::new();
	let mut store = open(&misplaced).await;
	append(&mut store, (1..=3).map(|index| blank(1, index))).await;
	drop(store);

	let db = raw(&misplaced);
	let logs = db.cf_handle("logs").expect("logs family");
	let second = db
		.get_cf(&logs, 2_u64.to_be_bytes())
		.expect("entry is read")
		.expect("entry exists");
	db.put_cf(&logs, 3_u64.to_be_bytes(), second)
		.expect("an entry is stored under another index");
	drop(logs);
	drop(db);

	let mut store = open(&misplaced).await;
	let read = store
		.get_log_reader()
		.await
		.try_get_log_entries(3..4)
		.await;

	assert!(read.is_err(), "an entry that disagrees with its key is not returned");
}

#[tokio::test]
async fn markers_are_persistent_and_independent() {
	let dir = TempDir::new();
	let store = open(&dir).await;
	let meta = store.meta();

	assert_eq!(meta.leader_marker().await.expect("read"), None, "no leader marker at first");
	assert_eq!(meta.replace_marker().await.expect("read"), None, "no replace marker at first");

	meta.set_leader_marker(b"leader")
		.await
		.expect("set");
	meta.set_replace_marker(&[])
		.await
		.expect("an empty marker is a marker");
	drop((meta, store));

	let meta = open(&dir).await.meta();

	assert_eq!(meta.leader_marker().await.expect("read"), Some(b"leader".to_vec()), "kept");
	assert_eq!(meta.replace_marker().await.expect("read"), Some(Vec::new()), "kept empty");

	meta.clear_leader_marker().await.expect("clear");
	meta.clear_leader_marker()
		.await
		.expect("clearing twice is fine");

	assert_eq!(meta.leader_marker().await.expect("read"), None, "cleared");
	assert_eq!(meta.replace_marker().await.expect("read"), Some(Vec::new()), "the other stays");
}

#[test]
fn a_replicated_write_serializes_through_postcard() {
	let entry = replica();

	entry
		.validate()
		.expect("the sample is a valid entry");

	let bytes = postcard::to_allocvec(&entry).expect("entry encodes");
	let decoded: Entry = postcard::from_bytes(&bytes).expect("entry decodes");

	assert_eq!(decoded, entry, "entry survives the round trip");
	assert_eq!(
		postcard::to_allocvec(&Request(entry.clone())).expect("request encodes"),
		bytes,
		"the request is the entry on the wire"
	);

	let request = Request(entry);

	for text in [format!("{request}"), format!("{request:?}")] {
		assert!(text.contains("origin: 000000000000feed"), "{text}");
		assert!(!text.contains("secret"), "records stay out of logs: {text}");
	}
}
