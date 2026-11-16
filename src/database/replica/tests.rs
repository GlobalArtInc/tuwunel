use std::{
	any::Any,
	env::temp_dir,
	fs::{read_dir, read_to_string, remove_dir_all},
	ops::Range,
	panic::{AssertUnwindSafe, catch_unwind},
	path::{Path, PathBuf},
	process::id as process_id,
	slice::from_ref,
	sync::{Arc, Mutex},
	thread::scope,
};

use futures::future::BoxFuture;
use rocksdb::{AsColumnFamilyRef, IteratorMode, Options, WriteBatch, WriteBatchIteratorCf};
use tokio::runtime::Handle;
use tracing::subscriber::NoSubscriber;
use tuwunel_core::{
	Result, Server,
	config::{Config, Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture::State},
	metrics::Metrics,
};

use super::{
	BATCH_HEADER, Barrier, BarrierError, Entry, MARKER_CF, Op, Records, Role, encode,
	put_varint32,
};
use crate::{Database, Txn};

const COLUMNS: [&str; 5] = [
	"alias_roomid",
	"alias_userid",
	"presenceid_presence",
	"roomid_joinedcount",
	"roomuserid_joined",
];

type Dump = Vec<(Box<[u8]>, Box<[u8]>)>;

type Seen = Vec<(Op, u32, Vec<u8>, Vec<u8>)>;

type Write<'a> = Box<dyn Fn() + 'a>;

type Damage<'a> = Box<dyn Fn(&mut Entry) + 'a>;

struct Collect(Seen);

impl WriteBatchIteratorCf for Collect {
	fn put_cf(&mut self, cf: u32, key: &[u8], val: &[u8]) {
		self.0
			.push((Op::Put, cf, key.to_vec(), val.to_vec()));
	}

	fn delete_cf(&mut self, cf: u32, key: &[u8]) {
		self.0
			.push((Op::Delete, cf, key.to_vec(), Vec::new()));
	}

	fn merge_cf(&mut self, _: u32, _: &[u8], _: &[u8]) { unreachable!("merge is never written") }
}

struct Link<'a> {
	leader: &'a Database,
	follower: &'a Database,
	next: u64,
}

impl<'a> Link<'a> {
	fn new(leader: &'a Database, follower: &'a Database) -> Self {
		Self {
			leader,
			follower,
			next: leader.engine.current_sequence(),
		}
	}

	fn pump(&mut self, max_bytes: usize) -> Result<bool> {
		let Some(entry) = self.leader.engine.drain_captured(max_bytes)? else {
			return Ok(false);
		};

		assert_eq!(entry.seq.start, self.next, "entries follow each other without a gap");
		self.next = entry.seq.end;
		self.follower
			.engine
			.apply_replicated(from_ref(&entry), &self.next.to_be_bytes())?;

		Ok(true)
	}

	fn finish(&mut self) -> Result {
		while self.pump(4096)? {}

		assert_eq!(self.next, self.leader.engine.current_sequence(), "everything was shipped");
		assert_eq!(
			self.follower.engine.applied_marker()?,
			Some(self.next.to_be_bytes().to_vec()),
			"marker names the last shipped entry"
		);

		Ok(())
	}
}

struct Gate(Mutex<Vec<u64>>, Result<(), BarrierError>);

impl Barrier for Gate {
	fn wait(&self, seq: u64) -> BoxFuture<'_, Result<(), BarrierError>> {
		self.0.lock().expect("gate locked").push(seq);
		let verdict = self.1;

		Box::pin(async move { verdict })
	}
}

fn path(name: &str) -> PathBuf {
	temp_dir()
		.join("tuwunel")
		.join(format!("replica-{name}-{}", process_id()))
}

async fn open(name: &str) -> Result<Arc<Database>> { open_at(&path(name), true).await }

async fn open_at(path: &Path, scratch: bool) -> Result<Arc<Database>> {
	let mut raw = Figment::new()
		.merge(("server_name", "localhost"))
		.merge(("database_path", path));

	if scratch {
		raw = raw.merge(("test", ["fresh", "cleanup"]));
	}

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

async fn pair(name: &str) -> Result<(Arc<Database>, Arc<Database>)> {
	let leader = open(&format!("{name}-leader")).await?;
	let follower = open(&format!("{name}-follower")).await?;
	leader.engine.set_role(Role::Leader)?;
	follower.engine.set_role(Role::Follower)?;

	Ok((leader, follower))
}

fn dump(db: &Database, column: &str) -> Dump {
	db.engine
		.db
		.iterator_cf(&db.engine.cf(column), IteratorMode::Start)
		.map(|row| row.expect("iterate column"))
		.collect()
}

fn assert_same(leader: &Database, follower: &Database) {
	for column in COLUMNS {
		assert_eq!(dump(leader, column), dump(follower, column), "column {column} differs");
	}
}

fn drain_each(db: &Database) -> Vec<Entry> {
	std::iter::from_fn(|| {
		db.engine
			.drain_captured(0)
			.expect("drain captured")
	})
	.collect()
}

fn next(state: &mut u64) -> u64 {
	*state = state
		.wrapping_mul(6_364_136_223_846_793_005)
		.wrapping_add(1_442_695_040_888_963_407);

	state.rotate_right(33)
}

fn churn(db: &Database, seed: u64) {
	let maps: Vec<_> = COLUMNS
		.iter()
		.map(|name| db.get(name).expect("column exists"))
		.collect();

	let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
	for round in 0_u64..150 {
		let map = &maps[usize::try_from(next(&mut state) % 5).expect("index")];
		let other = &maps[usize::try_from(next(&mut state) % 5).expect("index")];
		let key = format!("key-{}", next(&mut state) % 24);
		let val = format!("{seed}-{round}");

		match next(&mut state) % 3 {
			| 0 => map.insert(&key, &val),
			| 1 => map.remove(&key),
			| _ => {
				let mut txn = db.txn();
				txn.insert_raw(map, &key, &val);
				txn.del_raw(other, &key);
				txn.insert_raw(other, format!("{key}-b"), &val);
				txn.execute();
			},
		}
	}
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
	payload
		.downcast_ref::<String>()
		.cloned()
		.or_else(|| {
			payload
				.downcast_ref::<&str>()
				.map(ToString::to_string)
		})
		.expect("panic carries a message")
}

#[tokio::test]
async fn sixteen_writers_replay_into_equal_columns() -> Result {
	let (leader, follower) = pair("writers").await?;
	let mut link = Link::new(&leader, &follower);

	scope(|scope| -> Result {
		let writers: Vec<_> = (0..16_u64)
			.map(|seed| {
				let leader = &leader;
				scope.spawn(move || churn(leader, seed))
			})
			.collect();

		while writers.iter().any(|writer| !writer.is_finished()) {
			link.pump(2048)?;
		}

		for writer in writers {
			writer.join().expect("writer finished");
		}

		Ok(())
	})?;

	link.finish()?;
	assert_same(&leader, &follower);
	assert!(!leader.engine.is_deposed(), "no write bypassed the hook");
	assert_eq!(leader.engine.captured_bytes(), 0, "queue is empty");

	Ok(())
}

#[tokio::test]
async fn replaying_a_prefix_is_idempotent() -> Result {
	let (leader, follower) = pair("prefix").await?;
	let map = leader.get(COLUMNS[0])?;
	for n in 0..12_u8 {
		map.insert(&[n % 5], [n]);
		if n % 4 == 3 {
			map.remove(&[n % 3]);
		}
	}

	let entries = drain_each(&leader);
	let (prefix, rest) = entries.split_at(entries.len() / 2);
	assert!(!prefix.is_empty() && !rest.is_empty(), "both parts are nonempty");

	follower.engine.apply_replicated(prefix, b"m1")?;
	let once = dump(&follower, COLUMNS[0]);
	follower.engine.apply_replicated(prefix, b"m1")?;
	assert_eq!(dump(&follower, COLUMNS[0]), once, "second application changed the state");

	follower.engine.apply_replicated(rest, b"m2")?;
	assert_eq!(
		dump(&follower, COLUMNS[0]),
		dump(&leader, COLUMNS[0]),
		"replay then rest converges"
	);

	for (name, replay) in [("from the middle", rest), ("from the start", entries.as_slice())] {
		follower.engine.apply_replicated(replay, b"m3")?;
		assert_eq!(
			dump(&follower, COLUMNS[0]),
			dump(&leader, COLUMNS[0]),
			"replay {name} after everything diverged"
		);
	}

	Ok(())
}

#[tokio::test]
async fn marker_is_written_with_the_data_it_names() -> Result {
	let dir = path("marker-follower");
	remove_dir_all(&dir).ok();

	let leader = open("marker-leader").await?;
	leader.engine.set_role(Role::Leader)?;
	let follower = open_at(&dir, false).await?;
	follower.engine.set_role(Role::Follower)?;

	let map = leader.get(COLUMNS[0])?;
	map.insert(b"a", b"1");
	map.insert(b"b", b"2");
	let first = leader
		.engine
		.drain_captured(usize::MAX)?
		.expect("captured");
	follower
		.engine
		.apply_replicated(&[first], b"after-first")?;

	map.insert(b"c", b"3");
	map.remove(b"a");
	let second = leader
		.engine
		.drain_captured(usize::MAX)?
		.expect("captured");
	let before = follower.engine.current_sequence();
	follower
		.engine
		.apply_replicated(&[second], b"after-second")?;

	let batches = follower
		.engine
		.db
		.get_updates_since(before)
		.expect("wal reads")
		.collect::<Result<Vec<_>, _>>()
		.expect("wal batches read");

	let [(_, batch)] = batches.as_slice() else {
		panic!("the entries and the marker are one write, found {} writes", batches.len());
	};

	let mut seen = Collect(Vec::new());
	batch.iterate_cf(&mut seen);
	let column = follower.engine.cf(COLUMNS[0]).id();
	let marker = follower.engine.cf(MARKER_CF).id();
	assert!(seen.0.iter().any(|row| row.1 == column), "data is in the write");
	assert!(seen.0.iter().any(|row| row.1 == marker), "marker is in the write");

	drop(follower);
	let reopened = open_at(&dir, false).await?;
	assert_eq!(reopened.engine.applied_marker()?, Some(b"after-second".to_vec()));
	assert_eq!(dump(&reopened, COLUMNS[0]), dump(&leader, COLUMNS[0]));

	drop(reopened);
	remove_dir_all(&dir).ok();

	Ok(())
}

#[tokio::test]
async fn follower_refuses_local_writes() -> Result {
	let db = open("follower").await?;
	db.engine.set_role(Role::Follower)?;

	let map = db.get(COLUMNS[0])?;
	let before = db.engine.current_sequence();
	let writes: [(&str, Write<'_>); 4] = [
		("insert", Box::new(|| map.insert(b"k", b"v"))),
		("remove", Box::new(|| map.remove(b"k"))),
		("txn", Box::new(|| Txn::insert(map, [(b"k", b"v")]).execute())),
		("marker", Box::new(|| db.engine.write_marker(b"m").expect("unreachable"))),
	];

	for (name, write) in writes {
		let payload = catch_unwind(AssertUnwindSafe(write)).expect_err(name);
		assert_eq!(panic_message(&*payload), "database write on a cluster follower");
	}

	assert_eq!(db.engine.current_sequence(), before, "nothing was written");

	Ok(())
}

#[tokio::test]
async fn write_outside_the_hook_deposes_the_leader() -> Result {
	let db = open("bypass").await?;
	db.engine.set_role(Role::Leader)?;

	let map = db.get(COLUMNS[0])?;
	map.insert(b"hooked", b"1");
	assert!(!db.engine.is_deposed(), "hooked write is fine");

	db.engine
		.db
		.put_cf(&db.engine.cf(COLUMNS[0]), b"stray", b"2")
		.expect("stray write");

	Txn::insert(map, [(b"after", b"3")])
		.try_write()
		.expect_err("the write after a stray write is refused");

	assert!(db.engine.is_deposed(), "stray write was detected");
	catch_unwind(AssertUnwindSafe(|| map.insert(b"again", b"4"))).expect_err("insert is refused");
	db.engine
		.drain_captured(usize::MAX)
		.expect_err("captured writes are not handed out");

	assert_eq!(db.barrier().await, Err(BarrierError::NotLeader));
	assert!(
		map.get(b"after")
			.await
			.unwrap_err()
			.is_not_found(),
		"refused write was not made"
	);

	Ok(())
}

#[tokio::test]
async fn leader_marker_is_written_but_not_captured() -> Result {
	let db = open("leader-marker").await?;
	db.engine.set_role(Role::Leader)?;

	let map = db.get(COLUMNS[0])?;
	let start = db.engine.current_sequence();
	assert_eq!(db.engine.applied_marker()?, None, "no marker yet");

	db.engine.write_marker(b"one")?;
	assert_eq!(db.engine.applied_marker()?, Some(b"one".to_vec()));
	assert_eq!(db.engine.current_sequence(), start.saturating_add(1));
	assert_eq!(db.engine.captured_bytes(), 0, "marker is not queued");

	map.insert(b"k", b"v");
	db.engine.write_marker(b"two")?;
	assert_eq!(db.engine.applied_marker()?, Some(b"two".to_vec()));
	assert!(!db.engine.is_deposed(), "marker writes keep the sequence in step");

	let entry = db
		.engine
		.drain_captured(usize::MAX)?
		.expect("insert is captured");
	assert_eq!(entry.seq, start.saturating_add(1)..start.saturating_add(2));
	assert_eq!(entry.cfs, [COLUMNS[0]], "marker column is not replicated");
	assert!(db.engine.drain_captured(usize::MAX)?.is_none(), "nothing else is queued");

	Ok(())
}

#[tokio::test]
async fn leader_refuses_a_foreign_batch_tag_before_writing() -> Result {
	let db = open("foreign-tag").await?;
	db.engine.set_role(Role::Leader)?;

	let before = db.engine.current_sequence();
	let column = db.engine.cf(COLUMNS[0]).id();
	let mut data = vec![0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0];
	encode(&mut data, Op::Put, Some(column), b"k", b"v");
	data.push(0x8);
	put_varint32(&mut data, column);
	data.extend([1, b'k']);

	db.engine
		.write_batch(&WriteBatch::from_data(&data), &db.engine.write_options)
		.expect_err("a single deletion cannot be replicated");

	assert_eq!(db.engine.current_sequence(), before, "nothing was written");
	assert!(!db.engine.is_deposed(), "refusal is not a fault");
	assert_eq!(db.engine.captured_bytes(), 0);

	db.get(COLUMNS[0])?.insert(b"k", b"v");
	assert!(!db.engine.is_deposed(), "leader keeps working");

	Ok(())
}

#[tokio::test]
async fn missing_column_family_is_created_on_apply() -> Result {
	let leader = open("missing-leader").await?;
	let follower = open("missing-follower").await?;
	leader
		.engine
		.db
		.create_cf("extra_cf", &Options::default())
		.expect("create column");

	leader.engine.set_role(Role::Leader)?;
	follower.engine.set_role(Role::Follower)?;
	assert!(!follower.engine.has_cf("extra_cf"), "follower lacks the column");

	let extra = leader
		.open_cf("extra_cf")?
		.expect("column exists");
	extra.insert(b"k", b"v");
	extra.remove(b"gone");
	let entry = leader
		.engine
		.drain_captured(usize::MAX)?
		.expect("captured");
	assert_eq!(entry.cfs, ["extra_cf"]);
	follower.engine.apply_replicated(&[entry], b"m")?;

	let cf = follower.engine.cf("extra_cf");
	assert_eq!(
		follower
			.engine
			.db
			.get_cf(&cf, b"k")
			.expect("read"),
		Some(b"v".to_vec())
	);
	assert!(follower.engine.has_cf(MARKER_CF), "marker column is created on the fly");

	Ok(())
}

#[tokio::test]
async fn malformed_entries_are_refused_before_anything_is_written() -> Result {
	let (leader, follower) = pair("malformed").await?;
	leader.get(COLUMNS[0])?.insert(b"k", b"v");
	let good = leader
		.engine
		.drain_captured(usize::MAX)?
		.expect("captured");
	good.validate()?;

	let bad_cases: Vec<(&str, Damage<'_>)> = vec![
		("version", Box::new(|e: &mut Entry| e.version = 2)),
		("own origin", Box::new(|e: &mut Entry| e.origin = follower.engine.origin())),
		(
			"record count",
			Box::new(|e: &mut Entry| e.seq.end = e.seq.end.saturating_add(1)),
		),
		("reversed range", Box::new(|e: &mut Entry| e.seq = Range { start: 5, end: 4 })),
		("merge tag", Box::new(|e: &mut Entry| e.rep.extend([0x2, 1, b'k', 1, b'v']))),
		("single delete tag", Box::new(|e: &mut Entry| e.rep.extend([0x7, 1, b'k']))),
		(
			"truncated",
			Box::new(|e: &mut Entry| e.rep.truncate(e.rep.len().saturating_sub(1))),
		),
		(
			"overlong varint",
			Box::new(|e: &mut Entry| e.rep = vec![0x1, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
		),
		("unknown slot", Box::new(|e: &mut Entry| e.cfs.clear())),
		("empty name", Box::new(|e: &mut Entry| e.cfs = vec![String::new()])),
		("marker column", Box::new(|e: &mut Entry| e.cfs = vec![MARKER_CF.into()])),
	];

	for (name, damage) in bad_cases {
		let mut entry = good.clone();
		damage(&mut entry);
		follower
			.engine
			.apply_replicated(&[good.clone(), entry], b"bad")
			.expect_err(name);
	}

	leader
		.engine
		.apply_replicated(from_ref(&good), b"x")
		.expect_err("a leader does not apply");

	assert_eq!(follower.engine.applied_marker()?, None, "nothing was applied");
	assert!(dump(&follower, COLUMNS[0]).is_empty(), "no data was written");
	assert!(!follower.engine.has_cf(MARKER_CF), "no column was created");

	follower.engine.apply_replicated(&[good], b"ok")?;
	assert_eq!(follower.engine.applied_marker()?, Some(b"ok".to_vec()));

	Ok(())
}

#[tokio::test]
async fn record_parser_agrees_with_rocksdb() -> Result {
	let db = open("parser").await?;
	let (first, second) = (db.engine.cf(COLUMNS[0]), db.engine.cf(COLUMNS[1]));
	let mut batch = WriteBatch::default();
	batch.put(b"d", b"default");
	batch.delete(b"gone");
	batch.put_cf(&first, b"k1", b"v1");
	batch.delete_cf(&second, b"k2");
	batch.put_cf(&second, [7_u8; 300], vec![9_u8; 70_000]);
	batch.put_cf(&first, b"", b"");

	let mut expected = Collect(Vec::new());
	batch.iterate_cf(&mut expected);

	let body = batch
		.data()
		.get(BATCH_HEADER..)
		.expect("batch has a header");

	let parsed: Seen = Records::new(body)
		.map(|row| row.expect("record parses"))
		.map(|row| (row.op, row.cf.unwrap_or(0), row.key.to_vec(), row.val.to_vec()))
		.collect();

	assert_eq!(parsed, expected.0);

	let mut again = Vec::new();
	Records::new(body)
		.map(|row| row.expect("record parses"))
		.for_each(|row| encode(&mut again, row.op, row.cf, row.key, row.val));

	assert_eq!(again, body, "encoding reproduces the batch");

	Ok(())
}

#[tokio::test]
async fn standalone_changes_nothing_and_roles_only_advance() -> Result {
	let db = open("standalone").await?;
	let map = db.get(COLUMNS[0])?;
	map.insert(b"a", b"1");
	map.remove(b"a");
	Txn::insert(map, [(b"b", b"2")]).execute();

	assert_eq!(db.engine.role(), Role::Standalone);
	assert_eq!(db.engine.captured_bytes(), 0);
	assert!(db.engine.drain_captured(usize::MAX)?.is_none(), "nothing is captured");
	assert!(!db.engine.is_deposed());
	db.barrier().await.expect("no barrier installed");
	db.engine.write_marker(b"standalone")?;
	assert_eq!(db.engine.applied_marker()?, Some(b"standalone".to_vec()));

	let gate = Arc::new(Gate(Mutex::new(Vec::new()), Err(BarrierError::Timeout)));
	db.engine.set_barrier(gate.clone())?;
	db.engine
		.set_barrier(gate.clone())
		.expect_err("a second barrier is refused");

	assert_eq!(db.barrier().await, Err(BarrierError::Timeout));
	assert_eq!(*gate.0.lock().expect("gate locked"), [db.engine.current_sequence()]);

	db.engine.set_role(Role::Follower)?;
	db.engine
		.set_role(Role::Standalone)
		.expect_err("follower cannot leave the cluster");

	db.engine.set_role(Role::Leader)?;
	for role in [Role::Standalone, Role::Follower, Role::Leader] {
		db.engine
			.set_role(role)
			.expect_err("leader keeps its role");
	}

	Ok(())
}

#[tokio::test]
async fn drain_merges_whole_captures_up_to_the_limit() -> Result {
	let db = open("drain").await?;
	db.engine.set_role(Role::Leader)?;

	let map = db.get(COLUMNS[0])?;
	let start = db.engine.current_sequence();
	let at = |offset: u64| start.saturating_add(offset);
	for n in 0..6_u8 {
		map.insert(&[n], [n; 100]);
	}

	assert!(db.engine.captured_bytes() > 600, "queued bytes are accounted");
	let head = db
		.engine
		.drain_captured(0)?
		.expect("first capture is always taken");
	assert_eq!(head.seq, at(0)..at(1));

	let merged = db.engine.drain_captured(250)?.expect("captures");
	assert_eq!(merged.seq, at(1)..at(3), "limit stops the merge");

	let tail = db
		.engine
		.drain_captured(usize::MAX)?
		.expect("captures");
	assert_eq!(tail.seq, at(3)..at(6));
	assert!(db.engine.drain_captured(usize::MAX)?.is_none(), "queue is empty");
	assert_eq!(db.engine.captured_bytes(), 0);

	for entry in [&head, &merged, &tail] {
		entry.validate()?;
		let wire = serde_json::to_vec(entry).expect("entry serializes");
		assert_eq!(&serde_json::from_slice::<Entry>(&wire).expect("entry deserializes"), entry);
	}

	Ok(())
}

#[tokio::test]
async fn checkpoint_holds_everything_before_its_first_sequence() -> Result {
	let db = open("checkpoint").await?;
	db.engine.set_role(Role::Leader)?;

	let map = db.get(COLUMNS[0])?;
	let start = db.engine.current_sequence();
	for n in 0..20_u8 {
		map.insert(&[n], [n]);
	}

	let image = path("checkpoint-image");
	remove_dir_all(&image).ok();
	let (before, after) = db.engine.checkpoint_bracketed(&image)?;
	assert_eq!(before, start.saturating_add(20));
	assert!(after >= before, "second sequence is not behind the first");

	let copy = open_at(&image, false).await?;
	assert_eq!(
		copy.get(COLUMNS[0])?
			.get(&[19_u8])
			.await?
			.as_ref(),
		&[19_u8]
	);

	drop(copy);
	remove_dir_all(&image).ok();

	let corked = path("checkpoint-corked-image");
	remove_dir_all(&corked).ok();
	let cork = db.cork();
	for n in 20..25_u8 {
		map.insert(&[n], [n]);
	}

	let (before, _) = db.engine.checkpoint_bracketed(&corked)?;
	drop(cork);
	assert_eq!(before, start.saturating_add(25));

	let copy = open_at(&corked, false).await?;
	assert_eq!(
		copy.get(COLUMNS[0])?
			.get(&[24_u8])
			.await?
			.as_ref(),
		&[24_u8],
		"writes held back by a cork are in the checkpoint"
	);

	drop(copy);
	remove_dir_all(&corked).ok();

	Ok(())
}

#[test]
fn sources_never_bypass_the_hook() {
	const FORBIDDEN: [&str; 4] =
		["ingest_external", "merge_cf", "delete_range_cf", "disable_wal(true)"];

	const HOOKED: [&str; 6] = [
		"write_opt(",
		"put_cf_opt(",
		"delete_cf_opt(",
		"write_without_wal",
		"single_delete_cf",
		"set_disable_wal(",
	];

	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
	let mut pending = vec![root.clone()];
	let mut scanned = 0_usize;
	while let Some(dir) = pending.pop() {
		for entry in read_dir(&dir).expect("source directory reads") {
			let file = entry.expect("source entry reads").path();
			if file.is_dir() {
				pending.push(file);
				continue;
			}

			let relative = file
				.strip_prefix(&root)
				.expect("file is under the root");
			let is_test = relative
				.file_name()
				.is_some_and(|name| name == "tests.rs")
				|| relative
					.components()
					.any(|part| part.as_os_str() == "tests");

			if file.extension().is_none_or(|ext| ext != "rs")
				|| file.ends_with("database/replica/tests.rs")
			{
				continue;
			}

			let hooks = file.ends_with("database/replica.rs");
			let text = read_to_string(&file).expect("source file reads");
			scanned = scanned.saturating_add(1);

			for line in text
				.lines()
				.filter(|line| !line.trim_start().starts_with("//"))
			{
				for needle in FORBIDDEN {
					assert!(!line.contains(needle), "{file:?} uses {needle}");
				}

				for needle in HOOKED.iter().filter(|_| !hooks && !is_test) {
					assert!(
						!line.contains(needle),
						"{file:?} writes with {needle} outside the hook"
					);
				}
			}
		}
	}

	assert!(scanned > 100, "the scan found only {scanned} files");
}
