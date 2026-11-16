use std::{collections::BTreeMap, fs, io::ErrorKind, path::Path, sync::Arc};

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use tuwunel_core::Result;
use tuwunel_database::Role;

use super::{
	Pending, Step, decode_leader_marker, decode_pending, encode_leader_marker, encode_pending,
};
use crate::{
	LogStore, Machine, Recovery, SnapshotDir, decode_marker,
	snapshot::is_immutable,
	testing::{
		Dir, Donor, Layout, MAP, Pump, Rows, Started, copy_tree, donor, drain, feed, leader,
		log_id, logged, machine, open, parts, rows, script, snapshot_dirs, stage, start,
		write_script,
	},
};

/// Writes rows into the database at `path` before it joins any cluster.
async fn seed(path: &Path) -> Result {
	let db = open(path).await?;
	db.get(MAP)?.insert(b"stale", b"old");
	db.get(MAP)?.insert(b"same", b"old");

	Ok(())
}

/// The files of a directory with their lengths, to see that nothing changed.
fn listing(path: &Path) -> BTreeMap<String, u64> {
	fs::read_dir(path)
		.expect("directory reads")
		.map(|entry| {
			let entry = entry.expect("entry reads");
			let len = entry.metadata().expect("metadata reads").len();

			(entry.file_name().to_string_lossy().into_owned(), len)
		})
		.collect()
}

/// Starts a node that holds other data than the donor, and crashes the
/// installation of the donor's snapshot at `step`.
async fn crash_install(layout: &Layout, donor: &Donor, step: Step) -> Result {
	let Started { log, store, db, .. } = start(layout).await?;
	assert_ne!(rows(&db).await?, donor.rows, "{step:?}: the node starts with other data");

	store.set_hook(Some(Arc::new(move |seen| {
		assert_ne!(seen, step, "injected crash at {step:?}");
	})));

	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let (mut applier, _) = machine(db, &store);
	let meta = donor.snapshot.meta.clone();
	let outcome =
		tokio::spawn(async move { applier.install_snapshot(&meta, staged).await }).await;

	assert!(!matches!(outcome, Ok(Ok(()))), "{step:?}: the installation stopped there");

	drop(outcome);
	drop(store);
	drop(log);

	Ok(())
}

/// Crashes an installation at each of `steps` in turn, on a node that starts
/// with other data, and checks what the next start makes of it.
async fn crash_at(steps: &[Step]) -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let last = u64::try_from(donor.items.len()).expect("length fits");

	seed(&dir.layout("template").database).await?;

	for &step in steps {
		let layout = dir.layout(&format!("case-{step:?}"));
		copy_tree(&dir.layout("template").database, &layout.database);

		crash_install(&layout, &donor, step).await?;

		let Started { log, store, db, recovery } = start(&layout).await?;
		let expected = match step {
			| Step::Cleared => Recovery::Clean,
			| _ => Recovery::Resumed,
		};

		assert_eq!(recovery, expected, "{step:?}: what the start found");
		assert_eq!(rows(&db).await?, donor.rows, "{step:?}: the database is the snapshot's");
		assert_eq!(
			db.engine
				.applied_marker()?
				.map(|marker| decode_marker(&marker).expect("marker decodes").0),
			Some(Some(log_id(1, last))),
			"{step:?}: the position is the snapshot's"
		);
		assert_eq!(store.inner.meta.replace_marker().await?, None, "{step:?}: marker is removed");
		assert_eq!(snapshot_dirs(&layout.snapshots).len(), 1, "{step:?}: one snapshot is kept");

		if step == Step::Positioned {
			drop(db);
			drop(store);
			drop(log);

			let again = start(&layout).await?;
			assert_eq!(again.recovery, Recovery::Clean, "a second start has nothing to do");
			assert_eq!(rows(&again.db).await?, donor.rows, "and changes nothing");
		}
	}

	Ok(())
}

struct Past {
	at_snapshot: Rows,
	at_commit: Rows,
	confirmed: u64,
}

/// A leader that confirms a script, snapshots, confirms more, takes one write
/// nobody confirms, and stops without handing over.
async fn lead_and_die(layout: &Layout, pump: &mut Pump) -> Result<Past> {
	let Started { log, store, db, .. } = start(layout).await?;
	db.engine.set_role(Role::Leader)?;

	let (mut applier, _) = machine(Arc::clone(&db), &store);
	write_script(&db);
	pump.run(&mut applier, &db).await;
	let confirmed = pump.last();

	db.engine.sort()?;
	applier
		.try_create_snapshot_builder(false)
		.await
		.expect("a planned builder")
		.build_snapshot()
		.await?;

	let at_snapshot = rows(&db).await?;

	db.get(MAP)?.insert(b"later", b"confirmed");
	pump.run(&mut applier, &db).await;
	let at_commit = rows(&db).await?;

	db.get(MAP)?.insert(b"dirty", b"unconfirmed");
	assert!(
		rows(&db)
			.await?
			.contains(&(b"dirty".to_vec(), b"unconfirmed".to_vec())),
		"the leader holds a write nobody confirmed"
	);

	store
		.inner
		.meta
		.set_leader_marker(&encode_leader_marker(db.engine.origin()))
		.await?;

	drop(log);

	Ok(Past { at_snapshot, at_commit, confirmed })
}

#[tokio::test]
async fn a_crash_before_the_files_change_is_finished_at_start() -> Result {
	crash_at(&[Step::Marked, Step::Removed(1)]).await
}

#[tokio::test]
async fn a_crash_while_the_files_change_is_finished_at_start() -> Result {
	crash_at(&[Step::Removed(2), Step::Cleaned, Step::Placed(1), Step::Placed(2), Step::Linked])
		.await
}

#[tokio::test]
async fn a_crash_once_the_files_are_in_place_is_finished_at_start() -> Result {
	crash_at(&[Step::Synced, Step::Opened, Step::Positioned, Step::Cleared]).await
}

#[tokio::test]
async fn a_dirty_leader_restores_its_own_snapshot_and_replays_the_log() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("leader");
	let mut pump = Pump::new();
	let Past { at_snapshot, at_commit, confirmed } = lead_and_die(&layout, &mut pump).await?;

	let Started { log, store, db, recovery } = start(&layout).await?;
	assert_eq!(recovery, Recovery::Restored);
	assert_eq!(rows(&db).await?, at_snapshot, "it holds the snapshot and nothing newer");
	assert_eq!(store.inner.meta.leader_marker().await?, None, "the node is clean again");
	assert_eq!(store.inner.meta.replace_marker().await?, None);

	let (mut applier, _) = machine(db, &store);
	assert_eq!(applier.applied_state().await?.0, Some(log_id(1, confirmed)));

	let first = usize::try_from(confirmed).expect("index fits");
	applier
		.apply(feed(pump.log[first..].to_vec()))
		.await?;

	assert_eq!(
		rows(applier.database().expect("database")).await?,
		at_commit,
		"the log brings back what was confirmed and only that"
	);

	drop(log);

	Ok(())
}

async fn mark_dirty(layout: &Layout) -> Result {
	let Started { store, .. } = start(layout).await?;
	store
		.inner
		.meta
		.set_leader_marker(&encode_leader_marker(7))
		.await?;

	Ok(())
}

#[tokio::test]
async fn a_dirty_node_without_a_snapshot_refuses_to_start() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("node");

	mark_dirty(&layout).await?;

	let Err(error) = start(&layout).await else {
		panic!("a dirty node without a snapshot started");
	};

	assert_eq!(error.kind(), ErrorKind::InvalidData);
	assert!(error.to_string().contains("no snapshot"), "{error}");

	let meta = LogStore::open(&layout.raft).await?.meta();
	assert_eq!(
		meta.leader_marker().await?,
		Some(encode_leader_marker(7)),
		"the marker stays, so the node cannot start on the database later"
	);

	meta.set_leader_marker(&[9, 0]).await?;
	drop(meta);

	let Err(error) = start(&layout).await else {
		panic!("a leader marker of an unknown version was ignored");
	};

	assert_eq!(error.kind(), ErrorKind::InvalidData);

	Ok(())
}

#[tokio::test]
async fn an_installation_that_cannot_close_the_database_is_finished_at_the_next_start() -> Result
{
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let layout = dir.layout("node");

	seed(&layout.database).await?;

	let Started { log, store, db, .. } = start(&layout).await?;
	let before = rows(&db).await?;
	let spare = Arc::clone(&db);

	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let (mut applier, _) = machine(db, &store);
	let error = applier
		.install_snapshot(&donor.snapshot.meta, staged)
		.await
		.expect_err("a database somebody else holds is not replaced");

	assert_eq!(error.kind(), ErrorKind::ResourceBusy);
	assert!(
		store.inner.meta.replace_marker().await?.is_some(),
		"the replacement is stored for the next start"
	);
	assert_eq!(rows(&spare).await?, before, "nothing was touched in this process");
	assert!(applier.database().is_some(), "the machine still has its database");

	drop(applier);
	drop(spare);
	drop(store);
	drop(log);

	let Started { store, db, recovery, .. } = start(&layout).await?;
	assert_eq!(recovery, Recovery::Resumed);
	assert_eq!(rows(&db).await?, donor.rows);
	assert_eq!(store.inner.meta.replace_marker().await?, None);

	Ok(())
}

#[tokio::test]
async fn a_snapshot_is_installed_by_a_follower_from_the_staging_directory_or_its_own() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let layout = dir.layout("node");

	let Started { store, db, .. } = start(&layout).await?;
	let before = rows(&db).await?;
	let (mut applier, _) = machine(db, &store);
	let meta = &donor.snapshot.meta;

	let foreign = donor.snapshot.snapshot.clone();
	let error = applier
		.install_snapshot(meta, foreign)
		.await
		.expect_err("a directory of another place is refused");

	assert_eq!(error.kind(), ErrorKind::InvalidInput);

	let mut damaged = stage(&store, &donor.snapshot.snapshot).await;
	let victim = damaged.path().join(&damaged.files()[0].name);
	fs::write(&victim, b"short")?;
	let error = applier
		.install_snapshot(meta, damaged.clone())
		.await
		.expect_err("a file of another length is refused");

	assert_eq!(error.kind(), ErrorKind::InvalidData);
	damaged = stage(&store, &donor.snapshot.snapshot).await;
	fs::write(damaged.path().join("stranger"), b"x")?;
	let error = applier
		.install_snapshot(meta, damaged)
		.await
		.expect_err("a file that is no snapshot file is refused");

	assert_eq!(error.kind(), ErrorKind::InvalidData);

	let staged = stage(&store, &donor.snapshot.snapshot).await;
	fs::write(staged.path().join("META"), b"x")?;
	SnapshotDir::scan(staged.path()).expect_err("a metadata file in a transfer is refused");

	let error = applier
		.install_snapshot(meta, staged)
		.await
		.expect_err("a staging directory with a metadata file is refused");

	assert_eq!(error.kind(), ErrorKind::InvalidData);
	assert!(error.to_string().contains("META"), "{error}");
	assert_eq!(store.inner.meta.replace_marker().await?, None, "nothing was started");
	assert_eq!(rows(applier.database().expect("database")).await?, before);

	let mut empty = meta.clone();
	empty.last_log_id = None;
	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let error = applier
		.install_snapshot(&empty, staged)
		.await
		.expect_err("a snapshot with no position is refused");

	assert_eq!(error.kind(), ErrorKind::InvalidInput);

	let leading = leader(&dir, "leading").await?;
	let (mut leader, _) = machine(leading, &store);
	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let error = leader
		.install_snapshot(meta, staged)
		.await
		.expect_err("a leader installs nothing");

	assert_eq!(error.kind(), ErrorKind::InvalidInput);

	Ok(())
}

#[tokio::test]
async fn the_replacement_removes_database_files_and_leaves_the_rest_of_the_volume() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let layout = dir.legacy("legacy");
	let volume = &layout.database;

	fs::create_dir_all(volume.join("backup"))?;
	fs::create_dir_all(volume.join("media"))?;
	fs::create_dir_all(volume.join("archive"))?;
	fs::write(volume.join("backup/1.sst"), b"backup")?;
	fs::write(volume.join("media/blob"), b"media")?;
	fs::write(volume.join("archive/000001.log"), b"old wal")?;
	fs::write(volume.join("notes.log"), b"notes")?;
	fs::write(volume.join("LOCKED"), b"not a database file")?;
	fs::write(volume.join("LOG.old.1"), b"old info log")?;

	seed(volume).await?;

	let Started { store, db, .. } = start(&layout).await?;
	assert!(
		rows(&db)
			.await?
			.contains(&(b"stale".to_vec(), b"old".to_vec())),
		"the volume held a database"
	);

	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let (mut applier, _) = machine(db, &store);
	applier
		.install_snapshot(&donor.snapshot.meta, staged)
		.await?;

	assert_eq!(rows(applier.database().expect("database")).await?, donor.rows);

	for kept in ["backup/1.sst", "media/blob", "notes.log", "LOCKED"] {
		assert!(volume.join(kept).is_file(), "{kept} survives");
	}

	for removed in ["archive/000001.log", "LOG.old.1"] {
		assert!(!volume.join(removed).exists(), "{removed} is removed");
	}

	assert!(
		layout.raft.is_dir() && layout.snapshots.is_dir(),
		"the log and the snapshots stay"
	);
	assert_eq!(snapshot_dirs(&layout.snapshots).len(), 1);

	Ok(())
}

#[test]
fn the_leader_marker_holds_a_version_and_the_origin() {
	let marker = encode_leader_marker(0x0102_0304_0506_0708);

	assert_eq!(marker, [1, 1, 2, 3, 4, 5, 6, 7, 8], "the encoding is fixed");
	assert_eq!(decode_leader_marker(&marker).expect("decodes"), 0x0102_0304_0506_0708);

	for (name, damaged) in [
		("empty", Vec::new()),
		("version", vec![2, 0, 0, 0, 0, 0, 0, 0, 0]),
		("short", marker[..8].to_vec()),
		("long", [marker.as_slice(), &[0]].concat()),
	] {
		let error = decode_leader_marker(&damaged).expect_err(name);
		assert_eq!(error.kind(), ErrorKind::InvalidData, "{name}");
	}
}

#[test]
fn a_damaged_replace_marker_is_refused() {
	let pending = Pending {
		snapshot: "snapshot-00000000000000000007".to_owned(),
		marker: vec![1, 2, 3],
	};

	let bytes = encode_pending(&pending).expect("encodes");
	assert_eq!(decode_pending(&bytes).expect("decodes"), pending);

	let mut future = bytes.clone();
	future[0] = 2;
	let mut longer = bytes.clone();
	longer.push(0);
	let escaping = encode_pending(&Pending {
		snapshot: "../snapshot-00000000000000000007".to_owned(),
		marker: Vec::new(),
	})
	.expect("encodes");

	for (name, damaged) in [
		("empty", Vec::new()),
		("version", future),
		("trailing byte", longer),
		("short", bytes[..bytes.len() - 1].to_vec()),
		("name", escaping),
	] {
		let error = decode_pending(&damaged).expect_err(name);
		assert_eq!(error.kind(), ErrorKind::InvalidData, "{name}");
	}
}

#[tokio::test]
async fn a_promoted_nodes_snapshot_names_its_own_position_and_not_the_stale_marker() -> Result {
	let dir = Dir::new();
	let source = leader(&dir, "source").await?;
	let items = logged(1, 1, script(&source));
	let before = u64::try_from(items.len()).expect("length fits");

	let Started { store, db, .. } = start(&dir.layout("promoted")).await?;
	let (mut applier, _) = machine(Arc::clone(&db), &store);
	applier.apply(feed(items)).await?;

	db.engine.set_role(Role::Leader)?;
	db.get(MAP)?
		.insert(b"own", b"written by the leader");
	applier
		.apply(feed(logged(2, before.saturating_add(1), drain(&db, 0))))
		.await?;

	let snapshot = applier
		.get_snapshot_builder()
		.await
		.build_snapshot()
		.await?;

	let position = log_id(2, before.saturating_add(1));
	assert_eq!(snapshot.meta.last_log_id, Some(position));
	assert_eq!(
		db.engine
			.applied_marker()?
			.map(|marker| decode_marker(&marker).expect("marker decodes").0),
		Some(Some(log_id(1, before))),
		"the marker in the database is from before the node led"
	);

	let fresh = start(&dir.layout("fresh")).await?;
	let staged = stage(&fresh.store, &snapshot.snapshot).await;
	let (mut receiver, _) = machine(fresh.db, &fresh.store);
	receiver
		.install_snapshot(&snapshot.meta, staged)
		.await?;

	assert_eq!(
		receiver.applied_state().await?.0,
		Some(position),
		"the receiver is where the snapshot says, not where the stale marker says"
	);
	assert_eq!(rows(receiver.database().expect("database")).await?, rows(&db).await?);

	Ok(())
}

#[tokio::test]
async fn a_replacement_whose_snapshot_is_gone_or_damaged_leaves_the_database_alone() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;

	seed(&dir.layout("template").database).await?;

	for case in ["gone", "truncated", "missing a file"] {
		let layout = dir.layout(case);
		copy_tree(&dir.layout("template").database, &layout.database);
		crash_install(&layout, &donor, Step::Marked).await?;

		let snapshot = layout
			.snapshots
			.join(&snapshot_dirs(&layout.snapshots)[0]);

		match case {
			| "gone" => fs::remove_dir_all(&snapshot)?,
			| "truncated" => fs::write(snapshot.join("CURRENT"), b"x")?,
			| _ => fs::remove_file(snapshot.join("CURRENT"))?,
		}

		let before = listing(&layout.database);
		let Err(error) = start(&layout).await else {
			panic!("{case}: a node started in the middle of a replacement it cannot finish");
		};

		let message = error.to_string();
		assert!(message.contains("cannot be installed"), "{case}: {message}");
		assert!(message.contains("emptied") && message.contains("restored"), "{case}: {message}");
		assert_eq!(
			listing(&layout.database),
			before,
			"{case}: the database files are as they were"
		);

		let meta = LogStore::open(&layout.raft).await?.meta();
		assert!(
			meta.replace_marker().await?.is_some(),
			"{case}: the marker stays, so nothing runs on a half replaced database"
		);

		drop(meta);

		let old = open(&layout.database).await?;
		assert_eq!(
			rows(&old).await?,
			vec![(b"same".to_vec(), b"old".to_vec()), (b"stale".to_vec(), b"old".to_vec())],
			"{case}: the old data is still there"
		);
	}

	Ok(())
}

#[tokio::test]
async fn a_snapshot_that_cannot_be_put_in_place_is_refused_before_anything_changes() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let layout = dir.layout("node");
	seed(&layout.database).await?;

	let Started { store, db, .. } = start(&layout).await?;
	let before = rows(&db).await?;
	let files = listing(&layout.database);
	let (mut applier, _) = machine(db, &store);
	let meta = &donor.snapshot.meta;

	let needed = donor
		.snapshot
		.snapshot
		.files()
		.iter()
		.filter(|file| !is_immutable(&file.name))
		.map(|file| file.len)
		.sum::<u64>();

	let untouched = async |what: &str,
	                       error: std::io::Error,
	                       kind: ErrorKind,
	                       applier: &Machine|
	       -> Result {
		assert_eq!(error.kind(), kind, "{what}: {error}");
		assert_eq!(store.inner.meta.replace_marker().await?, None, "{what}: no marker is stored");
		assert!(snapshot_dirs(&layout.snapshots).is_empty(), "{what}: nothing is adopted");
		assert_eq!(
			listing(&layout.database),
			files,
			"{what}: the database files are as they were"
		);
		assert_eq!(
			rows(applier.database().expect("database")).await?,
			before,
			"{what}: the data is as it was"
		);

		Ok(())
	};

	store.refuse_links(true);
	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let error = applier
		.install_snapshot(meta, staged)
		.await
		.expect_err("a snapshot on another filesystem is refused");

	assert!(
		error
			.to_string()
			.contains("filesystem of the database"),
		"{error}"
	);
	untouched("another filesystem", error, ErrorKind::CrossesDevices, &applier).await?;
	store.refuse_links(false);

	store.set_free_space(Some(needed.saturating_sub(1)));
	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let error = applier
		.install_snapshot(meta, staged)
		.await
		.expect_err("a snapshot without room for its copies is refused");

	assert!(error.to_string().contains("bytes free"), "{error}");
	untouched("no room", error, ErrorKind::StorageFull, &applier).await?;

	for (what, current) in [
		("no CURRENT", None),
		("a manifest the snapshot lacks", Some("MANIFEST-999999\n".to_owned())),
		("not a manifest", Some("junk\n".to_owned())),
		("a name of another kind", Some("000001.sst\n".to_owned())),
		("too long", Some(format!("MANIFEST-{}", "1".repeat(300)))),
	] {
		let staged = stage(&store, &donor.snapshot.snapshot).await;
		match current {
			| None => fs::remove_file(staged.path().join("CURRENT"))?,
			| Some(text) => fs::write(staged.path().join("CURRENT"), text)?,
		}

		let error = applier
			.install_snapshot(meta, SnapshotDir::scan(staged.path())?)
			.await
			.expect_err(what);

		assert!(error.to_string().contains("CURRENT"), "{what}: {error}");
		untouched(what, error, ErrorKind::InvalidData, &applier).await?;
	}

	store.set_free_space(Some(needed));
	let staged = stage(&store, &donor.snapshot.snapshot).await;
	applier.install_snapshot(meta, staged).await?;

	assert_eq!(
		rows(applier.database().expect("database")).await?,
		donor.rows,
		"with exactly the room it needs the snapshot is installed"
	);

	Ok(())
}

#[tokio::test]
async fn a_replacement_that_cannot_link_when_it_resumes_leaves_the_database_alone() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let layout = dir.layout("node");
	seed(&layout.database).await?;
	crash_install(&layout, &donor, Step::Marked).await?;

	let before = listing(&layout.database);
	let (log, store) = parts(&layout).await?;
	store.refuse_links(true);

	let Err(error) = store.open_database().await else {
		panic!("a replacement resumed where it cannot link");
	};

	assert_eq!(error.kind(), ErrorKind::CrossesDevices);
	assert_eq!(listing(&layout.database), before, "the database files are as they were");
	assert!(store.inner.meta.replace_marker().await?.is_some(), "the marker stays");

	store.refuse_links(false);
	let (db, recovery) = store.open_database().await?;

	assert_eq!(recovery, Recovery::Resumed);
	assert_eq!(rows(&db).await?, donor.rows);
	drop(log);

	Ok(())
}
