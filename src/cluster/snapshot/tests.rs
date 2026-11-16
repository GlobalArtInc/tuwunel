use std::{
	collections::BTreeSet,
	fs,
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::{Duration, Instant},
};

use openraft::{
	Raft,
	storage::{RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine, Snapshot, StorageHelper},
	type_config::alias::VoteOf,
};
use tokio::time::sleep;
use tuwunel_core::Result;
use tuwunel_database::Role;

use super::{
	SnapshotDir, SnapshotFile,
	files::{META, discard, retire, snapshot_name},
};
use crate::{
	Recovery, TypeConfig,
	testing::{
		Dir, MAP, Offline, Pump, Started, append, apply_and_stop, blank, config, copy_tree,
		donor, drain, feed, leader, log_id, logged, machine, node, normal, open, parts, put_keys,
		rows, script, snapshot_dirs, stage, start, within,
	},
};

fn file(name: &str) -> SnapshotFile { SnapshotFile { name: name.to_owned(), len: 1 } }

#[test]
fn only_database_files_with_plain_names_make_a_snapshot() {
	let path = std::path::PathBuf::from("/nowhere");
	let named = |names: &[&str]| {
		SnapshotDir::new(path.clone(), names.iter().map(|name| file(name)).collect())
	};

	named(&[
		"000012.sst",
		"000013.blob",
		"000014.log",
		"MANIFEST-000005",
		"OPTIONS-000007",
		"CURRENT",
		"IDENTITY",
	])
	.expect("the files of a checkpoint are accepted");

	for strange in [
		"../escape.sst",
		"a/b.sst",
		"sst",
		".sst",
		"12x.sst",
		"MANIFEST-",
		"MANIFEST-1.dbtmp",
		"LOCK",
		"LOG",
		"META",
		"raft",
		"CURRENT.tmp",
		"",
	] {
		named(&["CURRENT", strange]).expect_err(strange);
	}

	named(&["CURRENT", "CURRENT"]).expect_err("a name is listed once");
	named(&[]).expect_err("a snapshot holds files");
}

#[tokio::test]
async fn a_follower_snapshot_installs_on_an_empty_node() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let last = u64::try_from(donor.items.len()).expect("length fits");

	assert_eq!(
		donor.snapshot.meta.last_log_id,
		Some(log_id(1, last)),
		"it names the last entry"
	);
	for prefix in ["CURRENT", "MANIFEST-", "OPTIONS-"] {
		assert!(
			donor
				.snapshot
				.snapshot
				.files()
				.iter()
				.any(|file| file.name.starts_with(prefix)),
			"{prefix} is in the snapshot"
		);
	}

	let target = start(&dir.layout("target")).await?;
	let staged = stage(&target.store, &donor.snapshot.snapshot).await;
	let meta = target.store.clone();
	let layout = dir.layout("target");

	let (mut applier, _) = machine(target.db, &target.store);
	applier
		.install_snapshot(&donor.snapshot.meta, staged)
		.await?;

	assert_eq!(rows(applier.database().expect("database")).await?, donor.rows);
	assert_eq!(
		applier.applied_state().await?.0,
		Some(log_id(1, last)),
		"the position of the snapshot is the position of the node"
	);

	let current = applier
		.get_current_snapshot()
		.await?
		.expect("the installed snapshot is current");

	assert_eq!(current.meta, donor.snapshot.meta);
	assert_eq!(current.snapshot.files(), donor.snapshot.snapshot.files());
	assert!(!layout.snapshots.join("staging").exists(), "the staging directory was consumed");
	assert_eq!(snapshot_dirs(&layout.snapshots).len(), 1, "only the installed snapshot is kept");
	assert!(meta.current().await?.is_some());

	Ok(())
}

#[tokio::test]
async fn a_second_snapshot_replaces_the_first() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("node");
	let source = leader(&dir, "source").await?;
	let items = logged(1, 1, script(&source));
	let (first, second) = items.split_at(6);

	let Started { store, db, .. } = start(&layout).await?;
	let (mut applier, _) = machine(db, &store);

	applier.apply(feed(first.to_vec())).await?;
	let one = applier
		.get_snapshot_builder()
		.await
		.build_snapshot()
		.await?;

	applier.apply(feed(second.to_vec())).await?;
	let two = applier
		.get_snapshot_builder()
		.await
		.build_snapshot()
		.await?;

	let total = u64::try_from(items.len()).expect("length fits");

	assert_eq!(one.meta.last_log_id, Some(log_id(1, 6)));
	assert_eq!(two.meta.last_log_id, Some(log_id(1, total)));
	assert_eq!(snapshot_dirs(&layout.snapshots).len(), 1, "one snapshot is kept");
	assert!(two.snapshot.path().is_dir(), "and it is the newest");
	assert!(!one.snapshot.path().exists(), "the older one is gone");

	let current = applier
		.get_current_snapshot()
		.await?
		.expect("a current snapshot");

	assert_eq!(current.meta, two.meta);
	assert!(!layout.snapshots.join("building").exists());

	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_snapshot_waits_for_confirmation_and_replays_to_the_final_state() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("leader");
	let Started { store, db, .. } = start(&layout).await?;
	db.engine.set_role(Role::Leader)?;

	let (mut applier, committed) = machine(Arc::clone(&db), &store);
	let bounds = Arc::new(Mutex::new(None));
	let slot = Arc::clone(&bounds);
	store.set_bounds_hook(Some(Arc::new(move |before, after| {
		*slot.lock().expect("bounds locked") = Some((before, after));
	})));

	let mut pump = Pump::new();
	let stop = Arc::new(AtomicBool::new(false));
	#[expect(clippy::needless_collect)]
	let writers: Vec<_> = (0..8_u8)
		.map(|id| {
			let map = Arc::clone(db.get(MAP).expect("map exists"));
			let stop = Arc::clone(&stop);

			thread::spawn(move || {
				let mut written = 0_u32;
				while !stop.load(Ordering::Relaxed) {
					map.insert(format!("w{id}-{written:08}").as_bytes(), written.to_be_bytes());
					written = written.saturating_add(1);
					if written.is_multiple_of(16) {
						thread::yield_now();
					}
				}

				written
			})
		})
		.collect();

	let mut applied: BTreeSet<Vec<u8>> = BTreeSet::new();
	let mut absorbed = 0_usize;

	let until = Instant::now() + Duration::from_millis(200);
	while Instant::now() < until {
		pump.run(&mut applier, &db).await;
		applied.extend(put_keys(&pump.log[absorbed..]));
		absorbed = pump.log.len();
		sleep(Duration::from_millis(5)).await;
	}

	let confirmed = pump.last();
	assert!(confirmed > 0, "some writes were confirmed");

	let mut builder = applier
		.try_create_snapshot_builder(false)
		.await
		.expect("a planned builder");

	let build = tokio::spawn(async move { builder.build_snapshot().await });

	let (before, after) = within(async {
		loop {
			let found = *bounds.lock().expect("bounds locked");
			if let Some(bounds) = found {
				return bounds;
			}

			sleep(Duration::from_millis(1)).await;
		}
	})
	.await;

	assert!(after > before, "the writers wrote while the checkpoint was taken");
	assert!(
		applier
			.try_create_snapshot_builder(false)
			.await
			.is_none(),
		"a planned snapshot is deferred while another is being built"
	);

	while *committed.borrow() < before {
		pump.step(&mut applier, &db, 1).await;
		applied.extend(put_keys(&pump.log[absorbed..]));
		absorbed = pump.log.len();
	}

	assert!(*committed.borrow() < after, "what the checkpoint may hold is not confirmed yet");
	sleep(Duration::from_millis(300)).await;
	assert!(
		!build.is_finished(),
		"confirming what came before the checkpoint is not enough: the checkpoint may hold more"
	);

	let began = Instant::now();
	while !build.is_finished() {
		assert!(began.elapsed() < Duration::from_mins(1), "the snapshot is built in time");

		pump.step(&mut applier, &db, 4).await;
		applied.extend(put_keys(&pump.log[absorbed..]));
		absorbed = pump.log.len();
		sleep(Duration::from_millis(1)).await;
	}

	let snapshot = build.await.expect("the build task finishes")?;

	stop.store(true, Ordering::Relaxed);
	let written: u32 = writers
		.into_iter()
		.map(|writer| writer.join().expect("writer finishes"))
		.sum();

	while pump.run(&mut applier, &db).await > 0 {}

	assert_eq!(
		snapshot.meta.last_log_id,
		Some(log_id(1, confirmed)),
		"it claims the position that was applied when it began"
	);

	let target = start(&dir.layout("target")).await?;
	let staged = stage(&target.store, &snapshot.snapshot).await;
	let (mut receiver, _) = machine(target.db, &target.store);
	receiver
		.install_snapshot(&snapshot.meta, staged)
		.await?;

	let partial = rows(receiver.database().expect("database")).await?;
	let expected = rows(&db).await?;

	assert_eq!(
		u32::try_from(expected.len()).expect("fits"),
		written,
		"every write is in the final state"
	);
	assert!(partial.len() < expected.len(), "the snapshot is not the final state");

	let unconfirmed = partial
		.iter()
		.filter(|(key, _)| !applied.contains(key))
		.count();

	assert_eq!(
		unconfirmed, 0,
		"every row of the snapshot was applied by the time the snapshot was done"
	);

	let first = usize::try_from(confirmed).expect("index fits");
	receiver
		.apply(feed(pump.log[first..].to_vec()))
		.await?;

	assert_eq!(
		rows(receiver.database().expect("database")).await?,
		expected,
		"the entries after its position complete it"
	);

	Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_snapshot_is_discarded_when_the_state_machine_stops_first() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("leader");
	let Started { store, db, .. } = start(&layout).await?;
	db.engine.set_role(Role::Leader)?;

	let (mut applier, _) = machine(Arc::clone(&db), &store);
	applier.apply(feed(vec![blank(1, 1)])).await?;
	db.get(MAP)?.insert(b"unconfirmed", b"1");

	let mut builder = applier
		.try_create_snapshot_builder(true)
		.await
		.expect("a forced builder");

	let build = tokio::spawn(async move { builder.build_snapshot().await });
	sleep(Duration::from_millis(400)).await;
	assert!(!build.is_finished(), "an unconfirmed write holds the snapshot back");

	drop(applier);

	within(build)
		.await
		.expect("the build task finishes")
		.map(drop)
		.expect_err("the build fails");

	assert!(snapshot_dirs(&layout.snapshots).is_empty(), "no snapshot is kept");
	assert!(!layout.snapshots.join("building").exists(), "the checkpoint is thrown away");

	Ok(())
}

#[tokio::test]
async fn the_initial_state_builds_a_snapshot_on_a_follower_over_a_hole() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("node");
	let source = leader(&dir, "source").await?;
	let items = logged(1, 1, script(&source));
	let last = u64::try_from(items.len()).expect("length fits");

	apply_and_stop(&layout, items).await?;

	let Started { mut log, store, db, recovery } = start(&layout).await?;
	assert_eq!(recovery, Recovery::Clean);

	append(&mut log, (1..=3).map(|index| blank(1, index))).await;

	let (mut applier, _) = machine(db, &store);
	let state = within(
		StorageHelper::new(&mut log, &mut applier)
			.with_id(1)
			.get_initial_state(),
	)
	.await
	.expect("the initial state is read over the hole");

	assert_eq!(state.snapshot_meta.last_log_id, Some(log_id(1, last)), "a snapshot covers it");
	assert_eq!(
		log.get_log_state().await?.last_purged_log_id,
		Some(log_id(1, last)),
		"the log was purged past its own end"
	);

	let current = applier
		.get_current_snapshot()
		.await?
		.expect("the snapshot is current");

	assert_eq!(current.meta.last_log_id, Some(log_id(1, last)));
	assert!(!current.snapshot.is_empty());

	Ok(())
}

#[tokio::test]
async fn the_initial_state_restores_a_snapshot_the_database_is_behind() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let last = u64::try_from(donor.items.len()).expect("length fits");
	let layout = dir.layout("node");

	apply_and_stop(&layout, donor.items[..4].to_vec()).await?;

	let name = donor
		.snapshot
		.snapshot
		.path()
		.file_name()
		.expect("a snapshot directory has a name");

	copy_tree(donor.snapshot.snapshot.path(), &layout.snapshots.join(name));

	let Started { mut log, store, db, .. } = start(&layout).await?;
	let (mut applier, _) = machine(db, &store);
	let state = within(
		StorageHelper::new(&mut log, &mut applier)
			.with_id(1)
			.get_initial_state(),
	)
	.await
	.expect("the initial state is read");

	assert_eq!(state.snapshot_meta.last_log_id, Some(log_id(1, last)));
	assert_eq!(applier.applied_state().await?.0, Some(log_id(1, last)), "the node moved up");
	assert_eq!(
		rows(applier.database().expect("database")).await?,
		donor.rows,
		"its database is the snapshot's"
	);
	assert_eq!(store.inner.meta.replace_marker().await?, None, "the replacement is complete");

	Ok(())
}

#[tokio::test]
async fn raft_starts_over_the_machine_and_accepts_a_snapshot() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let last = u64::try_from(donor.items.len()).expect("length fits");

	let Started { log, store, db, .. } = start(&dir.layout("node")).await?;
	let staged = stage(&store, &donor.snapshot.snapshot).await;
	let (applier, _) = machine(db, &store);

	let raft = within(Raft::new(2, config(), Offline, log, applier))
		.await
		.expect("raft starts");

	let vote = VoteOf::<TypeConfig>::new_committed(1, 1);
	within(raft.install_full_snapshot(vote, Snapshot {
		meta: donor.snapshot.meta.clone(),
		snapshot: staged,
	}))
	.await
	.expect("the snapshot is installed");

	let wait = raft.wait(Some(Duration::from_secs(20)));
	wait.snapshot(log_id(1, last), "the snapshot is current")
		.await
		.expect("the snapshot becomes current");
	wait.purged(Some(log_id(1, last)), "the log is purged to it")
		.await
		.expect("the log is purged past its own end");

	let installed = raft
		.with_state_machine(|applier| {
			Box::pin(async move {
				rows(applier.database().expect("database"))
					.await
					.expect("rows read")
			})
		})
		.await
		.expect("the state machine answers");

	assert_eq!(installed, donor.rows);
	raft.shutdown().await.expect("raft stops");

	Ok(())
}

#[tokio::test]
async fn the_first_snapshot_of_a_database_that_joins_a_cluster_serves_a_new_node() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("legacy");

	let legacy = open(&layout.database).await?;
	for index in 0..200_u16 {
		legacy
			.get(MAP)?
			.insert(format!("room{index:04}").as_bytes(), index.to_be_bytes());
	}

	let expected = rows(&legacy).await?;
	drop(legacy);

	let Started { log, store, db, recovery } = start(&layout).await?;
	assert_eq!(recovery, Recovery::Clean);

	let (applier, _) = machine(db, &store);
	let raft = within(Raft::new(1, config(), Offline, log, applier))
		.await
		.expect("raft starts");

	raft.initialize(std::collections::BTreeMap::from([(1, node(1))]))
		.await
		.expect("the cluster is formed");

	let wait = raft.wait(Some(Duration::from_secs(20)));
	wait.current_leader(1, "the single voter leads")
		.await
		.expect("it leads");
	wait.applied_index_at_least(Some(1), "the first entries are applied")
		.await
		.expect("they are applied");

	raft.trigger()
		.snapshot()
		.await
		.expect("a snapshot is triggered");

	let metrics = wait
		.metrics(|metrics| metrics.snapshot.is_some(), "a snapshot is built")
		.await
		.expect("a snapshot is built");

	let at = metrics.snapshot.expect("a snapshot position");
	raft.trigger()
		.purge_log(at.index)
		.await
		.expect("the purge is triggered");
	wait.purged(Some(at), "the log is purged to the snapshot")
		.await
		.expect("the log is purged");

	let current = raft
		.get_snapshot()
		.await
		.expect("the snapshot is read")
		.expect("there is a snapshot");

	let fresh = start(&dir.layout("fresh")).await?;
	let staged = stage(&fresh.store, &current.snapshot).await;
	let (mut receiver, _) = machine(fresh.db, &fresh.store);
	receiver
		.install_snapshot(&current.meta, staged)
		.await?;

	assert_eq!(
		rows(receiver.database().expect("database")).await?,
		expected,
		"a node with an empty database gets the whole database from the snapshot"
	);

	Ok(())
}

#[tokio::test]
async fn a_database_that_takes_leadership_snapshots_without_being_deposed() -> Result {
	let dir = Dir::new();
	let layout = dir.layout("legacy");

	let legacy = open(&layout.database).await?;
	for index in 0..20_u8 {
		legacy.get(MAP)?.insert(&[b'r', index], [index]);
	}

	drop(legacy);

	let Started { store, db, .. } = start(&layout).await?;
	db.engine.set_role(Role::Leader)?;

	let (mut applier, _) = machine(Arc::clone(&db), &store);
	db.get(MAP)?.insert(b"promoted", b"1");

	let captured = drain(&db, 0);
	assert_eq!(captured.len(), 1, "the write was captured");

	applier
		.apply(feed(vec![blank(1, 1), normal(1, 2, captured[0].clone())]))
		.await?;

	let snapshot = within(
		applier
			.try_create_snapshot_builder(false)
			.await
			.expect("a planned builder")
			.build_snapshot(),
	)
	.await?;

	assert!(!db.engine.is_deposed(), "building does not write to a leader");
	assert_eq!(snapshot.meta.last_log_id, Some(log_id(1, 2)));

	let fresh = start(&dir.layout("fresh")).await?;
	let staged = stage(&fresh.store, &snapshot.snapshot).await;
	let (mut receiver, _) = machine(fresh.db, &fresh.store);
	receiver
		.install_snapshot(&snapshot.meta, staged)
		.await?;

	assert_eq!(
		rows(receiver.database().expect("database")).await?,
		rows(&db).await?,
		"the snapshot holds the old data and the first write of the leader"
	);

	Ok(())
}

#[tokio::test]
async fn a_snapshot_is_renamed_out_of_sight_before_it_is_removed() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let root = dir.layout("donor").snapshots;

	let older = snapshot_name(1);
	let newer = snapshot_name(2);
	copy_tree(&root.join(&older), &root.join(&newer));
	fs::create_dir_all(root.join("pruned-snapshot-left-by-a-crash"))?;

	let retired = retire(&root, &newer)?;

	assert_eq!(
		snapshot_dirs(&root),
		std::slice::from_ref(&newer),
		"only the snapshot that stays is in sight"
	);
	assert_eq!(retired.len(), 2, "the old snapshot and what a crash left are to be removed");
	assert!(retired.iter().all(|path| path.is_dir()), "nothing is removed yet");
	assert!(donor.snapshot.snapshot.path().ends_with(&older));

	discard(&retired)?;

	assert!(retired.iter().all(|path| !path.exists()), "and then they are gone");
	assert!(root.join(&newer).join(META).is_file(), "the snapshot that stays is whole");

	Ok(())
}

#[tokio::test]
async fn the_current_snapshot_is_looked_for_again_when_a_build_replaces_it() -> Result {
	let dir = Dir::new();
	let donor = donor(&dir, "donor").await?;
	let layout = dir.layout("donor");
	let (_log, store) = parts(&layout).await?;
	let root = layout.snapshots.clone();
	let fired = Arc::new(AtomicBool::new(false));

	for (picked, removal) in [(1_u64, "vanishes"), (2, "is half removed")] {
		fired.store(false, Ordering::Relaxed);

		let (hooked, fired) = (root.clone(), Arc::clone(&fired));
		store.set_lookup_hook(Some(Arc::new(move || {
			if fired.swap(true, Ordering::Relaxed) {
				return;
			}

			let old = hooked.join(snapshot_name(picked));
			let next = hooked.join(snapshot_name(picked.saturating_add(1)));
			copy_tree(&old, &next);

			if picked == 1 {
				fs::remove_dir_all(&old).expect("the old snapshot vanishes");
			} else {
				fs::remove_file(old.join("CURRENT")).expect("the old snapshot loses a file");
			}
		})));

		let latest = store
			.current()
			.await?
			.expect("a snapshot is found");

		assert_eq!(
			latest.snapshot.path(),
			root.join(snapshot_name(picked.saturating_add(1))),
			"the snapshot that {removal} is not the answer"
		);
		assert_eq!(latest.meta, donor.snapshot.meta);
	}

	Ok(())
}
