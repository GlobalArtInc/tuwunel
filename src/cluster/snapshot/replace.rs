//! Replacing the database of a node by a snapshot, in place and crash-safe.
//!
//! RocksDB keeps its files in the database directory, and on a node that began
//! as a single server that directory is the root of the volume, next to the
//! Raft log, the snapshots and the backups. The directory cannot be swapped as
//! a whole, so the replacement works on the files of the database by name:
//!
//! 1. the snapshot to install is checked, and made a complete snapshot
//!    directory of the node. The check refuses, with the database as it was, a
//!    snapshot without a `CURRENT` file that names a manifest it holds, one
//!    whose files cannot be hard linked into the database directory, and one
//!    the database directory has no room for. Then a *replace marker* naming
//!    the snapshot and the position it stands for is stored in the log store,
//!    synced to disk;
//! 2. the engine is closed;
//! 3. every file of the database on the whitelist is removed: `<n>.sst`,
//!    `<n>.blob`, `<n>.log`, `MANIFEST-<n>`, `OPTIONS-<n>`, `CURRENT`,
//!    `IDENTITY`, `LOG`, `LOG.old.*`, and the `archive` directory. Nothing else
//!    in the directory is touched;
//! 4. the files of the snapshot are put in their place: the immutable ones
//!    (`*.sst`, `*.blob`) as hard links, which RocksDB never rewrites, the others
//!    as copies, which it does;
//! 5. the directory is synced, the database is opened again as a follower, the
//!    position of the snapshot is written to it and synced, and the replace
//!    marker is removed.
//!
//! Every step can be repeated from the start, and the snapshot directory is
//! never changed, so a crash at any point is finished the same way: at startup
//! [`SnapshotStore::open_database`] finds the replace marker and runs the steps
//! again, after checking the snapshot and the directory once more before the
//! first file goes. A replacement is never rolled back, because the old files
//! are gone after step 3, and the node does not use its database in between. A
//! snapshot that has gone missing or damaged since stops the start with the
//! database untouched and an error that tells the operator to restore the
//! snapshot or to empty the volume.
//!
//! # The leader marker
//!
//! A leader applies writes to its database before the cluster confirms them, so
//! a leader that stops without a clean hand-over leaves a database that may hold
//! writes no other node has. The runtime stores a *leader marker* in the log
//! store when the node becomes leader and removes it only after a clean
//! hand-over. A node that starts and finds the marker restores its own newest
//! snapshot, which holds committed writes only, and replays the log from there.
//! The log and the vote stay as they are. A node with the marker and no snapshot
//! cannot be made clean and refuses to start.
//!
//! The marker is the format version, 1, and the origin of the leader process as
//! eight big-endian bytes. The origin is for diagnosis only; the marker's presence
//! is what counts, and a marker of an unknown version fails the start rather
//! than being taken for absent.

#[cfg(test)]
mod tests;

use std::{
	fs::{self, File, FileType},
	io,
	path::Path,
	sync::Arc,
};

use serde::{Deserialize, Serialize};
use tuwunel_database::{Database, Role};

use super::{
	SnapshotDir, SnapshotStore,
	check::check_contents,
	files::{clean, prune, read_info, snapshot_number, sync_dir},
	is_immutable, is_snapshot_file,
};
use crate::util::{blocking, invalid};

const PENDING_VERSION: u8 = 1;
const LEADER_VERSION: u8 = 1;

/// What the node found at startup, besides a database in order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Recovery {
	/// Nothing was pending and the database was opened as it was.
	Clean,

	/// A replacement that was interrupted was finished.
	Resumed,

	/// The database may have held writes the cluster never confirmed, and was
	/// replaced by the newest local snapshot.
	Restored,
}

/// A step of a replacement, for the test hook.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Step {
	/// The replace marker is stored.
	Marked,

	/// The n-th file of the database was removed.
	Removed(usize),

	/// Every file of the database is removed.
	Cleaned,

	/// The n-th file of the snapshot was put in place.
	Placed(usize),

	/// Every file of the snapshot is in place.
	Linked,

	/// The database directory is synced.
	Synced,

	/// The database is open again.
	Opened,

	/// The position of the snapshot is in the database and synced.
	Positioned,

	/// The replace marker is removed.
	Cleared,
}

/// The replace marker.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Pending {
	pub(crate) snapshot: String,
	pub(crate) marker: Vec<u8>,
}

/// What closing a database came to.
pub(crate) enum Closed {
	/// The engine is closed and its files are free.
	Done,

	/// The database has other owners and is still open.
	Shared(Arc<Database>),
}

/// Encodes the leader marker for [`crate::Meta::set_leader_marker`].
///
/// `origin` is [`tuwunel_database::Engine::origin`] of the leader process.
#[must_use]
pub fn encode_leader_marker(origin: u64) -> Vec<u8> {
	let mut marker = vec![LEADER_VERSION];
	marker.extend_from_slice(&origin.to_be_bytes());

	marker
}

/// Decodes a leader marker, returning the origin of the process that set it.
///
/// A marker of an unknown version, or of the wrong length, is an error of kind
/// `InvalidData`.
pub fn decode_leader_marker(marker: &[u8]) -> io::Result<u64> {
	let Some((&version, body)) = marker.split_first() else {
		return Err(invalid("the leader marker is empty".to_owned()));
	};

	if version != LEADER_VERSION {
		return Err(invalid(format!(
			"leader marker version {version} is not supported, expected {LEADER_VERSION}"
		)));
	}

	let origin: [u8; 8] = body.try_into().map_err(|_| {
		invalid(format!("the leader marker has {} bytes, expected 8", body.len()))
	})?;

	Ok(u64::from_be_bytes(origin))
}

pub(crate) fn encode_pending(pending: &Pending) -> io::Result<Vec<u8>> {
	postcard::to_extend(pending, vec![PENDING_VERSION])
		.map_err(|error| invalid(format!("replace marker is not encodable: {error}")))
}

pub(crate) fn decode_pending(bytes: &[u8]) -> io::Result<Pending> {
	let Some((&version, body)) = bytes.split_first() else {
		return Err(invalid("the replace marker is empty".to_owned()));
	};

	if version != PENDING_VERSION {
		return Err(invalid(format!(
			"replace marker version {version} is not supported, expected {PENDING_VERSION}"
		)));
	}

	let (pending, rest) = postcard::take_from_bytes::<Pending>(body)
		.map_err(|error| invalid(format!("replace marker is damaged: {error}")))?;

	if !rest.is_empty() || snapshot_number(&pending.snapshot).is_none() {
		return Err(invalid("replace marker is damaged".to_owned()));
	}

	Ok(pending)
}

impl SnapshotStore {
	/// Brings the database of the node into order and opens it, as a follower.
	///
	/// Called once at startup, before anything else touches the database and
	/// before Raft starts. Leftovers of an interrupted snapshot transfer or
	/// checkpoint are removed. A replacement that was interrupted is finished. A
	/// leader marker is answered by restoring the newest local snapshot, and a
	/// node that has the marker and no snapshot fails with an error. Otherwise
	/// the database is opened as it is.
	///
	/// The database comes back in the follower role, which is where Raft starts
	/// the state machine and applies the committed part of the log from the
	/// position in the database.
	pub async fn open_database(&self) -> io::Result<(Arc<Database>, Recovery)> {
		let store = self.clone();
		blocking(move || clean(store.snapshots())).await?;

		let meta = &self.inner.meta;
		if let Some(bytes) = meta.replace_marker().await? {
			let db = self.finish(decode_pending(&bytes)?).await?;
			meta.clear_leader_marker().await?;

			return Ok((db, Recovery::Resumed));
		}

		if let Some(bytes) = meta.leader_marker().await? {
			decode_leader_marker(&bytes)?;

			let store = self.clone();
			let latest = blocking(move || store.latest())
				.await?
				.ok_or_else(|| {
					invalid(
						"the database may hold unconfirmed writes and the node has no snapshot \
						 to restore it from"
							.to_owned(),
					)
				})?;

			let pending = Pending {
				snapshot: latest.name,
				marker: latest.marker,
			};

			meta.set_replace_marker(&encode_pending(&pending)?)
				.await?;
			meta.clear_leader_marker().await?;

			let db = self.finish(pending).await?;

			return Ok((db, Recovery::Restored));
		}

		let db = (self.inner.open)().await?;
		db.engine
			.set_role(Role::Follower)
			.map_err(io::Error::other)?;

		Ok((db, Recovery::Clean))
	}

	/// Stores the replace marker.
	pub(crate) async fn begin(&self, pending: &Pending) -> io::Result<()> {
		self.inner
			.meta
			.set_replace_marker(&encode_pending(pending)?)
			.await?;

		self.fire(Step::Marked);

		Ok(())
	}

	/// Closes `db` if nothing else owns it.
	///
	/// Blocking work is done off the async workers. A database somebody else
	/// still holds is handed back untouched; a database that closed while
	/// something still referenced its engine is an error, because the files stay
	/// locked.
	pub(crate) async fn close(db: Arc<Database>) -> io::Result<Closed> {
		blocking(move || match Arc::try_unwrap(db) {
			| Err(db) => Ok(Closed::Shared(db)),
			| Ok(database) => {
				let engine = Arc::downgrade(&database.engine);
				drop(database);

				if engine.strong_count() == 0 {
					Ok(Closed::Done)
				} else {
					Err(io::Error::new(
						io::ErrorKind::ResourceBusy,
						"the database engine is still referenced after the database was closed",
					))
				}
			},
		})
		.await
	}

	/// Carries out the replacement the marker `pending` stands for and removes
	/// the marker. The database must be closed. Returns it, open, in the follower
	/// role, at the position of the snapshot.
	pub(crate) async fn finish(&self, pending: Pending) -> io::Result<Arc<Database>> {
		let source = self.inner.snapshots.join(&pending.snapshot);
		let store = self.clone();
		blocking(move || store.swap_files(&source)).await?;

		let db = (self.inner.open)().await?;
		self.fire(Step::Opened);

		db.engine
			.set_role(Role::Follower)
			.map_err(io::Error::other)?;

		let positioned = Arc::clone(&db);
		let marker = pending.marker.clone();
		blocking(move || {
			positioned
				.engine
				.apply_replicated(&[], &marker)
				.map_err(io::Error::other)?;

			positioned.engine.sync().map_err(io::Error::other)
		})
		.await?;

		self.fire(Step::Positioned);

		self.inner.meta.clear_replace_marker().await?;
		self.fire(Step::Cleared);

		let store = self.clone();
		let keep = pending.snapshot;
		blocking(move || prune(store.snapshots(), &keep)).await?;

		Ok(db)
	}

	/// Reads the snapshot a replace marker names and checks it. A snapshot that
	/// is gone or damaged cannot be put back by this node, so the error says
	/// what an operator can do.
	fn inspect(source: &Path) -> io::Result<SnapshotDir> {
		let found = read_info(source)
			.and_then(|info| SnapshotDir::new(source.to_owned(), info.files))
			.and_then(|snapshot| {
				snapshot
					.verify()
					.and_then(|()| check_contents(&snapshot))
					.map(|()| snapshot)
			});

		found.map_err(|error| {
			io::Error::new(
				error.kind(),
				format!(
					"snapshot {} named by the replace marker cannot be installed: {error}; the \
					 database of this node was not touched, but it cannot start until that \
					 directory is restored under the snapshot path, or the volume of the node \
					 is emptied so that it joins the cluster as a new node",
					source.display()
				),
			)
		})
	}

	fn swap_files(&self, source: &Path) -> io::Result<()> {
		let snapshot = Self::inspect(source)?;
		self.check_environment(&snapshot)?;

		let database = &self.inner.database;

		let mut victims = Vec::new();
		for entry in fs::read_dir(database)? {
			let entry = entry?;
			let file_type = entry.file_type()?;
			if entry
				.file_name()
				.to_str()
				.is_some_and(|name| is_database_file(name, file_type))
			{
				victims.push((entry.path(), file_type));
			}
		}

		for (index, (path, file_type)) in victims.iter().enumerate() {
			if file_type.is_dir() {
				fs::remove_dir_all(path)?;
			} else {
				fs::remove_file(path)?;
			}

			self.fire(Step::Removed(index.saturating_add(1)));
		}

		self.fire(Step::Cleaned);
		sync_dir(database)?;

		for (index, file) in snapshot.files().iter().enumerate() {
			let from = source.join(&file.name);
			let to = database.join(&file.name);

			if is_immutable(&file.name) {
				fs::hard_link(&from, &to)?;
			} else {
				fs::copy(&from, &to)?;
				File::open(&to)?.sync_all()?;
			}

			self.fire(Step::Placed(index.saturating_add(1)));
		}

		self.fire(Step::Linked);
		sync_dir(database)?;
		self.fire(Step::Synced);

		Ok(())
	}
}

/// Reports whether an entry of the database directory is one the replacement
/// removes.
fn is_database_file(name: &str, file_type: FileType) -> bool {
	if file_type.is_dir() {
		return name == "archive";
	}

	is_snapshot_file(name) || name == "LOG" || name.starts_with("LOG.old.")
}
