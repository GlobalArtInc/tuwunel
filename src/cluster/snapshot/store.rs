//! The handle to the snapshot directory of a node.

use std::{
	fs, io,
	path::{Path, PathBuf},
	sync::Arc,
};

use futures::future::BoxFuture;
use openraft::{storage::Snapshot, type_config::alias::SnapshotOf};
use tuwunel_database::Database;

use super::{
	Info, SnapshotDir,
	files::{
		BUILDING, META, STAGING, complete, next_number, prune, read_info, remove_tree, seal,
		snapshot_name, snapshot_number, sync_root, write_info,
	},
	meta_of,
};
use crate::{store::Meta, types::TypeConfig, util::blocking};

/// Opens the database of the node, once more.
///
/// Called whenever the database was closed to be replaced. It must open the
/// database at the directory the store was given, with the same configuration
/// as the first time.
pub type Reopen = Arc<dyn Fn() -> BoxFuture<'static, io::Result<Arc<Database>>> + Send + Sync>;

/// The snapshots of one node, and the means to replace its database by one.
///
/// Cloning gives another handle to the same directories. The state machine, the
/// transfer of snapshots and the startup of the node share one.
#[derive(Clone)]
pub struct SnapshotStore {
	pub(super) inner: Arc<Inner>,
}

pub(super) struct Inner {
	pub(super) database: PathBuf,
	pub(super) snapshots: PathBuf,
	pub(super) meta: Meta,
	pub(super) open: Reopen,
	pub(super) building: tokio::sync::Mutex<()>,
	#[cfg(test)]
	pub(super) faults: std::sync::Mutex<super::faults::Faults>,
}

/// How often a snapshot that vanished while it was read is looked for again.
const LOOKUPS: usize = 3;

/// The newest complete snapshot on disk.
pub(super) struct Latest {
	pub(super) name: String,
	pub(super) dir: SnapshotDir,
	pub(super) marker: Vec<u8>,
}

impl SnapshotStore {
	/// Creates the handle.
	///
	/// `database` is the directory of the database, `snapshots` the directory
	/// snapshots are kept and staged in, on the same filesystem. `meta` is where
	/// the node keeps its replace marker and leader marker. `open` opens the
	/// database again after it was closed to be replaced.
	#[must_use]
	pub fn new(database: PathBuf, snapshots: PathBuf, meta: Meta, open: Reopen) -> Self {
		Self {
			inner: Arc::new(Inner {
				database,
				snapshots,
				meta,
				open,
				building: tokio::sync::Mutex::new(()),
				#[cfg(test)]
				faults: std::sync::Mutex::default(),
			}),
		}
	}

	/// The directory a received snapshot is written to before it is installed.
	///
	/// [`SnapshotStore::prepare_staging`] empties it.
	#[must_use]
	pub fn staging(&self) -> PathBuf { self.inner.snapshots.join(STAGING) }

	/// Empties the staging directory, creating it when it is missing, and
	/// returns its path.
	///
	/// A snapshot transfer calls it before writing the first file, so that
	/// nothing an earlier, broken transfer left behind is installed.
	pub async fn prepare_staging(&self) -> io::Result<PathBuf> {
		let path = self.staging();
		let target = path.clone();

		blocking(move || {
			remove_tree(&target)?;
			fs::create_dir_all(&target)?;

			Ok(())
		})
		.await?;

		Ok(path)
	}

	/// The current snapshot: the newest complete one, with its files checked
	/// against their listed lengths. `None` when the node has none.
	pub async fn current(&self) -> io::Result<Option<SnapshotOf<TypeConfig, SnapshotDir>>> {
		let store = self.clone();

		blocking(move || {
			store
				.latest()?
				.map(|latest| {
					Ok(Snapshot {
						meta: meta_of(&latest.marker)?,
						snapshot: latest.dir,
					})
				})
				.transpose()
		})
		.await
	}

	/// The newest complete snapshot. A build that finishes meanwhile retires the
	/// snapshot this one picked, so one that vanished or is found damaged is
	/// looked for again before it is an error.
	pub(super) fn latest(&self) -> io::Result<Option<Latest>> {
		for _ in 0..LOOKUPS {
			let found = self.newest();
			if found.as_ref().is_err_and(|error| {
				matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::InvalidData)
			}) {
				continue;
			}

			return found;
		}

		self.newest()
	}

	fn newest(&self) -> io::Result<Option<Latest>> {
		let root = &self.inner.snapshots;
		let Some((_, name)) = complete(root)?.pop() else {
			return Ok(None);
		};

		let path = root.join(&name);
		let info = read_info(&path)?;
		self.glimpse();

		let dir = SnapshotDir::new(path, info.files)?;
		dir.verify()?;

		Ok(Some(Latest { name, dir, marker: info.marker }))
	}

	pub(crate) fn is_building(&self) -> bool { self.inner.building.try_lock().is_err() }

	pub(super) fn snapshots(&self) -> &Path { &self.inner.snapshots }

	pub(super) fn checkpoint(&self, db: &Database) -> io::Result<(u64, u64)> {
		let path = self.inner.snapshots.join(BUILDING);

		remove_tree(&path)?;
		fs::create_dir_all(&self.inner.snapshots)?;

		db.engine
			.checkpoint_bracketed(&path)
			.map_err(io::Error::other)
	}

	pub(super) fn discard_build(&self) -> io::Result<()> {
		remove_tree(&self.inner.snapshots.join(BUILDING))
	}

	pub(super) fn finish_build(&self, marker: &[u8]) -> io::Result<SnapshotDir> {
		let root = &self.inner.snapshots;
		let building = root.join(BUILDING);
		let listed = SnapshotDir::scan(&building)?;

		let info = Info {
			marker: marker.to_vec(),
			files: listed.files().to_vec(),
		};

		write_info(&building, &info)?;
		seal(&building)?;

		let name = snapshot_name(next_number(root)?);
		fs::rename(&building, root.join(&name))?;
		sync_root(root)?;
		prune(root, &name)?;

		SnapshotDir::new(root.join(&name), info.files)
	}

	/// Makes `data` a complete snapshot directory of this node and returns its
	/// name, checking it first.
	///
	/// A snapshot that is already one is only compared with `marker`. One in the
	/// staging directory gets its metadata, is flushed to disk and takes the next
	/// number. A directory from anywhere else is refused.
	pub(crate) fn adopt(&self, data: &SnapshotDir, marker: &[u8]) -> io::Result<String> {
		let root = &self.inner.snapshots;
		data.verify()?;
		self.check(data)?;

		if data.path() == self.staging() {
			if data.path().join(META).exists() {
				return Err(crate::util::invalid(
					"the staging directory holds a META file, which only this node writes"
						.to_owned(),
				));
			}

			let info = Info {
				marker: marker.to_vec(),
				files: data.files().to_vec(),
			};

			write_info(data.path(), &info)?;
			seal(data.path())?;

			let name = snapshot_name(next_number(root)?);
			fs::rename(data.path(), root.join(&name))?;
			sync_root(root)?;

			return Ok(name);
		}

		let name = data
			.path()
			.file_name()
			.and_then(|name| name.to_str())
			.filter(|name| snapshot_number(name).is_some())
			.filter(|_| data.path().parent() == Some(root.as_path()))
			.ok_or_else(|| {
				io::Error::new(
					io::ErrorKind::InvalidInput,
					format!(
						"snapshot directory {:?} is neither the staging directory nor a \
						 snapshot of this node",
						data.path().display()
					),
				)
			})?;

		let info = read_info(data.path())?;
		if info.marker != marker || info.files != data.files() {
			return Err(crate::util::invalid(format!(
				"snapshot {name} does not match the position it is installed at"
			)));
		}

		Ok(name.to_owned())
	}
}
