//! Snapshots of the replicated database, kept on disk.
//!
//! A snapshot is a physical RocksDB checkpoint taken while the database stays
//! in use, plus a small metadata file. It is the unit a node restores from, in
//! three situations: a node that has fallen behind the purged log receives one
//! from the leader, a node whose database may hold writes the cluster never
//! confirmed restores its own, and a node that starts with a database older
//! than its newest snapshot restores that.
//!
//! # Layout
//!
//! Everything lives under one directory, `snapshot_path`:
//!
//! - `snapshot-<number>`: a complete snapshot. The directory holds the RocksDB
//!   files, flat, and a `META` file written last. A directory without `META`
//!   is not a snapshot. Numbers only grow; the highest one is the current
//!   snapshot and the only one kept.
//! - `building`: the checkpoint being taken. It becomes a numbered directory
//!   in one rename once it is sealed.
//! - `staging`: where a snapshot received from the leader is written, file by
//!   file, before it is installed.
//! - `pruned-snapshot-<number>`: a snapshot that is being removed. A snapshot
//!   is renamed first, in one step, and only then removed, so that a lookup of
//!   the current snapshot never sees a directory that is half gone. Startup
//!   removes what a crash left.
//!
//! The directory must be on the filesystem of the database, because installing
//! hard links the immutable files instead of copying them. Installing checks
//! that before it changes anything, as it checks that the snapshot has a
//! `CURRENT` file naming a manifest it holds and that the database directory has
//! room for the files that are copied.
//!
//! # Metadata
//!
//! `META` is the snapshot format version, 1, followed by the postcard encoding
//! of the position marker the state machine keeps in the database
//! ([`encode_marker`](crate::encode_marker)) and the list of the files with
//! their sizes. The marker names the last log entry the snapshot covers and the
//! membership in force there. A snapshot never claims more than it holds, but
//! it may hold more than it claims: the entries after the named one are
//! replayed over it, and replaying a committed entry is idempotent.
//!
//! # The leader's snapshot
//!
//! A leader writes to its database before the cluster confirms. The checkpoint
//! may therefore hold writes that are not committed yet, and a snapshot is
//! handed out only once everything up to the database sequence read after the
//! checkpoint is committed. The position claimed is the one the state machine
//! had applied before the checkpoint began, which is committed by definition.
//!
//! # The first snapshot of a cluster
//!
//! A node that joins a cluster with an existing database, as the legacy node of
//! a single-node installation does, keeps all its data outside the log. Right
//! after `initialize` the state machine applies the first log entries, and a
//! snapshot built then, before the node takes leadership, holds the whole
//! database at that position; purging the log up to it makes a node with an
//! empty database receive the snapshot before it replays anything. The same
//! build works after promotion, once an entry the leader captured is committed.
//!
//! The runtime should build it before it stores the leader marker: a node that
//! has the marker and no snapshot refuses to start, and until the first
//! snapshot exists the database of a legacy node has no other clean copy.
//!
//! # Trust
//!
//! A received snapshot comes from an authenticated peer, so the contents of its
//! files are trusted. The names and sizes are not: they are checked against the
//! file name whitelist and the listed lengths before anything is installed.

mod build;
mod check;
mod faults;
mod files;
pub(crate) mod replace;
mod store;
#[cfg(test)]
mod tests;

use std::{
	collections::BTreeSet,
	fs, io,
	path::{Path, PathBuf},
};

use openraft::{storage::SnapshotMeta, type_config::alias::SnapshotMetaOf};
use serde::{Deserialize, Serialize};

pub use self::{
	build::Snapshots,
	replace::{Recovery, decode_leader_marker, encode_leader_marker},
	store::{Reopen, SnapshotStore},
};
use crate::{machine::decode_marker, types::TypeConfig, util::invalid};

/// Format version of the `META` file, its first byte.
const FORMAT: u8 = 1;

/// One file of a snapshot.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SnapshotFile {
	/// Name of the file inside the snapshot directory.
	pub name: String,

	/// Length of the file in bytes.
	pub len: u64,
}

/// A snapshot on disk: a directory and the files in it.
///
/// This is what Raft hands around as the snapshot data. The files stay on disk;
/// the value only describes them, so a snapshot of any size costs nothing to
/// pass around. Whoever transmits a snapshot reads the files named here from
/// [`SnapshotDir::path`], and whoever receives one writes them to the staging
/// directory of the [`SnapshotStore`] and builds the description with
/// [`SnapshotDir::new`] or [`SnapshotDir::scan`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotDir {
	path: PathBuf,
	files: Vec<SnapshotFile>,
}

/// Contents of the `META` file.
#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Info {
	pub(crate) marker: Vec<u8>,
	pub(crate) files: Vec<SnapshotFile>,
}

impl SnapshotDir {
	/// Describes the snapshot in `path` as holding `files`.
	///
	/// Every name must be one a snapshot may hold and appear once. Nothing is
	/// read from the disk; [`SnapshotDir::verify`] compares the description with
	/// what is there.
	pub fn new(path: PathBuf, mut files: Vec<SnapshotFile>) -> io::Result<Self> {
		files.sort_unstable_by(|a, b| a.name.cmp(&b.name));

		let mut seen = BTreeSet::new();
		for file in &files {
			if !is_snapshot_file(&file.name) {
				return Err(rejected(format!(
					"{:?} is not a file a snapshot may hold",
					file.name
				)));
			}

			if !seen.insert(file.name.as_str()) {
				return Err(rejected(format!("snapshot lists {:?} twice", file.name)));
			}
		}

		if files.is_empty() {
			return Err(rejected("a snapshot holds no files".to_owned()));
		}

		Ok(Self { path, files })
	}

	/// Describes whatever is in the directory `path`, reading it from the disk.
	///
	/// Blocking. The directory must hold only regular files with names a
	/// snapshot may hold. A metadata file is refused: the node writes that
	/// itself, and one that arrives with the files was not written by it.
	pub fn scan(path: &Path) -> io::Result<Self> { Self::list(path, false) }

	fn list(path: &Path, meta_allowed: bool) -> io::Result<Self> {
		let mut files = Vec::new();
		for entry in fs::read_dir(path)? {
			let entry = entry?;
			let name = entry.file_name().into_string().map_err(|name| {
				invalid(format!("snapshot file name {} is not valid unicode", name.display()))
			})?;

			if name == files::META {
				if meta_allowed {
					continue;
				}

				return Err(invalid(format!(
					"{name} is the metadata file of a snapshot, which a transfer does not write"
				)));
			}

			let kind = entry.file_type()?;
			if kind.is_dir() || kind.is_symlink() {
				return Err(invalid(format!("snapshot entry {name:?} is not a regular file")));
			}

			files.push(SnapshotFile { name, len: entry.metadata()?.len() });
		}

		Self::new(path.to_owned(), files).map_err(|error| invalid(error.to_string()))
	}

	/// Checks that the directory holds exactly the files listed, with their
	/// lengths, and no other file apart from the metadata file.
	///
	/// Blocking.
	pub fn verify(&self) -> io::Result<()> {
		let found = Self::list(&self.path, true)?;
		if found.files != self.files {
			return Err(invalid(format!(
				"snapshot directory {} does not match its description: {} files listed, {} found",
				self.path.display(),
				self.files.len(),
				found.files.len()
			)));
		}

		Ok(())
	}

	/// The directory the files are in.
	#[must_use]
	pub fn path(&self) -> &Path { &self.path }

	/// The files of the snapshot, ordered by name.
	#[must_use]
	pub fn files(&self) -> &[SnapshotFile] { &self.files }

	/// The total length of the files in bytes.
	#[must_use]
	pub fn len(&self) -> u64 {
		self.files
			.iter()
			.fold(0_u64, |total, file| total.saturating_add(file.len))
	}

	/// Reports whether the snapshot holds no bytes.
	#[must_use]
	pub fn is_empty(&self) -> bool { self.len() == 0 }
}

/// Reports whether `name` is a file the RocksDB checkpoint of a snapshot may
/// hold.
pub(crate) fn is_snapshot_file(name: &str) -> bool {
	let numbered = |suffix: &str| {
		name.strip_suffix(suffix)
			.is_some_and(|stem| !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit()))
	};

	let versioned = |prefix: &str| {
		name.strip_prefix(prefix)
			.is_some_and(|stem| !stem.is_empty() && stem.bytes().all(|b| b.is_ascii_digit()))
	};

	numbered(".sst")
		|| numbered(".blob")
		|| numbered(".log")
		|| versioned("MANIFEST-")
		|| versioned("OPTIONS-")
		|| name == "CURRENT"
		|| name == "IDENTITY"
}

/// Reports whether a snapshot file is immutable and so can be shared with the
/// database by a hard link.
pub(crate) fn is_immutable(name: &str) -> bool {
	Path::new(name)
		.extension()
		.is_some_and(|extension| extension == "sst" || extension == "blob")
}

pub(crate) fn encode_info(info: &Info) -> io::Result<Vec<u8>> {
	postcard::to_extend(info, vec![FORMAT])
		.map_err(|error| invalid(format!("snapshot metadata is not encodable: {error}")))
}

pub(crate) fn decode_info(bytes: &[u8]) -> io::Result<Info> {
	let Some((&format, body)) = bytes.split_first() else {
		return Err(invalid("snapshot metadata is empty".to_owned()));
	};

	if format != FORMAT {
		return Err(invalid(format!(
			"snapshot metadata format {format} is not supported, expected {FORMAT}"
		)));
	}

	let (info, rest) = postcard::take_from_bytes::<Info>(body)
		.map_err(|error| invalid(format!("snapshot metadata is damaged: {error}")))?;

	if !rest.is_empty() {
		return Err(invalid(format!("snapshot metadata has {} bytes after its end", rest.len())));
	}

	Ok(info)
}

/// The Raft metadata a position marker stands for. The marker of a snapshot
/// always names a log entry.
pub(crate) fn meta_of(marker: &[u8]) -> io::Result<SnapshotMetaOf<TypeConfig>> {
	let (last_log_id, last_membership) = decode_marker(marker)?;
	if last_log_id.is_none() {
		return Err(invalid("snapshot position names no log entry".to_owned()));
	}

	Ok(SnapshotMeta { last_log_id, last_membership })
}

fn rejected(message: String) -> io::Error { io::Error::new(io::ErrorKind::InvalidInput, message) }
