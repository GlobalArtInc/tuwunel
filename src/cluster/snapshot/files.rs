//! The blocking filesystem side of the snapshot directory.

use std::{
	fs::{self, File},
	io::{self, Write},
	path::{Path, PathBuf},
};

use super::{Info, decode_info, encode_info};

/// Name of the metadata file inside a snapshot directory.
pub(super) const META: &str = "META";

/// Name of the directory a checkpoint is taken into.
pub(super) const BUILDING: &str = "building";

/// Name of the directory a received snapshot is staged in.
pub(super) const STAGING: &str = "staging";

const PREFIX: &str = "snapshot-";

pub(super) fn snapshot_name(number: u64) -> String { format!("{PREFIX}{number:020}") }

/// The number in the name of a snapshot directory; names that are not exactly
/// what [`snapshot_name`] writes are not snapshots.
pub(super) fn snapshot_number(name: &str) -> Option<u64> {
	let number: u64 = name.strip_prefix(PREFIX)?.parse().ok()?;

	(snapshot_name(number) == name).then_some(number)
}

/// The complete snapshots under `root`, oldest first, as number and name.
///
/// A numbered directory without its metadata file is not complete and is left
/// out.
pub(super) fn complete(root: &Path) -> io::Result<Vec<(u64, String)>> {
	let entries = match fs::read_dir(root) {
		| Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
		| entries => entries?,
	};

	let mut found = Vec::new();
	for entry in entries {
		let entry = entry?;
		let Ok(name) = entry.file_name().into_string() else {
			continue;
		};

		let Some(number) = snapshot_number(&name) else {
			continue;
		};

		if entry.file_type()?.is_dir() && entry.path().join(META).is_file() {
			found.push((number, name));
		}
	}

	found.sort_unstable();

	Ok(found)
}

/// The number the next snapshot gets.
pub(super) fn next_number(root: &Path) -> io::Result<u64> {
	let mut highest = 0_u64;
	let entries = match fs::read_dir(root) {
		| Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(1),
		| entries => entries?,
	};

	for entry in entries {
		let entry = entry?;
		if let Some(number) = entry
			.file_name()
			.to_str()
			.and_then(snapshot_number)
		{
			highest = highest.max(number);
		}
	}

	highest
		.checked_add(1)
		.ok_or_else(|| io::Error::other("snapshot numbers are exhausted"))
}

/// Removes a directory tree; one that is already gone is not an error.
pub(super) fn remove_tree(path: &Path) -> io::Result<()> {
	match fs::remove_dir_all(path) {
		| Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
		| result => result,
	}
}

/// The prefix a snapshot directory gets while it is being removed. The name is
/// not one of a snapshot, so nothing that looks for the current snapshot sees a
/// directory that is half gone.
const RETIRED: &str = "pruned-";

/// Renames every snapshot directory under `root` except `keep` out of the way
/// of the lookup for the current snapshot, and returns what is to be removed:
/// those directories and what an earlier crash left behind.
pub(super) fn retire(root: &Path, keep: &str) -> io::Result<Vec<PathBuf>> {
	let entries = match fs::read_dir(root) {
		| Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
		| entries => entries?.collect::<io::Result<Vec<_>>>()?,
	};

	let mut retired = Vec::new();
	for entry in entries {
		let Ok(name) = entry.file_name().into_string() else {
			continue;
		};

		if name.starts_with(RETIRED) {
			retired.push(entry.path());
		} else if name != keep && snapshot_number(&name).is_some() {
			let target = root.join(format!("{RETIRED}{name}"));

			fs::rename(entry.path(), &target)?;
			retired.push(target);
		}
	}

	sync_root(root)?;

	Ok(retired)
}

/// Removes the directories [`retire`] returned.
pub(super) fn discard(paths: &[PathBuf]) -> io::Result<()> {
	paths
		.iter()
		.try_for_each(|path| remove_tree(path))
}

/// Removes every snapshot directory under `root` except `keep`, and the
/// checkpoint in progress. The staging directory is left alone: a transfer may
/// be writing into it.
pub(super) fn prune(root: &Path, keep: &str) -> io::Result<()> {
	remove_tree(&root.join(BUILDING))?;
	discard(&retire(root, keep)?)
}

/// Removes what an interrupted run left in the snapshot directory: a transfer,
/// a checkpoint, snapshots that were being removed.
pub(super) fn clean(root: &Path) -> io::Result<()> {
	remove_tree(&root.join(STAGING))?;
	remove_tree(&root.join(BUILDING))?;

	let entries = match fs::read_dir(root) {
		| Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		| entries => entries?.collect::<io::Result<Vec<_>>>()?,
	};

	for entry in entries {
		if entry
			.file_name()
			.to_str()
			.is_some_and(|name| name.starts_with(RETIRED))
		{
			remove_tree(&entry.path())?;
		}
	}

	Ok(())
}

/// Flushes a directory's entries to disk.
pub(super) fn sync_dir(path: &Path) -> io::Result<()> { File::open(path)?.sync_all() }

/// Flushes the snapshot directory and the directory it is in, so that the
/// directory itself survives a crash the first time it is created.
pub(super) fn sync_root(root: &Path) -> io::Result<()> {
	sync_dir(root)?;

	match root.parent() {
		| Some(parent) if parent.as_os_str().is_empty() => sync_dir(Path::new(".")),
		| Some(parent) => sync_dir(parent),
		| None => Ok(()),
	}
}

/// Flushes every file of a flat directory and the directory itself to disk.
pub(super) fn seal(dir: &Path) -> io::Result<()> {
	for entry in fs::read_dir(dir)? {
		let entry = entry?;
		if !entry.file_type()?.is_dir() {
			File::open(entry.path())?.sync_all()?;
		}
	}

	sync_dir(dir)
}

/// Writes the metadata file of a snapshot directory and syncs it.
pub(super) fn write_info(dir: &Path, info: &Info) -> io::Result<()> {
	let mut file = File::create_new(dir.join(META))?;
	file.write_all(&encode_info(info)?)?;

	file.sync_all()
}

/// Reads the metadata file of a snapshot directory.
pub(super) fn read_info(dir: &Path) -> io::Result<Info> {
	decode_info(&fs::read(dir.join(META))?)
}
