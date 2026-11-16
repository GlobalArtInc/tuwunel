//! What is checked before a database is replaced by a snapshot.
//!
//! A replacement removes the files of the database and cannot go back, so
//! everything that can be found out beforehand is: that the snapshot names a
//! database, that its files can be hard linked into the database directory,
//! and that there is room for the files that are copied. A snapshot that fails
//! a check is refused before the replace marker is stored, with the database as
//! it was.

use std::{fs, io, path::Path};

use tuwunel_core::utils::sys::storage::available_space;

use super::{SnapshotDir, SnapshotStore, is_immutable, is_snapshot_file};
use crate::util::invalid;

/// Name of the file linked into the database directory to see that it can be.
const PROBE: &str = "tuwunel-link-probe";

/// The most a `CURRENT` file can hold: the name of a manifest and a newline.
const CURRENT_LIMIT: u64 = 256;

impl SnapshotStore {
	/// Checks that `snapshot` can replace the database. Blocking.
	pub(crate) fn check(&self, snapshot: &SnapshotDir) -> io::Result<()> {
		check_contents(snapshot)?;

		self.check_environment(snapshot)
	}

	/// Checks the directory of the database against the snapshot: hard links
	/// and room. Blocking.
	pub(super) fn check_environment(&self, snapshot: &SnapshotDir) -> io::Result<()> {
		let database = &self.inner.database;
		fs::create_dir_all(database)?;

		self.check_links(snapshot, database)?;

		let needed = snapshot
			.files()
			.iter()
			.filter(|file| !is_immutable(&file.name))
			.fold(0_u64, |total, file| total.saturating_add(file.len));

		let free = match self.free_space_replaced() {
			| Some(free) => free,
			| None => available_space(database).map_err(io::Error::other)?,
		};

		if free < needed {
			return Err(io::Error::new(
				io::ErrorKind::StorageFull,
				format!(
					"the database directory has {free} bytes free and the files of the snapshot \
					 that are copied take {needed}"
				),
			));
		}

		Ok(())
	}

	fn check_links(&self, snapshot: &SnapshotDir, database: &Path) -> io::Result<()> {
		let Some(file) = snapshot
			.files()
			.iter()
			.find(|file| is_immutable(&file.name))
		else {
			return Ok(());
		};

		let probe = database.join(PROBE);
		if let Err(error) = fs::remove_file(&probe)
			&& error.kind() != io::ErrorKind::NotFound
		{
			return Err(error);
		}

		let linked = if self.links_refused() {
			Err(io::ErrorKind::CrossesDevices.into())
		} else {
			fs::hard_link(snapshot.path().join(&file.name), &probe)
		};

		linked.map_err(|error| {
			io::Error::new(
				error.kind(),
				format!(
					"the files of a snapshot cannot be hard linked into the database directory: \
					 {error}; the snapshot directory must be on the filesystem of the database"
				),
			)
		})?;

		fs::remove_file(&probe)
	}
}

/// Checks that the files of the snapshot are a database: a `CURRENT` file that
/// names a manifest the snapshot holds.
pub(super) fn check_contents(snapshot: &SnapshotDir) -> io::Result<()> {
	let current = snapshot
		.files()
		.iter()
		.find(|file| file.name == "CURRENT")
		.ok_or_else(|| {
			invalid("the snapshot has no CURRENT file, so it names no database".into())
		})?;

	if current.len > CURRENT_LIMIT {
		return Err(invalid(format!(
			"the CURRENT file of the snapshot is {} bytes long",
			current.len
		)));
	}

	let text = fs::read_to_string(snapshot.path().join("CURRENT"))?;
	let manifest = text.strip_suffix('\n').unwrap_or(&text);

	if !manifest.starts_with("MANIFEST-") || !is_snapshot_file(manifest) {
		return Err(invalid(format!(
			"the CURRENT file of the snapshot names {manifest:?}, which is not a manifest"
		)));
	}

	if !snapshot
		.files()
		.iter()
		.any(|file| file.name == manifest)
	{
		return Err(invalid(format!(
			"the CURRENT file of the snapshot names {manifest}, which the snapshot does not hold"
		)));
	}

	Ok(())
}
