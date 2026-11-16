//! The blocking side of the log store: RocksDB access, key and value codecs.

use std::{fs, io, ops::Range, path::Path, sync::Arc};

use openraft::{
	LogState,
	entry::RaftEntry,
	type_config::alias::{EntryOf, LogIdOf, VoteOf},
};
use rocksdb::{
	BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode,
	MultiThreaded, Options, ReadOptions, WriteBatch,
};
use serde::{Serialize, de::DeserializeOwned};

#[cfg(test)]
use super::probe::Probe;
use crate::types::TypeConfig;

type Inner = DBWithThreadMode<MultiThreaded>;

const LOGS: &str = "logs";
const META: &str = "meta";

const FORMAT: &[u8] = b"format";
const INCARNATION: &[u8] = b"incarnation";
const VOTE: &[u8] = b"vote";
const COMMITTED: &[u8] = b"committed";
const PURGED: &[u8] = b"purged";

pub(super) const LEADER_MARKER: &[u8] = b"leader_marker";
pub(super) const REPLACE_MARKER: &[u8] = b"replace_marker";

const FORMAT_VERSION: &[u8] = &[1];

const CHUNK: usize = 16_384;

/// The opened log database.
///
/// Log entries live in the `logs` column family under their big-endian index.
/// Everything else lives in `meta` under fixed names. Appended entries and the
/// committed position are written without a sync; every other write is synced
/// to disk before the call returns.
pub(super) struct Db {
	inner: Inner,
	incarnation: u128,
	#[cfg(test)]
	probe: Arc<Probe>,
}

impl Db {
	/// Opens the database at `path`, creating it when the directory holds none.
	///
	/// A new database gets its incarnation here, in one write with the format
	/// version. An existing one keeps what it has: a database that has data but
	/// no format version, or a version without an incarnation, is refused
	/// rather than given a new identity.
	pub(super) fn open(path: &Path) -> io::Result<Self> {
		fs::create_dir_all(path)?;

		let mut options = Options::default();
		options.create_if_missing(true);
		options.create_missing_column_families(true);

		let families =
			[LOGS, META].map(|name| ColumnFamilyDescriptor::new(name, Options::default()));

		let inner = Inner::open_cf_descriptors(&options, path, families).map_err(rocks)?;
		let incarnation = identify(&inner)?;
		let db = Self {
			inner,
			incarnation,
			#[cfg(test)]
			probe: Arc::default(),
		};

		db.finish_purge()?;

		Ok(db)
	}

	pub(super) fn incarnation(&self) -> u128 { self.incarnation }

	#[cfg(test)]
	pub(super) fn probe(&self) -> Arc<Probe> { Arc::clone(&self.probe) }

	pub(super) fn read_vote(&self) -> io::Result<Option<VoteOf<TypeConfig>>> {
		self.get_meta(VOTE)?
			.map(|bytes| decode(&bytes))
			.transpose()
	}

	pub(super) fn save_vote(&self, vote: &VoteOf<TypeConfig>) -> io::Result<()> {
		self.put_meta(VOTE, &encode(vote)?)
	}

	pub(super) fn read_committed(&self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
		self.get_meta(COMMITTED)?
			.map(|bytes| decode(&bytes))
			.transpose()
	}

	/// Writes the committed position without a sync.
	///
	/// It only bounds how far a restart may fall back, and the log it refers to
	/// is already synced. The next sync of the write-ahead log makes it durable
	/// as well.
	pub(super) fn save_committed(
		&self,
		committed: Option<&LogIdOf<TypeConfig>>,
	) -> io::Result<()> {
		let meta = self.family(META)?;
		let mut batch = WriteBatch::default();
		match committed {
			| Some(id) => batch.put_cf(&meta, COMMITTED, encode(id)?),
			| None => batch.delete_cf(&meta, COMMITTED),
		}

		self.write(&batch)
	}

	pub(super) fn log_state(&self) -> io::Result<LogState<TypeConfig>> {
		let purged = self.purged()?;
		let last = match (self.last_entry()?, purged) {
			| (Some(entry), Some(purged)) if entry.index() <= purged.index => Some(purged),
			| (Some(entry), _) => Some(entry.log_id()),
			| (None, purged) => purged,
		};

		Ok(LogState {
			last_purged_log_id: purged,
			last_log_id: last,
		})
	}

	/// Reads the entries with an index in `start..end`, no end meaning without
	/// limit.
	///
	/// A range may begin or end beyond what the log holds, but not have a hole
	/// anywhere else. Past the purge position the log is contiguous up to its
	/// last entry, so a missing entry there, between two stored entries or at
	/// either edge of the range, is corruption and an error. Before a purge
	/// the log may begin at any index, and nothing is expected of the start.
	pub(super) fn entries(
		&self,
		start: u64,
		end: Option<u64>,
	) -> io::Result<Vec<EntryOf<TypeConfig>>> {
		if end.is_some_and(|end| end <= start) {
			return Ok(Vec::new());
		}

		let logs = self.family(LOGS)?;
		let meta = self.family(META)?;
		let snapshot = self.inner.snapshot();

		let purged: Option<LogIdOf<TypeConfig>> = snapshot
			.get_cf(&meta, PURGED)
			.map_err(rocks)?
			.map(|bytes| decode(&bytes))
			.transpose()?;

		let last = snapshot
			.iterator_cf(&logs, IteratorMode::End)
			.next()
			.map(|item| {
				item.map_err(rocks)
					.and_then(|(key, _)| index_of(&key))
			})
			.transpose()?;

		let mut options = ReadOptions::default();
		if let Some(end) = end {
			options.set_iterate_upper_bound(index_key(end));
		}

		let start_key = index_key(start);
		let mode = IteratorMode::From(&start_key, Direction::Forward);
		let mut entries: Vec<EntryOf<TypeConfig>> = Vec::new();
		for item in snapshot.iterator_cf_opt(&logs, options, mode) {
			let (key, value) = item.map_err(rocks)?;
			let entry = decode_entry(&key, &value)?;
			if let Some(previous) = entries.last()
				&& previous.index().checked_add(1) != Some(entry.index())
			{
				return Err(corrupt(format!(
					"log has a gap between index {} and {}",
					previous.index(),
					entry.index()
				)));
			}

			entries.push(entry);
		}

		let floor = purged.map(|purged| start.max(purged.index.saturating_add(1)));
		let ceiling = last.map(|last| end.map_or(last, |end| last.min(end.saturating_sub(1))));
		match (entries.first(), entries.last()) {
			| (Some(first), Some(newest)) => {
				if let Some(floor) = floor
					&& first.index() > floor
				{
					return Err(missing(floor, first.index()));
				}

				if let Some(ceiling) = ceiling
					&& newest.index() < ceiling
				{
					return Err(missing(newest.index(), ceiling));
				}
			},
			| _ =>
				if let (Some(floor), Some(ceiling)) = (floor, ceiling)
					&& floor <= ceiling
				{
					return Err(missing(floor, ceiling));
				},
		}

		Ok(entries)
	}

	/// Appends entries behind the end of the log, without a sync.
	///
	/// The entries must be consecutive and continue the log exactly: Raft
	/// truncates a conflicting tail before it appends. A log that is empty
	/// after a purge continues the purge position; a log that was never purged
	/// may begin at any index. The entries are readable when this returns and
	/// durable after [`Db::sync_appended`].
	pub(super) fn append(&self, entries: &[EntryOf<TypeConfig>]) -> io::Result<()> {
		let Some(first) = entries.first() else {
			return Ok(());
		};

		for pair in entries.windows(2) {
			if pair[0].index().checked_add(1) != Some(pair[1].index()) {
				return Err(invalid(format!(
					"appended entries are not consecutive: {} then {}",
					pair[0].index(),
					pair[1].index()
				)));
			}
		}

		let behind = match (self.last_index()?, self.purged()?) {
			| (Some(last), _) => Some(last),
			| (None, Some(purged)) => Some(purged.index),
			| (None, None) => None,
		};

		if let Some(behind) = behind
			&& behind.checked_add(1) != Some(first.index())
		{
			return Err(invalid(format!(
				"append at index {} does not continue the log that ends at {behind}",
				first.index()
			)));
		}

		let logs = self.family(LOGS)?;
		let mut batch = WriteBatch::default();
		for entry in entries {
			batch.put_cf(&logs, index_key(entry.index()), encode(entry)?);
		}

		self.write(&batch)
	}

	/// Syncs the write-ahead log, making every entry appended so far durable.
	pub(super) fn sync_appended(&self) -> io::Result<()> {
		#[cfg(test)]
		self.probe.pass()?;

		self.sync()
	}

	/// Removes every entry behind `after`, or all of them for none.
	///
	/// Entries go from the end backward, so an interrupted truncation leaves a
	/// log without a gap. Truncating into the purged part is refused.
	pub(super) fn truncate_after(&self, after: Option<&LogIdOf<TypeConfig>>) -> io::Result<()> {
		let keep = after.map(|id| id.index);
		if let Some(purged) = self.purged()?
			&& keep.is_none_or(|keep| keep < purged.index)
		{
			return Err(invalid(format!(
				"truncation after {keep:?} reaches into the log purged up to {}",
				purged.index
			)));
		}

		let logs = self.family(LOGS)?;
		loop {
			let mut keys: Vec<Box<[u8]>> = Vec::new();
			for item in self.inner.iterator_cf(&logs, IteratorMode::End) {
				let (key, _) = item.map_err(rocks)?;
				let index = index_of(&key)?;
				if keys.len() == CHUNK || keep.is_some_and(|keep| index <= keep) {
					break;
				}

				keys.push(key);
			}

			if keys.is_empty() {
				return Ok(());
			}

			let mut batch = WriteBatch::default();
			for key in keys {
				batch.delete_cf(&logs, key);
			}

			self.commit(&batch)?;
		}
	}

	/// Removes every entry up to and including `through`.
	///
	/// The purge position is stored with the first batch of removals, so the
	/// stored position is never behind what was removed. Positions at or behind
	/// the stored one change nothing.
	pub(super) fn purge(&self, through: &LogIdOf<TypeConfig>) -> io::Result<()> {
		let from = match self.purged()? {
			| Some(purged) if purged.index >= through.index => return Ok(()),
			| Some(purged) => purged.index.saturating_add(1),
			| None => 0,
		};

		self.remove_range(from..through.index.saturating_add(1), Some(encode(through)?))
	}

	pub(super) fn get_meta(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
		self.inner
			.get_cf(&self.family(META)?, key)
			.map_err(rocks)
	}

	pub(super) fn put_meta(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
		let mut batch = WriteBatch::default();
		batch.put_cf(&self.family(META)?, key, value);

		self.commit(&batch)
	}

	pub(super) fn delete_meta(&self, key: &[u8]) -> io::Result<()> {
		let mut batch = WriteBatch::default();
		batch.delete_cf(&self.family(META)?, key);

		self.commit(&batch)
	}

	fn purged(&self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
		self.get_meta(PURGED)?
			.map(|bytes| decode(&bytes))
			.transpose()
	}

	fn last_index(&self) -> io::Result<Option<u64>> {
		let logs = self.family(LOGS)?;

		self.inner
			.iterator_cf(&logs, IteratorMode::End)
			.next()
			.map(|item| {
				item.map_err(rocks)
					.and_then(|(key, _)| index_of(&key))
			})
			.transpose()
	}

	fn last_entry(&self) -> io::Result<Option<EntryOf<TypeConfig>>> {
		let logs = self.family(LOGS)?;

		self.inner
			.iterator_cf(&logs, IteratorMode::End)
			.next()
			.map(|item| {
				let (key, value) = item.map_err(rocks)?;
				decode_entry(&key, &value)
			})
			.transpose()
	}

	/// Completes a purge a crash interrupted: entries the stored purge
	/// position already covers must not outlive it.
	fn finish_purge(&self) -> io::Result<()> {
		let Some(purged) = self.purged()? else {
			return Ok(());
		};

		let logs = self.family(LOGS)?;
		let first = self
			.inner
			.iterator_cf(&logs, IteratorMode::Start)
			.next()
			.map(|item| {
				item.map_err(rocks)
					.and_then(|(key, _)| index_of(&key))
			})
			.transpose()?;

		if first.is_some_and(|first| first <= purged.index) {
			self.remove_range(0..purged.index.saturating_add(1), None)?;
		}

		Ok(())
	}

	/// Removes the entries with an index in `range`, in batches. The optional
	/// purge position goes into the first batch.
	fn remove_range(&self, range: Range<u64>, mut position: Option<Vec<u8>>) -> io::Result<()> {
		let logs = self.family(LOGS)?;
		let meta = self.family(META)?;
		let mut cursor = range.start;

		loop {
			let mut options = ReadOptions::default();
			options.set_iterate_upper_bound(index_key(range.end));

			let cursor_key = index_key(cursor);
			let mode = IteratorMode::From(&cursor_key, Direction::Forward);
			let keys: Vec<Box<[u8]>> = self
				.inner
				.iterator_cf_opt(&logs, options, mode)
				.take(CHUNK)
				.map(|item| item.map(|(key, _)| key).map_err(rocks))
				.collect::<io::Result<_>>()?;

			let mut batch = WriteBatch::default();
			if let Some(position) = position.take() {
				batch.put_cf(&meta, PURGED, position);
			}

			for key in &keys {
				batch.delete_cf(&logs, key);
			}

			if !batch.is_empty() {
				self.commit(&batch)?;
			}

			let Some(last) = keys.last().filter(|_| keys.len() == CHUNK) else {
				return Ok(());
			};

			cursor = index_of(last)?.saturating_add(1);
		}
	}

	fn commit(&self, batch: &WriteBatch) -> io::Result<()> {
		self.write(batch)?;
		self.sync()
	}

	fn write(&self, batch: &WriteBatch) -> io::Result<()> {
		self.inner.write(batch).map_err(rocks)
	}

	fn sync(&self) -> io::Result<()> {
		#[cfg(test)]
		self.probe.count();

		self.inner.flush_wal(true).map_err(rocks)
	}

	fn family(&self, name: &str) -> io::Result<Arc<BoundColumnFamily<'_>>> {
		family(&self.inner, name)
	}
}

fn family<'a>(inner: &'a Inner, name: &str) -> io::Result<Arc<BoundColumnFamily<'a>>> {
	inner
		.cf_handle(name)
		.ok_or_else(|| io::Error::other(format!("column family {name} is missing")))
}

/// Writes the batch and syncs the write-ahead log before returning.
fn commit(inner: &Inner, batch: &WriteBatch) -> io::Result<()> {
	inner.write(batch).map_err(rocks)?;
	inner.flush_wal(true).map_err(rocks)
}

fn identify(inner: &Inner) -> io::Result<u128> {
	let meta = family(inner, META)?;
	let Some(format) = inner.get_cf(&meta, FORMAT).map_err(rocks)? else {
		if !is_empty(inner)? {
			return Err(corrupt("directory holds a database that is not a log store".to_owned()));
		}

		let incarnation = random_incarnation();
		let mut batch = WriteBatch::default();
		batch.put_cf(&meta, FORMAT, FORMAT_VERSION);
		batch.put_cf(&meta, INCARNATION, incarnation.to_be_bytes());
		commit(inner, &batch)?;

		return Ok(incarnation);
	};

	if format != FORMAT_VERSION {
		return Err(corrupt(format!("log store format {format:?} is not supported")));
	}

	let stored = inner
		.get_cf(&meta, INCARNATION)
		.map_err(rocks)?
		.ok_or_else(|| corrupt("log store has no incarnation".to_owned()))?;

	let bytes: [u8; 16] = stored
		.as_slice()
		.try_into()
		.map_err(|_| corrupt(format!("incarnation has {} bytes, expected 16", stored.len())))?;

	match u128::from_be_bytes(bytes) {
		| 0 => Err(corrupt("log store has a zero incarnation".to_owned())),
		| incarnation => Ok(incarnation),
	}
}

fn is_empty(inner: &Inner) -> io::Result<bool> {
	for name in ["default", LOGS, META] {
		let family = family(inner, name)?;
		if inner
			.iterator_cf(&family, IteratorMode::Start)
			.next()
			.transpose()
			.map_err(rocks)?
			.is_some()
		{
			return Ok(false);
		}
	}

	Ok(true)
}

fn random_incarnation() -> u128 {
	loop {
		let mut bytes = [0_u8; 16];
		rand::fill(&mut bytes);

		let incarnation = u128::from_be_bytes(bytes);
		if incarnation != 0 {
			return incarnation;
		}
	}
}

fn index_key(index: u64) -> [u8; 8] { index.to_be_bytes() }

fn index_of(key: &[u8]) -> io::Result<u64> {
	let bytes: [u8; 8] = key
		.try_into()
		.map_err(|_| corrupt(format!("log key has {} bytes, expected 8", key.len())))?;

	Ok(u64::from_be_bytes(bytes))
}

fn encode<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
	postcard::to_allocvec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
	postcard::from_bytes(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn decode_entry(key: &[u8], value: &[u8]) -> io::Result<EntryOf<TypeConfig>> {
	let index = index_of(key)?;
	let entry: EntryOf<TypeConfig> = decode(value)?;
	if entry.index() != index {
		return Err(corrupt(format!(
			"log entry stored under index {index} claims index {}",
			entry.index()
		)));
	}

	Ok(entry)
}

fn rocks(error: rocksdb::Error) -> io::Error { io::Error::other(error) }

fn invalid(message: String) -> io::Error { io::Error::new(io::ErrorKind::InvalidInput, message) }

fn missing(from: u64, to: u64) -> io::Error {
	corrupt(format!("log is missing entries within the indexes {from} to {to}"))
}

fn corrupt(message: String) -> io::Error { io::Error::new(io::ErrorKind::InvalidData, message) }
