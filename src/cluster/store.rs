//! Durable storage of the Raft log.
//!
//! The log lives in a RocksDB of its own, apart from the database it
//! replicates. A vote, a truncation, a purge and a marker are synced to disk
//! before the call returns. Appended entries are readable when `append`
//! returns, and their callback fires only after they are synced, so what the
//! node has acknowledged to the group survives a crash. Opening an empty
//! directory creates an empty store: it holds no vote, no log and no
//! membership, and the group is formed only by an explicit request to Raft.

mod db;
mod meta;
#[cfg(test)]
pub(crate) mod probe;

use std::{
	fmt::Debug,
	io,
	ops::{Bound, RangeBounds},
	path::Path,
	sync::Arc,
};

use openraft::{
	LogState, OptionalSend, RaftLogReader,
	storage::{IOFlushed, RaftLogStorage},
	type_config::alias::{EntryOf, LogIdOf, VoteOf},
};
use tokio::task::JoinHandle;

use self::db::Db;
pub use self::meta::Meta;
use crate::{types::TypeConfig, util::blocking};

/// Raft log storage over a dedicated RocksDB directory.
///
/// Raft is its only writer: the trait methods take `&mut self` so writes are
/// issued one at a time. Each finishes before the next begins, except that the
/// sync behind an `append` runs on in the background. The syncs of successive
/// appends run in order, each followed by its callback, and a vote, a
/// truncation or a purge waits for the last one before it writes. Identity and
/// markers are reached through [`LogStore::meta`] instead.
pub struct LogStore {
	db: Arc<Db>,
	flushing: Option<JoinHandle<()>>,
}

/// Read access to the log, shared by the replication tasks.
pub struct LogReader {
	db: Arc<Db>,
}

impl LogStore {
	/// Opens the store at `path`, creating the directory and the database when
	/// they do not exist.
	///
	/// A directory that holds some other database, or a log store this version
	/// cannot read, is an error.
	pub async fn open(path: &Path) -> io::Result<Self> {
		let path = path.to_owned();
		let db = blocking(move || Db::open(&path)).await?;

		Ok(Self { db: Arc::new(db), flushing: None })
	}

	/// A handle to the identity and the markers kept beside the log.
	#[must_use]
	pub fn meta(&self) -> Meta { Meta::new(Arc::clone(&self.db)) }

	#[cfg(test)]
	pub(crate) fn probe(&self) -> Arc<probe::Probe> { self.db.probe() }

	async fn settle(&mut self) -> io::Result<()> {
		if let Some(flushing) = self.flushing.as_mut() {
			let result = flushing.await;
			self.flushing = None;
			result.map_err(io::Error::other)?;
		}

		Ok(())
	}
}

impl RaftLogStorage<TypeConfig> for LogStore {
	type LogReader = LogReader;

	async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
		let db = Arc::clone(&self.db);

		blocking(move || db.log_state()).await
	}

	async fn get_log_reader(&mut self) -> Self::LogReader {
		LogReader { db: Arc::clone(&self.db) }
	}

	async fn save_vote(&mut self, vote: &VoteOf<TypeConfig>) -> io::Result<()> {
		self.settle().await?;

		let db = Arc::clone(&self.db);
		let vote = *vote;

		blocking(move || db.save_vote(&vote)).await
	}

	async fn save_committed(&mut self, committed: Option<LogIdOf<TypeConfig>>) -> io::Result<()> {
		let db = Arc::clone(&self.db);

		blocking(move || db.save_committed(committed.as_ref())).await
	}

	async fn read_committed(&mut self) -> io::Result<Option<LogIdOf<TypeConfig>>> {
		let db = Arc::clone(&self.db);

		blocking(move || db.read_committed()).await
	}

	async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
	where
		I: IntoIterator<Item = EntryOf<TypeConfig>> + OptionalSend,
		I::IntoIter: OptionalSend,
	{
		let db = Arc::clone(&self.db);
		let entries: Vec<_> = entries.into_iter().collect();

		blocking(move || db.append(&entries)).await?;

		let db = Arc::clone(&self.db);
		let previous = self.flushing.take();
		self.flushing = Some(tokio::spawn(async move {
			let previous = match previous {
				| Some(previous) => previous.await.map_err(io::Error::other),
				| None => Ok(()),
			};

			let synced = match previous {
				| Ok(()) => blocking(move || db.sync_appended()).await,
				| Err(error) => Err(error),
			};

			callback.io_completed(synced);
		}));

		Ok(())
	}

	async fn truncate_after(
		&mut self,
		last_log_id: Option<LogIdOf<TypeConfig>>,
	) -> io::Result<()> {
		self.settle().await?;

		let db = Arc::clone(&self.db);

		blocking(move || db.truncate_after(last_log_id.as_ref())).await
	}

	async fn purge(&mut self, log_id: LogIdOf<TypeConfig>) -> io::Result<()> {
		self.settle().await?;

		let db = Arc::clone(&self.db);

		blocking(move || db.purge(&log_id)).await
	}
}

impl RaftLogReader<TypeConfig> for LogReader {
	async fn try_get_log_entries<RB>(&mut self, range: RB) -> io::Result<Vec<EntryOf<TypeConfig>>>
	where
		RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
	{
		let db = Arc::clone(&self.db);
		let start = match range.start_bound() {
			| Bound::Included(start) => *start,
			| Bound::Excluded(start) => start.saturating_add(1),
			| Bound::Unbounded => 0,
		};

		let end = match range.end_bound() {
			| Bound::Included(end) => end.checked_add(1),
			| Bound::Excluded(end) => Some(*end),
			| Bound::Unbounded => None,
		};

		blocking(move || db.entries(start, end)).await
	}

	async fn read_vote(&mut self) -> io::Result<Option<VoteOf<TypeConfig>>> {
		let db = Arc::clone(&self.db);

		blocking(move || db.read_vote()).await
	}
}
