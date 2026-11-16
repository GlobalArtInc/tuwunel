//! Physical replication of database writes between cluster nodes.
//!
//! A cluster leader applies each write to its own RocksDB first and captures
//! it, in application order, as a replication [`Entry`]. A follower applies
//! committed entries as one atomic write batch together with a marker naming
//! the position it reached. A standalone engine does neither: its writes go
//! straight to RocksDB.
//!
//! Every logical write reaches RocksDB through exactly three calls, one in
//! `Map::insert`, one in `Map::remove` and one in `Txn`. Each of them passes
//! through the hooks in this module, which dispatch on the engine [`Role`].
//! On a leader the hook holds the write mutex across the RocksDB call and the
//! queue push, so queue order equals application order, and it checks that the
//! database sequence number still equals the one the hook expects. A write
//! which bypassed the hook shows up as a mismatch; the engine is then deposed
//! for good: it refuses writes, stops handing out captured entries and the
//! commit barrier fails with [`BarrierError::NotLeader`].
//!
//! Only puts and deletes with absolute values are replicable. The record
//! parser rejects every other batch tag, so replaying an entry any number of
//! times, in order, converges to the same state.

#[cfg(test)]
mod tests;

use std::{
	collections::{BTreeMap, VecDeque},
	error::Error as StdError,
	fmt::{self, Display},
	hash::{BuildHasher, RandomState},
	ops::Range,
	path::Path,
	sync::{
		Arc, Mutex, OnceLock,
		atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
	},
};

use futures::future::{BoxFuture, Either, ready};
use rocksdb::{AsColumnFamilyRef, BoundColumnFamily, Options, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tuwunel_core::{Err, Result, err, error, utils::MutexExt};

use crate::{
	Database, Engine,
	util::{map_err, or_else},
};

/// Size of the batch representation header: a fixed64 sequence and a fixed32
/// count.
const BATCH_HEADER: usize = 12;

/// Worst-case per-record overhead: a type tag and three varint32s.
const RECORD_OVERHEAD: usize = 16;

/// Column family holding the position marker of the last applied entries.
const MARKER_CF: &str = "raft_applied";

/// Key of the marker inside [`MARKER_CF`].
const MARKER_KEY: &[u8] = b"applied";

/// The column family every RocksDB database has; batch records without a
/// column family tag address it.
const DEFAULT_CF: &str = "default";

/// Replication role of an engine.
///
/// The role is fixed by the process that embeds the database and only moves
/// forward: a standalone engine may become a follower or a leader, a follower
/// may become a leader, and a leader never changes role again.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Role {
	/// No cluster. Writes go straight to RocksDB with nothing captured.
	#[default]
	Standalone,

	/// Cluster follower. Local writes through `Map` and `Txn` panic; the
	/// database is changed only by [`Engine::apply_replicated`].
	Follower,

	/// Cluster leader. Every write is applied locally and captured, in order,
	/// for [`Engine::drain_captured`].
	Leader,
}

/// Failure of the commit barrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BarrierError {
	/// This node is not, or is no longer, the leader. The request was not
	/// confirmed and can safely be retried elsewhere.
	NotLeader,

	/// Confirmation did not arrive in time. The outcome of the writes is
	/// unknown.
	Timeout,
}

/// Waits until writes are confirmed by the cluster.
///
/// Implemented by the cluster runtime and installed with
/// [`Engine::set_barrier`]. Without one, [`Database::barrier`] passes at once.
pub trait Barrier: Send + Sync {
	/// Resolves once every write up to and including database sequence `seq`
	/// is committed by a quorum, or fails with the reason it cannot be.
	fn wait(&self, seq: u64) -> BoxFuture<'_, Result<(), BarrierError>>;
}

/// One replicated unit: the writes of one or more consecutive captures.
///
/// Version 1 layout of `rep`: a sequence of records with no header, each being
/// a RocksDB write batch record whose column family id is replaced by an index
/// into `cfs`.
///
/// ```text
/// record  = tag [cf] key [value]
/// tag     = 0x1 put, 0x0 delete (default column family),
///           0x5 put, 0x4 delete (column family `cf`)
/// cf      = varint32 index into `cfs`
/// key     = varint32 length, bytes
/// value   = varint32 length, bytes (puts only)
/// ```
///
/// Any other tag is invalid.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Entry {
	/// Format version, [`Entry::VERSION`].
	pub version: u8,

	/// Random number of the capturing engine, used to recognise own entries.
	pub origin: u64,

	/// Database sequence numbers of the capturing engine: `start` is the
	/// sequence before the first write, `end` the sequence after the last one.
	/// The range length equals the number of records in `rep`.
	pub seq: Range<u64>,

	/// Names of the column families `rep` refers to by index.
	pub cfs: Vec<String>,

	/// Records, see the type documentation.
	pub rep: Vec<u8>,

	/// Opaque side information for followers; empty for now.
	pub hints: Option<Vec<u8>>,
}

/// Replication state embedded in an [`Engine`].
pub(crate) struct Replica {
	role: AtomicU8,
	origin: u64,
	deposed: AtomicBool,
	capture: Mutex<Capture>,
	queued: AtomicUsize,
	wake: Notify,
	names: Mutex<BTreeMap<u32, Arc<str>>>,
	barrier: OnceLock<Arc<dyn Barrier>>,
}

/// State guarded by the leader write mutex.
struct Capture {
	/// Database sequence number after the last captured write.
	expected_seq: u64,

	/// Captured writes awaiting the pipeline, oldest first.
	queue: VecDeque<Captured>,
}

/// One captured write, with column family ids of the capturing engine.
struct Captured {
	seq: Range<u64>,
	rep: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Op {
	Put,
	Delete,
}

/// Batch record tags per RocksDB `write_batch.cc`; puts and deletes against
/// column family id 0 encode as the untagged types.
#[derive(Clone, Copy)]
enum Tag {
	Deletion,
	Value,
	CfDeletion,
	CfValue,
}

/// One decoded record borrowing from the buffer it was parsed from.
struct Record<'a> {
	op: Op,
	cf: Option<u32>,
	key: &'a [u8],
	val: &'a [u8],
}

/// Record parser over a batch body.
struct Records<'a> {
	data: &'a [u8],
	len: usize,
}

#[derive(Clone, Copy, Debug)]
enum Malformed {
	Tag(u8),
	Truncated,
	Varint,
}

impl Role {
	fn from_repr(repr: u8) -> Self {
		match repr {
			| 0 => Self::Standalone,
			| 1 => Self::Follower,
			| 2 => Self::Leader,
			| other => unreachable!("invalid role representation {other}"),
		}
	}
}

impl From<Role> for u8 {
	fn from(role: Role) -> Self {
		match role {
			| Role::Standalone => 0,
			| Role::Follower => 1,
			| Role::Leader => 2,
		}
	}
}

impl Display for BarrierError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			| Self::NotLeader => f.write_str("this node is not the cluster leader"),
			| Self::Timeout => f.write_str("cluster commit confirmation timed out"),
		}
	}
}

impl StdError for BarrierError {}

impl Entry {
	/// Current format version.
	pub const VERSION: u8 = 1;

	/// Checks the entry without touching any database.
	///
	/// Verifies the version, the sequence range, the column family names, the
	/// record framing and tags, the column family indices, and that the number
	/// of records equals the length of the sequence range.
	pub fn validate(&self) -> Result {
		if self.version != Self::VERSION {
			return Err!(Database(
				"replica entry version {} is not supported, expected {}",
				self.version,
				Self::VERSION
			));
		}

		let Some(expected) = self.seq.end.checked_sub(self.seq.start) else {
			return Err!(Database(
				"replica entry sequence range {}..{} is reversed",
				self.seq.start,
				self.seq.end
			));
		};

		if let Some(name) = self
			.cfs
			.iter()
			.find(|name| name.is_empty() || name.contains('\0'))
		{
			return Err!(Database("replica entry column family name {name:?} is invalid"));
		}

		let mut records = 0_u64;
		for record in self.records() {
			let record = record?;
			if let Some(slot) = record.cf
				&& self.cf_name(slot).is_none()
			{
				return Err!(Database(
					"replica entry record refers to column family slot {slot} of {}",
					self.cfs.len()
				));
			}

			records = records.saturating_add(1);
		}

		if records != expected {
			return Err!(Database(
				"replica entry holds {records} records for sequence range {}..{}",
				self.seq.start,
				self.seq.end
			));
		}

		Ok(())
	}

	fn records(&self) -> Records<'_> { Records::new(&self.rep) }

	fn cf_name(&self, slot: u32) -> Option<&str> {
		usize::try_from(slot)
			.ok()
			.and_then(|slot| self.cfs.get(slot))
			.map(String::as_str)
	}
}

impl Default for Replica {
	fn default() -> Self {
		Self {
			role: AtomicU8::new(Role::Standalone.into()),
			origin: RandomState::new().hash_one(std::process::id()),
			deposed: AtomicBool::new(false),
			capture: Mutex::new(Capture { expected_seq: 0, queue: VecDeque::new() }),
			queued: AtomicUsize::new(0),
			wake: Notify::new(),
			names: Mutex::new(BTreeMap::new()),
			barrier: OnceLock::new(),
		}
	}
}

impl Replica {
	#[inline]
	fn role(&self) -> Role { Role::from_repr(self.role.load(Ordering::Acquire)) }

	#[inline]
	fn is_deposed(&self) -> bool { self.deposed.load(Ordering::Acquire) }

	fn flag_deposed(&self) { self.deposed.store(true, Ordering::Release); }

	fn report_deposed(&self, reason: &str) {
		error!(%reason, "Replication fault: this database is deposed and refuses writes");
		self.wake.notify_one();
	}
}

impl Engine {
	/// Reports the replication role of this engine.
	#[inline]
	#[must_use]
	pub fn role(&self) -> Role { self.replica.role() }

	/// Changes the replication role.
	///
	/// A leader's sequence expectation starts at the current database sequence,
	/// so the caller must have applied everything it means to keep and must not
	/// let any other write run concurrently; a write racing the switch is
	/// detected as a bypass and deposes the engine. Roles only move forward and
	/// a read-only or secondary database is always standalone.
	pub fn set_role(&self, role: Role) -> Result {
		if role != Role::Standalone && self.is_read_only() {
			return Err!(Database("a read-only database cannot join a cluster as a {role:?}"));
		}

		let mut capture = self.replica.capture.lock_adopting();
		let current = self.replica.role();
		let allowed = match (current, role) {
			| (Role::Standalone, _) | (Role::Follower, Role::Follower | Role::Leader) => true,
			| (Role::Follower, Role::Standalone) | (Role::Leader, _) => false,
		};

		if !allowed {
			return Err!(Database("replication role cannot change from {current:?} to {role:?}"));
		}

		if role == Role::Leader {
			capture.expected_seq = self.db.latest_sequence_number();
			capture.queue.clear();
			self.replica.queued.store(0, Ordering::Relaxed);
		}

		self.replica
			.role
			.store(role.into(), Ordering::Release);

		Ok(())
	}

	/// Reports whether a write bypassed the replication hook.
	///
	/// Once set the flag never clears: the engine refuses writes and
	/// [`Engine::drain_captured`] fails. The cluster runtime must step down.
	#[inline]
	#[must_use]
	pub fn is_deposed(&self) -> bool { self.replica.is_deposed() }

	/// Random number identifying this engine in the entries it captures.
	#[inline]
	#[must_use]
	pub fn origin(&self) -> u64 { self.replica.origin }

	/// Reports the bytes of captured writes not yet drained.
	#[inline]
	#[must_use]
	pub fn captured_bytes(&self) -> usize { self.replica.queued.load(Ordering::Relaxed) }

	/// Installs the commit barrier used by [`Database::barrier`].
	///
	/// A barrier can be installed once; a second call fails.
	pub fn set_barrier(&self, barrier: Arc<dyn Barrier>) -> Result {
		self.replica
			.barrier
			.set(barrier)
			.map_err(|_| err!(Database("a commit barrier is already installed")))
	}

	/// Waits until captured writes are queued or the engine is deposed.
	pub async fn wait_captured(&self) {
		loop {
			let notified = self.replica.wake.notified();
			{
				let capture = self.replica.capture.lock_adopting();
				if !capture.queue.is_empty() || self.replica.is_deposed() {
					return;
				}
			}

			notified.await;
		}
	}

	/// Takes captured writes from the queue as one entry.
	///
	/// Merges the oldest captures into a single entry until adding another
	/// would exceed `max_bytes` of records; the first capture is always taken,
	/// however large. Returns `None` when nothing is queued. Fails once the
	/// engine is deposed, or when a capture cannot be expressed as an entry, in
	/// which case the engine is deposed too.
	pub fn drain_captured(&self, max_bytes: usize) -> Result<Option<Entry>> {
		let taken = {
			let mut capture = self.replica.capture.lock_adopting();
			if self.replica.is_deposed() {
				return Err!(Database("database is deposed, captured writes are discarded"));
			}

			let mut taken = Vec::new();
			let mut bytes = 0_usize;
			while let Some(front) = capture.queue.front() {
				let next = bytes.saturating_add(front.rep.len());
				if !taken.is_empty() && next > max_bytes {
					break;
				}

				bytes = next;
				if let Some(item) = capture.queue.pop_front() {
					taken.push(item);
				}
			}

			self.replica
				.queued
				.fetch_sub(bytes, Ordering::Relaxed);

			taken
		};

		if taken.is_empty() {
			return Ok(None);
		}

		self.assemble(&taken)
			.map(Some)
			.inspect_err(|error| {
				self.replica.flag_deposed();
				self.replica.report_deposed(&error.to_string());
			})
	}

	/// Applies committed entries, in order, as one atomic write.
	///
	/// All entries are validated before anything is written. Missing column
	/// families are created. `marker` is stored in the same write batch, so
	/// after a crash the marker names exactly the entries which are present.
	/// Applying an entry again is harmless as long as the following entries are
	/// applied after it. Only a follower applies entries, and never its own.
	/// One caller at a time, and not concurrently with [`Engine::set_role`].
	pub fn apply_replicated(&self, entries: &[Entry], marker: &[u8]) -> Result {
		let role = self.role();
		if role != Role::Follower {
			return Err!(Database("replicated writes are applied by a follower, not a {role:?}"));
		}

		for entry in entries {
			entry.validate()?;
			if entry.origin == self.replica.origin {
				return Err!(Database(
					"replica entry {:?} was captured by this engine",
					entry.seq
				));
			}

			if entry.cfs.iter().any(|name| name == MARKER_CF) {
				return Err!(Database(
					"replica entry writes the node-local column {MARKER_CF:?}"
				));
			}
		}

		let default = self.ensure_cf(DEFAULT_CF)?;
		let marker_cf = self.ensure_cf(MARKER_CF)?;
		let mut batch = WriteBatch::default();
		for entry in entries {
			let cfs = entry
				.cfs
				.iter()
				.map(|name| self.ensure_cf(name))
				.collect::<Result<Vec<_>>>()?;

			for record in entry.records() {
				let record = record?;
				let cf = match record.cf {
					| None => &default,
					| Some(slot) => usize::try_from(slot)
						.ok()
						.and_then(|slot| cfs.get(slot))
						.ok_or_else(|| {
							err!(Database("replica record column slot {slot} is out of range"))
						})?,
				};

				match record.op {
					| Op::Put => batch.put_cf(cf, record.key, record.val),
					| Op::Delete => batch.delete_cf(cf, record.key),
				}
			}
		}

		batch.put_cf(&marker_cf, MARKER_KEY, marker);
		self.db
			.write_opt(&batch, &self.write_options)
			.or_else(or_else)?;

		self.flush()
	}

	/// Stores `marker` as this node's own position, without replicating it.
	///
	/// A leader applies no replicated entries, so its marker would go stale.
	/// On a leader the write runs under the write mutex like any other and is
	/// checked against the sequence expectation, which it then advances by one,
	/// but it is not queued. A standalone engine writes it directly and a
	/// follower panics: its marker is written by [`Engine::apply_replicated`].
	/// The write is flushed to the OS only; call [`Engine::sync`] when the
	/// marker must survive power loss.
	pub fn write_marker(&self, marker: &[u8]) -> Result {
		match self.role() {
			| Role::Follower => follower_write(),
			| Role::Standalone => {
				let cf = self.ensure_cf(MARKER_CF)?;

				self.put_marker(&cf, marker)?;
			},
			| Role::Leader => {
				let cf = self.ensure_cf(MARKER_CF)?;

				self.write_captured(1, None, || self.put_marker(&cf, marker))?;
			},
		}

		self.flush()
	}

	fn put_marker(&self, cf: &impl AsColumnFamilyRef, marker: &[u8]) -> Result {
		self.db
			.put_cf_opt(cf, MARKER_KEY, marker, &self.write_options)
			.or_else(or_else)
	}

	/// Reads the marker stored by the last [`Engine::apply_replicated`] or
	/// [`Engine::write_marker`].
	///
	/// `None` when no entries were ever applied to this database.
	pub fn applied_marker(&self) -> Result<Option<Vec<u8>>> {
		match self.db.cf_handle(MARKER_CF) {
			| None => Ok(None),
			| Some(cf) => self.db.get_cf(&cf, MARKER_KEY).map_err(map_err),
		}
	}

	/// Creates a checkpoint without flushing memtables and reports the
	/// database sequence numbers around it.
	///
	/// The checkpoint holds every write up to the first returned sequence and
	/// nothing beyond the second; it carries the live write-ahead logs instead
	/// of forcing a flush. On a leader the sequence is read between writes. The
	/// caller keeps the checkpoint only once everything up to the second number
	/// is committed.
	pub fn checkpoint_bracketed(&self, path: &Path) -> Result<(u64, u64)> {
		if self.replica.is_deposed() {
			return Err!(Database("database is deposed, no checkpoint is taken"));
		}

		let before = self.settled_sequence();
		self.flush()?;
		self.checkpoint(path, u64::MAX)?;
		let after = self.settled_sequence();

		Ok((before, after))
	}

	/// Writes one key through the replication hook.
	#[inline]
	pub(crate) fn write_put<K, V>(
		&self,
		cf: &impl AsColumnFamilyRef,
		key: &K,
		val: V,
		opts: &WriteOptions,
	) -> Result
	where
		K: AsRef<[u8]> + ?Sized,
		V: AsRef<[u8]>,
	{
		match self.replica.role() {
			| Role::Standalone => self
				.db
				.put_cf_opt(cf, key, val, opts)
				.or_else(or_else),
			| Role::Follower => follower_write(),
			| Role::Leader => self.leader_put(cf, key.as_ref(), val.as_ref(), opts),
		}
	}

	/// Deletes one key through the replication hook.
	#[inline]
	pub(crate) fn write_delete<K>(
		&self,
		cf: &impl AsColumnFamilyRef,
		key: &K,
		opts: &WriteOptions,
	) -> Result
	where
		K: AsRef<[u8]> + ?Sized,
	{
		match self.replica.role() {
			| Role::Standalone => self
				.db
				.delete_cf_opt(cf, key, opts)
				.or_else(or_else),
			| Role::Follower => follower_write(),
			| Role::Leader => self.leader_delete(cf, key.as_ref(), opts),
		}
	}

	/// Writes a batch through the replication hook.
	#[inline]
	pub(crate) fn write_batch(&self, batch: &WriteBatch, opts: &WriteOptions) -> Result {
		match self.replica.role() {
			| Role::Standalone => self.db.write_opt(batch, opts).or_else(or_else),
			| Role::Follower => follower_write(),
			| Role::Leader if batch.is_empty() => self.db.write_opt(batch, opts).or_else(or_else),
			| Role::Leader => self.leader_batch(batch, opts),
		}
	}

	fn leader_put(
		&self,
		cf: &impl AsColumnFamilyRef,
		key: &[u8],
		val: &[u8],
		opts: &WriteOptions,
	) -> Result {
		let rep = single(Op::Put, cf.id(), key, val);

		self.write_captured(1, Some(rep), || {
			self.db
				.put_cf_opt(cf, key, val, opts)
				.or_else(or_else)
		})
	}

	fn leader_delete(
		&self,
		cf: &impl AsColumnFamilyRef,
		key: &[u8],
		opts: &WriteOptions,
	) -> Result {
		let rep = single(Op::Delete, cf.id(), key, &[]);

		self.write_captured(1, Some(rep), || {
			self.db
				.delete_cf_opt(cf, key, opts)
				.or_else(or_else)
		})
	}

	fn leader_batch(&self, batch: &WriteBatch, opts: &WriteOptions) -> Result {
		let Some(body) = batch.data().get(BATCH_HEADER..) else {
			return Err!(Database("write batch is shorter than its header"));
		};

		let count = u64::try_from(batch.len()).expect("batch length fits in 64 bits");

		let mut records = 0_u64;
		for record in Records::new(body) {
			record?;
			records = records.saturating_add(1);
		}

		if records != count {
			return Err!(Database("write batch holds {records} records but counts {count}"));
		}

		self.write_captured(count, Some(body.to_vec()), || {
			self.db.write_opt(batch, opts).or_else(or_else)
		})
	}

	/// Leader write: applies `write` under the capture mutex and, given `rep`,
	/// queues it covering the next `count` sequence numbers. Without `rep` the
	/// sequence numbers are accounted for but nothing is queued.
	fn write_captured<W>(&self, count: u64, rep: Option<Vec<u8>>, write: W) -> Result
	where
		W: FnOnce() -> Result,
	{
		let replica = &self.replica;
		let bytes = rep.as_ref().map_or(0, Vec::len);
		let mut capture = replica.capture.lock_adopting();

		if replica.is_deposed() {
			return Err!(Database("database is deposed and refuses writes"));
		}

		let start = capture.expected_seq;
		let head = self.db.latest_sequence_number();
		if head != start {
			replica.flag_deposed();
			drop(capture);
			replica.report_deposed(&format!(
				"a write bypassed the replication hook: database sequence is {head}, expected \
				 {start}"
			));

			return Err!(Database("write refused: the database was written outside the hook"));
		}

		let result = write();
		let tail = self.db.latest_sequence_number();
		let end = start.saturating_add(count);

		match result {
			| Ok(()) if tail == end => {
				capture.expected_seq = end;
				let queued = rep.is_some();
				if let Some(rep) = rep {
					capture
						.queue
						.push_back(Captured { seq: start..end, rep });

					replica.queued.fetch_add(bytes, Ordering::Relaxed);
				}

				drop(capture);
				if queued {
					replica.wake.notify_one();
				}

				Ok(())
			},
			| Ok(()) => {
				replica.flag_deposed();
				drop(capture);
				replica.report_deposed(&format!(
					"a write of {count} records moved the database sequence from {start} to \
					 {tail}"
				));

				Ok(())
			},
			| Err(error) => {
				if tail != start {
					replica.flag_deposed();
					drop(capture);
					replica.report_deposed(&format!(
						"a failed write moved the database sequence from {start} to {tail}"
					));
				}

				Err(error)
			},
		}
	}

	fn settled_sequence(&self) -> u64 {
		let _capture =
			(self.role() == Role::Leader).then(|| self.replica.capture.lock_adopting());

		self.db.latest_sequence_number()
	}

	fn assemble(&self, taken: &[Captured]) -> Result<Entry> {
		let start = taken.first().map_or(0, |first| first.seq.start);
		let mut end = start;
		let mut cfs: Vec<String> = Vec::new();
		let mut slots: BTreeMap<u32, u32> = BTreeMap::new();
		let mut rep = Vec::with_capacity(taken.iter().map(|item| item.rep.len()).sum());

		for item in taken {
			if item.seq.start != end {
				return Err!(Database(
					"captured writes are not contiguous: {end} then {:?}",
					item.seq
				));
			}

			let mut records = 0_u64;
			for record in Records::new(&item.rep) {
				let record = record?;
				let slot = record
					.cf
					.map(|id| self.slot(id, &mut slots, &mut cfs))
					.transpose()?;

				encode(&mut rep, record.op, slot, record.key, record.val);
				records = records.saturating_add(1);
			}

			if item.seq.end.checked_sub(item.seq.start) != Some(records) {
				return Err!(Database("captured write {:?} holds {records} records", item.seq));
			}

			end = item.seq.end;
		}

		Ok(Entry {
			version: Entry::VERSION,
			origin: self.replica.origin,
			seq: start..end,
			cfs,
			rep,
			hints: None,
		})
	}

	fn slot(
		&self,
		id: u32,
		slots: &mut BTreeMap<u32, u32>,
		cfs: &mut Vec<String>,
	) -> Result<u32> {
		if let Some(&slot) = slots.get(&id) {
			return Ok(slot);
		}

		let name = self.cf_name(id)?;
		if &*name == MARKER_CF {
			return Err!(Database(
				"a replicated write targets the node-local column {MARKER_CF:?}"
			));
		}

		let slot = u32::try_from(cfs.len())
			.map_err(|_| err!(Database("captured write spans too many column families")))?;

		cfs.push(name.to_string());
		slots.insert(id, slot);

		Ok(slot)
	}

	fn cf_name(&self, id: u32) -> Result<Arc<str>> {
		let mut names = self.replica.names.lock_adopting();
		if let Some(name) = names.get(&id) {
			return Ok(name.clone());
		}

		for name in self.db.cf_names() {
			if let Some(cf) = self.db.cf_handle(&name) {
				names.insert(cf.id(), name.into());
			}
		}

		names
			.get(&id)
			.cloned()
			.ok_or_else(|| err!(Database("captured write targets unknown column family id {id}")))
	}

	fn ensure_cf(&self, name: &str) -> Result<Arc<BoundColumnFamily<'_>>> {
		if let Some(cf) = self.db.cf_handle(name) {
			return Ok(cf);
		}

		let created = self.db.create_cf(name, &Options::default());
		self.db.cf_handle(name).ok_or_else(|| {
			created.map_or_else(map_err, |()| {
				err!(Database("column family {name:?} vanished after creation"))
			})
		})
	}
}

impl Database {
	/// Waits until every write made so far is confirmed by the cluster.
	///
	/// The database sequence is sampled when this is called. Without an
	/// installed [`Barrier`] it resolves at once; a deposed engine always
	/// fails with [`BarrierError::NotLeader`].
	pub fn barrier(&self) -> impl Future<Output = Result<(), BarrierError>> + Send + '_ {
		let replica = &self.engine.replica;

		if replica.is_deposed() {
			return Either::Right(ready(Err(BarrierError::NotLeader)));
		}

		match replica.barrier.get() {
			| Some(barrier) => Either::Left(barrier.wait(self.engine.current_sequence())),
			| None => Either::Right(ready(Ok(()))),
		}
	}
}

#[cold]
#[inline(never)]
#[track_caller]
fn follower_write() -> ! { panic!("database write on a cluster follower") }

impl TryFrom<u8> for Tag {
	type Error = Malformed;

	fn try_from(byte: u8) -> Result<Self, Self::Error> {
		match byte {
			| 0x0 => Ok(Self::Deletion),
			| 0x1 => Ok(Self::Value),
			| 0x4 => Ok(Self::CfDeletion),
			| 0x5 => Ok(Self::CfValue),
			| other => Err(Malformed::Tag(other)),
		}
	}
}

impl Tag {
	fn byte(self) -> u8 {
		match self {
			| Self::Deletion => 0x0,
			| Self::Value => 0x1,
			| Self::CfDeletion => 0x4,
			| Self::CfValue => 0x5,
		}
	}

	fn of(op: Op, cf: Option<u32>) -> Self {
		match (op, cf) {
			| (Op::Put, None) => Self::Value,
			| (Op::Put, Some(_)) => Self::CfValue,
			| (Op::Delete, None) => Self::Deletion,
			| (Op::Delete, Some(_)) => Self::CfDeletion,
		}
	}

	fn op(self) -> Op {
		match self {
			| Self::Value | Self::CfValue => Op::Put,
			| Self::Deletion | Self::CfDeletion => Op::Delete,
		}
	}

	fn has_cf(self) -> bool { matches!(self, Self::CfValue | Self::CfDeletion) }
}

impl Display for Malformed {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			| Self::Tag(tag) => write!(f, "unsupported record tag {tag:#04X}"),
			| Self::Truncated => f.write_str("truncated record"),
			| Self::Varint => f.write_str("invalid varint"),
		}
	}
}

impl<'a> Records<'a> {
	fn new(data: &'a [u8]) -> Self { Self { data, len: data.len() } }

	fn record(&mut self) -> Result<Record<'a>, Malformed> {
		let data = &mut self.data;
		let (&tag, rest) = data.split_first().ok_or(Malformed::Truncated)?;
		*data = rest;

		let tag = Tag::try_from(tag)?;
		let cf = tag
			.has_cf()
			.then(|| take_varint32(data))
			.transpose()?;

		let key = take_varstring(data)?;
		let val = match tag.op() {
			| Op::Put => take_varstring(data)?,
			| Op::Delete => &[],
		};

		Ok(Record { op: tag.op(), cf, key, val })
	}
}

impl<'a> Iterator for Records<'a> {
	type Item = Result<Record<'a>>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.data.is_empty() {
			return None;
		}

		let offset = self.len.saturating_sub(self.data.len());
		let record = self.record().map_err(|malformed| {
			self.data = &[];
			err!(Database("malformed replica record at byte {offset}: {malformed}"))
		});

		Some(record)
	}
}

fn single(op: Op, cf: u32, key: &[u8], val: &[u8]) -> Vec<u8> {
	let capacity = key
		.len()
		.saturating_add(val.len())
		.saturating_add(RECORD_OVERHEAD);

	let mut rep = Vec::with_capacity(capacity);
	encode(&mut rep, op, (cf != 0).then_some(cf), key, val);

	rep
}

fn encode(out: &mut Vec<u8>, op: Op, cf: Option<u32>, key: &[u8], val: &[u8]) {
	out.push(Tag::of(op, cf).byte());
	if let Some(cf) = cf {
		put_varint32(out, cf);
	}

	put_varstring(out, key);
	if op == Op::Put {
		put_varstring(out, val);
	}
}

fn put_varstring(out: &mut Vec<u8>, string: &[u8]) {
	let len = u32::try_from(string.len()).expect("record field fits in 32 bits");

	put_varint32(out, len);
	out.extend_from_slice(string);
}

fn put_varint32(out: &mut Vec<u8>, mut value: u32) {
	loop {
		let byte = u8::try_from(value & 0x7F).expect("seven bits fit in a byte");
		value >>= 7;
		if value == 0 {
			out.push(byte);
			return;
		}

		out.push(byte | 0x80);
	}
}

fn take_varstring<'a>(data: &mut &'a [u8]) -> Result<&'a [u8], Malformed> {
	let len = usize::try_from(take_varint32(data)?).map_err(|_| Malformed::Varint)?;
	let (string, rest) = data
		.split_at_checked(len)
		.ok_or(Malformed::Truncated)?;

	*data = rest;

	Ok(string)
}

fn take_varint32(data: &mut &[u8]) -> Result<u32, Malformed> {
	let mut result = 0_u32;

	for shift in (0_u32..35).step_by(7) {
		let (&byte, rest) = data.split_first().ok_or(Malformed::Truncated)?;
		*data = rest;

		let bits = u32::from(byte & 0x7F);
		if shift == 28 && bits > 0x0F {
			return Err(Malformed::Varint);
		}

		result |= bits.checked_shl(shift).ok_or(Malformed::Varint)?;
		if byte & 0x80 == 0 {
			return Ok(result);
		}
	}

	Err(Malformed::Varint)
}
