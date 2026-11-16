//! The Raft state machine of the cluster: the replicated database itself.
//!
//! The database leader writes first and captures its writes as log entries,
//! so the leader's state machine applies nothing. For an entry of its own it
//! only publishes how far the cluster has confirmed the database sequence,
//! which is what the commit barrier waits for. A follower writes every entry
//! of another origin into its database, together with a marker naming the log
//! position reached, in one atomic write.
//!
//! The marker lives in the database column `raft_applied`. Its first byte is
//! the format version, [`MARKER_VERSION`]; the rest is the postcard encoding of
//! the pair of the applied log id and the membership in force at that position.
//! Both are needed to answer `applied_state` after a restart.
//!
//! With a [`SnapshotStore`] attached the machine also builds, serves and
//! installs snapshots of the database; the layout of the snapshots and the
//! replacement of the database are described with [`SnapshotStore`].
//! Installing replaces the database of the machine by a new one, so the machine
//! hands out its database by reference and not by clone, and the runtime reads
//! it back with [`Machine::database`].

#[cfg(test)]
mod tests;

use std::{io, ops::Range, sync::Arc};

use futures::{Stream, StreamExt};
use openraft::{
	OptionalSend, StoredMembership,
	entry::RaftEntry,
	impls::EntryPayload,
	storage::{EntryResponder, RaftStateMachine},
	type_config::alias::{
		EntryPayloadOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
	},
};
use tokio::sync::{RwLock, watch};
use tuwunel_database::{Database, Role};

use crate::{
	snapshot::{
		SnapshotDir, SnapshotStore, Snapshots,
		replace::{Closed, Pending},
	},
	types::{Request, TypeConfig},
	util::{blocking, invalid},
};

/// Format version of the position marker, its first byte.
const MARKER_VERSION: u8 = 1;

type Applied = (Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>);

/// State machine over the cluster database.
///
/// Raft is its only caller: the trait methods take `&mut self`, so entries are
/// applied one at a time and in log order. The database must not be changed
/// from anywhere else on a follower, and its role must not change while an
/// apply runs.
///
/// Snapshots need a [`SnapshotStore`], attached with
/// [`Machine::with_snapshots`]. Without one a planned snapshot is deferred, so
/// the Raft core carries on, and a forced one, which a lagging follower or a
/// restart after a purge would need, fails with an error.
///
/// Installing a snapshot closes the database and opens a new one in its place.
/// That works only while the machine is the sole owner of the database. If
/// something else still holds it, the installation stops after it has stored
/// the replace marker and fails; Raft treats that as fatal, the process ends,
/// and the replacement is finished at the next start by
/// [`SnapshotStore::open_database`]. A machine whose installation failed after
/// the database was closed has no database any more, and every call on it fails.
pub struct Machine {
	db: Option<Arc<Database>>,
	committed: watch::Sender<u64>,
	applied: Option<Applied>,
	snapshots: Option<SnapshotStore>,
	gate: Arc<RwLock<()>>,
}

impl Machine {
	/// Creates the state machine over `db`.
	///
	/// `committed` is the cluster-confirmed database sequence of a leader. An
	/// entry the leader captured itself moves it up to the sequence the entry
	/// ends at, and it never moves down. The barrier of the leader waits on the
	/// receiving side, and so does a snapshot of the leader. The sender is not
	/// touched on a follower.
	#[must_use]
	pub fn new(db: Arc<Database>, committed: watch::Sender<u64>) -> Self {
		Self {
			db: Some(db),
			committed,
			applied: None,
			snapshots: None,
			gate: Arc::new(RwLock::new(())),
		}
	}

	/// Lets the machine build, serve and install snapshots through `store`.
	#[must_use]
	pub fn with_snapshots(mut self, store: SnapshotStore) -> Self {
		self.snapshots = Some(store);

		self
	}

	/// The database the machine applies to.
	///
	/// It changes when a snapshot is installed, so do not keep the handle: a
	/// copy that is still held then stops the installation. `None` after an
	/// installation failed past the point where the old database was closed.
	#[must_use]
	pub fn database(&self) -> Option<&Arc<Database>> { self.db.as_ref() }

	fn db(&self) -> io::Result<&Arc<Database>> { self.db.as_ref().ok_or_else(closed) }

	fn store(&self) -> io::Result<&SnapshotStore> {
		self.snapshots.as_ref().ok_or_else(unavailable)
	}

	async fn stored(&self) -> io::Result<Applied> {
		let db = Arc::clone(self.db()?);

		blocking(move || {
			match db
				.engine
				.applied_marker()
				.map_err(io::Error::other)?
			{
				| None => Ok((None, StoredMembership::default())),
				| Some(marker) => decode_marker(&marker),
			}
		})
		.await
	}

	fn lead(
		&self,
		log_id: &LogIdOf<TypeConfig>,
		payload: &EntryPayloadOf<TypeConfig>,
	) -> io::Result<()> {
		match payload {
			| EntryPayload::Normal(Request(entry))
				if entry.origin == self.db()?.engine.origin() =>
				self.confirm(&entry.seq),
			| EntryPayload::Normal(Request(entry)) => Err(io::Error::other(format!(
				"leader cannot apply entry {log_id} of origin {:016x}: only its own entries are \
				 committed here",
				entry.origin
			))),
			| EntryPayload::Blank | EntryPayload::Membership(_) => Ok(()),
		}
	}

	fn confirm(&self, seq: &Range<u64>) -> io::Result<()> {
		let head = self.db()?.engine.current_sequence();
		if seq.start > seq.end || seq.end > head {
			return Err(invalid(format!(
				"own entry covers sequence {}..{} of a database at {head}",
				seq.start, seq.end
			)));
		}

		self.committed.send_if_modified(|committed| {
			let advance = seq.end > *committed;
			if advance {
				*committed = seq.end;
			}

			advance
		});

		Ok(())
	}

	async fn follow(
		&self,
		log_id: LogIdOf<TypeConfig>,
		payload: EntryPayloadOf<TypeConfig>,
		membership: StoredMembershipOf<TypeConfig>,
	) -> io::Result<StoredMembershipOf<TypeConfig>> {
		let (entries, membership) = match payload {
			| EntryPayload::Normal(Request(entry)) => (vec![entry], membership),
			| EntryPayload::Blank => (Vec::new(), membership),
			| EntryPayload::Membership(members) =>
				(Vec::new(), StoredMembership::new(Some(log_id), members)),
		};

		let marker = encode_marker(&log_id, &membership)?;
		let db = Arc::clone(self.db()?);

		blocking(move || {
			db.engine
				.apply_replicated(&entries, &marker)
				.map_err(io::Error::other)
		})
		.await?;

		Ok(membership)
	}

	/// The position and membership entries are applied on top of: the one the
	/// leader keeps in memory, or the marker of the database.
	async fn position(&self) -> io::Result<Applied> {
		match &self.applied {
			| Some(applied) if self.db()?.engine.role() == Role::Leader => Ok(applied.clone()),
			| _ => self.stored().await,
		}
	}

	async fn builder(&self, force: bool) -> Option<Snapshots> {
		let Some(store) = self.snapshots.clone() else {
			return force.then(Snapshots::disabled);
		};

		let gate = Arc::clone(&self.gate);
		let permit = match force {
			| true => gate.read_owned().await,
			| false => gate.try_read_owned().ok()?,
		};

		if !force && store.is_building() {
			return None;
		}

		let applied = match self.applied.clone() {
			| Some(applied) => Ok(applied),
			| None => self.stored().await,
		};

		let ready = match (applied, self.db()) {
			| (Err(error), _) | (_, Err(error)) => Err(error),
			| (Ok((None, _)), _) if !force => return None,
			| (Ok((None, _)), _) => Err(invalid(
				"no log entry is applied yet, so there is nothing to snapshot".to_owned(),
			)),
			| (Ok((Some(log_id), membership)), Ok(db)) =>
				Ok((Arc::clone(db), log_id, membership)),
		};

		Some(match ready {
			| Err(error) => Snapshots::failed(error),
			| Ok((db, log_id, membership)) => Snapshots::ready(
				db,
				store,
				(log_id, membership),
				self.committed.subscribe(),
				permit,
			),
		})
	}
}

impl RaftStateMachine<TypeConfig> for Machine {
	type SnapshotBuilder = Snapshots;
	type SnapshotData = SnapshotDir;

	async fn applied_state(&mut self) -> io::Result<Applied> {
		let applied = self.stored().await?;
		self.applied = Some(applied.clone());

		Ok(applied)
	}

	async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
	where
		Strm: Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + OptionalSend,
	{
		let mut position: Option<Applied> = None;

		while let Some(item) = entries.next().await {
			let (entry, responder) = item?;
			let log_id = entry.log_id();
			let current = match position.take() {
				| Some(current) => current,
				| None => self.position().await?,
			};

			let membership = if self.db()?.engine.role() == Role::Leader {
				self.lead(&log_id, &entry.payload)?;

				match entry.payload {
					| EntryPayload::Membership(members) =>
						StoredMembership::new(Some(log_id), members),
					| EntryPayload::Normal(_) | EntryPayload::Blank => current.1,
				}
			} else {
				self.follow(log_id, entry.payload, current.1)
					.await?
			};

			let applied = (Some(log_id), membership);
			self.applied = Some(applied.clone());
			position = Some(applied);

			if let Some(responder) = responder {
				responder.send(());
			}
		}

		Ok(())
	}

	async fn try_create_snapshot_builder(
		&mut self,
		force: bool,
	) -> Option<Self::SnapshotBuilder> {
		self.builder(force).await
	}

	async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
		self.builder(true)
			.await
			.unwrap_or_else(Snapshots::disabled)
	}

	async fn install_snapshot(
		&mut self,
		meta: &SnapshotMetaOf<TypeConfig>,
		snapshot: SnapshotDir,
	) -> io::Result<()> {
		let store = self.store()?.clone();
		let (Some(log_id), membership) = (meta.last_log_id, &meta.last_membership) else {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"a snapshot that names no log entry cannot be installed",
			));
		};

		let marker = encode_marker(&log_id, membership)?;
		let gate = Arc::clone(&self.gate);
		let _exclusive = gate.write().await;

		let role = self.db()?.engine.role();
		if role != Role::Follower {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("a snapshot is installed by a follower, not a {role:?}"),
			));
		}

		let staged = snapshot.clone();
		let adopting = store.clone();
		let adopted_marker = marker.clone();
		let name = blocking(move || adopting.adopt(&staged, &adopted_marker)).await?;

		let pending = Pending { snapshot: name, marker };
		store.begin(&pending).await?;

		let db = self.db.take().ok_or_else(closed)?;
		match SnapshotStore::close(db).await? {
			| Closed::Shared(db) => {
				self.db = Some(db);

				Err(io::Error::new(
					io::ErrorKind::ResourceBusy,
					"the database is still in use elsewhere, so it cannot be replaced in this \
					 process; the replacement is stored and the next start finishes it",
				))
			},
			| Closed::Done => {
				let marker = pending.marker.clone();
				self.db = Some(store.finish(pending).await?);
				self.applied = Some(decode_marker(&marker)?);

				Ok(())
			},
		}
	}

	async fn get_current_snapshot(
		&mut self,
	) -> io::Result<Option<SnapshotOf<TypeConfig, SnapshotDir>>> {
		match &self.snapshots {
			| Some(store) => store.current().await,
			| None => Ok(None),
		}
	}
}

/// Encodes the position marker the state machine keeps in the database, for
/// [`tuwunel_database::Engine::apply_replicated`] and
/// [`tuwunel_database::Engine::write_marker`].
///
/// The first byte is the format version, 1; the rest is the postcard encoding
/// of the pair of the applied log id and the membership in force at that log
/// id. [`decode_marker`] reads it back. Any change of `Node`, of the log id or
/// membership types, or of the openraft version that alters these bytes must
/// raise the marker version.
pub fn encode_marker(
	applied: &LogIdOf<TypeConfig>,
	membership: &StoredMembershipOf<TypeConfig>,
) -> io::Result<Vec<u8>> {
	postcard::to_extend(&(applied, membership), vec![MARKER_VERSION])
		.map_err(|error| invalid(format!("raft_applied marker is not encodable: {error}")))
}

/// Decodes a position marker written by [`encode_marker`].
///
/// An empty marker means nothing was applied: no log id and the empty
/// membership. A marker of an unknown version, a damaged one, and one with
/// bytes after its end are errors of kind `InvalidData`.
pub fn decode_marker(
	marker: &[u8],
) -> io::Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>)> {
	let Some((&version, body)) = marker.split_first() else {
		return Ok((None, StoredMembership::default()));
	};

	if version != MARKER_VERSION {
		return Err(invalid(format!(
			"raft_applied marker version {version} is not supported, expected {MARKER_VERSION}"
		)));
	}

	let ((applied, membership), rest) =
		postcard::take_from_bytes::<(LogIdOf<TypeConfig>, StoredMembershipOf<TypeConfig>)>(body)
			.map_err(|error| invalid(format!("raft_applied marker is damaged: {error}")))?;

	if !rest.is_empty() {
		return Err(invalid(format!(
			"raft_applied marker has {} bytes after its end",
			rest.len()
		)));
	}

	Ok((Some(applied), membership))
}

fn closed() -> io::Error {
	io::Error::other("the database was closed by an installation that did not finish")
}

fn unavailable() -> io::Error {
	io::Error::new(io::ErrorKind::Unsupported, "snapshots are not available yet")
}
