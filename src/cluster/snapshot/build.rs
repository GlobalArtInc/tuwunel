//! Building a snapshot from the live database.

use std::{io, sync::Arc, time::Duration};

use openraft::{
	storage::{RaftSnapshotBuilder, Snapshot, SnapshotMeta},
	type_config::alias::{LogIdOf, SnapshotOf, StoredMembershipOf},
};
use tokio::{
	sync::{OwnedRwLockReadGuard, watch},
	time::sleep,
};
use tuwunel_database::{Database, Role};

use super::{SnapshotDir, SnapshotStore};
use crate::{machine::encode_marker, types::TypeConfig, util::blocking};

/// How often a leader that waits for confirmation checks that it still leads.
const POLL: Duration = Duration::from_millis(100);

/// Snapshot builder of the state machine.
///
/// Raft runs it in a task of its own while the state machine goes on applying
/// entries. It takes the position the state machine had applied when it was
/// created, makes a checkpoint of the database, and keeps it as the current
/// snapshot under that position. Entries applied meanwhile may be in the
/// checkpoint too; they come after the position and are replayed over it.
///
/// On a leader the checkpoint can hold writes not yet confirmed. The builder
/// then waits until everything up to the database sequence read after the
/// checkpoint is committed before it keeps the snapshot, and throws the
/// checkpoint away when the leader is deposed or the state machine goes first.
/// A follower, or a node that has not been promoted yet, applies committed
/// entries only and does not wait.
///
/// One snapshot is built at a time; a second builder waits for the first. A
/// builder keeps the database from being replaced for as long as it lives.
pub struct Snapshots {
	state: State,
}

enum State {
	Disabled,
	Failed(Option<io::Error>),
	Ready(Box<Ready>),
}

/// The database is released before the permit that keeps it from being
/// replaced, so a replacement that waits for the permit finds the database
/// free.
struct Ready {
	db: Arc<Database>,
	store: SnapshotStore,
	applied: (LogIdOf<TypeConfig>, StoredMembershipOf<TypeConfig>),
	committed: watch::Receiver<u64>,
	_permit: OwnedRwLockReadGuard<()>,
}

impl Snapshots {
	/// A builder of a state machine that cannot snapshot: every build fails.
	pub(crate) fn disabled() -> Self { Self { state: State::Disabled } }

	pub(crate) fn failed(error: io::Error) -> Self { Self { state: State::Failed(Some(error)) } }

	pub(crate) fn ready(
		db: Arc<Database>,
		store: SnapshotStore,
		applied: (LogIdOf<TypeConfig>, StoredMembershipOf<TypeConfig>),
		committed: watch::Receiver<u64>,
		permit: OwnedRwLockReadGuard<()>,
	) -> Self {
		Self {
			state: State::Ready(Box::new(Ready {
				db,
				store,
				applied,
				committed,
				_permit: permit,
			})),
		}
	}
}

impl RaftSnapshotBuilder<TypeConfig> for Snapshots {
	type SnapshotData = SnapshotDir;

	async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<TypeConfig, SnapshotDir>> {
		match &mut self.state {
			| State::Disabled => Err(unavailable()),
			| State::Failed(error) => Err(error
				.take()
				.unwrap_or_else(|| io::Error::other("the snapshot builder failed earlier"))),
			| State::Ready(ready) => ready.build().await,
		}
	}
}

impl Ready {
	async fn build(&mut self) -> io::Result<SnapshotOf<TypeConfig, SnapshotDir>> {
		let store = self.store.clone();
		let _serial = store.inner.building.lock().await;

		let (log_id, membership) = self.applied.clone();
		let marker = encode_marker(&log_id, &membership)?;

		let db = Arc::clone(&self.db);
		let store = self.store.clone();
		let (before, after) = blocking(move || store.checkpoint(&db)).await?;
		self.store.checkpointed(before, after);

		if self.db.engine.role() == Role::Leader
			&& let Err(error) = self.confirmed(after).await
		{
			let store = self.store.clone();
			blocking(move || store.discard_build()).await?;

			return Err(error);
		}

		let store = self.store.clone();
		let snapshot = blocking(move || store.finish_build(&marker)).await?;

		Ok(Snapshot {
			meta: SnapshotMeta {
				last_log_id: Some(log_id),
				last_membership: membership,
			},
			snapshot,
		})
	}

	async fn confirmed(&mut self, upto: u64) -> io::Result<()> {
		loop {
			if *self.committed.borrow_and_update() >= upto {
				return Ok(());
			}

			if self.db.engine.is_deposed() {
				return Err(io::Error::other(
					"leadership was lost before the snapshot was confirmed, so it is discarded",
				));
			}

			tokio::select! {
				changed = self.committed.changed() => changed.map_err(|_| {
					io::Error::other("the state machine stopped before the snapshot was confirmed")
				})?,
				() = sleep(POLL) => {},
			}
		}
	}
}

fn unavailable() -> io::Error {
	io::Error::new(io::ErrorKind::Unsupported, "snapshots are not available yet")
}
