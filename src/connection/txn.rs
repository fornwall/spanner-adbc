//! Manual-transaction state shared by a connection and its statements.

use std::sync::{Arc, Mutex};

use adbc_core::error::Result;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::statement::Statement as SpannerSql;
use google_cloud_spanner::transaction::MultiUseReadOnlyTransaction;

use crate::error::invalid_state;

/// What a manual transaction holds, fixed by its first statement. DDL has no kind: it runs
/// immediately and leaves the transaction alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnKind {
    /// Queries sharing one multi-use read-only transaction.
    Read,
    /// DML and bulk-ingest mutations, applied atomically on commit.
    Dml,
}

impl TxnKind {
    /// How the kind reads in an error message.
    fn what(self) -> &'static str {
        match self {
            TxnKind::Read => "a query",
            TxnKind::Dml => "DML",
        }
    }

    /// Why work of the other kind cannot join a transaction of this kind.
    fn rationale(self) -> &'static str {
        match self {
            TxnKind::Read => "the transaction is pinned to a multi-use read-only snapshot",
            TxnKind::Dml => {
                "the buffered DML is applied at commit, so a query would not observe the \
                 pending writes (no read-your-writes)"
            }
        }
    }
}

/// A manual transaction's kind and payload. Keeping the payload in the variant makes a read-only
/// snapshot and buffered DML mutually exclusive.
#[derive(Debug, Default, Clone)]
pub(super) enum ManualTxn {
    /// No statement has fixed the kind yet.
    #[default]
    Unset,
    /// Shared by every query in the transaction. The `Arc` lets a statement execute on it without
    /// holding the [`SharedTxn`] lock; dropping it ends the transaction.
    Read(Arc<MultiUseReadOnlyTransaction>),
    /// Holds built statements, so parameterized DML keeps its bound values.
    /// Spanner applies `mutations` at commit, after every statement, whatever the issue order.
    Dml {
        statements: Vec<SpannerSql>,
        mutations: Vec<Mutation>,
    },
}

impl ManualTxn {
    fn kind(&self) -> Option<TxnKind> {
        match self {
            ManualTxn::Unset => None,
            ManualTxn::Read(_) => Some(TxnKind::Read),
            ManualTxn::Dml { .. } => Some(TxnKind::Dml),
        }
    }

    /// Whether a commit has anything to apply.
    fn has_pending_work(&self) -> bool {
        match self {
            ManualTxn::Unset | ManualTxn::Read(_) => false,
            ManualTxn::Dml {
                statements,
                mutations,
            } => !statements.is_empty() || !mutations.is_empty(),
        }
    }
}

/// Transaction state shared between a connection and the statements it creates.
#[derive(Debug)]
pub(crate) struct TxnState {
    pub(super) autocommit: bool,
    /// Always `Unset` in autocommit mode.
    pub(super) txn: ManualTxn,
}

impl TxnState {
    pub(super) fn new() -> Self {
        Self {
            autocommit: true,
            txn: ManualTxn::Unset,
        }
    }

    pub(crate) fn autocommit(&self) -> bool {
        self.autocommit
    }

    /// Fail with `InvalidState` if a manual transaction already holds work of the other kind.
    ///
    /// The buffer methods and [`Self::start_read_txn`] call this under the [`SharedTxn`] lock,
    /// which keeps the kinds exclusive under concurrent statements. Read paths also call it up
    /// front so a mixed-kind query fails before doing any work.
    pub(crate) fn check_kind_allowed(&self, attempted: TxnKind) -> Result<()> {
        if self.autocommit {
            return Ok(());
        }
        let Some(active) = self.txn.kind() else {
            return Ok(());
        };
        if active == attempted {
            return Ok(());
        }
        Err(invalid_state(format!(
            "cannot run {} in a manual transaction that began with {}: {}. A manual transaction \
             is either queries or DML — its kind is fixed by its first statement. Commit or \
             roll back the transaction first.",
            attempted.what(),
            active.what(),
            active.rationale(),
        )))
    }

    /// The shared read-only transaction, if one is active.
    pub(crate) fn read_txn(&self) -> Option<Arc<MultiUseReadOnlyTransaction>> {
        match &self.txn {
            ManualTxn::Read(txn) => Some(txn.clone()),
            _ => None,
        }
    }

    /// Install `txn` as the shared read-only transaction and return the effective one. If a
    /// concurrent statement installed one first, `txn` is dropped; with inline begin it has issued
    /// no RPC yet.
    ///
    /// The caller built `txn` outside the lock, so check the kind again: a concurrent statement
    /// may have buffered DML in the meantime.
    pub(crate) fn start_read_txn(
        &mut self,
        txn: Arc<MultiUseReadOnlyTransaction>,
    ) -> Result<Arc<MultiUseReadOnlyTransaction>> {
        self.check_kind_allowed(TxnKind::Read)?;
        match &self.txn {
            ManualTxn::Read(existing) => Ok(existing.clone()),
            _ => {
                self.txn = ManualTxn::Read(txn.clone());
                Ok(txn)
            }
        }
    }

    /// Forget the shared read-only transaction if it is still `txn` and never began. With an
    /// inline begin, the client marks a transaction whose first query failed as permanently
    /// failed, so every later query would fail until commit or rollback. Nothing began, so no
    /// snapshot is lost and the next query starts a fresh transaction.
    pub(crate) fn discard_unbegun_read_txn(&mut self, txn: &Arc<MultiUseReadOnlyTransaction>) {
        // Spanner returns the read timestamp when the transaction begins.
        if txn.read_timestamp().is_some() {
            return;
        }
        if matches!(&self.txn, ManualTxn::Read(current) if Arc::ptr_eq(current, txn)) {
            self.txn = ManualTxn::Unset;
        }
    }

    /// Buffer DML for the next commit.
    pub(crate) fn buffer_dml(&mut self, new: Vec<SpannerSql>) -> Result<()> {
        self.check_kind_allowed(TxnKind::Dml)?;
        if new.is_empty() {
            return Ok(());
        }
        match &mut self.txn {
            ManualTxn::Dml { statements, .. } => statements.extend(new),
            txn => {
                *txn = ManualTxn::Dml {
                    statements: new,
                    mutations: Vec::new(),
                }
            }
        }
        Ok(())
    }

    /// Buffer a bulk-ingest mutation for the next commit. Ingest counts as DML.
    pub(crate) fn buffer_mutation(&mut self, mutation: Mutation) -> Result<()> {
        self.check_kind_allowed(TxnKind::Dml)?;
        match &mut self.txn {
            ManualTxn::Dml { mutations, .. } => mutations.push(mutation),
            txn => {
                *txn = ManualTxn::Dml {
                    statements: Vec::new(),
                    mutations: vec![mutation],
                }
            }
        }
        Ok(())
    }

    /// Switch to autocommit and take the manual transaction for the caller to commit.
    ///
    /// The buffer methods check the mode under the same lock, so the taken state is complete.
    /// Flipping after the commit instead would strand DML buffered while it ran.
    pub(super) fn enter_autocommit(&mut self) -> ManualTxn {
        self.autocommit = true;
        std::mem::take(&mut self.txn)
    }

    /// Undo [`Self::enter_autocommit`] after a failed commit, keeping the transaction replayable.
    /// Nothing buffers in autocommit mode, so the current state is still `Unset`.
    pub(super) fn restore_manual(&mut self, work: ManualTxn) {
        self.autocommit = false;
        debug_assert!(matches!(self.txn, ManualTxn::Unset));
        self.txn = work;
    }

    /// Remove the `applied` work after a commit. Work buffered while the commit ran sits behind
    /// it and stays pending; if nothing remains, the state resets and the next statement picks a
    /// fresh kind.
    pub(super) fn finish_commit(&mut self, applied: &ManualTxn) {
        if let (
            ManualTxn::Dml {
                statements,
                mutations,
            },
            ManualTxn::Dml {
                statements: applied_statements,
                mutations: applied_mutations,
            },
        ) = (&mut self.txn, applied)
        {
            statements.drain(..applied_statements.len());
            mutations.drain(..applied_mutations.len());
        }
        if !self.txn.has_pending_work() {
            self.txn = ManualTxn::Unset;
        }
    }
}

/// Enforce `adbc.connection.readonly` on commit, including the commit done by enabling
/// autocommit. Only pending writes are rejected, so ending a query or empty transaction still
/// works. The caller keeps the buffer, so clearing the flag and committing again applies it.
pub(super) fn check_commit_writable(read_only: bool, work: &ManualTxn) -> Result<()> {
    if read_only && work.has_pending_work() {
        return Err(invalid_state(
            "cannot commit buffered DML: the connection is read-only. The buffered work is kept \
             and stays replayable: clear adbc.connection.readonly and commit again to apply it, \
             or roll back to discard it.",
        ));
    }
    Ok(())
}

/// A handle to a connection's transaction state, shared with its statements.
pub(crate) type SharedTxn = Arc<Mutex<TxnState>>;

/// Lock the shared [`TxnState`], ignoring poisoning.
///
/// No method leaves the state half-updated, so it is safe to use after a panic. Propagating the
/// poison would make every later transaction call on the connection panic across the C ABI.
pub(crate) fn lock_txn(txn: &SharedTxn) -> std::sync::MutexGuard<'_, TxnState> {
    txn.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests;
