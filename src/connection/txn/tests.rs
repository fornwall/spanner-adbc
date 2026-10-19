//! Offline tests for the manual-transaction state machine.

use adbc_core::error::Status;

use super::{ManualTxn, Mutation, SpannerSql, TxnKind, TxnState, check_commit_writable};

fn sql(s: &str) -> SpannerSql {
    SpannerSql::builder(s).build()
}

fn mutation(id: i64) -> Mutation {
    Mutation::new_insert_builder("t").set("Id").to(id).build()
}

fn manual() -> TxnState {
    let mut st = TxnState::new();
    st.autocommit = false;
    st
}

#[test]
fn autocommit_allows_every_kind() {
    let st = TxnState::new();
    for kind in [TxnKind::Read, TxnKind::Dml] {
        st.check_kind_allowed(kind)
            .expect("autocommit allows every kind");
    }
}

#[test]
fn first_statement_fixes_the_transaction_kind() {
    let mut st = manual();
    for kind in [TxnKind::Read, TxnKind::Dml] {
        st.check_kind_allowed(kind)
            .expect("an unset transaction allows either first kind");
    }
    st.buffer_dml(vec![sql("UPDATE a")])
        .expect("first DML fixes the kind");
    st.check_kind_allowed(TxnKind::Dml)
        .expect("more DML joins the transaction");
    let error = st.check_kind_allowed(TxnKind::Read).unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(
        error.message.contains("read-your-writes"),
        "the query rejection should explain the read-your-writes hazard: {}",
        error.message
    );
    assert!(
        error.message.contains("began with DML"),
        "the rejection should name the active kind: {}",
        error.message
    );
}

/// Ingest mutations count as DML.
#[test]
fn mutations_share_the_dml_kind() {
    let mut st = manual();
    st.buffer_mutation(mutation(1))
        .expect("first ingest fixes the kind to DML");
    st.buffer_dml(vec![sql("UPDATE a")])
        .expect("DML joins an ingest-started transaction");
    let error = st.check_kind_allowed(TxnKind::Read).unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
}

#[test]
fn enter_autocommit_flips_and_takes_in_one_step() {
    let mut st = manual();
    st.buffer_dml(vec![sql("UPDATE a"), sql("UPDATE b")])
        .unwrap();
    st.buffer_mutation(mutation(1)).unwrap();
    let taken = st.enter_autocommit();
    assert!(st.autocommit());
    assert!(matches!(st.txn, ManualTxn::Unset));
    let ManualTxn::Dml {
        statements,
        mutations,
    } = taken
    else {
        panic!("the taken state must be the DML transaction");
    };
    assert_eq!(
        statements.iter().map(|s| s.sql()).collect::<Vec<_>>(),
        ["UPDATE a", "UPDATE b"]
    );
    assert_eq!(mutations, [mutation(1)]);
}

#[test]
fn restore_manual_restores_the_taken_state() {
    let mut st = manual();
    st.buffer_dml(vec![sql("UPDATE a")]).unwrap();
    let taken = st.enter_autocommit();
    st.restore_manual(taken);
    assert!(!st.autocommit());
    assert!(matches!(&st.txn, ManualTxn::Dml { statements, .. } if statements.len() == 1));
}

/// `finish_commit` removes only the applied prefix; a drained transaction resets to `Unset`.
#[test]
fn finish_commit_keeps_concurrently_buffered_work() {
    let mut st = manual();
    st.buffer_dml(vec![sql("UPDATE a")]).unwrap();
    let applied = st.txn.clone();
    // A statement buffers more DML while the commit RPC is in flight.
    st.buffer_dml(vec![sql("UPDATE late")]).unwrap();
    st.finish_commit(&applied);
    let ManualTxn::Dml { statements, .. } = &st.txn else {
        panic!("the late DML must stay pending, keeping the kind");
    };
    assert_eq!(
        statements.iter().map(|s| s.sql()).collect::<Vec<_>>(),
        ["UPDATE late"]
    );
    let applied = st.txn.clone();
    st.finish_commit(&applied);
    assert!(matches!(st.txn, ManualTxn::Unset));
}

#[test]
fn read_only_rejects_a_commit_that_would_write() {
    for work in [
        ManualTxn::Dml {
            statements: vec![sql("UPDATE a SET x = 1 WHERE y = 2")],
            mutations: Vec::new(),
        },
        ManualTxn::Dml {
            statements: Vec::new(),
            mutations: vec![mutation(1)],
        },
    ] {
        let error = check_commit_writable(true, &work)
            .expect_err("a read-only connection must not commit buffered writes");
        assert_eq!(error.status, Status::InvalidState);
        assert!(
            error.message.contains("read-only"),
            "the rejection should name the read-only flag: {}",
            error.message
        );
        check_commit_writable(false, &work)
            .expect("a writable connection commits the buffered work");
    }
}

/// `ManualTxn::Read` cannot be built offline; `tests/mock_spanner.rs` covers committing it on a
/// read-only connection.
#[test]
fn read_only_allows_a_commit_with_nothing_to_write() {
    for work in [
        ManualTxn::Unset,
        ManualTxn::Dml {
            statements: Vec::new(),
            mutations: Vec::new(),
        },
    ] {
        check_commit_writable(true, &work)
            .expect("committing nothing writes nothing, so read-only must allow it");
    }
}

#[test]
fn lock_txn_recovers_a_poisoned_mutex() {
    use std::sync::{Arc, Mutex};

    use super::lock_txn;

    let shared: Arc<Mutex<TxnState>> = Arc::new(Mutex::new(manual()));

    let poisoner = Arc::clone(&shared);
    std::thread::spawn(move || {
        let mut guard = poisoner.lock().unwrap();
        guard.buffer_dml(vec![sql("UPDATE a")]).unwrap();
        panic!("poison the txn mutex");
    })
    .join()
    .unwrap_err();
    assert!(
        shared.is_poisoned(),
        "the panic should have poisoned the mutex"
    );

    // The guard sees the DML the poisoner buffered.
    let guard = lock_txn(&shared);
    guard
        .check_kind_allowed(TxnKind::Dml)
        .expect("the recovered DML transaction is still consistent and usable");
    assert_eq!(
        guard.check_kind_allowed(TxnKind::Read).unwrap_err().status,
        Status::InvalidState,
    );
}
