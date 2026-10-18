# Open project review findings

Three upstream items remain. Verified against the pinned `google-cloud-rust` `415dbbb8` and
`arrow-adbc` `0a40cf30` sources on 2026-10-18. Keep IDs stable; remove resolved items.

## google-cloud-rust

- [ ] **UP-6 — Explicit read/write transaction handles.** The public API uses
  `DatabaseClient::read_write_transaction()` and `TransactionRunner::run(work)`.
  `ReadWriteTransaction` has no public constructor, commit or rollback. A public handle would
  let the driver replace buffered manual transactions and support reads after writes.
  Sources: `src/spanner/src/{transaction_runner,read_write_transaction}.rs`.
- [ ] **UP-14 — Streaming retry elapsed-time limit.** Query and BatchWrite `check_retry` construct a fresh
  `RetryState` on each retry decision, resetting its start time. Thus
  `spanner.retry.max_elapsed_seconds` does not bound streaming retries. Attempt counting is now
  correct (`1 + retry_count`). Preserve the loop start with `RetryState::set_start` upstream;
  use query/fetch/update timeouts for a driver-side wall-clock bound. Sources:
  `src/spanner/src/{result_set,batch_write_transaction}.rs`, `src/gax/src/retry_state.rs` and local
  [src/retry.rs](src/retry.rs). Regression coverage: `retry_max_*` and
  `batch_write_honors_retry_attempt_limit` in [tests/mock_spanner.rs](tests/mock_spanner.rs).

## apache/arrow-adbc

- [ ] **UP-13 — Async-aware Rust traits (informational).** `rust/core/src/sync.rs` exposes
  synchronous traits, so this driver bridges them to Tokio with `block_on`. The current bridge
  panics when entered from a Tokio worker thread. An async or executor-aware interface would
  avoid requiring this bridge; the synchronous API alone does not require a panic.

The local CON-1 mitigation for async-context panics was declined as out of scope on 2026-09-15.
