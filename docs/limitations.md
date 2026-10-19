# Known limitations

These limitations come from the Google Cloud client and ADBC revisions pinned in
[Cargo.toml](../Cargo.toml). The [README](../README.md#limitations) lists unsupported features,
and [transactions](transactions.md) describes transaction behavior.

## Manual transactions buffer writes

The Google Cloud client only exposes read/write transactions through
`DatabaseClient::read_write_transaction()` and `TransactionRunner::run(work)`, which may replay
`work`. `ReadWriteTransaction` has no public constructor, commit or rollback. So the driver
buffers manual writes until commit, can't mix them with snapshot queries, and can't offer
read-your-writes. Explicit transaction handles in the client would allow this; see
`src/spanner/src/transaction_runner.rs` and `src/spanner/src/read_write_transaction.rs` upstream.

## Streaming retries ignore the elapsed-time limit

The client's query and BatchWrite retry loops create a fresh `RetryState` for each retry
decision, which resets its start time. As a result, `spanner.retry.max_elapsed_seconds` doesn't
bound streaming retries, though `spanner.retry.max_attempts` does. Use the query, fetch and update
timeouts to bound elapsed time; see [retry tuning](options.md#retry-tuning).

The fix belongs upstream: the loops in `src/spanner/src/result_set.rs` and
`src/spanner/src/batch_write_transaction.rs` should keep their start time with
`RetryState::set_start`. The driver side is in [src/retry.rs](../src/retry.rs), and the
`retry_max_*` and `batch_write_honors_retry_attempt_limit` tests in
[tests/mock_spanner.rs](../tests/mock_spanner.rs) cover the current behavior.

## Synchronous calls from Tokio

The ADBC Rust traits are synchronous. The driver runs them on its own Tokio runtime with
`block_on`, which panics when called from inside an async Tokio task. Dropping the last handle to
a Rust driver from async code can also panic while the runtime shuts down.

In async applications, create, use and drop the driver and its handles inside
`tokio::task::spawn_blocking` or on a dedicated thread. That includes reading results. An async
ADBC interface would remove this restriction.
