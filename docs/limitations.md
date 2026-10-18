# Known limitations

These limitations apply to the dependency revisions pinned in [Cargo.toml](../Cargo.toml).
See the [README](../README.md#transaction-and-api-limits) for unsupported features and
[transactions](transactions.md) for transaction semantics.

## Manual transactions buffer writes

The Google Cloud client exposes read/write transactions through
`DatabaseClient::read_write_transaction()` and `TransactionRunner::run(work)`.
`ReadWriteTransaction` has no public constructor, commit or rollback. The driver therefore
buffers manual writes until commit and cannot mix them with snapshot queries or support
read-your-writes. Explicit transaction handles in the client would allow a different design.
The relevant upstream files are `src/spanner/src/transaction_runner.rs` and
`src/spanner/src/read_write_transaction.rs`.

## Streaming retries need operation deadlines

The Google Cloud client's query and BatchWrite retry loops construct a fresh `RetryState` for
each retry decision, resetting its start time. As a result, `spanner.retry.max_elapsed_seconds`
does not bound streaming retries. `spanner.retry.max_attempts` does bound attempts; use
query/fetch/update timeouts for an elapsed-time bound. See [retry tuning](options.md#retry-tuning).

The upstream loops in `src/spanner/src/result_set.rs` and
`src/spanner/src/batch_write_transaction.rs` would need to retain their start time through
`RetryState::set_start`. Driver configuration is in [src/retry.rs](../src/retry.rs); regression
coverage is in the `retry_max_*` and `batch_write_honors_retry_attempt_limit` tests in
[tests/mock_spanner.rs](../tests/mock_spanner.rs).

## Synchronous calls from Tokio

The ADBC Rust traits are synchronous. This driver bridges them to its own Tokio runtime with
`block_on`, which panics when called directly from an async Tokio task. Dropping the final
native Rust driver handle in an async context can also panic during runtime shutdown.

In async applications, create, use and drop the driver and its handles inside
`tokio::task::spawn_blocking` or a dedicated blocking thread, including consuming result readers.
See the [ADBC interface](adbc.md#objects-and-loading). An async ADBC interface could remove
the need for a synchronous bridge; the present restriction comes from this driver's runtime
implementation.
