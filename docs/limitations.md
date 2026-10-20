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

A manual commit also applies all buffered work at once, so it is capped at Spanner's 80,000
mutations per commit, while autocommit ingest commits in chunks. Python DB-API connections start
in manual mode, so large pandas or Polars writes need `autocommit=True`.

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

## Interrupted writes can hold locks

When a cancel or the `update` timeout interrupts DML, the driver drops the client's transaction.
The client then sends `Rollback` in the background if it knows the transaction ID. During the
first request, which begins the transaction inline, it doesn't know the ID yet, so nothing rolls
the transaction back. Its locks stay held until Spanner aborts it as idle, normally after about
10 seconds. Other transactions touching the same rows can wait until then.

## Synchronous calls from Tokio

The ADBC Rust traits are synchronous, and the driver runs them on its own Tokio runtime. Called
from a task on a multi-thread Tokio runtime, each call blocks its worker thread, after handing
that worker's other tasks to another thread with `tokio::task::block_in_place`. A current-thread
runtime, such as the `#[tokio::test]` default, can't do that, so a call from its own thread
panics, as with other blocking clients such as `reqwest::blocking`.

In async applications, prefer to use the driver and its handles inside
`tokio::task::spawn_blocking` or on a dedicated thread, which works on either kind of runtime.
That includes reading results. An async ADBC interface would remove this restriction.

## Forked processes

A forked child, such as a Python `multiprocessing` worker started with the `fork` method,
inherits the driver's state but none of its runtime threads. Databases, connections and
statements created before the fork fail with `InvalidState` in the child. Databases created in
the child start a new runtime and work normally.
