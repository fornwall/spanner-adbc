//! Transport fault tests against a Spanner emulator behind Toxiproxy.
//!
//! ```sh
//! scripts/with-toxiproxy.sh cargo test --test resilience -- --test-threads=1
//! ```
//!
//! Tests skip unless `TOXIPROXY_URL` and `SPANNER_EMULATOR_HOST` are set. See
//! `tests/RESILIENCE.md` for coverage and limitations.

use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::ArrowError;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_lro::Poller;
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;
use google_cloud_spanner_admin_instance_v1::client::InstanceAdmin;
use google_cloud_spanner_admin_instance_v1::model::Instance;
use spanner_adbc::{SpannerConnection, SpannerDatabase, SpannerDriver, SpannerStatement};

// Same identifiers as tests/integration.rs.
const PROJECT: &str = "test-project";
const INSTANCE: &str = "test-instance";
const DATABASE: &str = "adbc-test";
const STREAM_TABLE: &str = "ResilStream";
const TXN_TABLE: &str = "ResilTxn";
// About 48 MB of rows. `execute()` returns once the ~12 MB gRPC receive buffer is full, so most
// of the result is fetched over the network while the reader is iterated, where the faults apply.
const STREAM_ROWS: i64 = 24000;
const PAYLOAD_BYTES: usize = 2000;
// Above the receive buffer, so `execute()` and the first batches succeed, but well below the
// full result.
const TRUNCATE_AFTER_BYTES: u64 = 20 * 1024 * 1024;

fn database_path() -> String {
    format!("projects/{PROJECT}/instances/{INSTANCE}/databases/{DATABASE}")
}

fn require_target() -> bool {
    matches!(
        std::env::var("ADBC_TEST_REQUIRE_TARGET").ok().as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// Claims the shared proxy and creates fixtures, or returns `None` when the suite is not
/// configured. Panics instead of skipping under `ADBC_TEST_REQUIRE_TARGET`.
fn begin() -> Option<(Toxiproxy, SerialGuard)> {
    let Some(toxi) = toxi() else {
        if require_target() {
            panic!(
                "ADBC_TEST_REQUIRE_TARGET is set; configure both TOXIPROXY_URL and \
                 SPANNER_EMULATOR_HOST to run the resilience suite"
            );
        }
        eprintln!("TOXIPROXY_URL / SPANNER_EMULATOR_HOST not set; skipping resilience tests");
        return None;
    };
    // After the skip check, so a plain multi-threaded `cargo test` still skips.
    let serial = SerialGuard::new();
    // A test that panicked between adding and removing a toxic would otherwise leave it active.
    toxi.reset();
    ensure_setup();
    Some((toxi, serial))
}

fn toxi() -> Option<Toxiproxy> {
    let url = std::env::var("TOXIPROXY_URL")
        .ok()
        .filter(|s| !s.is_empty())?;
    // Points at the proxy listener; the helper script sets both.
    std::env::var("SPANNER_EMULATOR_HOST")
        .ok()
        .filter(|s| !s.is_empty())?;
    let proxy = std::env::var("TOXIPROXY_PROXY").unwrap_or_else(|_| "spanner".to_string());
    Some(Toxiproxy { url, proxy })
}

/// Toxiproxy admin API client. It shells out to `curl`, which the harness scripts already need.
struct Toxiproxy {
    url: String,
    proxy: String,
}

impl Toxiproxy {
    /// Adds a downstream toxic with one attribute. Panics on a non-2xx response.
    fn add_toxic(&self, name: &str, kind: &str, attribute: &str, value: u64) {
        let body = format!(
            "{{\"name\":\"{name}\",\"type\":\"{kind}\",\"stream\":\"downstream\",\
              \"toxicity\":1.0,\"attributes\":{{\"{attribute}\":{value}}}}}"
        );
        let (code, out) = curl(&[
            "-s",
            "-X",
            "POST",
            &format!("{}/proxies/{}/toxics", self.url, self.proxy),
            "-H",
            "Content-Type: application/json",
            "-d",
            &body,
        ]);
        assert!(
            (200..300).contains(&code),
            "adding toxic failed (HTTP {code}): {out}\nbody was: {body}"
        );
    }

    /// Removes every toxic and re-enables the proxy. Panics on a non-2xx response.
    fn reset(&self) {
        let (code, out) = curl(&["-s", "-X", "POST", &format!("{}/reset", self.url)]);
        assert!(
            (200..300).contains(&code),
            "resetting toxiproxy failed (HTTP {code}): {out}"
        );
    }

    /// Ignores a missing toxic.
    fn remove_toxic(&self, name: &str) {
        let _ = curl(&[
            "-s",
            "-X",
            "DELETE",
            &format!("{}/proxies/{}/toxics/{}", self.url, self.proxy, name),
        ]);
    }

    /// Limits server-to-client throughput to `rate_kbps` KB/s.
    fn add_bandwidth(&self, name: &str, rate_kbps: u64) {
        self.add_toxic(name, "bandwidth", "rate", rate_kbps);
    }

    /// Resets the connection with a TCP RST after `timeout_ms`, or on the next data if 0.
    fn add_reset_peer(&self, name: &str, timeout_ms: u64) {
        self.add_toxic(name, "reset_peer", "timeout", timeout_ms);
    }

    /// Closes the connection cleanly after `bytes` have been sent to the client.
    fn add_limit_data(&self, name: &str, bytes: u64) {
        self.add_toxic(name, "limit_data", "bytes", bytes);
    }
}

/// Runs `curl` and returns `(http_status, body)`.
fn curl(args: &[&str]) -> (u16, String) {
    // `-w` appends the status code on its own line.
    let mut full: Vec<&str> = vec!["-w", "\n%{http_code}"];
    full.extend_from_slice(args);
    let output = Command::new("curl")
        .args(&full)
        .output()
        .expect("failed to run curl");
    let text = String::from_utf8_lossy(&output.stdout).to_string();
    let (body, code) = match text.rsplit_once('\n') {
        Some((b, c)) => (b.to_string(), c.trim().parse().unwrap_or(0)),
        None => (text, 0),
    };
    (code, body)
}

static ACTIVE_TESTS: AtomicUsize = AtomicUsize::new(0);

/// Fails a test that overlaps another, since all tests change faults on the same proxy.
struct SerialGuard;

impl SerialGuard {
    fn new() -> Self {
        let previous = ACTIVE_TESTS.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            previous, 0,
            "resilience tests share one proxy and must run serially; re-run with \
             `--test-threads=1`"
        );
        SerialGuard
    }
}

impl Drop for SerialGuard {
    fn drop(&mut self) {
        ACTIVE_TESTS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Creates the instance, database and tables through the emulator's admin port, bypassing the
/// proxy.
fn ensure_setup() {
    static DONE: Mutex<bool> = Mutex::new(false);
    let mut done = DONE.lock().expect("setup lock poisoned");
    if *done {
        return;
    }

    let direct = std::env::var("SPANNER_EMULATOR_DIRECT")
        .or_else(|_| std::env::var("SPANNER_EMULATOR_HOST"))
        .expect("SPANNER_EMULATOR_DIRECT or SPANNER_EMULATOR_HOST");
    let direct = direct.trim_end_matches('/');
    let host = direct
        .strip_prefix("http://")
        .unwrap_or(direct)
        .strip_suffix(":9010")
        .expect("the direct emulator endpoint must use gRPC port 9010");
    let endpoint = format!("http://{host}:9020");
    let credentials = AnonymousCredentials::new().build();

    // Creation errors are only logged: the resources may exist from an earlier run.
    tokio::runtime::Runtime::new()
        .expect("setup runtime")
        .block_on(async {
            let instance_admin = InstanceAdmin::builder()
                .with_endpoint(&endpoint)
                .with_credentials(credentials.clone())
                .build()
                .await
                .expect("instance admin");
            let instance = instance_admin
                .create_instance()
                .set_parent(format!("projects/{PROJECT}"))
                .set_instance_id(INSTANCE)
                .set_instance(
                    Instance::new()
                        .set_config(format!(
                            "projects/{PROJECT}/instanceConfigs/emulator-config"
                        ))
                        .set_display_name("ADBC resilience instance")
                        .set_node_count(1),
                )
                .poller()
                .until_done()
                .await;
            if let Err(e) = &instance {
                eprintln!("create_instance: {e}");
            }
            let database_admin = DatabaseAdmin::builder()
                .with_endpoint(&endpoint)
                .with_credentials(credentials)
                .build()
                .await
                .expect("database admin");
            let database = database_admin
                .create_database()
                .set_parent(format!("projects/{PROJECT}/instances/{INSTANCE}"))
                .set_create_statement(format!("CREATE DATABASE `{DATABASE}`"))
                .set_extra_statements(vec![
                    format!(
                        "CREATE TABLE {STREAM_TABLE} (Id INT64, Payload STRING(MAX)) \
                         PRIMARY KEY (Id)"
                    ),
                    format!("CREATE TABLE {TXN_TABLE} (Id INT64, Val INT64) PRIMARY KEY (Id)"),
                ])
                .poller()
                .until_done()
                .await;
            if let Err(e) = &database {
                eprintln!("create_database: {e}");
            }
        });

    *done = true;
}

/// Connects through the proxy, retrying while a new emulator database settles.
fn connect() -> SpannerConnection {
    let mut driver = SpannerDriver::try_new().expect("create driver");
    let database: SpannerDatabase = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String(format!("spanner:///{}", database_path())),
        )])
        .expect("create database");
    let mut last = None;
    for _ in 0..40 {
        match database.new_connection() {
            Ok(c) => return c,
            Err(e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }
    panic!("connect failed after retries: {last:?}");
}

fn run(connection: &mut SpannerConnection, sql: &str) {
    let mut s = connection.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    s.execute_update()
        .unwrap_or_else(|e| panic!("run {sql:?}: {e:?}"));
}

/// Refills the streaming table, in batches that stay under the commit size limit.
fn seed_stream_table(connection: &mut SpannerConnection) {
    run(
        connection,
        &format!("DELETE FROM {STREAM_TABLE} WHERE true"),
    );
    let batch = 500i64;
    let mut start = 1i64;
    while start <= STREAM_ROWS {
        let end = (start + batch - 1).min(STREAM_ROWS);
        run(
            connection,
            &format!(
                "INSERT INTO {STREAM_TABLE} (Id, Payload) \
                 SELECT n, REPEAT('x', {PAYLOAD_BYTES}) \
                 FROM UNNEST(GENERATE_ARRAY({start}, {end})) AS n"
            ),
        );
        start = end + 1;
    }
}

/// A full scan of the streaming table in 300-row batches, so many fetches follow `execute()`.
fn stream_statement(connection: &mut SpannerConnection) -> SpannerStatement {
    let mut statement = connection.new_statement().expect("new statement");
    statement
        .set_option(
            OptionStatement::Other(spanner_adbc::OPTION_ROWS_PER_BATCH.into()),
            OptionValue::Int(300),
        )
        .expect("set rows_per_batch");
    statement
        .set_sql_query(format!(
            "SELECT Id, Payload FROM {STREAM_TABLE} ORDER BY Id"
        ))
        .unwrap();
    statement
}

/// Resets the transaction table to the single row (Id=1, Val=0).
fn seed_txn_row(connection: &Arc<Mutex<SpannerConnection>>) {
    let mut conn = connection.lock().expect("connection lock");
    run(&mut conn, &format!("DELETE FROM {TXN_TABLE} WHERE true"));
    run(
        &mut conn,
        &format!("INSERT INTO {TXN_TABLE} (Id, Val) VALUES (1, 0)"),
    );
}

/// Cancelling a throttled stream fetch that is blocked on the network interrupts it promptly.
/// `cancel_between_stream_chunks_cancels_the_next_fetch` in `tests/integration.rs` covers a cancel
/// that lands between fetches.
#[test]
fn cancel_interrupts_in_flight_query() {
    let Some((toxi, _serial)) = begin() else {
        return;
    };
    let mut connection = connect();
    seed_stream_table(&mut connection);
    let mut statement = stream_statement(&mut connection);

    // At 2000 KB/s, the ~36 MB left after the receive buffer takes about 18 s to drain.
    toxi.add_bandwidth("resil_bandwidth", 2000);
    let reader = statement
        .execute()
        .expect("execute (first chunk) should succeed");

    // The reader shares the statement's cancel signal.
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let start = Instant::now();
        let result: Result<Vec<_>, _> = reader.collect();
        let elapsed = start.elapsed();
        let _ = tx.send((result.err(), elapsed));
    });

    // Let the worker drain the buffered rows and block on a throttled read. The cancel signal
    // latches, so a cancel between fetches would not be lost either; the sleep makes this test
    // exercise interrupting a read in progress.
    std::thread::sleep(Duration::from_millis(2000));
    let cancel_at = Instant::now();
    statement.get_cancel_handle().try_cancel().expect("cancel");

    let (error, worker_elapsed) = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("worker did not finish within 20s; cancel failed to interrupt the in-flight fetch");
    worker.join().ok();
    let cancel_latency = cancel_at.elapsed();

    // Remove the fault before asserting so a failure doesn't leave the proxy broken.
    toxi.remove_toxic("resil_bandwidth");

    let error = error.unwrap_or_else(|| {
        panic!(
            "cancelled streaming query should yield an error, got Ok. worker elapsed \
             {worker_elapsed:?}"
        )
    });
    let err_debug = format!("{error:?}");
    assert_eq!(
        adbc_error_of(&error).status,
        AdbcStatus::Cancelled,
        "error should be a cancellation, got: {err_debug}"
    );
    assert!(
        cancel_latency < Duration::from_secs(4),
        "cancel took {cancel_latency:?}; a full throttled read of the rest would take ~18s, so \
         cancellation did not interrupt the in-flight operation"
    );
    eprintln!(
        "cancel interrupted the in-flight streamed fetch in {cancel_latency:?} \
         (worker total {worker_elapsed:?}); error = {err_debug}"
    );
}

/// A query under `reset_peer` fails with a transport error once retries run out, and a later
/// query succeeds after the fault is removed.
#[test]
fn reset_peer_surfaces_error_then_recovers() {
    let Some((toxi, _serial)) = begin() else {
        return;
    };

    // Bound retries so the test doesn't wait out the client's default retry budget.
    let mut connection = connect();
    connection
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_RETRY_MAX_ATTEMPTS.into()),
            2i64.into(),
        )
        .unwrap();
    let connection = Arc::new(Mutex::new(connection));

    assert!(
        query_one(&connection).is_ok(),
        "baseline query (no toxics) should succeed"
    );

    toxi.add_reset_peer("resil_reset", 0);
    let faulted = run_query_bounded(&connection, Duration::from_secs(45));
    toxi.remove_toxic("resil_reset");

    match faulted {
        Some(Ok(())) => panic!(
            "query under reset_peer reported success; the RST never reached the driver \
             (toxiproxy problem?)"
        ),
        Some(Err(error)) => {
            assert_transport_error(&error, "the query run under a reset_peer toxic");
            eprintln!(
                "reset_peer surfaced {:?} (vendor_code {}): {}",
                error.status, error.vendor_code, error.message
            );
        }
        None => panic!("query under reset_peer hung past the watchdog; driver did not fail fast"),
    }

    assert!(
        recovers(&connection),
        "driver did not recover after the reset_peer toxic was removed"
    );
}

/// A buffered write survives a transport fault during commit. The pinned client retries
/// data-plane RPCs without an attempt cap (DML has `seqno` replay protection), so commit normally
/// blocks until the fault clears. If a future client returns the error instead, a retried commit
/// must replay the buffer. `SET Val = 1` is idempotent, so the check holds even if a faulted
/// attempt reached the emulator; a lost write leaves the seeded 0.
#[test]
fn commit_under_transport_fault_never_loses_the_write() {
    let Some((toxi, _serial)) = begin() else {
        return;
    };
    let connection = Arc::new(Mutex::new(connect()));
    seed_txn_row(&connection);

    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("false".into()),
        )
        .expect("disable autocommit");
    {
        let mut conn = connection.lock().expect("connection lock");
        let mut s = conn.new_statement().expect("new statement");
        s.set_sql_query(format!("UPDATE {TXN_TABLE} SET Val = 1 WHERE Id = 1"))
            .unwrap();
        assert_eq!(
            s.execute_update().expect("buffer update"),
            None,
            "manual mode must buffer the DML (count unknown until commit)"
        );
    }

    // Hold the fault for five seconds.
    toxi.add_reset_peer("resil_commit_reset", 0);
    let (tx, rx) = mpsc::channel();
    let commit_conn = connection.clone();
    let worker = std::thread::spawn(move || {
        let result = commit_conn.lock().expect("connection lock").commit();
        let _ = tx.send(result);
    });
    let during_fault = rx.recv_timeout(Duration::from_secs(5));
    toxi.remove_toxic("resil_commit_reset");

    match during_fault {
        // The pinned client is still retrying; allow for backoff and reconnection.
        Err(_) => {
            eprintln!("commit is blocked retrying under the fault (expected); removing the fault");
            let healed = rx
                .recv_timeout(Duration::from_secs(60))
                .expect("commit did not complete within 60s of the fault being removed");
            worker.join().expect("commit worker panicked");
            healed.expect("commit should succeed once the transport fault is gone");
        }
        Ok(Err(error)) => {
            eprintln!("commit surfaced the fault as an error ({error}); retrying the commit");
            worker.join().expect("commit worker panicked");
            let mut last = None;
            let retried =
                (0..20).any(
                    |_| match connection.lock().expect("connection lock").commit() {
                        Ok(()) => true,
                        Err(e) => {
                            last = Some(e);
                            std::thread::sleep(Duration::from_millis(500));
                            false
                        }
                    },
                );
            assert!(
                retried,
                "retried commit did not succeed after the fault was removed: {last:?}"
            );
        }
        Ok(Ok(())) => panic!(
            "commit reported success while the transport to the emulator was reset; \
             the fault did not take effect (toxiproxy problem?)"
        ),
    }

    assert_eq!(
        query_int(
            &connection,
            &format!("SELECT Val FROM {TXN_TABLE} WHERE Id = 1"),
        ),
        1,
        "the buffered update must not be lost by a commit under a transport fault"
    );

    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::AutoCommit,
            OptionValue::String("true".into()),
        )
        .expect("re-enable autocommit");
}

/// Under `reset_peer` an autocommit write retries forever (see the commit test), so
/// `spanner.rpc.timeout_seconds.update` must end it with `Timeout`, naming the option. After the
/// deadline is unset and the fault removed, the same write succeeds.
#[test]
fn update_timeout_bounds_a_faulted_write_then_recovers_when_unset() {
    let Some((toxi, _serial)) = begin() else {
        return;
    };
    let connection = Arc::new(Mutex::new(connect()));
    // Seeding also warms the session and channel before the fault.
    seed_txn_row(&connection);

    // Statements created below inherit the connection's deadline.
    let deadline_secs = 2.0_f64;
    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_RPC_TIMEOUT_UPDATE.into()),
            OptionValue::Double(deadline_secs),
        )
        .expect("set update timeout");

    toxi.add_reset_peer("resil_update_timeout_reset", 0);
    let (tx, rx) = mpsc::channel();
    let write_conn = connection.clone();
    let start = Instant::now();
    let worker = std::thread::spawn(move || {
        let mut conn = write_conn.lock().expect("connection lock");
        let mut s = conn.new_statement().expect("new statement");
        s.set_sql_query(format!("UPDATE {TXN_TABLE} SET Val = 1 WHERE Id = 1"))
            .unwrap();
        let result = s.execute_update();
        let _ = tx.send(result);
    });

    // If the deadline doesn't fire, the worker is leaked and the test fails here.
    let outcome = rx.recv_timeout(Duration::from_secs(30));
    let elapsed = start.elapsed();
    toxi.remove_toxic("resil_update_timeout_reset");

    let Ok(result) = outcome else {
        panic!(
            "execute_update under a transport fault hung past the 30s watchdog; the \
             spanner.rpc.timeout_seconds.update deadline ({deadline_secs}s) did not bound it"
        )
    };
    worker.join().ok();

    let error = result.expect_err(
        "a write under a persistent transport fault should fail with Timeout, not report success",
    );
    assert_eq!(
        error.status,
        AdbcStatus::Timeout,
        "the expired deadline must surface as ADBC Timeout; got: {error:?}"
    );
    assert!(
        error
            .message
            .contains(spanner_adbc::OPTION_RPC_TIMEOUT_UPDATE),
        "the timeout error must name the responsible option; got: {}",
        error.message
    );
    // Generous for retry backoff and scheduling, but well under the watchdog.
    assert!(
        elapsed < Duration::from_secs(15),
        "the timeout fired after {elapsed:?}; expected ~{deadline_secs}s, well under the watchdog"
    );

    // An empty string unsets the option.
    connection
        .lock()
        .expect("connection lock")
        .set_option(
            OptionConnection::Other(spanner_adbc::OPTION_RPC_TIMEOUT_UPDATE.into()),
            OptionValue::String(String::new()),
        )
        .expect("unset update timeout");
    let mut recovered = false;
    let mut last = None;
    for _ in 0..20 {
        let mut conn = connection.lock().expect("connection lock");
        let mut s = conn.new_statement().expect("new statement");
        s.set_sql_query(format!("UPDATE {TXN_TABLE} SET Val = 1 WHERE Id = 1"))
            .unwrap();
        match s.execute_update() {
            Ok(_) => {
                recovered = true;
                break;
            }
            Err(e) => {
                last = Some(e);
                drop(conn);
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
    assert!(
        recovered,
        "the write did not succeed after the deadline was unset and the fault removed: {last:?}"
    );

    assert_eq!(
        query_int(
            &connection,
            &format!("SELECT Val FROM {TXN_TABLE} WHERE Id = 1"),
        ),
        1,
        "the write must have applied once the deadline was unset and the transport healed"
    );
    eprintln!("update timeout bounded a faulted write in {elapsed:?}");
}

/// A reset after some batches were consumed fails a later fetch with a transport error; the
/// stream must not end as if complete. A fresh query then succeeds.
#[test]
fn mid_stream_disconnect_after_batches_surfaces_error_then_recovers() {
    let Some((toxi, _serial)) = begin() else {
        return;
    };
    let mut connection = connect();
    seed_stream_table(&mut connection);
    let mut statement = stream_statement(&mut connection);

    let mut reader = statement
        .execute()
        .expect("execute (first chunk) should succeed");

    // These batches were buffered by `execute()`, before any fault.
    let mut consumed_rows = 0usize;
    for _ in 0..3 {
        match reader.next() {
            Some(Ok(batch)) => consumed_rows += batch.num_rows(),
            Some(Err(e)) => panic!("a batch failed before any fault was injected: {e}"),
            None => break,
        }
    }
    assert!(
        consumed_rows > 0,
        "expected to consume at least one batch before injecting the fault"
    );

    toxi.add_reset_peer("resil_midstream_reset", 0);
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(drain_counting_rows(reader));
    });
    let outcome = rx.recv_timeout(Duration::from_secs(45));
    toxi.remove_toxic("resil_midstream_reset");

    let Ok((post_fault_rows, error)) = outcome else {
        panic!(
            "draining the reader after a mid-stream reset hung past the 45s watchdog; the driver \
             did not surface the disconnect"
        )
    };
    worker.join().ok();
    let error = error.unwrap_or_else(|| {
        panic!(
            "a mid-stream disconnect after {consumed_rows} rows were consumed must surface an \
             error, but the stream ended cleanly after {post_fault_rows} further rows; the \
             driver treated a truncated stream as complete"
        )
    });
    let error = adbc_error_of(&error);
    assert_transport_error(error, "the chunk fetch after a mid-stream reset");
    assert!(
        consumed_rows + post_fault_rows < STREAM_ROWS as usize,
        "the reset landed after all {STREAM_ROWS} rows had been delivered; nothing was \
         interrupted mid-stream"
    );
    eprintln!(
        "mid-stream disconnect surfaced {:?} after {consumed_rows} + {post_fault_rows} rows: {}",
        error.status, error.message
    );

    let connection = Arc::new(Mutex::new(connection));
    assert!(
        recovers(&connection),
        "driver did not recover after the mid-stream reset toxic was removed"
    );
}

/// `limit_data` closes the connection cleanly partway through the result. Unlike an RST, this
/// could be mistaken for the end of the stream, so the driver must report an error rather than a
/// short result. A fresh query then succeeds.
#[test]
fn truncated_stream_surfaces_error_then_recovers() {
    let Some((toxi, _serial)) = begin() else {
        return;
    };
    let mut connection = connect();
    seed_stream_table(&mut connection);
    let mut statement = stream_statement(&mut connection);

    // Install the cap before `execute()` so it counts from the start of the stream.
    toxi.add_limit_data("resil_truncate", TRUNCATE_AFTER_BYTES);
    let reader = statement
        .execute()
        .expect("execute (first chunk) should succeed under the byte cap");

    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = tx.send(drain_counting_rows(reader));
    });
    let outcome = rx.recv_timeout(Duration::from_secs(45));
    toxi.remove_toxic("resil_truncate");

    let Ok((rows, error)) = outcome else {
        panic!(
            "draining a truncated stream hung past the 45s watchdog; the driver did not surface \
             the truncation"
        )
    };
    worker.join().ok();
    let error = error.unwrap_or_else(|| {
        panic!(
            "a truncated stream must surface an error, but the stream ended cleanly with {rows} \
             rows; the driver treated a truncated result as complete (the query selects all \
             {STREAM_ROWS} rows)"
        )
    });
    assert!(
        rows > 0,
        "no rows were delivered before the truncation; the {TRUNCATE_AFTER_BYTES}-byte cap cut \
         the stream before any batch reached the consumer"
    );
    assert!(
        rows < STREAM_ROWS as usize,
        "all {STREAM_ROWS} rows arrived before the error; the byte cap did not truncate anything"
    );
    let error = adbc_error_of(&error);
    assert_transport_error(error, "the chunk fetch that ran past the truncation point");
    eprintln!(
        "truncated stream surfaced {:?} after {rows} of {STREAM_ROWS} rows: {}",
        error.status, error.message
    );

    let connection = Arc::new(Mutex::new(connection));
    assert!(
        recovers(&connection),
        "driver did not recover after the limit_data truncation toxic was removed"
    );
}

fn query_int(connection: &Arc<Mutex<SpannerConnection>>, sql: &str) -> i64 {
    let mut conn = connection.lock().expect("connection lock");
    let mut s = conn.new_statement().expect("new statement");
    s.set_sql_query(sql).unwrap();
    let batches: Vec<_> = s
        .execute()
        .expect("int query")
        .collect::<Result<Vec<_>, _>>()
        .expect("int query stream");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("INT64 column")
        .value(0)
}

/// Runs `SELECT 1`. Stream errors are unwrapped to the driver's own error so callers can check
/// its status.
fn query_one(connection: &Arc<Mutex<SpannerConnection>>) -> AdbcResult<()> {
    let mut conn = connection.lock().expect("connection lock");
    let mut s = conn.new_statement()?;
    s.set_sql_query("SELECT 1 AS one")?;
    let reader = s.execute()?;
    for batch in reader {
        batch.map_err(|e| adbc_error_of(&e).clone())?;
    }
    Ok(())
}

/// Retries `SELECT 1` for up to ten seconds while the channel reconnects.
fn recovers(connection: &Arc<Mutex<SpannerConnection>>) -> bool {
    for _ in 0..20 {
        if query_one(connection).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    false
}

/// Finds the driver's error inside a reader error, which wraps it in `ArrowError::ExternalError`.
fn adbc_error_of(error: &ArrowError) -> &AdbcError {
    std::iter::successors(Some(error as &(dyn std::error::Error + 'static)), |error| {
        error.source()
    })
    .find_map(|link| link.downcast_ref::<AdbcError>())
    .unwrap_or_else(|| panic!("the reader error does not wrap an ADBC error: {error:?}"))
}

/// Asserts that `error` is the driver's rendering of a Spanner client transport failure, not an
/// unrelated error. `error::from_spanner` prefixes `Spanner error:` and maps a transport error
/// without a gRPC status, which is what an RST or truncation usually produces, to `IO`, as it
/// does a gRPC `UNAVAILABLE` or `ABORTED`. A gRPC `INTERNAL` maps to `Internal`. Which one a TCP
/// fault becomes is up to the client.
fn assert_transport_error(error: &AdbcError, context: &str) {
    assert!(
        matches!(error.status, AdbcStatus::Internal | AdbcStatus::IO),
        "{context} must fail with a transport status (IO, or Internal for a gRPC INTERNAL), \
         got {:?} (vendor_code {}): {}",
        error.status,
        error.vendor_code,
        error.message
    );
    assert!(
        error.message.starts_with("Spanner error:"),
        "{context} must surface the Spanner client's own failure (a `Spanner error:` message from \
         error::from_spanner); got: {}",
        error.message
    );
}

/// Drains `reader` to its end or first error, returning the rows delivered before it.
fn drain_counting_rows(
    reader: impl Iterator<Item = Result<RecordBatch, ArrowError>>,
) -> (usize, Option<ArrowError>) {
    let mut rows = 0usize;
    for item in reader {
        match item {
            Ok(batch) => rows += batch.num_rows(),
            Err(error) => return (rows, Some(error)),
        }
    }
    (rows, None)
}

/// Runs [`query_one`] on a worker thread, returning `None` if it doesn't finish within `timeout`.
/// A hung worker is left behind.
fn run_query_bounded(
    connection: &Arc<Mutex<SpannerConnection>>,
    timeout: Duration,
) -> Option<AdbcResult<()>> {
    let conn = connection.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = query_one(&conn);
        let _ = tx.send(result);
    });
    rx.recv_timeout(timeout).ok()
}
