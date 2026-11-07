//! An in-process mock Spanner server for unit tests and `tests/mock_spanner.rs`, which includes this
//! file by path and so reaches the driver through `super`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver};
use spanner_grpc_mock::MockSpanner;
use spanner_grpc_mock::google::spanner::v1;

use super::{OPTION_EMULATOR, OPTION_ENDPOINT, SpannerConnection, SpannerDatabase, SpannerDriver};

pub(crate) const DATABASE_URI: &str = "spanner:///projects/p/instances/i/databases/d";

pub(crate) struct MockServer {
    pub(crate) endpoint: String,
    server: tokio::task::JoinHandle<()>,
    finished: Arc<AtomicBool>,
    _runtime: tokio::runtime::Runtime,
}

impl MockServer {
    /// Serve `mock`'s expectations, then sessions and an `UNIMPLEMENTED` answer to every other
    /// RPC. An unmatched call would panic the server task, which the client retries as a
    /// transport error forever, and a watchdog aborts a test still running after two minutes.
    pub(crate) fn serve(mut mock: MockSpanner) -> Self {
        mock.expect_create_session().returning(|request| {
            Ok(tonic::Response::new(v1::Session {
                name: format!("{}/sessions/mock", request.into_inner().database),
                multiplexed: true,
                ..Default::default()
            }))
        });
        macro_rules! reject {
            ($($method:ident),+) => {
                $(
                    mock.$method().returning(|_| {
                        Err(tonic::Status::unimplemented(concat!(
                            "mock spanner server: unscripted ",
                            stringify!($method),
                        )))
                    });
                )+
            };
        }
        reject!(
            expect_batch_create_sessions,
            expect_get_session,
            expect_list_sessions,
            expect_delete_session,
            expect_execute_sql,
            expect_execute_streaming_sql,
            expect_execute_batch_dml,
            expect_read,
            expect_streaming_read,
            expect_begin_transaction,
            expect_commit,
            expect_rollback,
            expect_partition_query,
            expect_partition_read,
            expect_batch_write,
            expect_fetch_cache_update
        );

        let finished = Arc::new(AtomicBool::new(false));
        let watched = finished.clone();
        let test = std::thread::current()
            .name()
            .unwrap_or("a mock server test")
            .to_string();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(120));
            if !watched.load(Ordering::SeqCst) {
                eprintln!("watchdog: {test} still running after two minutes; aborting");
                std::process::abort();
            }
        });

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (endpoint, server) = runtime
            .block_on(spanner_grpc_mock::start("127.0.0.1:0", mock))
            .unwrap();
        Self {
            endpoint,
            server,
            finished,
            _runtime: runtime,
        }
    }

    pub(crate) fn connect(&self) -> SpannerConnection {
        emulator_database(&self.endpoint).new_connection().unwrap()
    }
}

/// A database in emulator mode, which skips credentials and TLS. The explicit endpoint overrides
/// any ambient `SPANNER_EMULATOR_HOST`.
pub(crate) fn emulator_database(endpoint: &str) -> SpannerDatabase {
    SpannerDriver::try_new()
        .unwrap()
        .new_database_with_opts([
            (OptionDatabase::Uri, OptionValue::from(DATABASE_URI)),
            (OPTION_ENDPOINT.into(), endpoint.into()),
            (OPTION_EMULATOR.into(), "true".into()),
        ])
        .unwrap()
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::SeqCst);
        self.server.abort();
    }
}
