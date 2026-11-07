use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Duration;

use ::tonic;
use ::tonic::codegen::{BoxFuture, Service};
use google_cloud_lro::Poller;

use super::*;

const DATABASE: &str = "projects/p/instances/i/databases/d";
const OPERATION: &str = "projects/p/instances/i/databases/d/operations/ddl-test";

struct Recorded {
    polling: bool,
    request: WireUpdateDatabaseDdl,
    metadata: tonic::metadata::MetadataMap,
}

#[derive(Default)]
struct State {
    requests: Vec<Recorded>,
    responses: VecDeque<std::result::Result<WireOperation, tonic::Status>>,
    /// Polls left to answer by resetting their stream, as a lost connection would.
    reset_polls: usize,
    /// HTTP statuses to answer the next polls with instead of gRPC, as a proxy would.
    proxy_replies: VecDeque<u16>,
    /// Polls left to leave unanswered, as a silent network would.
    silent_polls: usize,
    submission_delay: Duration,
    poll_timeout: Option<Duration>,
}

// Both requests use a string field numbered 1 (database/name). Decode with the DDL
// type to inspect requests without a second mock service implementation.
#[derive(Clone)]
struct Mock<const POLLING: bool>(Arc<Mutex<State>>);

impl<const POLLING: bool> tonic::server::NamedService for Mock<POLLING> {
    const NAME: &'static str = if POLLING {
        "google.longrunning.Operations"
    } else {
        "google.spanner.admin.database.v1.DatabaseAdmin"
    };
}

impl<const POLLING: bool> tonic::server::UnaryService<WireUpdateDatabaseDdl> for Mock<POLLING> {
    type Response = WireOperation;
    type Future = BoxFuture<tonic::Response<WireOperation>, tonic::Status>;

    fn call(&mut self, request: tonic::Request<WireUpdateDatabaseDdl>) -> Self::Future {
        let (metadata, _, request) = request.into_parts();
        let mut state = self.0.lock().unwrap();
        state.requests.push(Recorded {
            polling: POLLING,
            request,
            metadata,
        });
        if POLLING && state.reset_polls > 0 {
            state.reset_polls -= 1;
            drop(state);
            panic!("the server task's panic resets the stream");
        }
        if POLLING && state.silent_polls > 0 {
            state.silent_polls -= 1;
            return Box::pin(std::future::pending());
        }
        let delay = if POLLING {
            Duration::ZERO
        } else {
            state.submission_delay
        };
        let response = state.responses.pop_front().expect("unexpected admin RPC");
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            response.map(tonic::Response::new)
        })
    }
}

impl<const POLLING: bool> Service<http::Request<tonic::body::Body>> for Mock<POLLING> {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            if POLLING {
                "/google.longrunning.Operations/GetOperation"
            } else {
                "/google.spanner.admin.database.v1.DatabaseAdmin/UpdateDatabaseDdl"
            }
        );
        if POLLING && let Some(status) = self.0.lock().unwrap().proxy_replies.pop_front() {
            let reply = http::Response::builder()
                .status(status)
                .header("content-type", "text/html")
                .body(tonic::body::Body::empty())
                .unwrap();
            return Box::pin(async { Ok(reply) });
        }
        let service = self.clone();
        Box::pin(async move {
            Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                .unary(service, request)
                .await)
        })
    }
}

fn run(
    responses: Vec<std::result::Result<WireOperation, tonic::Status>>,
) -> (Result<()>, Vec<Recorded>) {
    run_with(State {
        responses: responses.into(),
        ..Default::default()
    })
}

fn run_with(state: State) -> (Result<()>, Vec<Recorded>) {
    let runtime = crate::runtime::new_runtime().unwrap();
    let mut database = crate::driver::SpannerDatabase::new(runtime.clone());
    database.access_token = Some("admin-test-token".into());
    database.quota_project = Some("billing-project".into());
    let credentials = database
        .build_credentials(
            crate::driver::credentials::CredentialChoice::AccessToken,
            None,
        )
        .unwrap();
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ClientConfig::default();
        config.endpoint = Some(format!("http://{}", listener.local_addr().unwrap()));
        config.cred = Some(credentials);
        let poll_timeout = state.poll_timeout.unwrap_or(POLL_TIMEOUT);
        let state = Arc::new(Mutex::new(state));
        let incoming = futures_util::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(Mock::<false>(state.clone()))
                .add_service(Mock::<true>(state.clone()))
                .serve_with_incoming(incoming),
        );
        let client = database_admin(config, poll_timeout).await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            client
                .update_database_ddl()
                .set_database(DATABASE)
                .set_statements(["CREATE TABLE t (id INT64) PRIMARY KEY (id)"])
                .set_operation_id("ddl-test")
                .set_proto_descriptors(vec![1, 2, 3])
                .poller()
                .until_done(),
        )
        .await
        .expect("admin operation hung");
        server.abort();
        let requests = std::mem::take(&mut state.lock().unwrap().requests);
        (result, requests)
    })
}

#[test]
fn ddl_and_operation_polling_use_grpc_with_credentials_and_routing() {
    let (result, requests) = run(vec![pending(), done()]);
    result.unwrap();
    assert_eq!(requests.len(), 2);
    assert!(!requests[0].polling);
    assert!(requests[1].polling);
    assert_eq!(requests[0].request.database, DATABASE);
    assert_eq!(
        requests[0].request.statements,
        ["CREATE TABLE t (id INT64) PRIMARY KEY (id)"]
    );
    assert_eq!(requests[0].request.operation_id, "ddl-test");
    assert_eq!(requests[0].request.proto_descriptors, [1, 2, 3]);
    assert_eq!(requests[1].request.database, OPERATION);
    for request in &requests {
        assert_eq!(
            request.metadata.get("authorization").unwrap(),
            "Bearer admin-test-token"
        );
        assert_eq!(
            request.metadata.get("x-goog-user-project").unwrap(),
            "billing-project"
        );
        assert!(request.metadata.contains_key("x-goog-api-client"));
        let key = if request.polling { "name" } else { "database" };
        assert_eq!(
            request
                .metadata
                .get("x-goog-request-params")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("{key}={}", request.request.database.replace('/', "%2F"))
        );
    }
}

#[test]
fn terminal_operation_errors_are_returned_without_polling_again() {
    let (result, requests) = run(vec![
        pending(),
        Ok(WireOperation {
            name: OPERATION.into(),
            done: true,
            result: Some(OperationResult::Error(WireStatus {
                code: 14,
                message: "schema change failed".into(),
                details: Vec::new(),
            })),
        }),
    ]);
    let error = result.unwrap_err();
    assert_eq!(
        error.status().unwrap().code,
        google_cloud_gax::error::rpc::Code::Unavailable
    );
    assert_eq!(requests.len(), 2);
}

#[test]
fn ambiguous_ddl_failures_are_not_retried() {
    let (result, requests) = run(vec![Err(tonic::Status::unavailable(
        "connection lost after submit",
    ))]);
    assert_eq!(
        result.unwrap_err().status().unwrap().code,
        google_cloud_gax::error::rpc::Code::Unavailable
    );
    assert_eq!(requests.len(), 1);
}

fn pending() -> std::result::Result<WireOperation, tonic::Status> {
    Ok(WireOperation {
        name: OPERATION.into(),
        ..Default::default()
    })
}

fn done() -> std::result::Result<WireOperation, tonic::Status> {
    Ok(WireOperation {
        name: OPERATION.into(),
        done: true,
        result: Some(OperationResult::Response(prost_types::Any {
            type_url: "type.googleapis.com/google.protobuf.Empty".into(),
            value: Vec::new(),
        })),
    })
}

#[test]
fn polling_outlasts_a_reset_stream() {
    let (result, requests) = run_with(State {
        responses: vec![pending(), done()].into(),
        reset_polls: 1,
        ..Default::default()
    });
    result.unwrap();
    assert_eq!(requests.len(), 3);
}

/// Polling can repeat a read, so a poll gets a deadline. The submission can't be repeated.
#[test]
fn a_silent_poll_is_followed_by_another_and_only_polls_have_a_deadline() {
    let poll_timeout = Duration::from_millis(200);
    let (result, requests) = run_with(State {
        // Spares in case a loaded runner times out a poll that was answered.
        responses: vec![pending(), done(), done(), done()].into(),
        silent_polls: 1,
        submission_delay: poll_timeout * 3,
        poll_timeout: Some(poll_timeout),
        ..Default::default()
    });
    result.unwrap();
    assert!(requests.len() >= 3, "{}", requests.len());
}

#[test]
fn a_proxy_refusing_a_poll_ends_polling_but_a_failing_one_does_not() {
    for refusal in [401, 403, 404] {
        let (result, requests) = run_with(State {
            responses: vec![pending()].into(),
            proxy_replies: vec![refusal].into(),
            ..Default::default()
        });
        assert!(result.unwrap_err().is_transport(), "HTTP {refusal}");
        assert_eq!(requests.len(), 1, "HTTP {refusal}");
    }
    let (result, requests) = run_with(State {
        responses: vec![pending(), done()].into(),
        proxy_replies: vec![500, 502, 503].into(),
        ..Default::default()
    });
    result.unwrap();
    assert_eq!(requests.len(), 2);
}
