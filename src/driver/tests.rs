use super::*;
use adbc_core::error::{Error, Status};

pub(super) fn new_database() -> SpannerDatabase {
    SpannerDatabase::new(new_runtime().unwrap())
}

#[test]
fn ensure_scheme_adds_http_prefix() {
    assert_eq!(ensure_scheme("localhost:9010"), "http://localhost:9010");
    assert_eq!(ensure_scheme("http://host:1"), "http://host:1");
    assert_eq!(ensure_scheme("https://host:1"), "https://host:1");
}

#[test]
fn database_options_round_trip() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://localhost:9010".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_EMULATOR.into()),
        OptionValue::String("true".into()),
    )
    .unwrap();

    assert_eq!(
        db.get_option_string(OptionDatabase::Uri).unwrap(),
        "spanner:///projects/p/instances/i/databases/d"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_ENDPOINT.into()))
            .unwrap(),
        "http://localhost:9010"
    );
    assert!(db.emulator);
}

#[test]
fn omni_options_validate_before_connecting() {
    let mut db = new_database();
    let key = || OptionDatabase::Other(OPTION_INSTANCE_TYPE.into());
    assert_eq!(db.get_option_string(key()).unwrap(), "cloud");
    db.set_option(
        OptionDatabase::Uri,
        "spanner:///projects/default/instances/default/databases/test?spanner.instance_type=omni"
            .into(),
    )
    .unwrap();
    assert_eq!(db.get_option_string(key()).unwrap(), "omni");
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(
        error
            .message
            .contains("requires an explicit spanner.endpoint")
    );
    for invalid in ["", "Omni", "other"] {
        assert_eq!(
            db.set_option(key(), invalid.into()).unwrap_err().status,
            Status::InvalidArguments
        );
        assert_eq!(db.get_option_string(key()).unwrap(), "omni");
    }
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        "http://localhost:15000".into(),
    )
    .unwrap();
    db.set_option(OptionDatabase::Other(OPTION_EMULATOR.into()), "true".into())
        .unwrap();
    assert!(
        db.connect()
            .unwrap_err()
            .message
            .contains("cannot be combined")
    );
}

#[test]
fn debug_redacts_credential_fields() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.keyfile = Some("/etc/secret/key.json".into());
    db.keyfile_json = Some(r#"{"private_key":"SUPER-SECRET-PRIVATE-KEY"}"#.into());
    db.access_token = Some("ya29.LIVE-BEARER-TOKEN".into());

    let rendered = format!("{db:?}");

    assert!(
        !rendered.contains("SUPER-SECRET-PRIVATE-KEY"),
        "keyfile_json leaked: {rendered}"
    );
    assert!(
        !rendered.contains("ya29.LIVE-BEARER-TOKEN"),
        "access_token leaked: {rendered}"
    );
    assert!(
        !rendered.contains("/etc/secret/key.json"),
        "keyfile leaked: {rendered}"
    );
    assert!(
        rendered.contains(r#"keyfile: Some("<redacted>")"#),
        "keyfile presence not shown: {rendered}"
    );
    assert!(
        rendered.contains(r#"keyfile_json: Some("<redacted>")"#),
        "keyfile_json presence not shown: {rendered}"
    );
    assert!(
        rendered.contains(r#"access_token: Some("<redacted>")"#),
        "access_token presence not shown: {rendered}"
    );
    assert!(
        rendered.contains("projects/p/instances/i/databases/d"),
        "database not shown: {rendered}"
    );
}

#[test]
fn debug_shows_none_for_absent_credentials() {
    let db = new_database();
    let rendered = format!("{db:?}");
    assert!(
        rendered.contains("keyfile: None"),
        "absent keyfile not shown: {rendered}"
    );
    assert!(
        rendered.contains("access_token: None"),
        "absent access_token not shown: {rendered}"
    );
}

#[test]
fn debug_renders_the_client_stack_cache_presence_only() {
    // A built stack renders as `<client stack>`; tests/mock_spanner.rs covers that case.
    let db = new_database();
    let rendered = format!("{db:?}");
    assert!(
        rendered.contains("connected: None"),
        "absent client stack not shown: {rendered}"
    );
    // Driver managers share databases across threads.
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SpannerDatabase>();
}

#[test]
fn typed_option_getters_distinguish_unset_from_non_integer() {
    let mut db = new_database();

    let error = db
        .get_option_int(OptionDatabase::Other(OPTION_ENDPOINT.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(error.message.contains("is not set"), "{}", error.message);

    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
        OptionValue::String("900".into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_int(OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()))
            .unwrap(),
        900
    );
    assert_eq!(
        db.get_option_double(OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()))
            .unwrap(),
        900.0
    );

    // A set value that is not an integer is InvalidArguments; NotFound means unset or unknown.
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
    )
    .unwrap();
    let error = db.get_option_int(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("is not an integer"),
        "{}",
        error.message
    );
    let error = db.get_option_double(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn boolean_options_reject_int_typed_sets() {
    // An int set would not round-trip through the getters, which return "true"/"false".
    let mut db = new_database();
    let emulator = || OptionDatabase::Other(OPTION_EMULATOR.into());

    for i in [0, 1] {
        let error = db.set_option(emulator(), OptionValue::Int(i)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "Int({i})");
        assert!(error.message.contains(OPTION_EMULATOR), "{}", error.message);
        assert!(
            error.message.contains("\"true\"/\"false\""),
            "{}",
            error.message
        );
    }

    db.set_option(emulator(), OptionValue::String("true".into()))
        .unwrap();
    assert_eq!(db.get_option_string(emulator()).unwrap(), "true");
    db.set_option(emulator(), OptionValue::String("false".into()))
        .unwrap();
    assert_eq!(db.get_option_string(emulator()).unwrap(), "false");
}

#[test]
fn connecting_without_a_database_path_is_an_error() {
    let db = new_database();
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
}

#[test]
fn a_non_string_uri_is_rejected() {
    let mut db = new_database();
    let error = db
        .set_option(OptionDatabase::Uri, OptionValue::Int(42))
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn keyfile_path_round_trips_but_inline_json_is_write_only() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_KEYFILE.into()),
        OptionValue::String("/path/to/key.json".into()),
    )
    .unwrap();
    let secret_json = r#"{"type":"service_account","private_key":"SUPER-SECRET-PRIVATE-KEY"}"#;
    db.set_option(
        OptionDatabase::Other(OPTION_KEYFILE_JSON.into()),
        OptionValue::String(secret_json.into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_KEYFILE.into()))
            .unwrap(),
        "/path/to/key.json"
    );
    let error = db
        .get_option_string(OptionDatabase::Other(OPTION_KEYFILE_JSON.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(error.message.contains("write-only"), "{}", error.message);
    assert!(
        !error.message.contains("SUPER-SECRET-PRIVATE-KEY"),
        "keyfile_json leaked through get_option: {}",
        error.message
    );
    // Only reading it back is refused.
    assert_eq!(db.keyfile_json.as_deref(), Some(secret_json));
}

#[test]
fn unknown_database_option_is_not_implemented() {
    // ADBC distinguishes unknown options (NotImplemented) from invalid values.
    let mut db = new_database();
    let error = db
        .set_option(
            OptionDatabase::Other("this_option_does_not_exist".into()),
            OptionValue::String("x".into()),
        )
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

/// Databases run on their driver's runtime. `src/ffi/database/tests.rs` checks that the C ABI
/// uses one driver per process.
#[test]
fn every_database_a_driver_creates_shares_the_drivers_runtime() {
    let mut driver = SpannerDriver::try_new().unwrap();
    let first = driver.new_database().unwrap();
    let second = driver
        .new_database_with_opts([(
            OptionDatabase::Uri,
            OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
        )])
        .unwrap();

    assert!(
        Arc::ptr_eq(&first.runtime, &second.runtime),
        "two databases from one driver must share its runtime"
    );
    assert!(Arc::ptr_eq(&driver.runtime, &first.runtime));

    // Separate drivers have separate runtimes, so the checks above are meaningful.
    let other = SpannerDriver::try_new().unwrap();
    assert!(!Arc::ptr_eq(&driver.runtime, &other.runtime));
}

#[test]
fn ensure_scheme_keeps_any_explicit_scheme() {
    assert_eq!(ensure_scheme(" localhost:9010 "), "http://localhost:9010");
    assert_eq!(ensure_scheme("[::1]:9010"), "http://[::1]:9010");
    assert_eq!(ensure_scheme("HTTPS://host:1"), "HTTPS://host:1");
    assert_eq!(ensure_scheme(""), "");
}

/// tonic refuses to dial an endpoint without a scheme, so the documented
/// `spanner://host:port/...` form must reach the host as plaintext.
#[test]
fn a_uri_host_authority_endpoint_is_dialed() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String(format!(
            "spanner://127.0.0.1:{port}/projects/p/instances/i/databases/d?spanner.emulator=true"
        )),
    )
    .unwrap();
    // The listener speaks no gRPC, so the connect attempt never succeeds; leave it running.
    let connecting = std::thread::spawn(move || db.connect().map(|_| ()));
    let start = std::time::Instant::now();
    while listener.accept().is_err() {
        assert!(
            !connecting.is_finished(),
            "connect gave up without dialing the endpoint"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "the URI authority endpoint was never dialed"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// `""` used to be dialed as an endpoint, failing with `Internal` and hiding
/// `SPANNER_EMULATOR_HOST`.
#[test]
fn a_blank_endpoint_unsets_it() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
    )
    .unwrap();
    let key = || OptionDatabase::Other(OPTION_ENDPOINT.into());
    db.set_option(key(), "http://127.0.0.1:1".into()).unwrap();
    for blank in ["", "  "] {
        db.set_option(key(), blank.into()).unwrap();
        assert_eq!(
            db.get_option_string(key()).unwrap_err().status,
            Status::NotFound
        );
        let target = db.resolve_target(Some("localhost:9010")).unwrap();
        assert_eq!(target.endpoint.as_deref(), Some("http://localhost:9010"));
        assert!(target.emulator);
        assert_eq!(db.resolve_target(None).unwrap().endpoint, None);
    }
}

#[test]
fn emulator_mode_without_an_endpoint_is_rejected() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String(
            "spanner:///projects/p/instances/i/databases/d?spanner.emulator=true".into(),
        ),
    )
    .unwrap();
    let error = db.resolve_target(None).unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(
        error.message.contains("needs an endpoint"),
        "{}",
        error.message
    );

    let target = db.resolve_target(Some("localhost:9010")).unwrap();
    assert_eq!(target.endpoint.as_deref(), Some("http://localhost:9010"));
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        "localhost:9010".into(),
    )
    .unwrap();
    assert!(db.resolve_target(None).unwrap().emulator);
}

fn connect_timeout_key() -> OptionDatabase {
    OptionDatabase::Other(OPTION_RPC_TIMEOUT_CONNECT.into())
}

#[test]
fn connect_timeout_option_round_trips() {
    let mut db = new_database();
    let not_found = |db: &SpannerDatabase| {
        db.get_option_string(connect_timeout_key())
            .unwrap_err()
            .status
    };
    assert_eq!(not_found(&db), Status::NotFound);
    assert_eq!(db.connect_timeout(), Some(DEFAULT_CONNECT_TIMEOUT));

    db.set_option(connect_timeout_key(), " 2.5 ".into())
        .unwrap();
    assert_eq!(db.get_option_string(connect_timeout_key()).unwrap(), "2.5");
    assert_eq!(db.get_option_double(connect_timeout_key()).unwrap(), 2.5);
    assert_eq!(db.connect_timeout(), Some(Duration::from_millis(2500)));

    db.set_option(connect_timeout_key(), OptionValue::Int(0))
        .unwrap();
    assert_eq!(db.get_option_string(connect_timeout_key()).unwrap(), "0");
    assert_eq!(db.connect_timeout(), None, "0 disables the deadline");

    db.set_option(connect_timeout_key(), OptionValue::Double(7.0))
        .unwrap();
    assert_eq!(db.get_option_int(connect_timeout_key()).unwrap(), 7);
    for invalid in ["-1", "abc", "inf", "NaN", "1e300"] {
        let error = db
            .set_option(connect_timeout_key(), invalid.into())
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{invalid}");
    }
    assert_eq!(
        db.get_option_string(connect_timeout_key()).unwrap(),
        "7",
        "a rejected value keeps the previous one"
    );

    db.set_option(connect_timeout_key(), "".into()).unwrap();
    assert_eq!(not_found(&db), Status::NotFound);
    assert_eq!(db.connect_timeout(), Some(DEFAULT_CONNECT_TIMEOUT));
}

/// Connect on another thread, so a connect without a deadline fails the test instead of hanging
/// it. Returns the error and how long the attempt took.
fn connect_in_background(db: Arc<SpannerDatabase>) -> std::sync::mpsc::Receiver<(Error, Duration)> {
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let start = std::time::Instant::now();
        let error = db
            .new_connection()
            .expect_err("nothing serves Spanner, so connecting must fail");
        let _ = sender.send((error, start.elapsed()));
    });
    receiver
}

fn database_with_connect_timeout(endpoint_port: u16, seconds: &str) -> SpannerDatabase {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String(format!(
            "spanner://127.0.0.1:{endpoint_port}/projects/p/instances/i/databases/d\
             ?spanner.emulator=true"
        )),
    )
    .unwrap();
    db.set_option(connect_timeout_key(), seconds.into())
        .unwrap();
    db
}

fn assert_connect_timed_out(db: &SpannerDatabase, port: u16, error: &Error, elapsed: Duration) {
    assert_eq!(error.status, Status::Timeout, "{}", error.message);
    for needle in [
        format!("http://127.0.0.1:{port}"),
        "projects/p/instances/i/databases/d".to_owned(),
        OPTION_RPC_TIMEOUT_CONNECT.to_owned(),
    ] {
        assert!(error.message.contains(&needle), "{}", error.message);
    }
    assert!(
        !error.message.contains("SPANNER_EMULATOR_HOST"),
        "the endpoint was explicit: {}",
        error.message
    );
    assert!(elapsed >= Duration::from_millis(400), "{elapsed:?}");
    assert!(
        db.connected.lock().unwrap().is_none(),
        "a timed-out connect must not be cached"
    );
}

/// The client retries a refused connection without limit; the connect timeout ends it.
#[test]
fn connecting_to_a_closed_port_times_out() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let db = Arc::new(database_with_connect_timeout(port, "0.5"));
    let (error, elapsed) = connect_in_background(db.clone())
        .recv_timeout(Duration::from_secs(20))
        .expect("connecting to a closed port must end at the connect timeout");
    assert_connect_timed_out(&db, port, &error, elapsed);
}

/// A listener that accepts but never answers stalls the session RPC, not just the dial.
#[test]
fn connecting_to_a_silent_listener_times_out_without_blocking_debug() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let db = Arc::new(database_with_connect_timeout(port, "1.5"));
    let result = connect_in_background(db.clone());

    // Hold the accepted socket open without replying.
    let start = std::time::Instant::now();
    let _socket = loop {
        match listener.accept() {
            Ok((socket, _)) => break socket,
            Err(_) => {
                assert!(
                    start.elapsed() < Duration::from_secs(10),
                    "the endpoint was never dialed"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    };
    // The connecting thread holds the cache lock now; `Debug` must not wait for it.
    let rendered = format!("{db:?}");
    assert!(
        rendered.contains("connected: Some(\"<connecting>\")"),
        "{rendered}"
    );

    let (error, elapsed) = result
        .recv_timeout(Duration::from_secs(20))
        .expect("a silent endpoint must end at the connect timeout");
    assert_connect_timed_out(&db, port, &error, elapsed);
    assert!(format!("{db:?}").contains("connected: None"));
}

#[test]
fn the_timeout_error_can_tell_an_endpoint_from_the_environment() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("spanner:///projects/p/instances/i/databases/d".into()),
    )
    .unwrap();
    assert!(
        db.resolve_target(Some("localhost:9010"))
            .unwrap()
            .endpoint_from_env
    );
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        "localhost:9010".into(),
    )
    .unwrap();
    assert!(
        !db.resolve_target(Some("localhost:9010"))
            .unwrap()
            .endpoint_from_env
    );
}
