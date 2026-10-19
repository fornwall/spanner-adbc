use super::*;
use adbc_core::error::Status;

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
