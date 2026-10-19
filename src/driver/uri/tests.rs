use super::*;
use crate::driver::tests::new_database;
use adbc_core::error::Status;

const DB_PATH: &str = "projects/p/instances/i/databases/d";

fn set_uri(db: &mut SpannerDatabase, uri: &str) -> Result<()> {
    db.set_option(OptionDatabase::Uri, OptionValue::String(uri.into()))
}

#[test]
fn a_bare_database_path_is_rejected() {
    let mut db = new_database();
    let error = set_uri(&mut db, DB_PATH).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("spanner:///projects/"));
}

#[test]
fn malformed_uris_do_not_echo_credentials() {
    let secret = "ya29.private-token";
    for uri in [
        format!("postgres:///{DB_PATH}?{OPTION_ACCESS_TOKEN}={secret}"),
        format!("{DB_PATH}?{OPTION_ACCESS_TOKEN}={secret}"),
        format!("spanner:/{DB_PATH}?{OPTION_ACCESS_TOKEN}={secret}"),
        format!("spanner:///{secret}?{OPTION_ACCESS_TOKEN}={secret}"),
        format!("spanner:///{DB_PATH}?{OPTION_ACCESS_TOKEN}={secret}"),
        format!("spanner:///{DB_PATH}?{OPTION_ACCESS_TOKEN}%={secret}"),
        format!("spanner:///{DB_PATH}?{OPTION_KEYFILE_JSON}={secret}%"),
        format!("spanner:///{DB_PATH}?spanner.auth.keyfile={secret}%"),
    ] {
        let mut db = new_database();
        let error = set_uri(&mut db, &uri).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(!error.message.contains(secret), "{}", error.message);
        assert_eq!(db.uri, None);
    }
}

#[test]
fn uri_userinfo_is_rejected_without_storing_or_echoing_it() {
    let mut db = new_database();
    let original = format!("spanner:///{DB_PATH}");
    set_uri(&mut db, &original).unwrap();
    let error = set_uri(
        &mut db,
        &format!("spanner://user:private-password@localhost:9010/{DB_PATH}"),
    )
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("userinfo"));
    assert!(error.message.contains("spanner.auth."));
    assert!(!error.message.contains("private-password"));
    assert_eq!(db.uri.as_deref(), Some(original.as_str()));
    assert_eq!(db.endpoint, None);
    assert!(!format!("{db:?}").contains("private-password"));
}

#[test]
fn a_scheme_uri_sets_the_database_path() {
    for uri in [
        format!("spanner:///{DB_PATH}"),
        format!("Spanner:///{DB_PATH}"), // schemes are case-insensitive
    ] {
        let mut db = new_database();
        set_uri(&mut db, &uri).unwrap();
        assert_eq!(db.database.as_deref(), Some(DB_PATH), "uri: {uri}");
        assert_eq!(db.endpoint, None, "uri: {uri}");
    }
}

#[test]
fn a_scheme_uri_without_two_slashes_is_rejected() {
    for uri in [format!("spanner:{DB_PATH}"), format!("spanner:/{DB_PATH}")] {
        let mut db = new_database();
        let error = set_uri(&mut db, &uri).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "uri: {uri}");
        assert!(error.message.contains("spanner://"), "uri: {uri}");
    }
}

#[test]
fn a_cloudspanner_scheme_is_not_recognised() {
    // The JDBC driver's scheme.
    let mut db = new_database();
    let uri = format!("cloudspanner:///{DB_PATH}?spanner.emulator=true");
    let error = set_uri(&mut db, &uri).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn a_host_authority_becomes_the_endpoint() {
    let mut db = new_database();
    set_uri(&mut db, &format!("spanner://emu-host:9010/{DB_PATH}")).unwrap();
    assert_eq!(db.database.as_deref(), Some(DB_PATH));
    assert_eq!(db.endpoint.as_deref(), Some("emu-host:9010"));
}

#[test]
fn query_parameters_set_database_options() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!(
            "spanner:///{DB_PATH}?spanner.endpoint=http://localhost:9010\
             &spanner.emulator=true"
        ),
    )
    .unwrap();
    assert_eq!(db.database.as_deref(), Some(DB_PATH));
    assert_eq!(db.endpoint.as_deref(), Some("http://localhost:9010"));
    assert!(db.emulator);
}

#[test]
fn every_non_secret_database_level_option_is_accepted_as_a_query_parameter() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!(
            "spanner:///{DB_PATH}\
             ?spanner.auth.keyfile=/path/key.json\
             &spanner.auth.impersonate.target_principal=target%40p.iam.gserviceaccount.com\
             &spanner.auth.impersonate.delegates=a%40p.iam.gserviceaccount.com,b%40p.iam.gserviceaccount.com\
             &spanner.auth.impersonate.scopes=https://www.googleapis.com/auth/cloud-platform\
             &spanner.auth.impersonate.lifetime=900\
             &spanner.auth.quota_project=billing-project\
             &spanner.rpc.timeout_seconds.connect=2.5"
        ),
    )
    .unwrap();
    assert_eq!(db.keyfile.as_deref(), Some("/path/key.json"));
    assert_eq!(db.connect_timeout_secs, Some(2.5));
    assert_eq!(db.quota_project.as_deref(), Some("billing-project"));
    assert_eq!(
        db.impersonate_target_principal.as_deref(),
        Some("target@p.iam.gserviceaccount.com")
    );
    assert_eq!(
        db.impersonate_delegates,
        vec![
            "a@p.iam.gserviceaccount.com".to_string(),
            "b@p.iam.gserviceaccount.com".to_string()
        ]
    );
    assert_eq!(
        db.impersonate_scopes,
        vec!["https://www.googleapis.com/auth/cloud-platform".to_string()]
    );
    assert_eq!(db.impersonate_lifetime_secs, Some(900));
}

#[test]
fn an_explicit_option_set_after_the_uri_wins() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.endpoint=http://from-uri:9010"),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://explicit:9010".into()),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://explicit:9010"));
}

#[test]
fn a_uri_set_after_an_explicit_option_overwrites_only_what_it_carries() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://explicit:9010".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_EMULATOR.into()),
        OptionValue::String("true".into()),
    )
    .unwrap();
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.endpoint=http://from-uri:9010"),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://from-uri:9010"));
    assert!(db.emulator);
    set_uri(&mut db, "spanner:///projects/p2/instances/i2/databases/d2").unwrap();
    assert_eq!(
        db.database.as_deref(),
        Some("projects/p2/instances/i2/databases/d2")
    );
    assert_eq!(db.endpoint.as_deref(), Some("http://from-uri:9010"));
    assert!(db.emulator);
}

#[test]
fn a_query_parameter_beats_the_host_authority() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!("spanner://authority:9010/{DB_PATH}?spanner.endpoint=http://param:9010"),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://param:9010"));
}

#[test]
fn an_unknown_query_parameter_is_rejected_by_name() {
    let mut db = new_database();
    let error = set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.databoost=1"),
    )
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("spanner.databoost"));
    // The URI path is the only way to name the database.
    let error = set_uri(&mut db, &format!("spanner:///{DB_PATH}?uri=x")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("uri"));
}

#[test]
fn secret_bearing_query_parameters_are_rejected() {
    for (key, value) in [
        (
            OPTION_KEYFILE_JSON,
            "%7B%22type%22%3A%22service_account%22%7D",
        ),
        (OPTION_ACCESS_TOKEN, "ya29.uri-token"),
    ] {
        let mut db = new_database();
        let error = set_uri(&mut db, &format!("spanner:///{DB_PATH}?{key}={value}")).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "key: {key}");
        assert!(error.message.contains(key), "{}", error.message);
        assert!(error.message.contains("secret"), "{}", error.message);
        assert_eq!(db.keyfile_json, None, "key: {key}");
        assert_eq!(db.access_token, None, "key: {key}");

        // The option itself still works.
        let decoded = percent_decode(value).unwrap();
        db.set_option(
            OptionDatabase::Other(key.into()),
            OptionValue::String(decoded.clone()),
        )
        .unwrap();
        let stored = match key {
            OPTION_KEYFILE_JSON => db.keyfile_json.as_deref(),
            _ => db.access_token.as_deref(),
        };
        assert_eq!(stored, Some(decoded.as_str()), "key: {key}");
    }
}

#[test]
fn a_rejected_uri_leaves_the_configuration_untouched() {
    let mut db = new_database();
    set_uri(&mut db, &format!("spanner:///{DB_PATH}")).unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_ENDPOINT.into()),
        OptionValue::String("http://kept:9010".into()),
    )
    .unwrap();
    for bad in [
        "spanner:///projects/p2/instances/i2/databases/d2?bogus.key=1".to_string(),
        "spanner:///projects/p2/instances/i2/databases/d2?spanner.emulator=maybe".to_string(),
        "spanner://host:9010/projects/p2/instances/i2/databases/d2?spanner.auth.keyfile=%G1"
            .to_string(),
        format!(
            "spanner://host:9010/projects/p2/instances/i2/databases/d2?{OPTION_ACCESS_TOKEN}=ya29.x"
        ),
    ] {
        let error = set_uri(&mut db, &bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "uri: {bad}");
        assert_eq!(db.database.as_deref(), Some(DB_PATH), "uri: {bad}");
        assert_eq!(
            db.endpoint.as_deref(),
            Some("http://kept:9010"),
            "uri: {bad}"
        );
        assert!(!db.emulator, "uri: {bad}");
    }
}

#[test]
fn malformed_percent_encoding_is_rejected() {
    for bad in ["%G1", "%1", "%", "a%+5b"] {
        let error = percent_decode(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "input: {bad}");
        assert!(error.message.contains("percent-encoding"), "input: {bad}");
    }
    // RFC 3986, not form encoding: `+` stays a literal plus.
    assert_eq!(percent_decode("a+b%20c%3D1").unwrap(), "a+b c=1");
    let error = percent_decode("%FF%FE").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("UTF-8"));
}

#[test]
fn a_uri_with_a_bad_database_path_is_rejected() {
    let mut db = new_database();
    for bad in [
        "spanner:///",
        "spanner:///projects/p",
        "spanner:///projects//instances/i/databases/d",
        "spanner:///databases/d/instances/i/projects/p",
        "spanner:///projects/p/instances/i/databases/d/",
        "spanner:///projects/p%2Fx/instances/i/databases/d",
        "spanner:///projects/p/instances/i/databases/%2F",
    ] {
        let error = set_uri(&mut db, bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "uri: {bad}");
        assert!(error.message.contains("database path"), "uri: {bad}");
    }
    // Two slashes make `projects` the host authority.
    let error = set_uri(&mut db, &format!("spanner://{DB_PATH}")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("host authority"));
    assert!(error.message.contains("spanner:///projects/"));
}

#[test]
fn database_path_segments_are_percent_decoded() {
    let mut db = new_database();
    let uri = "spanner:///projects/example.com%3Amy-project/instances/i/databases/d";
    set_uri(&mut db, uri).unwrap();
    assert_eq!(
        db.database.as_deref(),
        Some("projects/example.com:my-project/instances/i/databases/d")
    );
    assert_eq!(db.uri.as_deref(), Some(uri));
}

#[test]
fn a_uri_fragment_is_rejected() {
    let mut db = new_database();
    let error = set_uri(&mut db, &format!("spanner:///{DB_PATH}#frag")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("#fragment"));
}

#[test]
fn get_option_uri_returns_the_uri_verbatim() {
    // adbc.h: `GetOption` returns the option value as set.
    let uri = format!(
        "spanner:///{DB_PATH}?spanner.endpoint=http://localhost:9010&spanner.emulator=true"
    );
    let mut db = new_database();
    set_uri(&mut db, &uri).unwrap();
    assert_eq!(db.get_option_string(OptionDatabase::Uri).unwrap(), uri);
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_ENDPOINT.into()))
            .unwrap(),
        "http://localhost:9010"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_EMULATOR.into()))
            .unwrap(),
        "true"
    );
}

#[test]
fn get_option_uri_is_not_found_until_a_uri_is_set() {
    let db = new_database();
    let error = db.get_option_string(OptionDatabase::Uri).unwrap_err();
    assert_eq!(error.status, Status::NotFound);
}

#[test]
fn the_last_uri_set_is_the_one_returned() {
    let mut db = new_database();
    set_uri(&mut db, &format!("spanner:///{DB_PATH}")).unwrap();
    let last = "spanner://host:9010/projects/p2/instances/i2/databases/d2?spanner.emulator=true";
    set_uri(&mut db, last).unwrap();
    assert_eq!(db.get_option_string(OptionDatabase::Uri).unwrap(), last);
    assert_eq!(
        db.database.as_deref(),
        Some("projects/p2/instances/i2/databases/d2")
    );
}

#[test]
fn a_uri_survives_a_set_get_set_replay() {
    // Dumping options and replaying them must reproduce the same state.
    let uri = format!(
        "spanner://authority:9010/{DB_PATH}\
         ?spanner.endpoint=http%3A%2F%2Flocalhost%3A9010\
         &spanner.emulator=true\
         &spanner.auth.keyfile=/path/key.json"
    );
    let mut db = new_database();
    set_uri(&mut db, &uri).unwrap();
    let dumped = db.get_option_string(OptionDatabase::Uri).unwrap();
    assert_eq!(dumped, uri);

    let mut replayed = new_database();
    set_uri(&mut replayed, &dumped).unwrap();
    assert_eq!(
        replayed.get_option_string(OptionDatabase::Uri).unwrap(),
        uri
    );
    assert_eq!(replayed.database, db.database);
    assert_eq!(replayed.endpoint, db.endpoint);
    assert_eq!(replayed.emulator, db.emulator);
    assert_eq!(replayed.keyfile, db.keyfile);
}

#[test]
fn a_rejected_uri_is_never_retrievable() {
    let mut db = new_database();
    for bad in [
        format!("spanner:///{DB_PATH}?{OPTION_ACCESS_TOKEN}=ya29.uri-token"),
        format!("spanner:///{DB_PATH}?{OPTION_KEYFILE_JSON}=%7B%7D"),
        format!("spanner:///{DB_PATH}?bogus.key=1"),
        format!("spanner:/{DB_PATH}"),
        DB_PATH.to_string(),
    ] {
        assert!(set_uri(&mut db, &bad).is_err(), "uri: {bad}");
        let error = db.get_option_string(OptionDatabase::Uri).unwrap_err();
        assert_eq!(error.status, Status::NotFound, "uri: {bad}");
    }

    // Nor does a rejected URI replace a stored one.
    let good = format!("spanner:///{DB_PATH}");
    set_uri(&mut db, &good).unwrap();
    assert!(set_uri(&mut db, &format!("spanner:///{DB_PATH}?bogus.key=1")).is_err());
    assert_eq!(db.get_option_string(OptionDatabase::Uri).unwrap(), good);
}

#[test]
fn uri_query_parameter_values_are_percent_decoded() {
    let mut db = new_database();
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?spanner.endpoint=http%3A%2F%2Flocalhost%3A9010"),
    )
    .unwrap();
    assert_eq!(db.endpoint.as_deref(), Some("http://localhost:9010"));
    // Keys are decoded too, and empty segments are tolerated.
    set_uri(
        &mut db,
        &format!("spanner:///{DB_PATH}?&spanner%2Eemulator=true&"),
    )
    .unwrap();
    assert!(db.emulator);
}
