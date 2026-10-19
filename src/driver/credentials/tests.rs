use super::*;
use crate::driver::tests::new_database;
use crate::runtime::new_runtime;
use crate::{OPTION_IMPERSONATE_DELEGATES, OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES};
use adbc_core::Optionable;
use adbc_core::error::Status;
use adbc_core::options::{OptionDatabase, OptionValue};

// The emulator tests set the `emulator` field because parallel tests cannot safely change
// `SPANNER_EMULATOR_HOST`, which resolves to the same flag. The guards fire before any network I/O.
#[test]
fn emulator_mode_with_an_explicit_keyfile_is_refused() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.keyfile = Some("/path/to/key.json".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("emulator mode"));
    assert!(error.message.contains(OPTION_KEYFILE));
    assert!(error.message.contains("`spanner.emulator` option"));
}

#[test]
fn emulator_mode_with_explicit_keyfile_json_is_refused() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.keyfile_json = Some("{\"type\":\"service_account\"}".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains(OPTION_KEYFILE_JSON));
}

#[test]
fn emulator_mode_with_an_impersonation_target_is_refused() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.impersonate_target_principal = Some("target@project.iam.gserviceaccount.com".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains(OPTION_IMPERSONATE_TARGET_PRINCIPAL));
}

#[test]
fn emulator_mode_with_an_access_token_is_refused() {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.access_token = Some("ya29.test-token".into());
    assert_eq!(db.explicit_credential_option(), Some(OPTION_ACCESS_TOKEN));
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("emulator mode"));
    assert!(error.message.contains(OPTION_ACCESS_TOKEN));
}

#[test]
fn access_token_is_write_only() {
    let mut db = new_database();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
            .unwrap_err()
            .status,
        Status::NotFound
    );
    db.set_option(
        OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()),
        OptionValue::String("ya29.LIVE-BEARER-TOKEN".into()),
    )
    .unwrap();
    let error = db
        .get_option_string(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert!(error.message.contains("write-only"), "{}", error.message);
    assert!(
        !error.message.contains("ya29.LIVE-BEARER-TOKEN"),
        "access_token leaked through get_option: {}",
        error.message
    );
    // The bytes getter funnels through the same guard.
    let error = db
        .get_option_bytes(OptionDatabase::Other(OPTION_ACCESS_TOKEN.into()))
        .unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    // Only reading it back is refused.
    assert_eq!(db.access_token.as_deref(), Some("ya29.LIVE-BEARER-TOKEN"));
}

#[test]
fn access_token_conflicts_with_other_credential_options() {
    // Not tested through `connect()`: CI sets `SPANNER_EMULATOR_HOST`, so the emulator guard
    // would fire first.
    let base = || {
        let mut db = new_database();
        db.access_token = Some("ya29.test-token".into());
        db
    };
    assert_eq!(base().conflicting_credential_with_access_token(), None);
    for (mutate, expected) in [
        (
            Box::new(|db: &mut SpannerDatabase| db.keyfile = Some("/path/key.json".into()))
                as Box<dyn Fn(&mut SpannerDatabase)>,
            OPTION_KEYFILE,
        ),
        (
            Box::new(|db: &mut SpannerDatabase| {
                db.keyfile_json = Some("{\"type\":\"service_account\"}".into())
            }),
            OPTION_KEYFILE_JSON,
        ),
        (
            Box::new(|db: &mut SpannerDatabase| {
                db.impersonate_target_principal =
                    Some("target@project.iam.gserviceaccount.com".into())
            }),
            OPTION_IMPERSONATE_TARGET_PRINCIPAL,
        ),
    ] {
        let mut db = base();
        mutate(&mut db);
        assert_eq!(
            db.conflicting_credential_with_access_token(),
            Some(expected),
            "conflict: {expected}"
        );
    }
}

#[test]
fn access_token_credentials_send_a_bearer_authorization_header() {
    let credentials = build_static_token_credentials("ya29.the-token", None).unwrap();
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let resource = credentials.headers(Extensions::new()).await.unwrap();
        let (headers, tag) = match resource {
            CacheableResource::New { entity_tag, data } => (data, entity_tag),
            CacheableResource::NotModified => panic!("expected fresh headers"),
        };
        let value = headers.get(AUTHORIZATION).expect("authorization header");
        assert_eq!(value.to_str().unwrap(), "Bearer ya29.the-token");
        assert!(
            value.is_sensitive(),
            "the bearer token must be marked sensitive"
        );
        assert!(headers.get(QUOTA_PROJECT_HEADER).is_none());

        // A matching entity tag means the headers are unchanged.
        let mut extensions = Extensions::new();
        extensions.insert(tag);
        assert!(matches!(
            credentials.headers(extensions).await.unwrap(),
            CacheableResource::NotModified
        ));
    });
}

#[test]
fn access_token_with_illegal_header_characters_is_rejected_without_leaking() {
    const TOKEN: &str = "bad\ntoken-SECRET-do-not-leak";
    let error = build_static_token_credentials(TOKEN, None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains(OPTION_ACCESS_TOKEN),
        "{}",
        error.message
    );
    assert!(
        !error.message.contains("SECRET"),
        "access-token error leaked token material: {}",
        error.message
    );
}

#[test]
fn quota_project_option_round_trips_and_unsets() {
    let mut db = new_database();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()))
            .unwrap_err()
            .status,
        Status::NotFound
    );
    db.set_option(
        OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()),
        OptionValue::String("my-billing-project".into()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()))
            .unwrap(),
        "my-billing-project"
    );
    db.set_option(
        OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()),
        OptionValue::String(String::new()),
    )
    .unwrap();
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_QUOTA_PROJECT.into()))
            .unwrap_err()
            .status,
        Status::NotFound
    );
}

#[test]
fn quota_project_renders_in_debug_and_is_not_a_secret() {
    let mut db = new_database();
    db.quota_project = Some("my-billing-project".into());
    let rendered = format!("{db:?}");
    assert!(
        rendered.contains(r#"quota_project: Some("my-billing-project")"#),
        "quota_project not shown verbatim: {rendered}"
    );
}

#[test]
fn emulator_mode_with_a_quota_project_is_refused() {
    // The emulator ignores billing, so the project would be silently dropped.
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.emulator = true;
    db.quota_project = Some("my-billing-project".into());
    let error = db.connect().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);
    assert!(error.message.contains("emulator mode"));
    assert!(error.message.contains(OPTION_QUOTA_PROJECT));
}

#[test]
fn access_token_credentials_send_the_quota_project_header() {
    // The builder paths send the same header but only produce headers after a network token
    // fetch, so this is the one offline check of it.
    let credentials =
        build_static_token_credentials("ya29.the-token", Some("my-billing-project")).unwrap();
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let resource = credentials.headers(Extensions::new()).await.unwrap();
        let headers = match resource {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => panic!("expected fresh headers"),
        };
        let value = headers
            .get(QUOTA_PROJECT_HEADER)
            .expect("x-goog-user-project header");
        assert_eq!(value.to_str().unwrap(), "my-billing-project");
        assert!(
            !value.is_sensitive(),
            "the quota project is a project id, not a secret"
        );
        let auth = headers.get(AUTHORIZATION).expect("authorization header");
        assert_eq!(auth.to_str().unwrap(), "Bearer ya29.the-token");
        assert!(auth.is_sensitive());
    });
}

#[test]
fn quota_project_does_not_conflict_with_an_access_token() {
    let mut db = new_database();
    db.access_token = Some("ya29.test-token".into());
    db.quota_project = Some("my-billing-project".into());
    assert_eq!(db.conflicting_credential_with_access_token(), None);
    assert_eq!(db.explicit_credential_option(), Some(OPTION_ACCESS_TOKEN));
}

#[test]
fn ambient_adc_and_inert_impersonation_options_do_not_trip_the_emulator_guard() {
    let mut db = new_database();
    assert_eq!(db.explicit_credential_option(), None);
    db.impersonate_delegates = vec!["delegate@p.iam.gserviceaccount.com".into()];
    db.impersonate_scopes = vec!["https://www.googleapis.com/auth/cloud-platform".into()];
    db.impersonate_lifetime_secs = Some(900);
    assert_eq!(db.explicit_credential_option(), None);
    db.keyfile = Some("/path/to/key.json".into());
    assert_eq!(db.explicit_credential_option(), Some(OPTION_KEYFILE));
}

#[test]
fn the_credential_ladder_selects_one_flow_in_precedence_order() {
    let mut db = new_database();
    assert_eq!(db.credential_choice(false, false), CredentialChoice::Adc);
    // A quota project does not select a flow.
    db.quota_project = Some("billing-project".into());
    assert_eq!(db.credential_choice(false, false), CredentialChoice::Adc);

    db.keyfile = Some("/path/to/key.json".into());
    assert_eq!(
        db.credential_choice(false, false),
        CredentialChoice::Keyfile
    );
    db.keyfile = None;
    db.keyfile_json = Some("{\"type\":\"authorized_user\"}".into());
    assert_eq!(
        db.credential_choice(false, false),
        CredentialChoice::Keyfile
    );

    // Impersonation uses the keyfile as its source credential.
    db.impersonate_target_principal = Some("target@p.iam.gserviceaccount.com".into());
    assert_eq!(
        db.credential_choice(false, false),
        CredentialChoice::Impersonate
    );

    // `connect` refuses the remaining combinations before consulting the ladder.
    db.access_token = Some("ya29.token".into());
    assert_eq!(
        db.credential_choice(false, false),
        CredentialChoice::AccessToken
    );
    assert_eq!(
        db.credential_choice(true, false),
        CredentialChoice::Anonymous
    );
}

#[test]
fn the_credential_ladder_ignores_impersonation_options_without_a_target() {
    let mut db = new_database();
    db.impersonate_delegates = vec!["delegate@p.iam.gserviceaccount.com".into()];
    db.impersonate_scopes = vec!["https://www.googleapis.com/auth/cloud-platform".into()];
    db.impersonate_lifetime_secs = Some(900);
    assert_eq!(db.credential_choice(false, false), CredentialChoice::Adc);
    db.keyfile = Some("/path/to/key.json".into());
    assert_eq!(
        db.credential_choice(false, false),
        CredentialChoice::Keyfile
    );
}

/// Omni and plaintext endpoints replace ADC, and so ambient ADC and `GOOGLE_CLOUD_QUOTA_PROJECT`,
/// with anonymous credentials, while explicit options still win.
#[test]
fn anonymous_default_replaces_only_adc() {
    let mut db = new_database();
    assert_eq!(
        db.credential_choice(false, true),
        CredentialChoice::Anonymous
    );
    db.impersonate_delegates = vec!["delegate@p.iam.gserviceaccount.com".into()];
    assert_eq!(
        db.credential_choice(false, true),
        CredentialChoice::Anonymous
    );
    db.keyfile = Some("/path/to/key.json".into());
    assert_eq!(db.credential_choice(false, true), CredentialChoice::Keyfile);
    db.impersonate_target_principal = Some("target@p.iam.gserviceaccount.com".into());
    assert_eq!(
        db.credential_choice(false, true),
        CredentialChoice::Impersonate
    );
    db.keyfile = None;
    db.impersonate_target_principal = None;
    db.access_token = Some("ya29.token".into());
    assert_eq!(
        db.credential_choice(false, true),
        CredentialChoice::AccessToken
    );
}

fn omni_database(endpoint: &str) -> SpannerDatabase {
    let mut db = new_database();
    db.database = Some("projects/p/instances/i/databases/d".into());
    db.instance_type = google_cloud_spanner::omni::InstanceType::Omni;
    db.endpoint = Some(endpoint.into());
    db
}

/// Without explicit credentials, a quota project used to switch anonymous Omni to ADC and send
/// Google tokens to the Omni host.
#[test]
fn anonymous_omni_refuses_a_quota_project() {
    for endpoint in [
        "http://omni.internal:15000",
        "omni:15000",
        "https://omni.internal",
    ] {
        let mut db = omni_database(endpoint);
        db.quota_project = Some("my-billing-project".into());
        // Omni ignores `SPANNER_EMULATOR_HOST`, so `connect` fails the same way in CI.
        for error in [
            db.resolve_target(None).unwrap_err(),
            db.resolve_target(Some("localhost:9010")).unwrap_err(),
            db.connect().unwrap_err(),
        ] {
            assert_eq!(error.status, Status::InvalidState, "{endpoint}");
            assert!(error.message.contains("Spanner Omni"), "{}", error.message);
            assert!(
                error.message.contains(OPTION_QUOTA_PROJECT),
                "{}",
                error.message
            );
            assert!(error.message.contains("anonymously"), "{}", error.message);
        }

        // Explicit credentials carry the quota project.
        db.access_token = Some("ya29.token".into());
        let target = db.resolve_target(None).unwrap();
        assert!(target.anonymous_default && !target.emulator);
        assert_eq!(
            db.credential_choice(target.emulator, target.anonymous_default),
            CredentialChoice::AccessToken
        );
    }
}

#[test]
fn a_plaintext_endpoint_without_credentials_refuses_a_quota_project() {
    // A scheme other than https is dialed without TLS too, so it counts as plaintext.
    for endpoint in [
        "http://10.0.0.5:9010",
        "HTTP://10.0.0.5:9010",
        "10.0.0.5:9010",
        "grpc://10.0.0.5:9010",
    ] {
        let mut db = new_database();
        db.database = Some("projects/p/instances/i/databases/d".into());
        db.endpoint = Some(endpoint.into());
        let target = db.resolve_target(None).unwrap();
        assert!(target.anonymous_default, "{endpoint}");
        assert_eq!(
            db.credential_choice(target.emulator, target.anonymous_default),
            CredentialChoice::Anonymous,
            "{endpoint}"
        );

        db.quota_project = Some("my-billing-project".into());
        let error = db.resolve_target(None).unwrap_err();
        assert_eq!(error.status, Status::InvalidState, "{endpoint}");
        assert!(error.message.contains("plaintext"), "{}", error.message);
        assert!(
            error.message.contains(OPTION_QUOTA_PROJECT),
            "{}",
            error.message
        );

        db.keyfile = Some("/path/to/key.json".into());
        let target = db.resolve_target(None).unwrap();
        assert_eq!(
            db.credential_choice(target.emulator, target.anonymous_default),
            CredentialChoice::Keyfile
        );
    }
}

#[test]
fn a_tls_cloud_endpoint_keeps_adc_and_the_quota_project() {
    for endpoint in [
        None,
        Some("https://spanner.example.com"),
        Some("HTTPS://h:443"),
    ] {
        let mut db = new_database();
        db.database = Some("projects/p/instances/i/databases/d".into());
        db.endpoint = endpoint.map(str::to_owned);
        db.quota_project = Some("my-billing-project".into());
        let target = db.resolve_target(None).unwrap();
        assert!(!target.anonymous_default, "{endpoint:?}");
        assert_eq!(
            db.credential_choice(target.emulator, target.anonymous_default),
            CredentialChoice::Adc
        );
    }
}

/// A missing `GOOGLE_APPLICATION_CREDENTIALS` file failed with `Internal` when the client loaded
/// ADC, but with `InvalidArguments` when the driver did.
#[test]
fn client_adc_failures_match_the_drivers_adc_failures() {
    const SECRET: &str = "leak-me-if-you-can-4c3b2a";
    let raw = ServiceAccountCredentials::new(serde_json::json!({
        "type": "service_account",
        "private_key": SECRET,
    }))
    .build()
    .unwrap_err();
    let error = from_client_builder(google_cloud_gax::client_builder::Error::cred(raw));
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("Application Default Credentials"),
        "{}",
        error.message
    );
    assert!(!error.message.contains(SECRET), "{}", error.message);

    let error = from_client_builder(google_cloud_gax::client_builder::Error::transport("bad"));
    assert_eq!(error.status, Status::Internal);
}

#[test]
fn missing_keyfile_is_an_error() {
    let mut db = new_database();
    db.keyfile = Some("/no/such/keyfile-does-not-exist.json".into());
    let error = db.credentials_json().unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn inline_keyfile_json_takes_precedence_over_path() {
    let mut db = new_database();
    db.keyfile = Some("/ignored/path.json".into());
    db.keyfile_json = Some("{\"inline\":true}".into());
    assert_eq!(
        db.credentials_json().unwrap(),
        Some("{\"inline\":true}".to_string())
    );
}

#[test]
fn malformed_credential_json_is_rejected() {
    let error = build_credentials_from_json("{ not valid json", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("invalid credential JSON key"));
}

#[test]
fn credential_json_without_a_type_is_rejected() {
    let error = build_credentials_from_json("{\"private_key\":\"x\"}", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("missing a string `type` field"));
    assert!(error.message.contains("service_account"));
}

#[test]
fn credential_json_with_a_non_string_type_is_rejected() {
    let error = build_credentials_from_json("{\"type\":42}", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("missing a string `type` field"));
}

#[test]
fn credential_json_with_an_unknown_type_is_rejected() {
    let error =
        build_credentials_from_json("{\"type\":\"gdch_service_account\"}", None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("unsupported credential `type`"));
    assert!(error.message.contains("gdch_service_account"));
    assert!(error.message.contains("external_account"));
}

// The builders spawn token-cache tasks and so need a runtime; tokens are fetched lazily.
#[test]
fn authorized_user_credential_json_is_accepted() {
    let json = r#"{
        "type": "authorized_user",
        "client_id": "test-client-id.apps.googleusercontent.com",
        "client_secret": "test-client-secret",
        "refresh_token": "test-refresh-token"
    }"#;
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let credentials = build_credentials_from_json(json, None).unwrap();
        let built = format!("{credentials:?}");
        assert!(built.contains("UserCredentials"), "{built}");
        assert!(!built.contains("ServiceAccount"), "{built}");
        assert!(built.contains("quota_project_id: None"), "{built}");

        let credentials = build_credentials_from_json(json, Some("billing-project")).unwrap();
        let built = format!("{credentials:?}");
        assert!(
            built.contains(r#"quota_project_id: Some("billing-project")"#),
            "{built}"
        );
    });
}

// An invalid key fails inside the service-account builder; the error names the type it reached.
#[test]
fn service_account_credential_json_is_routed_to_the_service_account_flow() {
    let error = build_credentials_from_json(
        "{\"type\":\"service_account\",\"private_key\":\"not-a-key\"}",
        None,
    )
    .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error
            .message
            .contains("failed to build service_account credentials")
    );
}

#[test]
fn credential_build_failure_never_leaks_key_material() {
    const SECRET: &str = "SUPER-SECRET-PRIVATE-KEY-DO-NOT-LEAK-abc123";
    let json = format!(
        "{{\"type\":\"service_account\",\"private_key\":\"{SECRET}\",\"private_key_id\":42}}"
    );
    let error = build_credentials_from_json(&json, None).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error
            .message
            .contains("failed to build service_account credentials"),
        "message should name the detected type: {}",
        error.message
    );
    assert!(
        !error.message.contains(SECRET),
        "credential-build error leaked key material: {}",
        error.message
    );
}

#[test]
fn scrub_credential_error_returns_fixed_phrase() {
    const SECRET: &str = "leak-me-if-you-can-9f8e7d";
    // `build_errors::Error` has no public constructor, so get one from a failing builder.
    let raw = ServiceAccountCredentials::new(serde_json::json!({
        "type": "service_account",
        "private_key": SECRET,
    }))
    .build()
    .unwrap_err();
    let scrubbed = scrub_credential_error(&raw);
    assert_eq!(scrubbed, "the credential JSON could not be parsed");
    assert!(
        !scrubbed.contains(SECRET),
        "scrubbed phrase must be a fixed string, got: {scrubbed}"
    );
}

#[test]
fn impersonation_options_round_trip_and_split() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()),
        OptionValue::String("target@project.iam.gserviceaccount.com".into()),
    )
    .unwrap();
    // Whitespace and a trailing comma are tolerated.
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_DELEGATES.into()),
        OptionValue::String("a@p.iam.gserviceaccount.com, b@p.iam.gserviceaccount.com,".into()),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_SCOPES.into()),
        OptionValue::String(
            "https://www.googleapis.com/auth/spanner.data,https://www.googleapis.com/auth/cloud-platform".into(),
        ),
    )
    .unwrap();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
        OptionValue::String("1800".into()),
    )
    .unwrap();

    assert_eq!(
        db.impersonate_target_principal.as_deref(),
        Some("target@project.iam.gserviceaccount.com")
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
        vec![
            "https://www.googleapis.com/auth/spanner.data".to_string(),
            "https://www.googleapis.com/auth/cloud-platform".to_string()
        ]
    );
    assert_eq!(db.impersonate_lifetime_secs, Some(1800));

    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()
        ))
        .unwrap(),
        "target@project.iam.gserviceaccount.com"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_IMPERSONATE_DELEGATES.into()))
            .unwrap(),
        "a@p.iam.gserviceaccount.com,b@p.iam.gserviceaccount.com"
    );
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()))
            .unwrap(),
        "1800"
    );
}

#[test]
fn impersonation_lifetime_defaults_to_one_hour_when_unset() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()),
        OptionValue::String("target@project.iam.gserviceaccount.com".into()),
    )
    .unwrap();
    assert_eq!(db.impersonate_lifetime_secs, None);
    let effective = Duration::from_secs(
        db.impersonate_lifetime_secs
            .unwrap_or(DEFAULT_IMPERSONATION_LIFETIME_SECS),
    );
    assert_eq!(effective, Duration::from_secs(3600));
}

#[test]
fn impersonation_target_is_disabled_by_default() {
    let db = new_database();
    assert!(db.impersonate_target_principal.is_none());
    assert!(db.impersonate_delegates.is_empty());
    assert!(db.impersonate_scopes.is_empty());
    assert_eq!(
        db.get_option_string(OptionDatabase::Other(
            OPTION_IMPERSONATE_TARGET_PRINCIPAL.into()
        ))
        .unwrap_err()
        .status,
        Status::NotFound
    );
}

#[test]
fn a_non_numeric_impersonation_lifetime_is_rejected() {
    let mut db = new_database();
    let error = db
        .set_option(
            OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
            OptionValue::String("not-a-number".into()),
        )
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("non-negative integer"));
}

#[test]
fn an_integer_impersonation_lifetime_is_accepted() {
    let mut db = new_database();
    db.set_option(
        OptionDatabase::Other(OPTION_IMPERSONATE_LIFETIME.into()),
        OptionValue::Int(900),
    )
    .unwrap();
    assert_eq!(db.impersonate_lifetime_secs, Some(900));
}

// The IAM `generateAccessToken` call only happens on first token use.
#[test]
fn impersonated_credentials_build_without_network() {
    let source_json = r#"{
        "type": "authorized_user",
        "client_id": "test-client-id.apps.googleusercontent.com",
        "client_secret": "test-client-secret",
        "refresh_token": "test-refresh-token"
    }"#;
    let runtime = new_runtime().unwrap();
    runtime.block_on(async {
        let source = build_credentials_from_json(source_json, None).unwrap();
        let impersonated = build_impersonated_credentials(
            source,
            "target@project.iam.gserviceaccount.com",
            &["delegate@project.iam.gserviceaccount.com".to_string()],
            &["https://www.googleapis.com/auth/cloud-platform".to_string()],
            Duration::from_secs(1200),
            Some("my-billing-project"),
        )
        .unwrap();

        // The impersonated builder's quota project is the one that applies.
        let built = format!("{impersonated:?}");
        assert!(built.contains("ImpersonatedServiceAccount"), "{built}");
        assert!(
            built.contains("source_credentials: Credentials { inner: UserCredentials"),
            "{built}"
        );
        assert!(
            built.contains(r#"quota_project_id: Some("my-billing-project")"#),
            "{built}"
        );
    });
}
