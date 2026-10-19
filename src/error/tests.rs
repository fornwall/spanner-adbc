use super::*;

#[test]
fn maps_grpc_codes_to_adbc_status() {
    assert_eq!(status_for_grpc_code(Code::NotFound), Status::NotFound);
    assert_eq!(
        status_for_grpc_code(Code::AlreadyExists),
        Status::AlreadyExists
    );
    assert_eq!(
        status_for_grpc_code(Code::Unauthenticated),
        Status::Unauthenticated
    );
    assert_eq!(
        status_for_grpc_code(Code::PermissionDenied),
        Status::Unauthorized
    );
    assert_eq!(
        status_for_grpc_code(Code::InvalidArgument),
        Status::InvalidArguments
    );
    assert_eq!(status_for_grpc_code(Code::OutOfRange), Status::InvalidData);
    assert_eq!(
        status_for_grpc_code(Code::FailedPrecondition),
        Status::InvalidState
    );
    assert_eq!(status_for_grpc_code(Code::Unknown), Status::Unknown);
    assert_eq!(status_for_grpc_code(Code::DataLoss), Status::IO);
    assert_eq!(
        status_for_grpc_code(Code::DeadlineExceeded),
        Status::Timeout
    );
    assert_eq!(status_for_grpc_code(Code::Cancelled), Status::Cancelled);
    assert_eq!(status_for_grpc_code(Code::Unavailable), Status::IO);
    assert_eq!(status_for_grpc_code(Code::Aborted), Status::IO);
    assert_eq!(
        status_for_grpc_code(Code::Unimplemented),
        Status::NotImplemented
    );
}

#[test]
fn from_status_parts_maps_numeric_codes_like_from_spanner() {
    // Ingest error handling relies on BatchWrite failures matching the write-only path.
    let adbc = from_status_parts(Code::AlreadyExists as i32, "Row already exists", &[]);
    assert_eq!(adbc.status, Status::AlreadyExists);
    assert_eq!(adbc.vendor_code, 6);
    assert!(adbc.message.contains("Row already exists"));
    assert_eq!(adbc.details, None);
    assert_eq!(
        from_status_parts(Code::NotFound as i32, "no table", &[]).status,
        Status::NotFound
    );
    assert_eq!(
        from_status_parts(Code::InvalidArgument as i32, "bad", &[]).status,
        Status::InvalidArguments
    );
    let internal = from_status_parts(13, "boom", &[]);
    assert_eq!(internal.status, Status::Internal);
    assert_eq!(internal.vendor_code, 13);
}

#[test]
fn unmapped_codes_fall_back_to_internal() {
    for code in [Code::ResourceExhausted, Code::Internal, Code::Ok] {
        assert_eq!(status_for_grpc_code(code), Status::Internal);
    }
    // An unrecognized wire code decodes to `Code::Unknown`.
    assert_eq!(status_for_grpc_code(Code::from(9999)), Status::Unknown);
}

#[test]
fn maps_a_real_gax_status_error() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::NotFound)
            .set_message("Table not found: Nope"),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::NotFound);
    assert!(adbc.message.starts_with("Spanner error:"));
    assert_eq!(adbc.vendor_code, Code::NotFound as i32);
    assert_eq!(adbc.vendor_code, 5);
    assert_eq!(adbc.details, None);
}

#[test]
fn aborted_keeps_its_grpc_code_in_vendor_code() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::Aborted)
            .set_message("Transaction was aborted"),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::IO);
    assert_eq!(adbc.vendor_code, Code::Aborted as i32);
    assert_eq!(adbc.vendor_code, 10);
}

#[test]
fn unimplemented_maps_to_not_implemented() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::Unimplemented)
            .set_message("Operation not supported"),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::NotImplemented);
    assert_eq!(adbc.vendor_code, Code::Unimplemented as i32);
    assert_eq!(adbc.vendor_code, 12);
}

#[test]
fn errors_without_a_grpc_status_have_no_vendor_code() {
    use google_cloud_gax::error::Error as GaxError;
    let adbc = from_spanner(GaxError::deser("no structured status here"));
    assert_eq!(adbc.status, Status::Internal);
    assert_eq!(adbc.vendor_code, 0);
    assert_eq!(adbc.details, None);
}

/// Build a `StatusDetails` from its ProtoJSON encoding.
fn detail_from_json(value: serde_json::Value) -> StatusDetails {
    serde_json::from_value(value).expect("valid StatusDetails ProtoJSON")
}

#[test]
fn aborted_forwards_retry_info_detail() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let retry_info = serde_json::json!({
        "@type": "type.googleapis.com/google.rpc.RetryInfo",
        "retryDelay": "1s",
    });
    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::Aborted)
            .set_message("Transaction was aborted")
            .set_details([detail_from_json(retry_info.clone())]),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::IO);
    assert_eq!(adbc.vendor_code, Code::Aborted as i32);

    let details = adbc.details.expect("RetryInfo detail forwarded");
    assert_eq!(details.len(), 1);
    let (key, value) = &details[0];
    assert_eq!(key, "google.rpc.retryinfo");
    let parsed: serde_json::Value = serde_json::from_slice(value).expect("UTF-8 JSON value");
    assert_eq!(parsed, retry_info);
    assert_eq!(parsed["retryDelay"], "1s");
}

#[test]
fn multiple_details_forward_in_order_under_typed_keys() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::InvalidArgument)
            .set_message("Bad query")
            .set_details([
                detail_from_json(serde_json::json!({
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "TEST_REASON",
                    "domain": "spanner.googleapis.com",
                })),
                detail_from_json(serde_json::json!({
                    "@type": "type.googleapis.com/google.rpc.BadRequest",
                    "fieldViolations": [{"field": "sql", "description": "syntax error"}],
                })),
                detail_from_json(serde_json::json!({
                    "@type": "type.googleapis.com/google.rpc.Help",
                    "links": [{"description": "docs", "url": "https://example.invalid"}],
                })),
            ]),
    );
    let adbc = from_spanner(gax);
    let details = adbc.details.expect("details forwarded");
    let keys: Vec<&str> = details.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(
        keys,
        [
            "google.rpc.errorinfo",
            "google.rpc.badrequest",
            "google.rpc.help"
        ]
    );
    let error_info: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
    assert_eq!(error_info["reason"], "TEST_REASON");
    assert_eq!(error_info["domain"], "spanner.googleapis.com");
}

#[test]
fn permission_denied_preserves_the_message_and_appends_iam_guidance() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::PermissionDenied)
            .set_message(
                "Caller is missing IAM permission spanner.databases.select on resource \
             projects/p/instances/i/databases/d.",
            ),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::Unauthorized);
    assert_eq!(adbc.vendor_code, Code::PermissionDenied as i32);
    assert_eq!(adbc.vendor_code, 7);
    assert!(
        adbc.message
            .contains("Caller is missing IAM permission spanner.databases.select")
    );
    assert!(
        adbc.message.contains("grant an IAM role that includes it"),
        "expected the appended IAM guidance, got: {}",
        adbc.message
    );
    assert!(
        adbc.message
            .contains("https://cloud.google.com/spanner/docs/iam")
    );
    assert!(
        !adbc.message.contains("roles/spanner."),
        "guidance must name no specific IAM role, got: {}",
        adbc.message
    );
}

#[test]
fn permission_denied_guidance_is_added_even_without_a_named_permission() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::PermissionDenied)
            .set_message("Permission denied on resource database."),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::Unauthorized);
    assert!(adbc.message.contains("grant an IAM role that includes it"));
    assert!(
        adbc.message
            .contains("https://cloud.google.com/spanner/docs/iam")
    );
    assert!(!adbc.message.contains("roles/spanner."));
}

#[test]
fn non_permission_errors_get_no_iam_guidance() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    // The guidance depends on the code, not on the message mentioning a permission.
    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::NotFound)
            .set_message("Table not found; unrelated to spanner.databases.select"),
    );
    let adbc = from_spanner(gax);
    assert_eq!(adbc.status, Status::NotFound);
    assert!(
        !adbc.message.contains("cloud.google.com/spanner/docs/iam"),
        "only PERMISSION_DENIED should carry the IAM guidance, got: {}",
        adbc.message
    );
}

#[test]
fn from_status_parts_adds_iam_guidance_on_permission_denied() {
    let adbc = from_status_parts(
        Code::PermissionDenied as i32,
        "Caller is missing IAM permission spanner.databases.write on resource d.",
        &[],
    );
    assert_eq!(adbc.status, Status::Unauthorized);
    assert_eq!(adbc.vendor_code, 7);
    assert!(adbc.message.contains("spanner.databases.write"));
    assert!(adbc.message.contains("grant an IAM role that includes it"));
    assert!(
        adbc.message
            .contains("https://cloud.google.com/spanner/docs/iam")
    );
    assert!(!adbc.message.contains("roles/spanner."));
    let already = from_status_parts(Code::AlreadyExists as i32, "Row already exists", &[]);
    assert!(
        !already
            .message
            .contains("cloud.google.com/spanner/docs/iam")
    );
}

/// Build a `wkt::Any` from its ProtoJSON form, as found in BatchWrite responses.
fn any_from_json(value: serde_json::Value) -> Any {
    serde_json::from_value(value).expect("valid Any ProtoJSON")
}

#[test]
fn from_status_parts_forwards_status_details_like_from_spanner() {
    let adbc = from_status_parts(
        Code::AlreadyExists as i32,
        "Row [v0] in table MockTable already exists",
        &[
            any_from_json(serde_json::json!({
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": "DUPLICATE_KEY",
                "domain": "spanner.googleapis.com",
            })),
            any_from_json(serde_json::json!({
                "@type": "type.googleapis.com/google.rpc.BadRequest",
                "fieldViolations": [{"field": "Id", "description": "duplicate"}],
            })),
        ],
    );
    assert_eq!(adbc.status, Status::AlreadyExists);
    assert_eq!(adbc.vendor_code, 6);

    let details = adbc
        .details
        .expect("the group status' details must be forwarded");
    let keys: Vec<&str> = details.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(keys, ["google.rpc.errorinfo", "google.rpc.badrequest"]);
    let error_info: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
    assert_eq!(
        error_info["@type"],
        "type.googleapis.com/google.rpc.ErrorInfo"
    );
    assert_eq!(error_info["reason"], "DUPLICATE_KEY");
    assert_eq!(error_info["domain"], "spanner.googleapis.com");
    let bad_request: serde_json::Value = serde_json::from_slice(&details[1].1).unwrap();
    assert_eq!(bad_request["fieldViolations"][0]["field"], "Id");
}

#[test]
fn from_status_parts_keys_an_unrecognised_detail_off_its_type_url() {
    // Unknown detail types decode to `StatusDetails::Other` and are still forwarded.
    let custom = serde_json::json!({
        "@type": "type.googleapis.com/mycompany.CustomDetail",
        "foo": "bar",
    });
    let adbc = from_status_parts(13, "boom", &[any_from_json(custom.clone())]);
    let details = adbc.details.expect("custom detail forwarded");
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].0, "mycompany.customdetail");
    let parsed: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
    assert_eq!(parsed, custom);
}

#[test]
fn unrecognised_detail_keys_off_its_type_url() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let custom = serde_json::json!({
        "@type": "type.googleapis.com/mycompany.CustomDetail",
        "foo": "bar",
    });
    let gax = GaxError::service(
        RpcStatus::default()
            .set_code(Code::Internal)
            .set_message("boom")
            .set_details([detail_from_json(custom.clone())]),
    );
    let adbc = from_spanner(gax);
    let details = adbc.details.expect("custom detail forwarded");
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].0, "mycompany.customdetail");
    let parsed: serde_json::Value = serde_json::from_slice(&details[0].1).unwrap();
    assert_eq!(parsed, custom);
}

#[test]
fn unsupported_keeps_the_message_verbatim() {
    let error = unsupported("Substrait plans: Spanner executes GoogleSQL text, not Substrait");
    assert_eq!(error.status, Status::NotImplemented);
    assert_eq!(
        error.message,
        "Substrait plans: Spanner executes GoogleSQL text, not Substrait"
    );
    assert_eq!(
        not_implemented("Substrait plans").message,
        "Substrait plans is not supported by the Spanner ADBC driver"
    );
}

#[test]
fn option_not_set_covers_the_unknown_key_case_too() {
    let error = option_not_set("spanner.read.stalenesss");
    assert_eq!(error.status, Status::NotFound);
    assert_eq!(
        error.message,
        "option spanner.read.stalenesss is not set or not recognized by this driver"
    );
}

#[test]
fn annotate_rewrites_only_the_message() {
    let mut original = err("boom", Status::Internal);
    original.vendor_code = 42;
    original.details = Some(vec![("google.rpc.retryinfo".to_string(), b"{}".to_vec())]);
    let annotated = annotate(original, |m| format!("table \"t\": {m}"));
    assert_eq!(annotated.message, "table \"t\": boom");
    assert_eq!(annotated.status, Status::Internal);
    assert_eq!(annotated.vendor_code, 42);
    assert_eq!(
        annotated.details,
        Some(vec![("google.rpc.retryinfo".to_string(), b"{}".to_vec())])
    );
}

#[cfg(feature = "ffi")]
#[test]
fn chain_walks_from_the_outermost_error_to_the_innermost() {
    use arrow_schema::ArrowError;

    let cancelled = err("operation cancelled", Status::Cancelled);
    let inner = ArrowError::ExternalError(Box::new(cancelled));
    let outer = ArrowError::ExternalError(Box::new(inner));

    let links: Vec<String> = chain(&outer).map(ToString::to_string).collect();
    assert_eq!(
        links.len(),
        3,
        "outer, inner, and the driver error: {links:?}"
    );
    assert!(
        links
            .last()
            .is_some_and(|last| last.contains("operation cancelled")),
        "{links:?}"
    );
    let recovered = chain(&outer)
        .find_map(|link| link.downcast_ref::<Error>())
        .expect("the driver error must be reachable through the chain");
    assert_eq!(recovered.status, Status::Cancelled);

    let alone = err("no source", Status::Internal);
    assert_eq!(chain(&alone).count(), 1);
}

/// A retry loop that runs out of time returns `Error::exhausted(last_error)`.
#[test]
fn an_exhausted_retry_keeps_the_wrapped_grpc_status() {
    use google_cloud_gax::error::Error as GaxError;
    use google_cloud_gax::error::rpc::{Code, Status as RpcStatus};

    let service = || {
        GaxError::service(
            RpcStatus::default()
                .set_code(Code::Unavailable)
                .set_message("backend unavailable"),
        )
    };
    for error in [
        GaxError::exhausted(service()),
        GaxError::exhausted(GaxError::exhausted(service())),
    ] {
        let adbc = from_spanner(error);
        assert_eq!(adbc.status, Status::IO, "{}", adbc.message);
        assert_eq!(adbc.vendor_code, Code::Unavailable as i32);
        assert!(
            adbc.message.contains("backend unavailable"),
            "{}",
            adbc.message
        );
    }

    let denied = GaxError::exhausted(GaxError::service(
        RpcStatus::default()
            .set_code(Code::PermissionDenied)
            .set_message("spanner.sessions.create denied"),
    ));
    let adbc = from_spanner(denied);
    assert_eq!(adbc.status, Status::Unauthorized);
    assert!(
        adbc.message.contains("grant an IAM role"),
        "{}",
        adbc.message
    );

    let adbc = from_spanner(GaxError::exhausted("no status anywhere"));
    assert_eq!(adbc.status, Status::Internal);
    assert_eq!(adbc.vendor_code, 0);
}

#[test]
fn credential_and_client_deadline_failures_map_to_their_status() {
    use google_cloud_gax::error::{CredentialsError, Error as GaxError};

    let auth = || GaxError::authentication(CredentialsError::from_msg(false, "invalid_grant"));
    for error in [auth(), GaxError::exhausted(auth())] {
        let adbc = from_spanner(error);
        assert_eq!(adbc.status, Status::Unauthenticated, "{}", adbc.message);
        assert_eq!(adbc.vendor_code, 0);
        assert!(adbc.message.contains("invalid_grant"), "{}", adbc.message);
    }

    let adbc = from_spanner(GaxError::timeout("attempt deadline"));
    assert_eq!(adbc.status, Status::Timeout, "{}", adbc.message);
    assert_eq!(adbc.vendor_code, 0);
}

#[test]
fn malformed_http_payload_is_not_treated_as_an_rpc_status() {
    for payload in [
        "<html>Bad gateway</html>",
        "{}",
        r#"{"code":400,"message":"HTTP status, not RPC status"}"#,
        r#"{"code":0,"message":"not an error"}"#,
        r#"{"code":9}"#,
        r#"{"code":"9","message":"invalid code type"}"#,
    ] {
        let error = google_cloud_gax::error::Error::http(
            400,
            Default::default(),
            payload.to_owned().into(),
        );
        let mapped = from_spanner(error);
        assert_eq!(mapped.status, Status::Internal, "{payload}");
        assert_eq!(mapped.vendor_code, 0, "{payload}");
        assert!(mapped.message.contains(payload));
    }
}
