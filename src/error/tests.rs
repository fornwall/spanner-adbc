use super::*;
use google_cloud_gax::error::Error as GaxError;
use google_cloud_gax::error::rpc::Status as RpcStatus;

fn service(code: Code, message: &str) -> GaxError {
    GaxError::service(RpcStatus::default().set_code(code).set_message(message))
}

#[test]
fn maps_grpc_codes_to_adbc_status() {
    for (code, status) in [
        (Code::NotFound, Status::NotFound),
        (Code::AlreadyExists, Status::Integrity),
        (Code::Unauthenticated, Status::Unauthenticated),
        (Code::PermissionDenied, Status::Unauthorized),
        (Code::InvalidArgument, Status::InvalidArguments),
        (Code::OutOfRange, Status::InvalidData),
        (Code::FailedPrecondition, Status::InvalidState),
        (Code::Unknown, Status::Unknown),
        (Code::DataLoss, Status::IO),
        (Code::DeadlineExceeded, Status::Timeout),
        (Code::Cancelled, Status::Cancelled),
        (Code::Unavailable, Status::IO),
        (Code::Aborted, Status::IO),
        (Code::ResourceExhausted, Status::IO),
        (Code::Unimplemented, Status::NotImplemented),
        (Code::Internal, Status::Internal),
        (Code::Ok, Status::Internal),
        // An unrecognized wire code decodes to `Code::Unknown`.
        (Code::from(9999), Status::Unknown),
    ] {
        assert_eq!(status_for_grpc_code(code), status, "{code:?}");
    }
}

#[test]
fn service_errors_lead_with_the_grpc_code_and_keep_it_as_vendor_code() {
    let adbc = from_spanner(service(Code::Aborted, "Transaction was aborted."));
    assert_eq!(adbc.status, Status::IO);
    assert_eq!(adbc.vendor_code, 10);
    assert_eq!(
        adbc.message,
        "Spanner error: ABORTED: Transaction was aborted."
    );
    assert_eq!(adbc.details, None);

    // BatchWrite reports per-group failures as status parts; ingest handles both paths alike.
    let parts = from_status_parts(Code::Aborted as i32, "Transaction was aborted.", &[]);
    assert_eq!(parts.status, adbc.status);
    assert_eq!(parts.vendor_code, adbc.vendor_code);
    assert_eq!(parts.message, adbc.message);
    assert_eq!(parts.details, None);
}

/// Constraint violations as Spanner (Omni) and the emulator report them.
const CONSTRAINT_VIOLATIONS: &[(Code, &str)] = &[
    // Spanner.
    (Code::AlreadyExists, "Row [1] in table P already exists"),
    (
        Code::AlreadyExists,
        "Unique index violation on index PByName at index key [a,1]. It conflicts with row [1] in \
         table P.",
    ),
    (
        Code::FailedPrecondition,
        "Name must not be NULL in table P.",
    ),
    (
        Code::FailedPrecondition,
        "A new row in table P does not specify a non-null value for these NOT NULL columns: Name.",
    ),
    (
        Code::FailedPrecondition,
        "Adding a NOT NULL constraint on a column D.V is not allowed because it has a NULL value \
         at key: [3]",
    ),
    (
        Code::FailedPrecondition,
        "Foreign key constraint `FkP` is violated on table `C`. Cannot find referenced values in \
         P(Id).",
    ),
    (
        Code::FailedPrecondition,
        "Foreign key constraint violation when deleting or updating referenced row(s): \
         referencing row(s) found in table `C`.",
    ),
    (
        Code::FailedPrecondition,
        "Found uniqueness violation: [x,1] [x,2].",
    ),
    (
        Code::OutOfRange,
        "Check constraint `P`.`CkAge` is violated for key (5)",
    ),
    (
        Code::NotFound,
        "Parent row for row [99,1] in table I is missing. Row cannot be written.",
    ),
    // The emulator.
    (
        Code::AlreadyExists,
        "Failed to insert row with primary key ({pk#Id:1}) due to previously existing row",
    ),
    (
        Code::AlreadyExists,
        "Table P: Row {Int64(1)} already exists.",
    ),
    (
        Code::AlreadyExists,
        "UNIQUE violation on index PByName,  duplicate key: {String(\"a\")} in this transaction.",
    ),
    (
        Code::FailedPrecondition,
        "Cannot specify a null value for column: P.Name in table: P",
    ),
    (
        Code::FailedPrecondition,
        "A new row in table P does not specify a non-null value for NOT NULL column: Name",
    ),
    (
        Code::FailedPrecondition,
        "Foreign key `FkP` constraint violation on table `C`. Cannot find referenced key \
         `{Int64(99)}` in table `P`.",
    ),
    (
        Code::FailedPrecondition,
        "Found uniqueness violation on index DByV,  duplicate key: {String(\"x\")}",
    ),
    (
        Code::OutOfRange,
        "Check constraint `P`.`CkAge` is violated for key {Int64(5)}",
    ),
    (
        Code::NotFound,
        "Insert failed because key was not found in parent table:  Parent Table: P  Child \
         Table: I  Key: {Int64(99)}",
    ),
];

#[test]
fn constraint_violations_map_to_integrity() {
    for &(code, message) in CONSTRAINT_VIOLATIONS {
        let adbc = from_spanner(service(code, message));
        assert_eq!(adbc.status, Status::Integrity, "{code:?}: {message}");
        assert_eq!(adbc.vendor_code, code as i32);
        assert!(adbc.message.contains(message), "{}", adbc.message);
        let batch_write = from_status_parts(code as i32, message, &[]);
        assert_eq!(batch_write.status, Status::Integrity, "{code:?}: {message}");
    }
}

#[test]
fn unrelated_errors_with_constraint_codes_keep_their_status() {
    for (code, message, status) in [
        (
            Code::FailedPrecondition,
            "Duplicate name in schema: P.",
            Status::InvalidState,
        ),
        (
            Code::FailedPrecondition,
            "Cannot start a read-write transaction in a read-only session",
            Status::InvalidState,
        ),
        (Code::NotFound, "Table not found: Missing", Status::NotFound),
        (
            Code::NotFound,
            "Column not found in table P: Nope",
            Status::NotFound,
        ),
        (
            Code::OutOfRange,
            "Integer overflow in addition",
            Status::InvalidData,
        ),
        (
            Code::InvalidArgument,
            "Foreign key `FkP` constraint violation on table `C`",
            Status::InvalidArguments,
        ),
    ] {
        assert_eq!(status_for_rpc(code, message), status, "{code:?}: {message}");
    }
}

#[test]
fn errors_without_a_grpc_status_map_by_kind() {
    use google_cloud_gax::error::CredentialsError;

    let auth = || GaxError::authentication(CredentialsError::from_msg(false, "invalid_grant"));
    for (error, status) in [
        (auth(), Status::Unauthenticated),
        (GaxError::exhausted(auth()), Status::Unauthenticated),
        (GaxError::timeout("attempt deadline"), Status::Timeout),
        (GaxError::connect("connection refused"), Status::IO),
        (GaxError::io("connection reset"), Status::IO),
        (
            GaxError::transport(Default::default(), "stream reset"),
            Status::IO,
        ),
        (GaxError::exhausted(GaxError::io("broken pipe")), Status::IO),
        (
            GaxError::deser("no structured status here"),
            Status::Internal,
        ),
        (GaxError::exhausted("no status anywhere"), Status::Internal),
    ] {
        let source = error.to_string();
        let adbc = from_spanner(error);
        assert_eq!(adbc.status, status, "{}", adbc.message);
        assert_eq!(adbc.vendor_code, 0, "{}", adbc.message);
        assert_eq!(adbc.message, format!("Spanner error: {source}"));
        assert_eq!(adbc.details, None);
    }
}

#[test]
fn an_http_payload_is_not_treated_as_an_rpc_status() {
    for payload in [
        "<html>Bad gateway</html>",
        "{}",
        r#"{"code":400,"message":"HTTP status, not RPC status"}"#,
        r#"{"code":9}"#,
    ] {
        let error = GaxError::http(400, Default::default(), payload.to_owned().into());
        let mapped = from_spanner(error);
        assert_eq!(mapped.vendor_code, 0, "{payload}");
        assert!(mapped.message.contains(payload));
    }
}

/// A retry loop that runs out of time returns `Error::exhausted(last_error)`.
#[test]
fn an_exhausted_retry_keeps_the_wrapped_grpc_status() {
    let unavailable = || service(Code::Unavailable, "backend unavailable");
    for error in [
        GaxError::exhausted(unavailable()),
        GaxError::exhausted(GaxError::exhausted(unavailable())),
    ] {
        let adbc = from_spanner(error);
        assert_eq!(adbc.status, Status::IO, "{}", adbc.message);
        assert_eq!(adbc.vendor_code, Code::Unavailable as i32);
        assert_eq!(
            adbc.message,
            "Spanner error: UNAVAILABLE: backend unavailable"
        );
    }
}

#[test]
fn status_details_forward_in_order_under_their_type_names() {
    let details = [
        serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.RetryInfo",
            "retryDelay": "1s",
        }),
        serde_json::json!({
            "@type": "type.googleapis.com/google.rpc.BadRequest",
            "fieldViolations": [{"field": "sql", "description": "syntax error"}],
        }),
        // Unknown types decode to `StatusDetails::Other` and are still forwarded.
        serde_json::json!({
            "@type": "type.googleapis.com/mycompany.CustomDetail",
            "foo": "bar",
        }),
    ];
    let expected_keys = [
        "google.rpc.retryinfo",
        "google.rpc.badrequest",
        "mycompany.customdetail",
    ];

    let status_details: Vec<StatusDetails> = details
        .iter()
        .map(|detail| serde_json::from_value(detail.clone()).unwrap())
        .collect();
    let from_rpc = from_spanner(GaxError::service(
        RpcStatus::default()
            .set_code(Code::Aborted)
            .set_message("Transaction was aborted")
            .set_details(status_details),
    ));
    let anys: Vec<Any> = details
        .iter()
        .map(|detail| serde_json::from_value(detail.clone()).unwrap())
        .collect();
    let from_parts = from_status_parts(Code::Aborted as i32, "Transaction was aborted", &anys);

    for adbc in [from_rpc, from_parts] {
        let forwarded = adbc.details.expect("details forwarded");
        let keys: Vec<&str> = forwarded.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, expected_keys);
        for ((_, value), original) in forwarded.iter().zip(&details) {
            let parsed: serde_json::Value = serde_json::from_slice(value).expect("UTF-8 JSON");
            assert_eq!(&parsed, original);
        }
    }
}

#[test]
fn permission_denied_appends_iam_guidance_without_naming_a_role() {
    let message = "Caller is missing IAM permission spanner.databases.select on resource \
                   projects/p/instances/i/databases/d.";
    for adbc in [
        from_spanner(service(Code::PermissionDenied, message)),
        from_spanner(GaxError::exhausted(service(
            Code::PermissionDenied,
            message,
        ))),
        from_status_parts(Code::PermissionDenied as i32, message, &[]),
    ] {
        assert_eq!(adbc.status, Status::Unauthorized);
        assert_eq!(adbc.vendor_code, 7);
        assert_eq!(
            adbc.message,
            format!(
                "Spanner error: PERMISSION_DENIED: {message} (grant an IAM role that includes \
                 it; see https://cloud.google.com/spanner/docs/iam)"
            )
        );
    }

    // The guidance depends on the code, not on the message mentioning a permission.
    let not_found = from_spanner(service(
        Code::NotFound,
        "Table not found; unrelated to spanner.databases.select",
    ));
    assert!(
        !not_found.message.contains("docs/iam"),
        "{}",
        not_found.message
    );
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
fn an_option_set_at_the_wrong_level_names_where_it_belongs() {
    for (kind, key, message) in [
        (
            "connection",
            crate::OPTION_ENDPOINT,
            "option spanner.endpoint is a database option and can't be set on a connection",
        ),
        (
            "connection",
            crate::OPTION_ROWS_PER_BATCH,
            "option spanner.rows_per_batch is a statement option and can't be set on a connection",
        ),
        (
            "statement",
            crate::OPTION_TRANSACTION_TAG,
            "option spanner.transaction.tag is a connection option and can't be set on a statement",
        ),
        (
            "database",
            crate::OPTION_READ_STALENESS,
            "option spanner.read.staleness is a connection or statement option and can't be set \
             on a database",
        ),
        (
            "statement",
            crate::OPTION_COMMIT_STATS_MUTATION_COUNT,
            "option spanner.commit_stats.mutation_count is read-only",
        ),
        (
            "statement",
            "spanner.read.stalenesss",
            "unsupported Spanner statement option: spanner.read.stalenesss",
        ),
    ] {
        let error = unknown_option(kind, key);
        assert_eq!(error.status, Status::NotImplemented);
        assert_eq!(error.message, message);
    }
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

#[test]
fn chain_walks_from_the_outermost_error_to_the_innermost() {
    use arrow_schema::ArrowError;

    let cancelled = err("operation cancelled", Status::Cancelled);
    let outer = ArrowError::ExternalError(Box::new(ArrowError::ExternalError(Box::new(cancelled))));
    assert_eq!(chain(&outer).count(), 3);
    let recovered = chain(&outer)
        .find_map(|link| link.downcast_ref::<Error>())
        .expect("the driver error must be reachable through the chain");
    assert_eq!(recovered.status, Status::Cancelled);
}
