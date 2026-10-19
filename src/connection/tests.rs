//! Offline tests for isolation levels, the fixed current catalog/schema and partition
//! descriptors.

use adbc_core::error::Status;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::transaction_options::IsolationLevel;

use super::{
    PARTITION_DESCRIPTOR_VERSION, check_fixed_catalog_or_schema, decode_partition,
    encode_partition, isolation_to_adbc_string, parse_isolation_level,
};

#[test]
fn parses_supported_isolation_levels() {
    use adbc_core::constants::*;
    let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE).unwrap(),
        IsolationLevel::Serializable
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ).unwrap(),
        IsolationLevel::RepeatableRead
    );
    // Spanner's REPEATABLE_READ is snapshot isolation.
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT).unwrap(),
        IsolationLevel::RepeatableRead
    );
    // No level is sent; Spanner defaults to SERIALIZABLE.
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_DEFAULT).unwrap(),
        IsolationLevel::Unspecified
    );
}

#[test]
fn promotes_unsupported_isolation_levels() {
    use adbc_core::constants::*;
    let parse = |s: &str| parse_isolation_level(OptionValue::String(s.to_string()));
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED).unwrap(),
        IsolationLevel::RepeatableRead
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED).unwrap(),
        IsolationLevel::RepeatableRead
    );
    assert_eq!(
        parse(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE).unwrap(),
        IsolationLevel::Serializable
    );
    assert_eq!(
        parse("not-a-level").unwrap_err().status,
        Status::InvalidArguments
    );
    assert_eq!(
        parse_isolation_level(OptionValue::Int(1))
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

#[test]
fn isolation_level_round_trips_to_adbc_string() {
    use adbc_core::constants::*;
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::Serializable),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
    );
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::RepeatableRead),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        isolation_to_adbc_string(&IsolationLevel::Unspecified),
        ADBC_OPTION_ISOLATION_LEVEL_DEFAULT
    );
}

#[test]
fn promoted_isolation_level_round_trips_to_effective_level() {
    use adbc_core::constants::*;
    // `get_option` reports the level that will run, not the one that was set.
    let effective = |s: &str| {
        let level = parse_isolation_level(OptionValue::String(s.to_string())).expect("parses");
        isolation_to_adbc_string(&level)
    };
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT),
        ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ
    );
    assert_eq!(
        effective(ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE),
        ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE
    );
}

#[test]
fn setting_current_catalog_or_schema_accepts_only_the_reported_value() {
    let set = |s: &str| {
        check_fixed_catalog_or_schema(
            OptionValue::String(s.to_string()),
            "current catalog",
            "adbc-test",
        )
    };
    assert!(set("adbc-test").is_ok());
    assert!(
        check_fixed_catalog_or_schema(OptionValue::String(String::new()), "current schema", "")
            .is_ok()
    );
    let err = set("foo").unwrap_err();
    assert_eq!(err.status, Status::NotImplemented);
    assert!(err.message.contains("\"foo\""), "{}", err.message);
    assert!(err.message.contains("\"adbc-test\""), "{}", err.message);
    assert_eq!(
        check_fixed_catalog_or_schema(OptionValue::Int(1), "current schema", "")
            .unwrap_err()
            .status,
        Status::InvalidArguments
    );
}

/// `read_partition` takes caller-supplied bytes, so decoding must reject garbage cleanly.
#[test]
fn garbage_partition_descriptors_error_cleanly() {
    let cases: [&[u8]; 6] = [
        b"",                      // empty
        b"\xff\xfe\x00 not json", // non-UTF-8, non-JSON bytes
        b"{",                     // truncated JSON
        b"{}",                    // valid JSON object missing every descriptor field
        br#"{"hello": "world"}"#, // valid JSON object of the wrong shape
        b"[1, 2, 3]",             // valid JSON that is not an object
    ];
    for descriptor in cases {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "descriptor {descriptor:?}"
        );
        assert!(
            error.message.contains("invalid partition descriptor"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
}

/// `encode_partition` writes the versioned envelope, and decode then encode is a fixed point.
#[test]
fn partition_descriptor_envelope_round_trips() {
    let descriptor = include_bytes!("../../fuzz/seeds/partition/enveloped-query-descriptor");
    let partition = decode_partition(descriptor).expect("enveloped descriptor decodes");

    let encoded = encode_partition(&partition).expect("encode");
    let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(value["v"], PARTITION_DESCRIPTOR_VERSION);
    assert!(
        value.get("partition").is_some(),
        "envelope carries the partition payload: {value}"
    );

    let again = decode_partition(&encoded).expect("enveloped descriptor decodes");
    assert_eq!(encode_partition(&again).expect("re-encode"), encoded);
}

#[test]
fn partition_descriptor_preserves_fallback_types_and_reader_precision() {
    use crate::conversion::TimestampPrecision;
    use arrow_schema::DataType;
    use google_cloud_spanner::model::{StructType, Type, TypeCode, struct_type};

    let mut descriptor = decode_partition(include_bytes!(
        "../../fuzz/seeds/partition/enveloped-query-descriptor"
    ))
    .unwrap();
    assert!(
        descriptor
            .schema(TimestampPrecision::default())
            .unwrap()
            .is_none()
    );
    descriptor.row_type = Some(
        StructType::new().set_fields([
            struct_type::Field::new()
                .set_name("ts")
                .set_type(Type::new().set_code(TypeCode::Timestamp)),
            struct_type::Field::new()
                .set_name("payload")
                .set_type(Type::new().set_code(TypeCode::Json)),
        ]),
    );
    let bytes = encode_partition(&descriptor).unwrap();
    let decoded = decode_partition(&bytes).unwrap();
    assert_eq!(encode_partition(&decoded).unwrap(), bytes);
    let schema = decoded
        .schema(TimestampPrecision::Microseconds)
        .unwrap()
        .unwrap();
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(
        schema.field(1).metadata()["ARROW:extension:name"],
        "arrow.json"
    );

    let mut malformed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    malformed["row_type"] = serde_json::json!(17);
    let error = decode_partition(&serde_json::to_vec(&malformed).unwrap()).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

/// A payload without the versioned envelope is rejected.
#[test]
fn bare_partition_descriptor_is_rejected() {
    let bare: &[u8] = br#"{"inner":{"Query":{"sql":"SELECT 1"}}}"#;
    let error = decode_partition(bare).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(
        error.message.contains("missing \"v\" version field"),
        "unexpected message: {}",
        error.message
    );
}

/// An unknown version is rejected on the version, not on the payload shape.
#[test]
fn unsupported_partition_descriptor_version_errors_cleanly() {
    for descriptor in [
        br#"{"v":2,"partition":{"future":"format"}}"#.as_slice(),
        br#"{"v":0}"#.as_slice(),
    ] {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert!(
            error.message.contains("partition descriptor version")
                && error.message.contains("not supported by this driver"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
    let error = decode_partition(br#"{"v":2,"partition":{}}"#).unwrap_err();
    assert!(
        error
            .message
            .contains("partition descriptor version 2 not supported by this driver"),
        "{}",
        error.message
    );
}

/// Non-integer versions and missing or malformed payloads are `InvalidArguments`.
#[test]
fn malformed_partition_descriptor_envelopes_error_cleanly() {
    let cases: [&[u8]; 4] = [
        br#"{"v":"one"}"#,            // non-integer version
        br#"{"v":-1}"#,               // negative version
        br#"{"v":1}"#,                // missing "partition" payload
        br#"{"v":1,"partition":{}}"#, // payload of the wrong shape
    ];
    for descriptor in cases {
        let error = decode_partition(descriptor).unwrap_err();
        assert_eq!(
            error.status,
            Status::InvalidArguments,
            "descriptor {descriptor:?}"
        );
        assert!(
            error.message.contains("invalid partition descriptor"),
            "unexpected message for {descriptor:?}: {}",
            error.message
        );
    }
}

#[test]
fn partition_descriptor_round_trips_large_floats() {
    // Integer literals beyond i64/u64 become f64 values in `_unknown_fields`. serde_json's
    // `float_roundtrip` feature keeps re-encoding them byte-stable.
    let descriptor = br#"{"v": 1, "partition": {"version": 1, "inner": {"Query": {"session":"projects/p/instances/i/databases/d/sessions/s","transaction":{"id":"dHg="},"partitionToken":"dG9rZW4=","sql":"SELECT 1","con[": 44444424444444444249, "/+%n":4444440000000000000000074074764, "/s%n": "prns/s"}}}}"#;

    let partition = decode_partition(descriptor).expect("descriptor decodes");
    let first = encode_partition(&partition).expect("a decoded partition re-encodes");
    let normalized = decode_partition(&first).expect("re-encoded descriptor decodes");
    let again = encode_partition(&normalized).expect("a decoded partition re-encodes");
    assert_eq!(
        first, again,
        "re-encoding the encoder's output must be byte-stable"
    );
}
