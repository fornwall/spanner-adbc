use super::*;
use adbc_core::error::Status;

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

fn sel(location: &str, kind: ReplicaType) -> Selection {
    Selection {
        location: location.to_string(),
        kind,
    }
}

#[test]
fn parses_include_and_exclude_with_locations_and_types() {
    assert_eq!(
        parse("include:us-east1").unwrap(),
        DirectedReadSpec {
            mode: Mode::Include,
            selections: vec![sel("us-east1", ReplicaType::Any)],
            auto_failover_disabled: false,
        }
    );
    assert_eq!(
        parse("include:us-east1:read_only,us-east4:read_write").unwrap(),
        DirectedReadSpec {
            mode: Mode::Include,
            selections: vec![
                sel("us-east1", ReplicaType::ReadOnly),
                sel("us-east4", ReplicaType::ReadWrite),
            ],
            auto_failover_disabled: false,
        }
    );
    assert_eq!(
        parse("exclude:us-central1").unwrap(),
        DirectedReadSpec {
            mode: Mode::Exclude,
            selections: vec![sel("us-central1", ReplicaType::Any)],
            auto_failover_disabled: false,
        }
    );
}

#[test]
fn trims_whitespace_around_tokens() {
    assert_eq!(
        parse("  include : us-east1 : read_only , us-east4 : any  ").unwrap(),
        DirectedReadSpec {
            mode: Mode::Include,
            selections: vec![
                sel("us-east1", ReplicaType::ReadOnly),
                sel("us-east4", ReplicaType::Any),
            ],
            auto_failover_disabled: false,
        }
    );
}

/// The mode, replica type and flag keywords are exact lowercase, like every option value in
/// the driver (ADBC option values are exact-match canonical strings): case variants are
/// rejected with `InvalidArguments`.
#[test]
fn rejects_case_variants_of_keywords() {
    for bad in [
        "INCLUDE:us-east1",
        "Include:us-east1",
        "EXCLUDE:us-central1",
        "include:us-east1:READ_ONLY",
        "include:us-east1:Read_Write",
        "include::ANY",
        "include:us-east1;AUTO_FAILOVER_DISABLED",
        "include:us-east1;Auto_Failover_Disabled",
    ] {
        let err = parse(bad).unwrap_err();
        assert_eq!(
            err.status,
            Status::InvalidArguments,
            "expected error for {bad:?}"
        );
    }
}

#[test]
fn parses_auto_failover_flag_and_type_only_selection() {
    assert_eq!(
        parse("include:us-east1;auto_failover_disabled").unwrap(),
        DirectedReadSpec {
            mode: Mode::Include,
            selections: vec![sel("us-east1", ReplicaType::Any)],
            auto_failover_disabled: true,
        }
    );
    // Type-only selection (location omitted).
    assert_eq!(
        parse("include::read_only").unwrap(),
        DirectedReadSpec {
            mode: Mode::Include,
            selections: vec![sel("", ReplicaType::ReadOnly)],
            auto_failover_disabled: false,
        }
    );
    // Include with only the flag and no selections is allowed.
    assert_eq!(
        parse("include;auto_failover_disabled").unwrap(),
        DirectedReadSpec {
            mode: Mode::Include,
            selections: vec![],
            auto_failover_disabled: true,
        }
    );
}

#[test]
fn rejects_malformed_values() {
    for bad in [
        "",                                        // empty mode
        "prefer:us-east1",                         // unknown mode
        "include",                                 // no selections and no flag
        "exclude",                                 // no selections
        "include:",                                // empty selection list
        "include:us-east1,",                       // trailing comma
        "include:us-east1:fast",                   // unknown type
        "include::",                               // empty location and empty type
        "include::any",                            // no constraint at all
        "exclude:us-east1;auto_failover_disabled", // flag illegal with exclude
        "include:us-east1;wat",                    // unknown flag
    ] {
        let err = parse(bad).unwrap_err();
        assert_eq!(
            err.status,
            Status::InvalidArguments,
            "expected error for {bad:?}"
        );
    }
}

#[test]
fn round_trips_and_unsets() {
    let mut config = DirectedRead::default();
    assert_eq!(config.option_string(), None);

    config.set(s("  include:us-east1:read_only  ")).unwrap();
    assert_eq!(config.option_string(), Some("include:us-east1:read_only"));

    // A bad value is rejected and leaves the stored value untouched.
    let err = config.set(s("prefer:us-east1")).unwrap_err();
    assert_eq!(err.status, Status::InvalidArguments);
    assert_eq!(config.option_string(), Some("include:us-east1:read_only"));

    // An empty string unsets.
    config.set(s("")).unwrap();
    assert_eq!(config.option_string(), None);
}

#[test]
fn non_string_values_are_rejected() {
    let mut config = DirectedRead::default();
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        let err = config.set(value).unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
    }
}

/// The built client options carry the include/exclude branch, the ordered selections (with
/// location and mapped type), and the auto-failover flag.
#[test]
fn builds_client_options() {
    let spec = parse("include:us-east1:read_only,us-east4;auto_failover_disabled").unwrap();
    let options = spec.to_options();
    let Some(Replicas::IncludeReplicas(include)) = options.replicas else {
        panic!("expected IncludeReplicas");
    };
    assert!(include.auto_failover_disabled);
    assert_eq!(include.replica_selections.len(), 2);
    assert_eq!(include.replica_selections[0].location, "us-east1");
    assert_eq!(include.replica_selections[0].r#type, Type::ReadOnly);
    assert_eq!(include.replica_selections[1].location, "us-east4");
    // `any` leaves the type unspecified (the proto default).
    assert_eq!(include.replica_selections[1].r#type, Type::default());

    let spec = parse("exclude:us-central1").unwrap();
    let options = spec.to_options();
    let Some(Replicas::ExcludeReplicas(exclude)) = options.replicas else {
        panic!("expected ExcludeReplicas");
    };
    assert_eq!(exclude.replica_selections.len(), 1);
    assert_eq!(exclude.replica_selections[0].location, "us-central1");
}

/// Statement inheritance is a plain clone of the connection's config (mirroring the other
/// per-statement options): the clone starts with the connection's value and overrides
/// independently.
#[test]
fn cloned_config_inherits_then_overrides_independently() {
    let mut connection = DirectedRead::default();
    connection.set(s("include:us-east1")).unwrap();

    let mut statement = connection.clone();
    assert_eq!(statement.option_string(), Some("include:us-east1"));

    statement.set(s("exclude:us-central1")).unwrap();
    assert_eq!(statement.option_string(), Some("exclude:us-central1"));
    // The connection is unaffected by the statement-level override.
    assert_eq!(connection.option_string(), Some("include:us-east1"));

    // Clearing on the statement does not touch the connection.
    statement.set(s("")).unwrap();
    assert_eq!(statement.option_string(), None);
    assert_eq!(connection.option_string(), Some("include:us-east1"));
}
