use super::*;
use adbc_core::error::Status;

fn selections(list: &[(&str, Type)]) -> Vec<ReplicaSelection> {
    list.iter()
        .map(|(location, kind)| {
            ReplicaSelection::new()
                .set_location(*location)
                .set_type(kind.clone())
        })
        .collect()
}

fn include(list: &[(&str, Type)], auto_failover_disabled: bool) -> DirectedReadOptions {
    DirectedReadOptions::new().set_replicas(Replicas::IncludeReplicas(Box::new(
        IncludeReplicas::new()
            .set_replica_selections(selections(list))
            .set_auto_failover_disabled(auto_failover_disabled),
    )))
}

fn exclude(list: &[(&str, Type)]) -> DirectedReadOptions {
    DirectedReadOptions::new().set_replicas(Replicas::ExcludeReplicas(Box::new(
        ExcludeReplicas::new().set_replica_selections(selections(list)),
    )))
}

#[test]
fn parses_modes_selections_and_the_failover_flag() {
    for (value, expected) in [
        (
            "include:us-east1",
            include(&[("us-east1", Type::Unspecified)], false),
        ),
        (
            "include:us-east1:read_only,us-east4:read_write",
            include(
                &[("us-east1", Type::ReadOnly), ("us-east4", Type::ReadWrite)],
                false,
            ),
        ),
        (
            "  include : us-east1 : read_only , us-east4 : any  ",
            include(
                &[
                    ("us-east1", Type::ReadOnly),
                    ("us-east4", Type::Unspecified),
                ],
                false,
            ),
        ),
        (
            "include::read_only",
            include(&[("", Type::ReadOnly)], false),
        ),
        (
            "include:us-east1;auto_failover_disabled",
            include(&[("us-east1", Type::Unspecified)], true),
        ),
        ("include;auto_failover_disabled", include(&[], true)),
        (
            "exclude:us-central1",
            exclude(&[("us-central1", Type::Unspecified)]),
        ),
    ] {
        assert_eq!(parse(value).unwrap(), expected, "{value}");
    }
}

#[test]
fn rejects_malformed_values_naming_the_problem() {
    for (bad, problem) in [
        ("", "unknown mode \"\""),
        ("prefer:us-east1", "unknown mode \"prefer\""),
        ("INCLUDE:us-east1", "unknown mode \"INCLUDE\""),
        ("include", "no replica selection"),
        ("exclude", "no replica selection"),
        ("include:", "empty replica selection"),
        ("include:us-east1,", "empty replica selection"),
        ("include:us-east1:fast", "unknown replica type \"fast\""),
        (
            "include:us-east1:READ_ONLY",
            "unknown replica type \"READ_ONLY\"",
        ),
        ("include::", "unknown replica type \"\""),
        (
            "include::any",
            "names neither a location nor a replica type",
        ),
        (
            "exclude:us-east1;auto_failover_disabled",
            "only valid with \"include\"",
        ),
        ("include:us-east1;wat", "unknown flag \"wat\""),
        (
            "include:us-east1;AUTO_FAILOVER_DISABLED",
            "unknown flag \"AUTO_FAILOVER_DISABLED\"",
        ),
    ] {
        let error = parse(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad:?}");
        assert!(
            error.message.starts_with("option spanner.directed_read: ")
                && error.message.contains(problem)
                && error.message.contains(&format!("in {bad:?}")),
            "{bad:?}: {}",
            error.message
        );
    }
}

#[test]
fn set_round_trips_the_trimmed_value_and_unsets() {
    let s = |v: &str| OptionValue::String(v.to_string());
    let mut config = DirectedRead::default();
    config.set(s("  include:us-east1:read_only  ")).unwrap();
    assert_eq!(config.option_string(), Some("include:us-east1:read_only"));

    // A rejected value keeps the previous one.
    config.set(s("prefer:us-east1")).unwrap_err();
    config.set(OptionValue::Int(1)).unwrap_err();
    assert_eq!(config.option_string(), Some("include:us-east1:read_only"));

    config.set(s("")).unwrap();
    assert_eq!(config.option_string(), None);
}
