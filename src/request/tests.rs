use super::*;
use adbc_core::error::Status;

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

#[test]
fn parses_exact_lowercase_priorities() {
    for (input, expected) in [
        ("low", Priority::Low),
        ("medium", Priority::Medium),
        ("high", Priority::High),
    ] {
        assert_eq!(parse_priority(input).unwrap(), expected, "{input}");
    }
}

#[test]
fn rejects_unknown_priorities() {
    for bad in [
        "urgent",
        "0",
        "priority_high",
        "hi gh",
        "médium",
        "HIGH",
        "High",
        "Medium",
        "LOW",
    ] {
        let error = parse_priority(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
}

#[test]
fn priority_round_trips_and_unsets() {
    let mut config = RequestConfig::default();
    assert_eq!(config.priority_string(), None);

    config.set_priority(s(" high ")).unwrap();
    assert_eq!(config.priority_string(), Some("high"));
    config.set_priority(s("medium")).unwrap();
    assert_eq!(config.priority_string(), Some("medium"));

    let error = config.set_priority(s("urgent")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(config.priority_string(), Some("medium"));

    config.set_priority(s("")).unwrap();
    assert_eq!(config.priority_string(), None);
}

#[test]
fn tags_round_trip_verbatim_and_unset_on_empty() {
    let mut config = RequestConfig::default();
    assert_eq!(config.request_tag_string(), None);
    assert_eq!(config.transaction_tag_string(), None);

    // Tags are not trimmed.
    config.set_request_tag(s(" my-App/query=1 ")).unwrap();
    assert_eq!(config.request_tag_string(), Some(" my-App/query=1 "));
    config.set_transaction_tag(s("nightly-etl")).unwrap();
    assert_eq!(config.transaction_tag_string(), Some("nightly-etl"));

    config.set_request_tag(s("")).unwrap();
    assert_eq!(config.request_tag_string(), None);
    assert_eq!(config.transaction_tag_string(), Some("nightly-etl"));
    config.set_transaction_tag(s("")).unwrap();
    assert_eq!(config.transaction_tag_string(), None);
}

#[test]
fn non_string_values_are_rejected() {
    let mut config = RequestConfig::default();
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        let error = config.set_priority(value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        let error = config.set_request_tag(value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        let error = config.set_transaction_tag(value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        let error = config.set_max_commit_delay(value).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

#[test]
fn parses_max_commit_delay_with_units_and_enforces_the_500ms_cap() {
    assert_eq!(
        parse_max_commit_delay("0.2s").unwrap(),
        Duration::from_millis(200)
    );
    assert_eq!(
        parse_max_commit_delay("200ms").unwrap(),
        Duration::from_millis(200)
    );
    assert_eq!(parse_max_commit_delay("0").unwrap(), Duration::ZERO);
    assert_eq!(
        parse_max_commit_delay("500ms").unwrap(),
        Duration::from_millis(500)
    );
    for bad in ["501ms", "1s", "0.6s", "-1ms", "abc", "1x"] {
        let error = parse_max_commit_delay(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
    }
}

#[test]
fn max_commit_delay_errors_name_their_option() {
    let mut config = RequestConfig::default();
    for bad in ["abc", "-1ms", "1e30", "2s"] {
        let error = config.set_max_commit_delay(s(bad)).unwrap_err();
        assert!(
            error.message.contains(crate::OPTION_MAX_COMMIT_DELAY),
            "{bad}: {}",
            error.message
        );
        assert!(
            !error.message.contains(crate::OPTION_READ_STALENESS),
            "{bad}: {}",
            error.message
        );
    }
}

#[test]
fn max_commit_delay_round_trips_and_unsets() {
    let mut config = RequestConfig::default();
    assert_eq!(config.max_commit_delay_string(), None);
    assert!(config.commit_delay().is_none());

    config.set_max_commit_delay(s(" 100ms ")).unwrap();
    assert_eq!(config.max_commit_delay_string(), Some("100ms"));
    assert_eq!(config.commit_delay(), Some(Duration::from_millis(100)));

    let error = config.set_max_commit_delay(s("2s")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(config.max_commit_delay_string(), Some("100ms"));

    config.set_max_commit_delay(s("")).unwrap();
    assert_eq!(config.max_commit_delay_string(), None);
    assert!(config.commit_delay().is_none());
}

#[test]
fn commit_flags_take_exact_booleans_and_unset_to_false() {
    type Set = fn(&mut RequestConfig, OptionValue) -> Result<()>;
    type Get = fn(&RequestConfig) -> bool;
    let flags: [(Set, Get); 2] = [
        (
            RequestConfig::set_commit_stats,
            RequestConfig::return_commit_stats,
        ),
        (
            RequestConfig::set_exclude_txn_from_change_streams,
            RequestConfig::exclude_txn_from_change_streams,
        ),
    ];
    for (set, get) in flags {
        let mut config = RequestConfig::default();
        assert!(!get(&config));
        set(&mut config, s("true")).unwrap();
        assert!(get(&config));
        for bad in [
            OptionValue::Int(1),
            s("1"),
            s("yes"),
            s("TRUE"),
            s("0"),
            s("False"),
        ] {
            let error = set(&mut config, bad.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad:?}");
            assert!(get(&config), "a rejected value keeps the old one");
        }
        set(&mut config, s("false")).unwrap();
        assert!(!get(&config));
        set(&mut config, s("true")).unwrap();
        set(&mut config, s("")).unwrap();
        assert!(!get(&config));
    }
}

#[test]
fn commit_stats_sink_records_most_recent_and_ignores_none() {
    let sink = CommitStats::default();
    assert_eq!(sink.mutation_count(), None);

    sink.record(None);
    assert_eq!(sink.mutation_count(), None);

    sink.record(Some(5));
    assert_eq!(sink.mutation_count(), Some(5));
    sink.record(Some(9));
    assert_eq!(sink.mutation_count(), Some(9));
    sink.record(None);
    assert_eq!(sink.mutation_count(), Some(9));

    // Clones share the cell.
    let clone = sink.clone();
    sink.record(Some(11));
    assert_eq!(clone.mutation_count(), Some(11));
}
