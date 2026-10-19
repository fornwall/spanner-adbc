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

/// The duration conversion runs before the 500ms check, so it must reject overflow itself.
#[test]
fn rejects_oversized_max_commit_delay_instead_of_panicking() {
    let error = parse_max_commit_delay("1e30").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
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
fn commit_stats_flag_round_trips_and_unsets() {
    let mut config = RequestConfig::default();
    assert_eq!(config.commit_stats_string(), "false");

    config.set_commit_stats(s("true")).unwrap();
    assert_eq!(config.commit_stats_string(), "true");
    let error = config.set_commit_stats(OptionValue::Int(1)).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(config.commit_stats_string(), "true");
    config.set_commit_stats(s("false")).unwrap();
    assert_eq!(config.commit_stats_string(), "false");
    for lenient in ["1", "yes", "TRUE", "0", "no", "False"] {
        let error = config.set_commit_stats(s(lenient)).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{lenient}");
    }

    config.set_commit_stats(s("true")).unwrap();
    assert_eq!(config.commit_stats_string(), "true");
    config.set_commit_stats(s("")).unwrap();
    assert_eq!(config.commit_stats_string(), "false");

    config.set_commit_stats(s("true")).unwrap();
    let error = config.set_commit_stats(s("maybe")).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(config.commit_stats_string(), "true");
}

#[test]
fn exclude_txn_from_change_streams_flag_round_trips_and_unsets() {
    let mut config = RequestConfig::default();
    assert_eq!(config.exclude_txn_from_change_streams_string(), "false");

    config
        .set_exclude_txn_from_change_streams(s("true"))
        .unwrap();
    assert_eq!(config.exclude_txn_from_change_streams_string(), "true");
    let error = config
        .set_exclude_txn_from_change_streams(OptionValue::Int(1))
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(config.exclude_txn_from_change_streams_string(), "true");
    config
        .set_exclude_txn_from_change_streams(s("false"))
        .unwrap();
    assert_eq!(config.exclude_txn_from_change_streams_string(), "false");
    for lenient in ["1", "yes", "TRUE", "0", "no", "False"] {
        let error = config
            .set_exclude_txn_from_change_streams(s(lenient))
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{lenient}");
    }

    config
        .set_exclude_txn_from_change_streams(s("true"))
        .unwrap();
    assert_eq!(config.exclude_txn_from_change_streams_string(), "true");
    config.set_exclude_txn_from_change_streams(s("")).unwrap();
    assert_eq!(config.exclude_txn_from_change_streams_string(), "false");
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

#[test]
fn cloned_config_inherits_then_overrides_independently() {
    let mut connection = RequestConfig::default();
    connection.set_priority(s("low")).unwrap();
    connection.set_request_tag(s("conn-tag")).unwrap();
    connection.set_transaction_tag(s("txn-tag")).unwrap();
    connection.set_max_commit_delay(s("100ms")).unwrap();
    connection.set_commit_stats(s("true")).unwrap();
    connection
        .set_exclude_txn_from_change_streams(s("true"))
        .unwrap();

    let mut statement = connection.clone();
    assert_eq!(statement.priority_string(), Some("low"));
    assert_eq!(statement.request_tag_string(), Some("conn-tag"));
    assert_eq!(statement.transaction_tag_string(), Some("txn-tag"));
    assert_eq!(statement.max_commit_delay_string(), Some("100ms"));
    assert_eq!(statement.commit_stats_string(), "true");
    assert_eq!(statement.exclude_txn_from_change_streams_string(), "true");

    statement.set_priority(s("high")).unwrap();
    statement.set_request_tag(s("")).unwrap();
    statement.set_max_commit_delay(s("250ms")).unwrap();
    statement.set_commit_stats(s("false")).unwrap();
    statement
        .set_exclude_txn_from_change_streams(s("false"))
        .unwrap();
    assert_eq!(statement.priority_string(), Some("high"));
    assert_eq!(statement.request_tag_string(), None);
    assert_eq!(statement.max_commit_delay_string(), Some("250ms"));
    assert_eq!(statement.commit_stats_string(), "false");
    assert_eq!(statement.exclude_txn_from_change_streams_string(), "false");
    assert_eq!(connection.priority_string(), Some("low"));
    assert_eq!(connection.request_tag_string(), Some("conn-tag"));
    assert_eq!(connection.max_commit_delay_string(), Some("100ms"));
    assert_eq!(connection.commit_stats_string(), "true");
    assert_eq!(connection.exclude_txn_from_change_streams_string(), "true");
}
