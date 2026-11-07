use super::*;
use crate::options::{SharedConfig, non_empty_string_option};
use crate::{OPTION_MAX_COMMIT_DELAY, OPTION_REQUEST_PRIORITY, OPTION_REQUEST_TAG};
use adbc_core::error::Status;
use adbc_core::options::OptionValue;

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
    let mut config = SharedConfig::default();
    assert_eq!(config.request.priority.raw(), None);

    config
        .set_option(OPTION_REQUEST_PRIORITY, s(" high "))
        .unwrap();
    assert_eq!(config.request.priority.raw(), Some("high"));
    config
        .set_option(OPTION_REQUEST_PRIORITY, s("medium"))
        .unwrap();
    assert_eq!(config.request.priority.raw(), Some("medium"));

    let error = config
        .set_option(OPTION_REQUEST_PRIORITY, s("urgent"))
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(config.request.priority.raw(), Some("medium"));

    config.set_option(OPTION_REQUEST_PRIORITY, s("")).unwrap();
    assert_eq!(config.request.priority.raw(), None);
}

#[test]
fn tags_round_trip_verbatim_and_unset_on_empty() {
    let mut config = SharedConfig::default();
    assert_eq!(config.request.request_tag, None);
    assert_eq!(config.request.transaction_tag, None);

    // Tags are not trimmed.
    config
        .set_option(OPTION_REQUEST_TAG, s(" my-App/query=1 "))
        .unwrap();
    assert_eq!(
        config.request.request_tag.as_deref(),
        Some(" my-App/query=1 ")
    );
    config.request.transaction_tag = non_empty_string_option(s("nightly-etl"), "o").unwrap();
    assert_eq!(
        config.request.transaction_tag.as_deref(),
        Some("nightly-etl")
    );

    config.set_option(OPTION_REQUEST_TAG, s("")).unwrap();
    assert_eq!(config.request.request_tag, None);
    assert_eq!(
        config.request.transaction_tag.as_deref(),
        Some("nightly-etl")
    );
    config.request.transaction_tag = non_empty_string_option(s(""), "o").unwrap();
    assert_eq!(config.request.transaction_tag, None);
}

#[test]
fn non_string_values_are_rejected() {
    let mut config = SharedConfig::default();
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        for key in [
            OPTION_REQUEST_PRIORITY,
            OPTION_REQUEST_TAG,
            OPTION_MAX_COMMIT_DELAY,
        ] {
            let error = config.set_option(key, value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments);
        }
        let error = non_empty_string_option(value, "o").unwrap_err();
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
    let mut config = SharedConfig::default();
    for bad in ["abc", "-1ms", "1e30", "2s"] {
        let error = config
            .set_option(OPTION_MAX_COMMIT_DELAY, s(bad))
            .unwrap_err();
        assert!(
            error.message.contains(OPTION_MAX_COMMIT_DELAY),
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
    let mut config = SharedConfig::default();
    assert_eq!(config.request.max_commit_delay.raw(), None);
    assert!(config.request.max_commit_delay.parsed().is_none());

    config
        .set_option(OPTION_MAX_COMMIT_DELAY, s(" 100ms "))
        .unwrap();
    assert_eq!(config.request.max_commit_delay.raw(), Some("100ms"));
    assert_eq!(
        config.request.max_commit_delay.parsed(),
        Some(&Duration::from_millis(100))
    );

    let error = config
        .set_option(OPTION_MAX_COMMIT_DELAY, s("2s"))
        .unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(config.request.max_commit_delay.raw(), Some("100ms"));

    config.set_option(OPTION_MAX_COMMIT_DELAY, s("")).unwrap();
    assert_eq!(config.request.max_commit_delay.raw(), None);
    assert!(config.request.max_commit_delay.parsed().is_none());
}

#[test]
fn commit_flags_take_exact_booleans_and_unset_to_false() {
    type Get = fn(&RequestConfig) -> bool;
    let flags: [(&str, Get); 2] = [
        (crate::OPTION_COMMIT_STATS, |request| {
            request.return_commit_stats
        }),
        (crate::OPTION_EXCLUDE_TXN_FROM_CHANGE_STREAMS, |request| {
            request.exclude_txn_from_change_streams
        }),
    ];
    for (key, get) in flags {
        let mut config = SharedConfig::default();
        assert!(!get(&config.request));
        config.set_option(key, s("true")).unwrap();
        assert!(get(&config.request));
        for bad in [
            OptionValue::Int(1),
            s("1"),
            s("yes"),
            s("TRUE"),
            s("0"),
            s("False"),
        ] {
            let error = config.set_option(key, bad.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "{bad:?}");
            assert!(get(&config.request), "a rejected value keeps the old one");
        }
        config.set_option(key, s("false")).unwrap();
        assert!(!get(&config.request));
        config.set_option(key, s("true")).unwrap();
        config.set_option(key, s("")).unwrap();
        assert!(!get(&config.request));
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
