use super::*;
use adbc_core::error::Status;

fn ts(s: &str) -> Timestamp {
    Timestamp::try_from(s).unwrap()
}

fn exact(d: Duration) -> TimestampBound {
    TimestampBound::exact_staleness(d)
}

fn read_at(s: &str) -> TimestampBound {
    TimestampBound::read_timestamp(ts(s))
}

#[test]
fn parses_exact_and_max_staleness_with_units() {
    for (value, duration) in [
        ("exact:10", Duration::from_secs(10)),
        ("exact:2.5s", Duration::from_millis(2500)),
        (" exact : 1h ", Duration::from_secs(3600)),
        ("exact:0", Duration::ZERO),
    ] {
        assert_eq!(
            parse_read_bound(value).unwrap(),
            both(exact(duration)),
            "{value}"
        );
    }
    // The multi-use bound pins `max:` to the stalest exact bound it allows.
    for (value, duration) in [
        ("max:500ms", Duration::from_millis(500)),
        ("max:1m", Duration::from_secs(60)),
    ] {
        assert_eq!(
            parse_read_bound(value).unwrap(),
            Bounds {
                single_use: TimestampBound::max_staleness(duration),
                multi_use: exact(duration),
            },
            "{value}"
        );
    }
}

#[test]
fn parses_read_and_min_timestamps() {
    for value in [
        "2026-07-07T00:00:00Z",
        "read:2026-07-07T00:00:00Z",
        "read : 2026-07-07T00:00:00Z",
        "read:2026-07-07T02:00:00+02:00",
    ] {
        assert_eq!(
            parse_read_bound(value).unwrap(),
            both(read_at("2026-07-07T00:00:00Z")),
            "{value}"
        );
    }
    assert_eq!(
        parse_read_bound("min:1969-12-31T23:59:59.25Z").unwrap(),
        Bounds {
            single_use: TimestampBound::min_read_timestamp(ts("1969-12-31T23:59:59.25Z")),
            multi_use: read_at("1969-12-31T23:59:59.25Z"),
        }
    );
}

#[test]
fn rejects_malformed_and_out_of_range_bounds() {
    for bad in [
        "10s",       // no kind, not a timestamp
        "exact:",    // no duration
        "exact:abc", // non-numeric duration
        "exact:-5",  // negative duration
        "soon:10s",  // unknown duration kind
        "exact:1x",  // unknown unit
        "EXACT:10s", // prefixes are case-sensitive
        "Read:2026-07-07T00:00:00Z",
        "not-a-time", // not a timestamp
        "read:",      // empty timestamp
        "2026-07-07", // date only, not a full RFC 3339 timestamp
        "min:12345",  // not a timestamp
        // Too large for a `Duration`; the unit multiplies before the conversion.
        "exact:1e20",
        "exact:1e19h",
        // Beyond the wire format's 10,000 years.
        "max:1e12",
        // Before the wire format's 0001-01-01.
        "read:0000-12-31T00:00:00Z",
    ] {
        let error = parse_read_bound(bad).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "{bad}");
        assert!(
            error
                .message
                .starts_with(&format!("option spanner.read.staleness: {:?} ", bad.trim())),
            "{}",
            error.message
        );
    }
}

#[test]
fn set_staleness_round_trips_and_unsets() {
    let mut s = ReadStaleness::default();
    assert_eq!(s.timestamp_bound(), None);

    s.set_staleness(OptionValue::String(" max:10s ".into()))
        .unwrap();
    assert_eq!(s.staleness_string(), Some("max:10s"));
    assert_eq!(
        s.timestamp_bound(),
        Some(TimestampBound::max_staleness(Duration::from_secs(10)))
    );
    assert_eq!(
        s.multi_use_timestamp_bound(),
        Some(exact(Duration::from_secs(10)))
    );

    // A rejected value keeps the previous one.
    s.set_staleness(OptionValue::String("exact:1e12".into()))
        .unwrap_err();
    assert_eq!(s.staleness_string(), Some("max:10s"));

    s.set_staleness(OptionValue::String(String::new())).unwrap();
    assert_eq!(s.staleness_string(), None);
    assert_eq!(s.multi_use_timestamp_bound(), None);
}

#[test]
fn grammar_rejection_omits_the_timestamp_parser_detail() {
    let error = parse_read_bound("stale:10s").unwrap_err();
    assert_eq!(
        error.message,
        "option spanner.read.staleness: \"stale:10s\" is not a valid read bound; expected \
         \"exact:<duration>\" (e.g. \"exact:10s\"), \"max:<duration>\" (e.g. \"max:500ms\"), \
         \"read:<rfc3339>\" (e.g. \"read:2026-07-07T00:00:00Z\") or \"min:<rfc3339>\" \
         (e.g. \"min:2026-07-07T00:00:00+02:00\")"
    );
    // With an explicit prefix, the parser's diagnosis is kept.
    let error = parse_read_bound("read:nope").unwrap_err();
    assert!(
        error.message.starts_with(
            "option spanner.read.staleness: \"read:nope\" does not carry a valid RFC 3339 \
             timestamp ("
        ),
        "{}",
        error.message
    );
}

#[test]
fn duration_rejection_names_its_option_and_the_units() {
    let error = parse_duration("1x", crate::OPTION_MAX_COMMIT_DELAY).unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(
        error.message,
        "option spanner.commit.max_delay: \"1x\" is not a valid duration; expected a number with \
         an optional unit suffix (s [default], ms, us/µs, ns, m, h), e.g. \"500ms\""
    );
}
