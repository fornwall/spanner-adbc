#![no_main]

use chrono::{DateTime, Datelike, Duration, NaiveDate};
use libfuzzer_sys::fuzz_target;
use spanner_adbc::fuzzing::{parse_date_days, parse_numeric_i128, parse_timestamp_nanos};

// Each parsed DATE, TIMESTAMP or NUMERIC must re-parse to the same value from its canonical form.
//
// Date and timestamp round-trips are limited to years 1..=9999, Spanner's range. Outside it,
// chrono's own formatting does not round-trip.
fuzz_target!(|s: String| {
    if let Some(days) = parse_date_days(&s) {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        // `days` came from a valid date, so this cannot overflow.
        let date = epoch + Duration::days(days as i64);
        if (1..=9999).contains(&date.year()) {
            let canonical = date.format("%Y-%m-%d").to_string();
            assert_eq!(
                parse_date_days(&canonical),
                Some(days),
                "date round-trip: {s:?}"
            );
        }
    }

    if let Some(nanos) = parse_timestamp_nanos(&s) {
        let dt: DateTime<chrono::Utc> = DateTime::from_timestamp_nanos(nanos);
        if (1..=9999).contains(&dt.year()) {
            // Spanner always sends a `Z` suffix, the only form the parser accepts.
            let canonical = dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true);
            assert_eq!(
                parse_timestamp_nanos(&canonical),
                Some(nanos),
                "timestamp round-trip: {s:?}"
            );
        }
    }

    if let Some(unscaled) = parse_numeric_i128(&s) {
        let neg = unscaled < 0;
        let mag = unscaled.unsigned_abs();
        let int = mag / 1_000_000_000;
        let frac = mag % 1_000_000_000;
        let canonical = format!("{}{int}.{frac:09}", if neg { "-" } else { "" });
        assert_eq!(
            parse_numeric_i128(&canonical),
            Some(unscaled),
            "numeric round-trip: {s:?}"
        );
    }
});
