use super::*;
use adbc_core::error::Status;

use crate::options::{F64Range, f64_option};

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

/// The `impl_shared_option_dispatch` arms that set these fields, as functions the tests can
/// call: each parses the value with the parser and range the dispatch uses, and stores it —
/// leaving the previous value in place when the parse is rejected.
fn set_max_elapsed_seconds(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
    config.max_elapsed_seconds = f64_option(
        value,
        crate::OPTION_RETRY_MAX_ELAPSED_SECONDS,
        F64Range::PositiveSeconds,
    )?;
    Ok(())
}

fn set_backoff_initial_seconds(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
    config.backoff_initial_seconds = f64_option(
        value,
        crate::OPTION_RETRY_BACKOFF_INITIAL_SECONDS,
        F64Range::PositiveSeconds,
    )?;
    Ok(())
}

fn set_backoff_max_seconds(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
    config.backoff_max_seconds = f64_option(
        value,
        crate::OPTION_RETRY_BACKOFF_MAX_SECONDS,
        F64Range::PositiveSeconds,
    )?;
    Ok(())
}

fn set_backoff_multiplier(config: &mut RetryConfig, value: OptionValue) -> Result<()> {
    config.backoff_multiplier = f64_option(
        value,
        crate::OPTION_RETRY_BACKOFF_MULTIPLIER,
        F64Range::PositiveFactor,
    )?;
    Ok(())
}

/// The canonical `get_option` string of a stored value, as the dispatch's getter arms render
/// it.
fn string<T: std::fmt::Display>(value: Option<T>) -> Option<String> {
    value.map(|v| v.to_string())
}

#[test]
fn parses_attempts_from_strings_ints_and_whole_doubles() {
    let mut config = RetryConfig::default();
    config.set_max_attempts(s(" 3 ")).unwrap();
    assert_eq!(string(config.max_attempts).as_deref(), Some("3"));
    config.set_max_attempts(OptionValue::Int(5)).unwrap();
    assert_eq!(string(config.max_attempts).as_deref(), Some("5"));
    config.set_max_attempts(OptionValue::Double(2.0)).unwrap();
    assert_eq!(string(config.max_attempts).as_deref(), Some("2"));
    // 1 is valid: one attempt, no retries.
    config.set_max_attempts(s("1")).unwrap();
    assert_eq!(string(config.max_attempts).as_deref(), Some("1"));
}

#[test]
fn parses_elapsed_from_strings_ints_and_doubles() {
    let mut config = RetryConfig::default();
    set_max_elapsed_seconds(&mut config, s(" 2.5 ")).unwrap();
    assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("2.5"));
    assert_eq!(
        config.max_elapsed_duration(),
        Some(Duration::from_millis(2500))
    );
    set_max_elapsed_seconds(&mut config, OptionValue::Int(30)).unwrap();
    assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("30"));
    set_max_elapsed_seconds(&mut config, OptionValue::Double(0.05)).unwrap();
    assert_eq!(
        config.max_elapsed_duration(),
        Some(Duration::from_millis(50))
    );
}

#[test]
fn empty_string_unsets_each_independently() {
    let mut config = RetryConfig::default();
    config.set_max_attempts(s("4")).unwrap();
    set_max_elapsed_seconds(&mut config, s("10")).unwrap();
    config.set_max_attempts(s("")).unwrap();
    assert_eq!(string(config.max_attempts), None);
    assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("10"));
    // Whitespace-only counts as empty too.
    set_max_elapsed_seconds(&mut config, s("  ")).unwrap();
    assert_eq!(string(config.max_elapsed_seconds), None);
}

#[test]
fn rejects_bad_attempts() {
    let mut config = RetryConfig::default();
    config.set_max_attempts(s("3")).unwrap();
    let bad = [
        OptionValue::Int(0),
        OptionValue::Int(-1),
        s("0"),
        s("-2"),
        s("1.5"),                 // fractional
        OptionValue::Double(2.5), // fractional double
        OptionValue::Double(f64::NAN),
        s("abc"),
        OptionValue::Bytes(vec![1]),
        // Above u32::MAX.
        OptionValue::Int(i64::from(u32::MAX) + 1),
    ];
    for value in bad {
        let error = config.set_max_attempts(value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        // The stored value is left untouched.
        assert_eq!(string(config.max_attempts).as_deref(), Some("3"));
    }
}

#[test]
fn rejects_bad_elapsed() {
    let mut config = RetryConfig::default();
    set_max_elapsed_seconds(&mut config, s("5")).unwrap();
    let bad = [
        OptionValue::Double(0.0), // zero budget is degenerate
        s("0"),
        OptionValue::Double(-1.0),
        OptionValue::Int(-3),
        OptionValue::Double(f64::NAN),
        OptionValue::Double(f64::INFINITY),
        s("inf"),
        s("abc"),
        s("1s"),
        OptionValue::Double(1e300), // too large for Duration
        OptionValue::Bytes(vec![1, 2]),
    ];
    for value in bad {
        let error = set_max_elapsed_seconds(&mut config, value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        assert_eq!(string(config.max_elapsed_seconds).as_deref(), Some("5"));
    }
}

#[test]
fn retry_policy_arg_is_none_until_configured() {
    let mut config = RetryConfig::default();
    assert!(config.retry_policy_arg().is_none());
    config.set_max_attempts(s("3")).unwrap();
    assert!(config.retry_policy_arg().is_some());
    config.set_max_attempts(s("")).unwrap();
    assert!(config.retry_policy_arg().is_none());
    set_max_elapsed_seconds(&mut config, s("10")).unwrap();
    assert!(config.retry_policy_arg().is_some());
    // Both set: still a policy (the loop stops at whichever limit fires first).
    config.set_max_attempts(s("5")).unwrap();
    assert!(config.retry_policy_arg().is_some());
}

/// Statement inheritance is a plain copy of the connection's config (mirroring `RpcTimeouts`):
/// the copy starts with the connection's values and overrides independently.
#[test]
fn copied_config_inherits_then_overrides_independently() {
    let mut connection = RetryConfig::default();
    connection.set_max_attempts(s("10")).unwrap();
    set_max_elapsed_seconds(&mut connection, s("20")).unwrap();

    let mut statement = connection;
    assert_eq!(string(statement.max_attempts).as_deref(), Some("10"));
    assert_eq!(string(statement.max_elapsed_seconds).as_deref(), Some("20"));

    statement.set_max_attempts(s("2")).unwrap();
    set_max_elapsed_seconds(&mut statement, s("")).unwrap();
    assert_eq!(string(statement.max_attempts).as_deref(), Some("2"));
    assert_eq!(string(statement.max_elapsed_seconds), None);
    // The connection is unaffected by statement-level overrides.
    assert_eq!(string(connection.max_attempts).as_deref(), Some("10"));
    assert_eq!(
        string(connection.max_elapsed_seconds).as_deref(),
        Some("20")
    );
}

#[test]
fn parses_backoff_knobs_from_strings_ints_and_doubles() {
    let mut config = RetryConfig::default();
    set_backoff_initial_seconds(&mut config, s(" 0.5 ")).unwrap();
    assert_eq!(
        string(config.backoff_initial_seconds).as_deref(),
        Some("0.5")
    );
    set_backoff_max_seconds(&mut config, OptionValue::Int(30)).unwrap();
    assert_eq!(string(config.backoff_max_seconds).as_deref(), Some("30"));
    set_backoff_multiplier(&mut config, OptionValue::Double(1.5)).unwrap();
    assert_eq!(string(config.backoff_multiplier).as_deref(), Some("1.5"));
}

#[test]
fn empty_string_unsets_each_backoff_knob_independently() {
    let mut config = RetryConfig::default();
    set_backoff_initial_seconds(&mut config, s("1")).unwrap();
    set_backoff_max_seconds(&mut config, s("10")).unwrap();
    set_backoff_multiplier(&mut config, s("2")).unwrap();

    set_backoff_initial_seconds(&mut config, s("")).unwrap();
    assert_eq!(string(config.backoff_initial_seconds), None);
    assert_eq!(string(config.backoff_max_seconds).as_deref(), Some("10"));
    assert_eq!(string(config.backoff_multiplier).as_deref(), Some("2"));
    // Whitespace-only counts as empty too.
    set_backoff_max_seconds(&mut config, s("  ")).unwrap();
    assert_eq!(string(config.backoff_max_seconds), None);
    set_backoff_multiplier(&mut config, s("")).unwrap();
    assert_eq!(string(config.backoff_multiplier), None);
}

#[test]
fn rejects_bad_backoff_seconds() {
    for setter in [
        set_backoff_initial_seconds as fn(&mut RetryConfig, OptionValue) -> _,
        set_backoff_max_seconds,
    ] {
        let mut config = RetryConfig::default();
        setter(&mut config, s("2")).unwrap();
        let bad = [
            OptionValue::Double(0.0),
            s("0"),
            OptionValue::Double(-1.0),
            OptionValue::Int(-3),
            OptionValue::Double(f64::NAN),
            OptionValue::Double(f64::INFINITY),
            s("inf"),
            s("abc"),
            s("1s"),
            OptionValue::Double(1e300), // too large for Duration
            OptionValue::Bytes(vec![1]),
        ];
        for value in bad {
            let error = setter(&mut config, value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        }
    }
}

#[test]
fn rejects_bad_backoff_multiplier() {
    let mut config = RetryConfig::default();
    set_backoff_multiplier(&mut config, s("2")).unwrap();
    let bad = [
        OptionValue::Double(0.0),
        s("0"),
        OptionValue::Double(-1.0),
        OptionValue::Int(-3),
        OptionValue::Double(f64::NAN),
        OptionValue::Double(f64::INFINITY),
        s("inf"),
        s("abc"),
        OptionValue::Bytes(vec![1]),
    ];
    for value in bad {
        let error = set_backoff_multiplier(&mut config, value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        // The stored value is left untouched.
        assert_eq!(string(config.backoff_multiplier).as_deref(), Some("2"));
    }
    // Sub-1.0 multipliers are accepted (floored to 1.0 at build time), not rejected.
    set_backoff_multiplier(&mut config, OptionValue::Double(0.5)).unwrap();
    assert_eq!(string(config.backoff_multiplier).as_deref(), Some("0.5"));
}

#[test]
fn backoff_policy_arg_is_none_until_configured() {
    let mut config = RetryConfig::default();
    assert!(config.backoff_policy_arg().is_none());
    // The attempt / elapsed-time limits alone do not produce a backoff policy.
    config.set_max_attempts(s("3")).unwrap();
    set_max_elapsed_seconds(&mut config, s("10")).unwrap();
    assert!(config.backoff_policy_arg().is_none());
    // Each backoff knob on its own is enough.
    set_backoff_initial_seconds(&mut config, s("0.25")).unwrap();
    assert!(config.backoff_policy_arg().is_some());
    set_backoff_initial_seconds(&mut config, s("")).unwrap();
    assert!(config.backoff_policy_arg().is_none());
    set_backoff_max_seconds(&mut config, s("30")).unwrap();
    assert!(config.backoff_policy_arg().is_some());
    set_backoff_max_seconds(&mut config, s("")).unwrap();
    assert!(config.backoff_policy_arg().is_none());
    set_backoff_multiplier(&mut config, s("3")).unwrap();
    assert!(config.backoff_policy_arg().is_some());
}

/// A backoff-only configuration builds a policy but leaves the retry (attempt/elapsed) policy
/// untouched, and vice versa — the two families are independent.
#[test]
fn retry_and_backoff_are_independent() {
    let mut backoff_only = RetryConfig::default();
    set_backoff_multiplier(&mut backoff_only, s("4")).unwrap();
    assert!(backoff_only.retry_policy_arg().is_none());
    assert!(backoff_only.backoff_policy_arg().is_some());

    let mut retry_only = RetryConfig::default();
    retry_only.set_max_attempts(s("5")).unwrap();
    assert!(retry_only.retry_policy_arg().is_some());
    assert!(retry_only.backoff_policy_arg().is_none());
}

/// Backoff knobs inherit into a copied (statement) config and override independently, mirroring
/// the attempt / elapsed-time inheritance test above.
#[test]
fn copied_config_inherits_then_overrides_backoff_independently() {
    let mut connection = RetryConfig::default();
    set_backoff_initial_seconds(&mut connection, s("0.5")).unwrap();
    set_backoff_max_seconds(&mut connection, s("40")).unwrap();
    set_backoff_multiplier(&mut connection, s("3")).unwrap();

    let mut statement = connection;
    assert_eq!(
        string(statement.backoff_initial_seconds).as_deref(),
        Some("0.5")
    );
    assert_eq!(string(statement.backoff_max_seconds).as_deref(), Some("40"));
    assert_eq!(string(statement.backoff_multiplier).as_deref(), Some("3"));

    set_backoff_max_seconds(&mut statement, s("")).unwrap();
    set_backoff_multiplier(&mut statement, s("2")).unwrap();
    assert_eq!(string(statement.backoff_max_seconds), None);
    assert_eq!(string(statement.backoff_multiplier).as_deref(), Some("2"));
    // The connection is unaffected by statement-level overrides.
    assert_eq!(
        string(connection.backoff_max_seconds).as_deref(),
        Some("40")
    );
    assert_eq!(string(connection.backoff_multiplier).as_deref(), Some("3"));
}

/// The three backoff knobs are opaque `f64`s at the option layer, so out-of-range values reach
/// the gax builder; [`ExponentialBackoffBuilder::clamp`] is what keeps building the policy
/// infallible. Nothing pinned that, and a switch to `build()` would turn a value the option
/// layer accepts into a failure (or a panic) at the first retried RPC.
#[test]
fn backoff_knobs_are_clamped_into_the_gax_recommended_ranges() {
    let rendered = |config: &RetryConfig| format!("{:?}", config.backoff_policy_arg().unwrap());

    // Everything below the floor: initial delay >= 1ms, maximum delay >= 1s, multiplier >= 1.0.
    let mut low = RetryConfig::default();
    set_backoff_initial_seconds(&mut low, s("0.0005")).unwrap();
    set_backoff_max_seconds(&mut low, s("0.1")).unwrap();
    set_backoff_multiplier(&mut low, OptionValue::Double(0.5)).unwrap();
    assert_eq!(
        rendered(&low),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1ms, maximum_delay: 1s, \
         scaling: 1.0 })"
    );

    // Everything above the ceiling: maximum delay <= 24h, multiplier <= 32.0.
    let mut high = RetryConfig::default();
    set_backoff_max_seconds(&mut high, s("200000")).unwrap();
    set_backoff_multiplier(&mut high, s("100")).unwrap();
    assert_eq!(
        rendered(&high),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1s, maximum_delay: 86400s, \
         scaling: 32.0 })"
    );

    // An initial delay past the maximum is an empty range, which `build()` rejects outright;
    // clamping collapses it onto the maximum instead.
    let mut inverted = RetryConfig::default();
    set_backoff_initial_seconds(&mut inverted, s("30")).unwrap();
    set_backoff_max_seconds(&mut inverted, s("5")).unwrap();
    assert_eq!(
        rendered(&inverted),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 5s, maximum_delay: 5s, \
         scaling: 2.0 })"
    );

    // Setting one knob leaves the other two at the client's own defaults (1s / 60s / x2).
    let mut one = RetryConfig::default();
    set_backoff_multiplier(&mut one, s("3")).unwrap();
    assert_eq!(
        rendered(&one),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1s, maximum_delay: 60s, \
         scaling: 3.0 })"
    );
}
