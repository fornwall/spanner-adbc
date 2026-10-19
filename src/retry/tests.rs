use super::*;
use adbc_core::error::Status;

use crate::options::SharedConfig;
use crate::{
    OPTION_RETRY_BACKOFF_INITIAL_SECONDS as BACKOFF_INITIAL_SECONDS,
    OPTION_RETRY_BACKOFF_MAX_SECONDS as BACKOFF_MAX_SECONDS,
    OPTION_RETRY_BACKOFF_MULTIPLIER as BACKOFF_MULTIPLIER,
    OPTION_RETRY_MAX_ATTEMPTS as MAX_ATTEMPTS,
    OPTION_RETRY_MAX_ELAPSED_SECONDS as MAX_ELAPSED_SECONDS,
};

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

#[test]
fn parses_attempts_from_strings_ints_and_whole_doubles() {
    let mut config = SharedConfig::default();
    config.set_option(MAX_ATTEMPTS, s(" 3 ")).unwrap();
    assert_eq!(config.option_string(MAX_ATTEMPTS).unwrap(), "3");
    config
        .set_option(MAX_ATTEMPTS, OptionValue::Int(5))
        .unwrap();
    assert_eq!(config.option_string(MAX_ATTEMPTS).unwrap(), "5");
    config
        .set_option(MAX_ATTEMPTS, OptionValue::Double(2.0))
        .unwrap();
    assert_eq!(config.option_string(MAX_ATTEMPTS).unwrap(), "2");
    // One attempt, no retries.
    config.set_option(MAX_ATTEMPTS, s("1")).unwrap();
    assert_eq!(config.option_string(MAX_ATTEMPTS).unwrap(), "1");
}

#[test]
fn parses_elapsed_from_strings_ints_and_doubles() {
    let mut config = SharedConfig::default();
    config.set_option(MAX_ELAPSED_SECONDS, s(" 2.5 ")).unwrap();
    assert_eq!(config.option_string(MAX_ELAPSED_SECONDS).unwrap(), "2.5");
    assert_eq!(
        config.retry.max_elapsed_duration(),
        Some(Duration::from_millis(2500))
    );
    config
        .set_option(MAX_ELAPSED_SECONDS, OptionValue::Int(30))
        .unwrap();
    assert_eq!(config.option_string(MAX_ELAPSED_SECONDS).unwrap(), "30");
    config
        .set_option(MAX_ELAPSED_SECONDS, OptionValue::Double(0.05))
        .unwrap();
    assert_eq!(
        config.retry.max_elapsed_duration(),
        Some(Duration::from_millis(50))
    );
}

#[test]
fn empty_string_unsets_each_independently() {
    let mut config = SharedConfig::default();
    config.set_option(MAX_ATTEMPTS, s("4")).unwrap();
    config.set_option(MAX_ELAPSED_SECONDS, s("10")).unwrap();
    config.set_option(MAX_ATTEMPTS, s("")).unwrap();
    assert_eq!(
        config.option_string(MAX_ATTEMPTS).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(config.option_string(MAX_ELAPSED_SECONDS).unwrap(), "10");
    // Whitespace-only counts as empty too.
    config.set_option(MAX_ELAPSED_SECONDS, s("  ")).unwrap();
    assert_eq!(
        config
            .option_string(MAX_ELAPSED_SECONDS)
            .unwrap_err()
            .status,
        Status::NotFound
    );
}

#[test]
fn rejects_bad_attempts() {
    let mut config = SharedConfig::default();
    config.set_option(MAX_ATTEMPTS, s("3")).unwrap();
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
        OptionValue::Int(i64::from(u32::MAX) + 1),
    ];
    for value in bad {
        let error = config.set_option(MAX_ATTEMPTS, value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        assert_eq!(config.option_string(MAX_ATTEMPTS).unwrap(), "3");
    }
}

#[test]
fn rejects_bad_elapsed() {
    let mut config = SharedConfig::default();
    config.set_option(MAX_ELAPSED_SECONDS, s("5")).unwrap();
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
        let error = config
            .set_option(MAX_ELAPSED_SECONDS, value.clone())
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        assert_eq!(config.option_string(MAX_ELAPSED_SECONDS).unwrap(), "5");
    }
}

#[test]
fn retry_policy_arg_is_none_until_configured() {
    let mut config = SharedConfig::default();
    assert!(config.retry.retry_policy_arg().is_none());
    config.set_option(MAX_ATTEMPTS, s("3")).unwrap();
    assert!(config.retry.retry_policy_arg().is_some());
    config.set_option(MAX_ATTEMPTS, s("")).unwrap();
    assert!(config.retry.retry_policy_arg().is_none());
    config.set_option(MAX_ELAPSED_SECONDS, s("10")).unwrap();
    assert!(config.retry.retry_policy_arg().is_some());
    config.set_option(MAX_ATTEMPTS, s("5")).unwrap();
    assert!(config.retry.retry_policy_arg().is_some());
}

#[test]
fn statement_inherits_then_overrides_independently() {
    let mut connection = SharedConfig::default();
    connection.set_option(MAX_ATTEMPTS, s("10")).unwrap();
    connection.set_option(MAX_ELAPSED_SECONDS, s("20")).unwrap();

    let mut statement = connection.inherit();
    assert_eq!(statement.option_string(MAX_ATTEMPTS).unwrap(), "10");
    assert_eq!(statement.option_string(MAX_ELAPSED_SECONDS).unwrap(), "20");

    statement.set_option(MAX_ATTEMPTS, s("2")).unwrap();
    statement.set_option(MAX_ELAPSED_SECONDS, s("")).unwrap();
    assert_eq!(statement.option_string(MAX_ATTEMPTS).unwrap(), "2");
    assert_eq!(
        statement
            .option_string(MAX_ELAPSED_SECONDS)
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(connection.option_string(MAX_ATTEMPTS).unwrap(), "10");
    assert_eq!(connection.option_string(MAX_ELAPSED_SECONDS).unwrap(), "20");
}

#[test]
fn parses_backoff_knobs_from_strings_ints_and_doubles() {
    let mut config = SharedConfig::default();
    config
        .set_option(BACKOFF_INITIAL_SECONDS, s(" 0.5 "))
        .unwrap();
    assert_eq!(
        config.option_string(BACKOFF_INITIAL_SECONDS).unwrap(),
        "0.5"
    );
    config
        .set_option(BACKOFF_MAX_SECONDS, OptionValue::Int(30))
        .unwrap();
    assert_eq!(config.option_string(BACKOFF_MAX_SECONDS).unwrap(), "30");
    config
        .set_option(BACKOFF_MULTIPLIER, OptionValue::Double(1.5))
        .unwrap();
    assert_eq!(config.option_string(BACKOFF_MULTIPLIER).unwrap(), "1.5");
}

#[test]
fn empty_string_unsets_each_backoff_knob_independently() {
    let mut config = SharedConfig::default();
    config.set_option(BACKOFF_INITIAL_SECONDS, s("1")).unwrap();
    config.set_option(BACKOFF_MAX_SECONDS, s("10")).unwrap();
    config.set_option(BACKOFF_MULTIPLIER, s("2")).unwrap();

    config.set_option(BACKOFF_INITIAL_SECONDS, s("")).unwrap();
    assert_eq!(
        config
            .option_string(BACKOFF_INITIAL_SECONDS)
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(config.option_string(BACKOFF_MAX_SECONDS).unwrap(), "10");
    assert_eq!(config.option_string(BACKOFF_MULTIPLIER).unwrap(), "2");
    // Whitespace-only counts as empty too.
    config.set_option(BACKOFF_MAX_SECONDS, s("  ")).unwrap();
    assert_eq!(
        config
            .option_string(BACKOFF_MAX_SECONDS)
            .unwrap_err()
            .status,
        Status::NotFound
    );
    config.set_option(BACKOFF_MULTIPLIER, s("")).unwrap();
    assert_eq!(
        config.option_string(BACKOFF_MULTIPLIER).unwrap_err().status,
        Status::NotFound
    );
}

#[test]
fn rejects_bad_backoff_seconds() {
    for key in [BACKOFF_INITIAL_SECONDS, BACKOFF_MAX_SECONDS] {
        let mut config = SharedConfig::default();
        config.set_option(key, s("2")).unwrap();
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
            let error = config.set_option(key, value.clone()).unwrap_err();
            assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        }
    }
}

#[test]
fn rejects_bad_backoff_multiplier() {
    let mut config = SharedConfig::default();
    config.set_option(BACKOFF_MULTIPLIER, s("2")).unwrap();
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
        let error = config
            .set_option(BACKOFF_MULTIPLIER, value.clone())
            .unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        assert_eq!(config.option_string(BACKOFF_MULTIPLIER).unwrap(), "2");
    }
    // Multipliers below 1.0 are accepted and clamped when building the policy.
    config
        .set_option(BACKOFF_MULTIPLIER, OptionValue::Double(0.5))
        .unwrap();
    assert_eq!(config.option_string(BACKOFF_MULTIPLIER).unwrap(), "0.5");
}

#[test]
fn backoff_policy_arg_is_none_until_configured() {
    let mut config = SharedConfig::default();
    assert!(config.retry.backoff_policy_arg().is_none());
    config.set_option(MAX_ATTEMPTS, s("3")).unwrap();
    config.set_option(MAX_ELAPSED_SECONDS, s("10")).unwrap();
    assert!(config.retry.backoff_policy_arg().is_none());
    config
        .set_option(BACKOFF_INITIAL_SECONDS, s("0.25"))
        .unwrap();
    assert!(config.retry.backoff_policy_arg().is_some());
    config.set_option(BACKOFF_INITIAL_SECONDS, s("")).unwrap();
    assert!(config.retry.backoff_policy_arg().is_none());
    config.set_option(BACKOFF_MAX_SECONDS, s("30")).unwrap();
    assert!(config.retry.backoff_policy_arg().is_some());
    config.set_option(BACKOFF_MAX_SECONDS, s("")).unwrap();
    assert!(config.retry.backoff_policy_arg().is_none());
    config.set_option(BACKOFF_MULTIPLIER, s("3")).unwrap();
    assert!(config.retry.backoff_policy_arg().is_some());
}

#[test]
fn retry_and_backoff_are_independent() {
    let mut backoff_only = SharedConfig::default();
    backoff_only.set_option(BACKOFF_MULTIPLIER, s("4")).unwrap();
    assert!(backoff_only.retry.retry_policy_arg().is_none());
    assert!(backoff_only.retry.backoff_policy_arg().is_some());

    let mut retry_only = SharedConfig::default();
    retry_only.set_option(MAX_ATTEMPTS, s("5")).unwrap();
    assert!(retry_only.retry.retry_policy_arg().is_some());
    assert!(retry_only.retry.backoff_policy_arg().is_none());
}

#[test]
fn statement_inherits_then_overrides_backoff_independently() {
    let mut connection = SharedConfig::default();
    connection
        .set_option(BACKOFF_INITIAL_SECONDS, s("0.5"))
        .unwrap();
    connection.set_option(BACKOFF_MAX_SECONDS, s("40")).unwrap();
    connection.set_option(BACKOFF_MULTIPLIER, s("3")).unwrap();

    let mut statement = connection.inherit();
    assert_eq!(
        statement.option_string(BACKOFF_INITIAL_SECONDS).unwrap(),
        "0.5"
    );
    assert_eq!(statement.option_string(BACKOFF_MAX_SECONDS).unwrap(), "40");
    assert_eq!(statement.option_string(BACKOFF_MULTIPLIER).unwrap(), "3");

    statement.set_option(BACKOFF_MAX_SECONDS, s("")).unwrap();
    statement.set_option(BACKOFF_MULTIPLIER, s("2")).unwrap();
    assert_eq!(
        statement
            .option_string(BACKOFF_MAX_SECONDS)
            .unwrap_err()
            .status,
        Status::NotFound
    );
    assert_eq!(statement.option_string(BACKOFF_MULTIPLIER).unwrap(), "2");
    assert_eq!(connection.option_string(BACKOFF_MAX_SECONDS).unwrap(), "40");
    assert_eq!(connection.option_string(BACKOFF_MULTIPLIER).unwrap(), "3");
}

/// Out-of-range backoff values pass option validation. Using `build()` instead of `clamp()` would
/// make them fail at the first retried RPC.
#[test]
fn backoff_knobs_are_clamped_into_the_gax_recommended_ranges() {
    let rendered =
        |config: &SharedConfig| format!("{:?}", config.retry.backoff_policy_arg().unwrap());

    // Floors: initial delay 1ms, maximum delay 1s, multiplier 1.0.
    let mut low = SharedConfig::default();
    low.set_option(BACKOFF_INITIAL_SECONDS, s("0.0005"))
        .unwrap();
    low.set_option(BACKOFF_MAX_SECONDS, s("0.1")).unwrap();
    low.set_option(BACKOFF_MULTIPLIER, OptionValue::Double(0.5))
        .unwrap();
    assert_eq!(
        rendered(&low),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1ms, maximum_delay: 1s, \
         scaling: 1.0 })"
    );

    // Ceilings: maximum delay 24h, multiplier 32.0.
    let mut high = SharedConfig::default();
    high.set_option(BACKOFF_MAX_SECONDS, s("200000")).unwrap();
    high.set_option(BACKOFF_MULTIPLIER, s("100")).unwrap();
    assert_eq!(
        rendered(&high),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1s, maximum_delay: 86400s, \
         scaling: 32.0 })"
    );

    // An initial delay past the maximum is clamped to it.
    let mut inverted = SharedConfig::default();
    inverted
        .set_option(BACKOFF_INITIAL_SECONDS, s("30"))
        .unwrap();
    inverted.set_option(BACKOFF_MAX_SECONDS, s("5")).unwrap();
    assert_eq!(
        rendered(&inverted),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 5s, maximum_delay: 5s, \
         scaling: 2.0 })"
    );

    // Unset values keep the client defaults.
    let mut one = SharedConfig::default();
    one.set_option(BACKOFF_MULTIPLIER, s("3")).unwrap();
    assert_eq!(
        rendered(&one),
        "BackoffPolicyArg(ExponentialBackoff { initial_delay: 1s, maximum_delay: 60s, \
         scaling: 3.0 })"
    );
}

/// Option validation accepts any value that fits in a `Duration`, but gax adds the limit to an
/// `Instant` before the first attempt, which panics on overflow.
#[test]
fn huge_elapsed_limit_does_not_overflow_instant_arithmetic() {
    use google_cloud_gax::retry_policy::RetryPolicy;
    use google_cloud_gax::retry_state::RetryState;

    let mut config = SharedConfig::default();
    config.set_option(MAX_ELAPSED_SECONDS, s("1e19")).unwrap();
    assert_eq!(
        config.option_string(MAX_ELAPSED_SECONDS).unwrap(),
        "10000000000000000000"
    );
    let policy: std::sync::Arc<dyn RetryPolicy> = config.retry.retry_policy_arg().unwrap().into();
    let remaining = policy.remaining_time(&RetryState::new(true));
    assert!(remaining.is_some_and(|r| r > Duration::from_secs(365 * 24 * 60 * 60)));
}
