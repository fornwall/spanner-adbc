use super::*;
use adbc_core::error::Status;

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

#[test]
fn bool_option_accepts_exact_true_false_only() {
    assert!(bool_option(s("true"), "o").unwrap());
    assert!(!bool_option(s("false"), "o").unwrap());
    for (value, shown) in [
        (s("TRUE"), "\"TRUE\""),
        (s("1"), "\"1\""),
        (s(""), "\"\""),
        (OptionValue::Int(1), "the integer 1"),
        (OptionValue::Double(0.0), "the double 0"),
    ] {
        let error = bool_option(value, "o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        assert_eq!(
            error.message,
            format!("option o expects the string \"true\" or \"false\", got {shown}")
        );
    }
    assert!(!bool_option_unsettable(s(" "), "o").unwrap());
}

#[test]
fn numeric_parsers_name_the_option_and_echo_the_value() {
    let error = f64_option(s("soon"), "o", F64Range::NonNegativeSeconds).unwrap_err();
    assert_eq!(
        error.message,
        "option o expects a finite, non-negative number of seconds, got \"soon\""
    );
    for bad in [s("-1"), s("inf"), s("NaN"), OptionValue::Double(1e20)] {
        assert!(f64_option(bad, "o", F64Range::NonNegativeSeconds).is_err());
    }
    // Only the seconds ranges must fit in a `Duration`.
    assert_eq!(
        f64_option(OptionValue::Double(1e20), "o", F64Range::PositiveFactor).unwrap(),
        Some(1e20)
    );
    assert_eq!(
        f64_option(s(" 0 "), "o", F64Range::NonNegativeSeconds).unwrap(),
        Some(0.0)
    );
    assert!(f64_option(s("0"), "o", F64Range::PositiveSeconds).is_err());
    assert_eq!(
        f64_option(s(""), "o", F64Range::PositiveSeconds).unwrap(),
        None
    );

    let error = positive_usize(OptionValue::Int(0), "o").unwrap_err();
    assert_eq!(
        error.message,
        "option o expects a positive integer, got the integer 0"
    );
}

#[test]
fn typed_getters_parse_the_stored_string() {
    assert_eq!(int_from_stored_string(Ok("-5".into()), "o").unwrap(), -5);
    assert_eq!(
        double_from_stored_string(Ok("1.5".into()), "o").unwrap(),
        1.5
    );
    // NotFound would mean the option is unset.
    let error = int_from_stored_string(Ok("1.5".into()), "o").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert_eq!(error.message, "option o value \"1.5\" is not an integer");
    let error = double_from_stored_string(Ok("true".into()), "o").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
}

#[test]
fn typed_getters_propagate_the_string_lookup_error_unchanged() {
    let unset = || crate::error::option_not_set("o");
    let expected = unset().message;
    let error = int_from_stored_string(Err(unset()), "o").unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert_eq!(error.message, expected);
    let error = double_from_stored_string(Err(unset()), "o").unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert_eq!(error.message, expected);
}
