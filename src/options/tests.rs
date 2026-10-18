use super::*;
use adbc_core::error::Status;

#[test]
fn int_getter_parses_integer_valued_options() {
    let get = |v: &str| int_from_stored_string(Ok(v.to_string()), "option o");
    assert_eq!(get("8192").unwrap(), 8192);
    assert_eq!(get("-5").unwrap(), -5);
    assert_eq!(get("0").unwrap(), 0);
}

#[test]
fn bool_option_accepts_exact_true_false_and_rejects_int_typed_values() {
    // The string forms are exactly "true"/"false": lenient spellings (case variants,
    // 1/0, yes/no) are rejected with an error naming the option and the expected spellings.
    assert!(bool_option(OptionValue::String("true".into()), "option o").unwrap());
    assert!(!bool_option(OptionValue::String("false".into()), "option o").unwrap());
    for s in ["TRUE", "1", "yes", "0", "No", "maybe"] {
        let error = bool_option(OptionValue::String(s.into()), "option o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "string {s:?}");
        assert!(error.message.contains("option o"), "{}", error.message);
        assert!(
            error.message.contains("\"true\" or \"false\""),
            "{}",
            error.message
        );
    }
    // An int-typed set is rejected too: the getters serve the canonical
    // "true"/"false" string, so accepting SetOptionInt(k, 1) would break the spec's set/get
    // type symmetry — and no surveyed ADBC driver accepts an int set for a boolean option.
    for i in [0, 1, -1] {
        let error = bool_option(OptionValue::Int(i), "option o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "Int({i})");
        assert!(error.message.contains("option o"), "{}", error.message);
        assert!(
            error.message.contains("\"true\"/\"false\""),
            "{}",
            error.message
        );
    }
}

#[test]
fn int_getter_reports_set_but_non_integer_values_as_invalid_arguments() {
    // A value that exists but is not an integer must NOT be NotFound (that would read as
    // "option unset"): it is an InvalidArguments error naming the option and the value.
    for value in ["true", "max:10s", "1.5", "", "projects/p"] {
        let error = int_from_stored_string(Ok(value.to_string()), "option o").unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments, "value {value:?}");
        assert!(error.message.contains("option o"), "{}", error.message);
        assert!(
            error.message.contains(&format!("{value:?}")),
            "{}",
            error.message
        );
    }
}

#[test]
fn typed_getters_propagate_the_string_lookup_error_unchanged() {
    // Unset (NotFound) and unknown-key errors from get_option_string pass through as-is.
    let unset = || crate::error::option_not_set("o");
    let expected = unset().message;
    let error = int_from_stored_string(Err(unset()), "option o").unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert_eq!(error.message, expected);
    let error = double_from_stored_string(Err(unset()), "option o").unwrap_err();
    assert_eq!(error.status, Status::NotFound);
    assert_eq!(error.message, expected);
}

#[test]
fn double_getter_parses_and_rejects() {
    let get = |v: &str| double_from_stored_string(Ok(v.to_string()), "option o");
    assert_eq!(get("1.5").unwrap(), 1.5);
    // Integer-valued options can always be represented as doubles.
    assert_eq!(get("3600").unwrap(), 3600.0);
    let error = get("true").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("option o"), "{}", error.message);
}
