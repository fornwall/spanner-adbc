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
    // NotFound would mean the option is unset.
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
    assert_eq!(get("3600").unwrap(), 3600.0);
    let error = get("true").unwrap_err();
    assert_eq!(error.status, Status::InvalidArguments);
    assert!(error.message.contains("option o"), "{}", error.message);
}
