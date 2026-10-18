use adbc_core::error::Status;
use adbc_core::options::OptionValue;

use crate::options::SharedConfig;
use crate::{
    OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE as STATISTICS_PACKAGE,
    OPTION_QUERY_OPTIMIZER_VERSION as VERSION,
};

fn s(v: &str) -> OptionValue {
    OptionValue::String(v.to_string())
}

#[test]
fn options_round_trip_verbatim_and_unset_on_empty() {
    let mut config = SharedConfig::default();
    assert_eq!(
        config.option_string(VERSION).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(
        config.option_string(STATISTICS_PACKAGE).unwrap_err().status,
        Status::NotFound
    );

    // Opaque values are stored verbatim (no trimming or case folding).
    config.set_option(VERSION, s("latest")).unwrap();
    assert_eq!(config.option_string(VERSION).unwrap(), "latest");
    config
        .set_option(STATISTICS_PACKAGE, s("auto_20240101"))
        .unwrap();
    assert_eq!(
        config.option_string(STATISTICS_PACKAGE).unwrap(),
        "auto_20240101"
    );

    // The two fields are independent.
    config.set_option(VERSION, s("")).unwrap();
    assert_eq!(
        config.option_string(VERSION).unwrap_err().status,
        Status::NotFound
    );
    assert_eq!(
        config.option_string(STATISTICS_PACKAGE).unwrap(),
        "auto_20240101"
    );
    config.set_option(STATISTICS_PACKAGE, s("")).unwrap();
    assert_eq!(
        config.option_string(STATISTICS_PACKAGE).unwrap_err().status,
        Status::NotFound
    );
}

#[test]
fn non_string_values_are_rejected() {
    let mut config = SharedConfig::default();
    for value in [OptionValue::Int(1), OptionValue::Double(1.0)] {
        let error = config.set_option(VERSION, value.clone()).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
        let error = config.set_option(STATISTICS_PACKAGE, value).unwrap_err();
        assert_eq!(error.status, Status::InvalidArguments);
    }
}

/// Statement options inherit connection values, then override them independently.
#[test]
fn statement_inherits_then_overrides_independently() {
    let mut connection = SharedConfig::default();
    connection.set_option(VERSION, s("6")).unwrap();
    connection
        .set_option(STATISTICS_PACKAGE, s("pkg_conn"))
        .unwrap();

    let mut statement = connection.inherit();
    assert_eq!(statement.option_string(VERSION).unwrap(), "6");
    assert_eq!(
        statement.option_string(STATISTICS_PACKAGE).unwrap(),
        "pkg_conn"
    );

    statement.set_option(VERSION, s("latest")).unwrap();
    statement.set_option(STATISTICS_PACKAGE, s("")).unwrap();
    assert_eq!(statement.option_string(VERSION).unwrap(), "latest");
    assert_eq!(
        statement
            .option_string(STATISTICS_PACKAGE)
            .unwrap_err()
            .status,
        Status::NotFound
    );
    // The connection is unaffected by statement-level overrides.
    assert_eq!(connection.option_string(VERSION).unwrap(), "6");
    assert_eq!(
        connection.option_string(STATISTICS_PACKAGE).unwrap(),
        "pkg_conn"
    );
}

/// `apply_to_statement` must leave the request's query options *absent* when neither knob is set
/// — an empty `QueryOptions` is not the same as none, and sending one would replace the service
/// default optimizer with an explicitly empty selection. When a knob is set, the value reaches the
/// built statement verbatim.
#[test]
fn apply_to_statement_sets_query_options_only_when_configured() {
    let builder = || google_cloud_spanner::statement::Statement::builder("SELECT 1");
    let rendered = |config: &SharedConfig| {
        format!(
            "{:?}",
            config.query_options.apply_to_statement(builder()).build()
        )
    };

    // Unset: the statement carries no query options at all.
    let unset = SharedConfig::default();
    let built = rendered(&unset);
    assert!(built.contains("query_options: None"), "{built}");

    // Each knob alone sets only its own field, leaving the other empty.
    let mut version_only = SharedConfig::default();
    version_only.set_option(VERSION, s("latest")).unwrap();
    let built = rendered(&version_only);
    assert!(built.contains(r#"optimizer_version: "latest""#), "{built}");
    assert!(
        built.contains(r#"optimizer_statistics_package: """#),
        "{built}"
    );

    // Both: both values pass through verbatim.
    let mut both = version_only;
    both.set_option(STATISTICS_PACKAGE, s("auto_20240101"))
        .unwrap();
    let built = rendered(&both);
    assert!(built.contains(r#"optimizer_version: "latest""#), "{built}");
    assert!(
        built.contains(r#"optimizer_statistics_package: "auto_20240101""#),
        "{built}"
    );
}
