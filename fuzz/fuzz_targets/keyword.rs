#![no_main]

use libfuzzer_sys::fuzz_target;
use spanner_adbc::fuzzing::{
    first_keyword, is_ddl, is_dml, split_statements, strip_trailing_terminators,
};

// Check statement classification and trailing-terminator stripping against independent
// restatements of their contracts.
fuzz_target!(|sql: String| {
    let keyword = first_keyword(&sql);

    if let Some(kw) = &keyword {
        assert!(!kw.is_empty(), "empty keyword for {sql:?}");
        assert!(
            kw.chars().all(|c| c.is_ascii_uppercase()),
            "keyword {kw:?} is not uppercase ASCII"
        );
        // The keyword comes from the input, never synthesized.
        assert!(
            sql.to_ascii_uppercase().contains(kw.as_str()),
            "keyword {kw:?} does not occur in {sql:?}"
        );
    }

    // `is_ddl` and `is_dml` test the first keyword against these lists, copied from the driver.
    const DDL: &[&str] = &[
        "CREATE", "DROP", "ALTER", "RENAME", "GRANT", "REVOKE", "ANALYZE",
    ];
    const DML: &[&str] = &["INSERT", "UPDATE", "DELETE"];
    let kw = keyword.as_deref();
    assert_eq!(is_ddl(&sql), kw.is_some_and(|k| DDL.contains(&k)));
    assert_eq!(is_dml(&sql), kw.is_some_and(|k| DML.contains(&k)));

    // Leading whitespace, comments and statement hints do not change the keyword.
    for prefix in [
        " \t\n",
        "-- line comment\n",
        "# hash comment\n",
        "/* block comment */",
        "@{USE_ADDITIONAL_PARALLELISM=TRUE} ",
        "@{HINT='}'}", // a `}` inside a hint's string literal must not close the hint
    ] {
        assert_eq!(
            first_keyword(&format!("{prefix}{sql}")),
            keyword,
            "prefix {prefix:?} changed the keyword of {sql:?}"
        );
    }

    // Stripping returns a substring of the input, is idempotent, and never changes how the
    // splitter divides the SQL (so `SELECT ';'` keeps its literal).
    let stripped = strip_trailing_terminators(&sql);
    assert!(
        sql.contains(&stripped),
        "{stripped:?} is not a substring of {sql:?}"
    );
    assert_eq!(
        strip_trailing_terminators(&stripped),
        stripped,
        "stripping is not idempotent for {sql:?}"
    );
    assert_eq!(
        split_statements(&stripped),
        split_statements(&sql),
        "stripping changed the statements of {sql:?}"
    );
});
