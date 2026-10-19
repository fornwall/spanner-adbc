#![no_main]

use libfuzzer_sys::fuzz_target;

// Statement splitting and DDL / `THEN RETURN` detection on arbitrary SQL.
fuzz_target!(|sql: String| {
    let statements = spanner_adbc::fuzzing::split_statements(&sql);

    // Statements are non-empty, trimmed slices of the input, in order.
    let mut cursor = 0;
    for statement in &statements {
        assert!(!statement.is_empty());
        assert_eq!(statement.as_str(), statement.trim());
        match sql[cursor..].find(statement.as_str()) {
            Some(pos) => cursor += pos + statement.len(),
            None => panic!("statement {statement:?} is not an in-order substring of {sql:?}"),
        }
    }

    let _ = spanner_adbc::fuzzing::is_ddl(&sql);
    let _ = spanner_adbc::fuzzing::is_dml_returning(&sql);
    for statement in &statements {
        let _ = spanner_adbc::fuzzing::is_ddl(statement);
        let _ = spanner_adbc::fuzzing::is_dml_returning(statement);
    }
});
