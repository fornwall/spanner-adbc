#![no_main]

use libfuzzer_sys::fuzz_target;
use spanner_adbc::fuzzing::{
    named_parameters, quote_ident, resolve_parameter_names, split_statements,
};

// Check parameter extraction, column-to-parameter mapping and identifier quoting against
// independent invariants for arbitrary queries and bound column names.
fuzz_target!(|input: (String, Vec<String>, bool)| {
    let (sql, column_names, bind_by_name) = input;

    // Names are non-empty, distinct ignoring case (as Spanner resolves them), free of delimiters
    // and escapes, and taken from the SQL. They need not follow the `@` directly: `@ p`,
    // `@/*c*/p` and ``@`p` `` all name `p`.
    let params = named_parameters(&sql);
    for (i, name) in params.iter().enumerate() {
        assert!(
            !params[..i]
                .iter()
                .any(|p| p.eq_ignore_ascii_case(name.as_str())),
            "duplicate parameter {name:?}"
        );
        assert!(!name.is_empty(), "empty parameter name");
        assert!(
            !name.contains(['`', '\\']),
            "parameter {name:?} carries a delimiter or escape"
        );
        assert!(
            sql.contains(name.as_str()),
            "{name} does not occur in {sql:?}"
        );
    }

    // By name, binding succeeds iff every column names a distinct parameter (ignoring case).
    // Positionally, it succeeds iff the counts match. The wrapper checks that rejections are
    // InvalidArguments.
    let matching = |column: &String| {
        params
            .iter()
            .find(|p| p.eq_ignore_ascii_case(column.as_str()))
    };
    let all_named = column_names.iter().all(|c| matching(c).is_some());
    let all_distinct = !column_names.iter().enumerate().any(|(i, c)| {
        column_names[..i]
            .iter()
            .any(|earlier| earlier.eq_ignore_ascii_case(c.as_str()))
    });
    match resolve_parameter_names(&sql, &column_names, bind_by_name) {
        Some(resolved) if bind_by_name => {
            assert!(
                all_named && all_distinct,
                "by-name accepted an unmatched or duplicated column: {column_names:?}"
            );
            // Each column binds under the query's own spelling of the parameter it names.
            let expected: Vec<String> = column_names
                .iter()
                .map(|c| matching(c).expect("checked above").clone())
                .collect();
            assert_eq!(resolved, expected);
        }
        Some(resolved) => assert_eq!(resolved, params),
        None if bind_by_name => assert!(
            !all_named || !all_distinct,
            "by-name rejected a fully-matching pairing: {sql:?} / {column_names:?}"
        ),
        None => assert!(
            params.len() != column_names.len(),
            "positional rejected a count-matching pairing: {sql:?} / {column_names:?}"
        ),
    }

    // An accepted identifier must have no backtick or backslash in its body. Spanner's query
    // parser honors `` \` `` inside backticks but its DDL parser does not, so only such a body is
    // safe under both.
    for ident in column_names
        .iter()
        .map(String::as_str)
        .chain([sql.as_str()])
    {
        let Some(quoted) = quote_ident(ident) else {
            continue;
        };
        assert!(
            quoted.len() >= 2 && quoted.starts_with('`') && quoted.ends_with('`'),
            "not backtick-delimited: {quoted:?}"
        );
        let body = &quoted[1..quoted.len() - 1];
        assert!(
            !body.contains(['`', '\\']),
            "accepted identifier {quoted:?} is not one opaque token"
        );
        assert_eq!(body, ident, "quoting {ident:?} altered it");

        let embedded = format!("SELECT {quoted} FROM t; SELECT 1");
        assert_eq!(
            split_statements(&embedded),
            vec![format!("SELECT {quoted} FROM t"), "SELECT 1".to_string()],
            "quoted identifier {quoted:?} leaked into the surrounding SQL"
        );
    }
});
