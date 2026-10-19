use super::*;
use adbc_core::error::Status;

#[test]
fn backslash_binds_the_next_char_in_every_literal_including_raw() {
    // The emulator parses each of these as a single SELECT of one string:
    //     SELECT r'x\'; SELECT 2'          ->  x\'; SELECT 2
    //     SELECT r'''a\'''; SELECT 2'''    ->  a\'''; SELECT 2
    assert_eq!(
        split_statements(r"SELECT r'x\'; DELETE FROM t WHERE true; SELECT 1'"),
        vec![r"SELECT r'x\'; DELETE FROM t WHERE true; SELECT 1'"]
    );
    assert_eq!(
        split_statements(r"SELECT r'''a\'''; DELETE FROM t WHERE true'''"),
        vec![r"SELECT r'''a\'''; DELETE FROM t WHERE true'''"]
    );
    for prefix in ["r", "R", "rb", "RB", "br", "Br", "b", "B", ""] {
        let sql = format!(r"SELECT {prefix}'x\'; DELETE FROM t'");
        assert_eq!(
            split_statements(&sql),
            vec![sql.clone()],
            "prefix {prefix:?}"
        );
    }
    // An even number of backslashes closes the literal.
    assert_eq!(
        split_statements(r"SELECT r'a\\'; DELETE FROM t WHERE stale"),
        vec![r"SELECT r'a\\'", "DELETE FROM t WHERE stale"]
    );
}

#[test]
fn detects_ddl() {
    for sql in [
        "CREATE TABLE t (id INT64) PRIMARY KEY (id)",
        "  drop table t",
        "ALTER TABLE t ADD COLUMN c STRING(MAX)",
        "RENAME TABLE a TO b",
        "-- a comment\nCREATE INDEX idx ON t(c)",
        "/* header */ create table t (id int64) primary key (id)",
        "; /* empty */ ; CREATE TABLE t (id INT64) PRIMARY KEY (id)",
    ] {
        assert!(is_ddl(sql), "should be DDL: {sql}");
    }
}

#[test]
fn detects_non_ddl() {
    for sql in [
        "SELECT 1",
        "INSERT INTO t (id) VALUES (1)",
        "UPDATE t SET c = 1 WHERE id = 1",
        "DELETE FROM t WHERE true",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "",
    ] {
        assert!(!is_ddl(sql), "should not be DDL: {sql}");
    }
}

#[test]
fn detects_dml() {
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "  update t SET c = 1 WHERE id = 1",
        "DELETE FROM t WHERE true",
        "/* c */ insert into t (id) values (1)",
        "; -- empty\n; UPDATE t SET c = 1 WHERE true",
    ] {
        assert!(is_dml(sql), "should be DML: {sql}");
    }
    for sql in [
        "SELECT 1",
        "WITH x AS (SELECT 1) SELECT * FROM x",
        "CREATE TABLE t (id INT64) PRIMARY KEY (id)",
        "",
    ] {
        assert!(!is_dml(sql), "should not be DML: {sql}");
    }
}

#[test]
fn classifies_statements_behind_statement_hints() {
    for sql in [
        "@{USE_ADDITIONAL_PARALLELISM=TRUE} UPDATE t SET c = 1 WHERE true",
        "@{PDML_MAX_PARALLELISM=8} delete from t where true",
        "/* c */ @{A=1} INSERT INTO t (id) VALUES (1)",
        "@{A=1} -- c\n UPDATE t SET c = 1 WHERE true",
        // A quoted `}` does not close the hint.
        "@{A='}'} DELETE FROM t WHERE true",
    ] {
        assert!(is_dml(sql), "should classify as DML: {sql}");
        assert!(!is_ddl(sql), "should not classify as DDL: {sql}");
    }
    assert!(is_ddl("@{A=1} CREATE TABLE t (id INT64) PRIMARY KEY (id)"));
    let hinted_query = "@{JOIN_METHOD=HASH_JOIN} SELECT * FROM t";
    assert!(!is_dml(hinted_query) && !is_ddl(hinted_query));
    // An unterminated hint swallows the rest.
    assert!(!is_dml("@{oops UPDATE t SET c = 1"));
}

#[test]
fn first_keyword_edge_cases() {
    assert_eq!(first_keyword("select 1"), Some("select"));
    assert_eq!(first_keyword("-- c\n# h\n/* b */ Update t"), Some("Update"));
    assert_eq!(
        first_keyword("@{A=1} @{B='}'} DELETE FROM t"),
        Some("DELETE")
    );
    for sql in [
        "",
        "  \n\t",
        "; -- only\n ; /* comments */ ;",
        "/* unterminated",
    ] {
        assert_eq!(first_keyword(sql), None, "no keyword in {sql:?}");
    }
    assert_eq!(first_keyword("`select` 1"), None);
    assert_eq!(first_keyword("'select'"), None);
    // Only the leading ASCII letters count.
    assert_eq!(first_keyword("select_into x"), Some("select"));
    assert_eq!(first_keyword("_select 1"), None);
    assert_eq!(first_keyword("9select 1"), None);
    assert_eq!(first_keyword("@p"), None);
    assert_eq!(first_keyword("@ p"), None);
    assert_eq!(first_keyword("@@seed UPDATE t"), None);
    // Spanner accepts whitespace or a comment between `@` and `{`. Missing the keyword would
    // send hinted DML to a read-only transaction, which rejects it.
    assert_eq!(first_keyword("@ {A=1} SELECT 1"), Some("SELECT"));
    assert_eq!(first_keyword("@\n{A=1} SELECT 1"), Some("SELECT"));
    assert_eq!(first_keyword("@/* c */{A=1} SELECT 1"), Some("SELECT"));
    assert!(is_dml(
        "@ {PDML_MAX_PARALLELISM=8} UPDATE t SET n = 1 WHERE true"
    ));
    assert!(is_ddl("@ {A=1} CREATE TABLE t (id INT64) PRIMARY KEY (id)"));
}

#[test]
fn detects_then_return() {
    for sql in [
        "INSERT INTO t (id) VALUES (1) THEN RETURN id",
        "insert into t (id) values (1) then return *",
        "UPDATE t SET c = 1 WHERE id = 1 THEN\n  RETURN c",
        "DELETE FROM t WHERE id = 1 then /* keep */ return id",
        "DELETE FROM t WHERE id = 1 THEN -- comment\n RETURN id",
    ] {
        assert!(is_dml_returning(sql), "should detect THEN RETURN: {sql}");
    }
    // `\'` does not close a raw literal; `\\'` does.
    assert!(!is_dml_returning(
        r"UPDATE t SET s = r'x\' WHERE true THEN RETURN Id"
    ));
    assert!(is_dml_returning(
        r"UPDATE t SET s = r'x\\' WHERE true THEN RETURN Id"
    ));
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "SELECT CASE WHEN a THEN b ELSE c END FROM t",
        // Inside literals and quoted identifiers.
        "INSERT INTO t (s) VALUES ('THEN RETURN')",
        "INSERT INTO t (s) VALUES ('''THEN RETURN''')",
        r"INSERT INTO t (s) VALUES (r'THEN RETURN\\')",
        "UPDATE t SET `then return` = 1 WHERE true",
        // Inside comments.
        "UPDATE t SET a = 1 /* THEN RETURN */ WHERE true",
        "UPDATE t SET a = 1 WHERE true -- THEN RETURN",
        "UPDATE t SET a = 1 WHERE true # THEN RETURN",
        // Adjacent words, not the two keywords.
        "UPDATE t SET a = thenreturn WHERE true",
        "UPDATE t SET a = x_then WHERE return_value = 1",
        // A literal between the words breaks the clause.
        "UPDATE t SET a = CASE WHEN b THEN 'x' ELSE returns END WHERE true",
        "",
    ] {
        assert!(!is_dml_returning(sql), "should not detect: {sql}");
    }
}

#[test]
fn then_return_ignores_case_expression_branches() {
    // `RETURN` is not reserved, so a CASE branch can be a column named `return`. Misdetecting it
    // as a clause rejects valid DML in manual transaction mode.
    for sql in [
        "UPDATE t SET x = CASE WHEN c THEN return ELSE 0 END WHERE true",
        "update t set x = case when c then RETURN else 0 end where true",
        // Searched and multi-branch forms.
        "UPDATE t SET x = CASE y WHEN 1 THEN return END WHERE true",
        "UPDATE t SET x = CASE WHEN a THEN return WHEN b THEN return ELSE return END \
         WHERE true",
        // Nested CASE: the inner branch is still inside the outer CASE.
        "UPDATE t SET x = CASE WHEN a THEN CASE WHEN b THEN return END END WHERE true",
        "UPDATE t SET x = CASE WHEN c THEN /* pick */ return ELSE 0 END WHERE true",
    ] {
        assert!(!is_dml_returning(sql), "CASE branch, not a clause: {sql}");
    }
    // A top-level clause after a CASE expression is still detected.
    for sql in [
        "UPDATE t SET x = CASE WHEN c THEN 1 ELSE 0 END WHERE true THEN RETURN x",
        "UPDATE t SET x = CASE WHEN c THEN return ELSE 0 END WHERE true THEN RETURN x",
        "UPDATE t SET x = CASE WHEN a THEN CASE WHEN b THEN 2 END ELSE 0 END WHERE true \
         then return *",
        // Quoted `CASE` does not open a level.
        "UPDATE t SET s = 'CASE' WHERE true THEN RETURN s",
        "UPDATE t SET `case` = 1 WHERE true THEN RETURN `case`",
    ] {
        assert!(is_dml_returning(sql), "top-level clause after CASE: {sql}");
    }
    // An unbalanced `END` saturates at depth zero.
    assert!(is_dml_returning(
        "UPDATE t SET a = b END WHERE true THEN RETURN a"
    ));
}

#[test]
fn lexer_partitions_input_byte_for_byte() {
    for sql in [
        r#"SELECT r'''a\''' /* c */ FROM `t` -- x
               WHERE s = "y;z" # h
               @{HINT=1} AND @p = @@v"#,
        "",
        "-- only a comment, unterminated",
        "/* unterminated block",
        "r'unterminated raw",
    ] {
        let rebuilt: String = lex(sql)
            .map(|lexeme| match lexeme {
                Lexeme::Word(s) | Lexeme::Quoted(s) | Lexeme::Comment(s) => s.to_string(),
                Lexeme::Other(c) => c.to_string(),
            })
            .collect();
        assert_eq!(rebuilt, sql, "lexer did not partition {sql:?} exactly");
    }
    assert_eq!(
        lex(r"a 'b' -- c").collect::<Vec<_>>(),
        vec![
            Lexeme::Word("a"),
            Lexeme::Other(' '),
            Lexeme::Quoted("'b'"),
            Lexeme::Other(' '),
            Lexeme::Comment("-- c"),
        ]
    );
    // `\'` is an escaped quote even behind a raw prefix.
    assert_eq!(
        lex(r"r'x\' y").collect::<Vec<_>>(),
        vec![Lexeme::Word("r"), Lexeme::Quoted(r"'x\' y")]
    );
    assert_eq!(
        lex(r"r'x\\' y").collect::<Vec<_>>(),
        vec![
            Lexeme::Word("r"),
            Lexeme::Quoted(r"'x\\'"),
            Lexeme::Other(' '),
            Lexeme::Word("y"),
        ]
    );
}

#[test]
fn splits_statements() {
    let batch = "CREATE TABLE tmp (id INT64) PRIMARY KEY (id);\n\
                 DROP TABLE target;\n\
                 RENAME TABLE tmp TO target;";
    assert_eq!(
        split_statements(batch),
        vec![
            "CREATE TABLE tmp (id INT64) PRIMARY KEY (id)",
            "DROP TABLE target",
            "RENAME TABLE tmp TO target",
        ]
    );
    assert_eq!(split_statements("   ;  ; "), Vec::<String>::new());
}

#[test]
fn split_drops_comment_only_segments() {
    // Spanner rejects a whole batch containing an empty statement.
    assert_eq!(
        split_statements("DELETE FROM t1; DELETE FROM t2; -- cleanup"),
        vec!["DELETE FROM t1", "DELETE FROM t2"]
    );
    // A comment leading a real statement is kept.
    assert_eq!(
        split_statements("SELECT 1;\n  \n-- a\nSELECT 2; # b\nSELECT 3; /* c */"),
        vec!["SELECT 1", "-- a\nSELECT 2", "# b\nSELECT 3"]
    );
    assert_eq!(
        split_statements("SELECT 1 -- done\n; SELECT 2 /* tail */"),
        vec!["SELECT 1 -- done", "SELECT 2 /* tail */"]
    );
    assert_eq!(
        split_statements("-- just a comment\n/* and a block */  ; # trailing"),
        Vec::<String>::new()
    );
    assert_eq!(strip_trailing_terminators("SELECT 1; -- done"), "SELECT 1");
}

#[test]
fn split_drops_interleaved_comment_only_segment_in_ddl_batch() {
    // dbt submits its swap DDL batch as one `UpdateDatabaseDdl` call, which Spanner rejects as a
    // whole if any statement is empty.
    let batch = "CREATE TABLE tmp (id INT64) PRIMARY KEY (id);\n\
                 DROP TABLE target;\n\
                 /* swap in the rebuilt table */;\n\
                 RENAME TABLE tmp TO target;\n\
                 # done";
    assert_eq!(
        split_statements(batch),
        vec![
            "CREATE TABLE tmp (id INT64) PRIMARY KEY (id)",
            "DROP TABLE target",
            "RENAME TABLE tmp TO target",
        ]
    );
}

#[test]
fn split_respects_raw_strings() {
    // A backslash escapes the quote in a raw literal too, so `r'C:\'` is unterminated. Reading
    // `\'` as a closing quote would turn the DELETE into a statement of its own.
    assert_eq!(
        split_statements(r"UPDATE t SET path = r'C:\'; DELETE FROM u WHERE stale"),
        vec![r"UPDATE t SET path = r'C:\'; DELETE FROM u WHERE stale"]
    );
    assert_eq!(
        split_statements(r"INSERT INTO t (b) VALUES (rb'\'); SELECT 1"),
        vec![r"INSERT INTO t (b) VALUES (rb'\'); SELECT 1"]
    );
    assert_eq!(
        split_statements(r#"SELECT BR"\"; SELECT R'\'"#),
        vec![r#"SELECT BR"\"; SELECT R'\'"#]
    );
    assert_eq!(
        split_statements(r"SELECT b'\';x'; SELECT 2"),
        vec![r"SELECT b'\';x'", "SELECT 2"]
    );
    // Closed literals separate normally.
    assert_eq!(
        split_statements(r"UPDATE t SET path = r'C:\\'; DELETE FROM u WHERE stale"),
        vec![r"UPDATE t SET path = r'C:\\'", "DELETE FROM u WHERE stale"]
    );
    assert_eq!(
        split_statements("SELECT r ';'; SELECT xr';'"),
        vec!["SELECT r ';'", "SELECT xr';'"]
    );
}

#[test]
fn split_respects_triple_quoted_strings() {
    assert_eq!(
        split_statements("INSERT INTO t (s) VALUES ('''don't; stop'''); DELETE FROM t"),
        vec![
            "INSERT INTO t (s) VALUES ('''don't; stop''')",
            "DELETE FROM t",
        ]
    );
    assert_eq!(
        split_statements(r#"SELECT """a;b"""; SELECT 2"#),
        vec![r#"SELECT """a;b""""#, "SELECT 2"]
    );
    // Fewer than three quotes do not close it.
    assert_eq!(
        split_statements("SELECT '''a''b'''; SELECT 2"),
        vec!["SELECT '''a''b'''", "SELECT 2"]
    );
    // In a raw triple-quoted literal, the backslash escapes the first closing quote too.
    assert_eq!(
        split_statements(r"SELECT r'''a\'''; SELECT 2"),
        vec![r"SELECT r'''a\'''; SELECT 2"]
    );
    assert_eq!(
        split_statements(r"SELECT r'''a\\'''; SELECT 2"),
        vec![r"SELECT r'''a\\'''", "SELECT 2"]
    );
    // An empty literal is not the start of a triple-quoted one.
    assert_eq!(
        split_statements(r#"SELECT ''; SELECT """#),
        vec!["SELECT ''", r#"SELECT """#]
    );
}

#[test]
fn split_respects_literals_and_comments() {
    assert_eq!(
        split_statements("INSERT INTO t (s) VALUES ('a;b'); DELETE FROM t WHERE true"),
        vec![
            "INSERT INTO t (s) VALUES ('a;b')",
            "DELETE FROM t WHERE true",
        ]
    );
    assert_eq!(
        split_statements(r"UPDATE t SET s = 'x\';y' WHERE id = 1"),
        vec![r"UPDATE t SET s = 'x\';y' WHERE id = 1"]
    );
    assert_eq!(
        split_statements("SELECT 1 -- a; b\n; SELECT 2"),
        vec!["SELECT 1 -- a; b", "SELECT 2"]
    );
}

#[test]
fn strips_trailing_query_terminators() {
    assert_eq!(strip_trailing_terminators("SELECT 1;"), "SELECT 1");
    assert_eq!(strip_trailing_terminators("SELECT 1;;;"), "SELECT 1");
    assert_eq!(strip_trailing_terminators("SELECT 1 ;  "), "SELECT 1");
    assert_eq!(strip_trailing_terminators("  SELECT 1  "), "SELECT 1");
    // From the conformance suite.
    assert_eq!(
        strip_trailing_terminators("SELECT current_date;;;"),
        "SELECT current_date"
    );
    assert_eq!(strip_trailing_terminators("SELECT 1"), "SELECT 1");
    assert_eq!(strip_trailing_terminators("SELECT ';'"), "SELECT ';'");
    assert_eq!(strip_trailing_terminators("SELECT ';';"), "SELECT ';'");
    // Multiple statements are left for Spanner to reject.
    assert_eq!(
        strip_trailing_terminators("SELECT 1; SELECT 2"),
        "SELECT 1; SELECT 2"
    );
}

#[test]
fn strip_trailing_terminators_handles_statementless_and_leading_semicolons() {
    assert_eq!(strip_trailing_terminators("; SELECT 1"), "SELECT 1");
    assert_eq!(strip_trailing_terminators(" ; SELECT 1 ; "), "SELECT 1");
    assert_eq!(strip_trailing_terminators(";;SELECT 1"), "SELECT 1");
    assert_eq!(
        strip_trailing_terminators("SELECT 1;\n-- tail\n;"),
        "SELECT 1"
    );
    assert_eq!(
        strip_trailing_terminators("SELECT 1 -- tail"),
        "SELECT 1 -- tail"
    );
    // Input without a statement is returned untouched for Spanner to reject.
    for sql in [
        "",
        ";",
        ";;",
        "  \n",
        "-- only a comment",
        "/* just a block */",
    ] {
        assert_eq!(strip_trailing_terminators(sql), sql, "for {sql:?}");
    }
}

#[test]
fn split_drops_only_comment_and_whitespace_segments() {
    // `trim` cannot remove the whitespace between two comments.
    assert_eq!(split_statements("/* a */ /* b */"), Vec::<String>::new());
    assert_eq!(split_statements("-- a\n /* b */ # c"), Vec::<String>::new());
    // Punctuation is kept for Spanner to reject.
    assert_eq!(split_statements("SELECT 1; ()"), vec!["SELECT 1", "()"]);
}

#[test]
fn split_statements_returns_verbatim_in_order_slices() {
    // `fuzz/fuzz_targets/keyword.rs` relies on statements being verbatim, in-order slices.
    for sql in [
        r"SELECT r'x\'; DELETE FROM t'",
        "SELECT '''a;b''' ; SELECT `c;d` ; -- t\n SELECT 2",
        "/* unterminated ; SELECT 1",
        "SELECT 'unterminated ; SELECT 1",
        "SELECT 1;;;SELECT 2",
        "SELECT '\u{2028}\u{0}é😀'; SELECT 2",
        "",
    ] {
        let mut cursor = 0;
        for statement in split_statements(sql) {
            assert_eq!(statement, statement.trim());
            assert!(!statement.is_empty());
            let found = sql[cursor..]
                .find(statement.as_str())
                .unwrap_or_else(|| panic!("{statement:?} is not an in-order slice of {sql:?}"));
            cursor += found + statement.len();
        }
    }
}

#[test]
fn extracts_named_parameters() {
    assert_eq!(
        named_parameters("SELECT @a, @b FROM t WHERE @a > 0"),
        vec!["a", "b"]
    );
    assert_eq!(named_parameters("SELECT 1"), Vec::<String>::new());
    assert_eq!(named_parameters("SELECT '@x', @y -- @z\n"), vec!["y"]);
    assert_eq!(named_parameters("SELECT @y /* @z */, @w"), vec!["y", "w"]);
    assert_eq!(named_parameters("SELECT `@col`, @p"), vec!["p"]);
    // Statement hints and system variables.
    assert_eq!(
        named_parameters("SELECT @{JOIN_METHOD=HASH_JOIN} * FROM t WHERE id = @id"),
        vec!["id"]
    );
    assert_eq!(named_parameters("SELECT @@rows"), Vec::<String>::new());
    assert_eq!(named_parameters("@b @a @a @b @c"), vec!["b", "a", "c"]);
}

#[test]
fn detects_positional_placeholders_outside_literals_and_comments() {
    for sql in [
        "SELECT * FROM t WHERE id = ?",
        "INSERT INTO t (a, b) VALUES (?,?)",
        "SELECT '?', ? -- ?",
        "UPDATE t SET a = @a WHERE b = ?;",
    ] {
        assert!(has_positional_placeholder(sql), "{sql}");
    }
    for sql in [
        "SELECT * FROM t WHERE id = @id",
        "SELECT '?', \"?\", r'?', b'?', '''?''', `?` FROM t",
        "SELECT 1 -- ?\n",
        "SELECT 1 # ?",
        "SELECT /* ? */ 1",
        "",
    ] {
        assert!(!has_positional_placeholder(sql), "{sql}");
    }
}

#[test]
fn named_parameters_skip_raw_and_triple_quoted_strings() {
    // `r'\'` is unterminated, so `@p` is inside the literal.
    assert_eq!(named_parameters(r"SELECT r'\', @p"), Vec::<String>::new());
    assert_eq!(
        named_parameters(r"SELECT rb'@x\', @p"),
        Vec::<String>::new()
    );
    assert_eq!(named_parameters(r"SELECT r'\\', @p"), vec!["p"]);
    assert_eq!(named_parameters(r"SELECT rb'@x\\', @p"), vec!["p"]);
    assert_eq!(named_parameters("SELECT '''@x''', @y"), vec!["y"]);
    assert_eq!(named_parameters(r#"SELECT """it's @x""", @y"#), vec!["y"]);
    assert_eq!(named_parameters("SELECT '', @z"), vec!["z"]);
    // `\'` is an escape here too, so the literal runs to the third quote.
    assert_eq!(named_parameters(r"SELECT xr'\' @a ', @b"), vec!["b"]);
}

#[test]
fn named_parameters_accept_every_spelling_spanner_does() {
    // The emulator treats each of these as a reference to `p`. Missing one makes valid SQL
    // unbindable, because `resolve_parameter_names` sees too few parameters.
    for sql in [
        "SELECT @p AS a",
        "SELECT @ p AS a",
        "SELECT @/* c */p AS a",
        "SELECT @`p` AS a",
        "SELECT @\n  p AS a",
    ] {
        assert_eq!(named_parameters(sql), vec!["p"], "for {sql:?}");
    }
    assert_eq!(named_parameters("SELECT @`a b` AS x"), vec!["a b"]);
    // Escaped, empty and unterminated quoted names are skipped.
    assert_eq!(named_parameters(r"SELECT @`a\`b`"), Vec::<String>::new());
    assert_eq!(named_parameters("SELECT @``"), Vec::<String>::new());
    assert_eq!(named_parameters("SELECT @`"), Vec::<String>::new());
    assert_eq!(named_parameters("SELECT @`θ"), Vec::<String>::new());
    assert_eq!(
        named_parameters("SELECT @`unterminated"),
        Vec::<String>::new()
    );
    // A hint is still not a parameter with a separator.
    assert_eq!(
        named_parameters("@ {JOIN_METHOD=HASH_JOIN} SELECT * FROM t WHERE id = @id"),
        vec!["id"]
    );
}

#[test]
fn named_parameters_dedupe_case_insensitively() {
    // Spanner rejects two spellings of one name as "Duplicate parameter name p". The first
    // spelling wins, matching what the query planner reports.
    assert_eq!(
        named_parameters("UPDATE t SET s = @v WHERE id = @V"),
        vec!["v"]
    );
    assert_eq!(named_parameters("SELECT @Name, @name, @NAME"), vec!["Name"]);
    assert_eq!(
        named_parameters("SELECT @b, @a, @B, @c, @A"),
        vec!["b", "a", "c"]
    );
}

#[test]
fn named_parameters_dedupe_scales_to_large_statements() {
    // Deduplication must not be quadratic in the number of parameters.
    let sql = format!(
        "SELECT {}",
        (0..20_000)
            .map(|i| format!("@p{i}"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let params = named_parameters(&sql);
    assert_eq!(params.len(), 20_000);
    assert_eq!(params[0], "p0");
    assert_eq!(params[19_999], "p19999");
}

#[test]
fn qualifies_table_names() {
    assert_eq!(qualified_table(None, "Users").unwrap(), "`Users`");
    assert_eq!(qualified_table(Some(""), "Users").unwrap(), "`Users`");
    assert_eq!(
        qualified_table(Some("app"), "Users").unwrap(),
        "`app`.`Users`"
    );
    // A dot cannot add a qualification level.
    assert_eq!(qualified_table(Some("a.b"), "t").unwrap(), "`a.b`.`t`");
    for (schema, table) in [
        (None, "a`b"),
        (Some("s`x"), "t"),
        (Some("s"), r"t\y"),
        (None, ""),
        (Some("s"), ""),
    ] {
        assert_eq!(
            qualified_table(schema, table).unwrap_err().status,
            Status::InvalidArguments,
            "should be rejected: {schema:?}.{table:?}"
        );
    }
}

#[test]
fn quote_ident_accepts_every_name_spanner_can_represent() {
    // The emulator accepts all of these as column names.
    assert_eq!(quote_ident("plain").unwrap(), "`plain`");
    assert_eq!(quote_ident("create").unwrap(), "`create`");
    assert_eq!(quote_ident("spaced name").unwrap(), "`spaced name`");
    assert_eq!(quote_ident("é").unwrap(), "`é`");
    assert_eq!(quote_ident("a.b").unwrap(), "`a.b`");
}

#[test]
fn quote_ident_rejects_names_backticks_cannot_contain() {
    // As an ingest column name, this could inject an INTERLEAVE clause with cascading delete.
    let payload = "k` INT64, Id INT64 NOT NULL) PRIMARY KEY (Id), \
                   INTERLEAVE IN PARENT parent ON DELETE CASCADE -- ";
    for ident in [
        payload, "a`b", r"a\b", r"a\`b", "a\nb", "a\rb", "a\tb", "a\0b", "",
    ] {
        let err = quote_ident(ident).unwrap_err();
        assert_eq!(
            err.status,
            Status::InvalidArguments,
            "should be rejected: {ident:?}"
        );
    }
    // An accepted name has no backtick or backslash inside its delimiters.
    for ident in ["plain", "spaced name", "é", "a.b", "créate", "x-y", "1"] {
        let quoted = quote_ident(ident).unwrap();
        assert!(quoted.starts_with('`') && quoted.ends_with('`'));
        assert!(
            !quoted[1..quoted.len() - 1].contains(['`', '\\']),
            "{quoted:?} is not one opaque token"
        );
    }
}
