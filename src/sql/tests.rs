use super::*;
use adbc_core::error::Status;

#[test]
fn backslash_binds_the_next_char_in_every_literal_including_raw() {
    // The emulator parses each of these as a single SELECT of one string:
    //     SELECT r'x\'; SELECT 2'          ->  x\'; SELECT 2
    //     SELECT r'''a\'''; SELECT 2'''    ->  a\'''; SELECT 2
    // Reading `\'` as a closing quote would turn the DELETE into a statement of its own.
    for prefix in ["r", "R", "rb", "RB", "br", "Br", "b", "B", ""] {
        for sql in [
            format!(r"SELECT {prefix}'x\'; DELETE FROM t'"),
            format!(r#"SELECT {prefix}"x\"; DELETE FROM t""#),
            format!(r"SELECT {prefix}'''a\'''; DELETE FROM t'''"),
        ] {
            assert_eq!(split_statements(&sql), vec![sql.clone()]);
        }
        // An even number of backslashes closes the literal.
        assert_eq!(
            split_statements(&format!(r"SELECT {prefix}'a\\'; DELETE FROM t")),
            vec![
                format!(r"SELECT {prefix}'a\\'"),
                "DELETE FROM t".to_string()
            ]
        );
    }
    // A prefix is a separate word, so neither of these is a raw literal with an escape.
    assert_eq!(
        split_statements("SELECT r ';'; SELECT xr';'"),
        vec!["SELECT r ';'", "SELECT xr';'"]
    );
}

#[test]
fn classifies_ddl_and_dml_by_first_keyword() {
    for (sql, ddl, dml) in [
        ("CREATE TABLE t (id INT64) PRIMARY KEY (id)", true, false),
        ("  drop table t", true, false),
        ("ALTER TABLE t ADD COLUMN c STRING(MAX)", true, false),
        ("RENAME TABLE a TO b", true, false),
        ("GRANT SELECT ON TABLE t TO ROLE r", true, false),
        ("ANALYZE", true, false),
        ("-- a comment\nCREATE INDEX idx ON t(c)", true, false),
        (
            "; /* empty */ ; create table t (id int64) primary key (id)",
            true,
            false,
        ),
        ("INSERT INTO t (id) VALUES (1)", false, true),
        ("  update t SET c = 1 WHERE id = 1", false, true),
        ("/* c */ delete FROM t WHERE true", false, true),
        ("; -- empty\n; UPDATE t SET c = 1 WHERE true", false, true),
        ("SELECT 1", false, false),
        ("WITH x AS (SELECT 1) SELECT * FROM x", false, false),
        ("(SELECT 1)", false, false),
        ("GRAPH g MATCH (n) RETURN n.id", false, false),
        ("", false, false),
    ] {
        assert_eq!((is_ddl(sql), is_dml(sql)), (ddl, dml), "{sql}");
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
fn splits_statements_dropping_empty_ones() {
    // dbt submits its swap DDL batch as one `UpdateDatabaseDdl` call, which Spanner rejects as a
    // whole if any statement is empty, so comment-only segments go.
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
    // Comments attached to a statement stay with it.
    assert_eq!(
        split_statements("SELECT 1;\n  \n-- a\nSELECT 2 -- done\n; # b\nSELECT 3 /* c */"),
        vec![
            "SELECT 1",
            "-- a\nSELECT 2 -- done",
            "# b\nSELECT 3 /* c */"
        ]
    );
    // `trim` cannot remove the whitespace between two comments.
    for sql in ["   ;  ; ", "/* a */ /* b */", "-- a\n /* b */ ; # c"] {
        assert!(split_statements(sql).is_empty(), "{sql:?}");
    }
    // Punctuation is kept for Spanner to reject.
    assert_eq!(split_statements("SELECT 1; ()"), vec!["SELECT 1", "()"]);
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
        split_statements("SELECT `a;b`, 1 -- a; b\n; SELECT 2 /* ; */"),
        vec!["SELECT `a;b`, 1 -- a; b", "SELECT 2 /* ; */"]
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
    // An empty literal is not the start of a triple-quoted one.
    assert_eq!(
        split_statements(r#"SELECT ''; SELECT """#),
        vec!["SELECT ''", r#"SELECT """#]
    );
}

#[test]
fn line_comments_end_at_a_carriage_return() {
    // The emulator returns two columns for `SELECT 1 -- c\r, 2`, so a bare `\r` ends the comment.
    for comment in ["--", "#"] {
        assert_eq!(
            split_statements(&format!("SELECT 1 {comment} c\r; SELECT 2")),
            vec![format!("SELECT 1 {comment} c"), "SELECT 2".to_string()]
        );
        let update =
            format!("{comment} c\rUPDATE t SET a = @a WHERE true {comment} c\rTHEN RETURN a");
        assert_eq!(first_keyword(&update), Some("UPDATE"));
        assert!(is_dml_returning(&update));
        assert_eq!(named_parameters(&update), vec!["a"]);
    }
}

#[test]
fn strips_terminators_from_a_single_statement() {
    // Spanner's query API rejects a trailing `;`; the conformance suite sends
    // `SELECT current_date;;;`.
    for (sql, stripped) in [
        ("SELECT current_date;;;", "SELECT current_date"),
        ("  SELECT 1 ;  ", "SELECT 1"),
        (";;SELECT 1", "SELECT 1"),
        ("SELECT 1;\n-- tail\n;", "SELECT 1"),
        ("SELECT 1 -- tail", "SELECT 1 -- tail"),
        ("SELECT ';';", "SELECT ';'"),
        // Anything but exactly one statement is left for Spanner to reject.
        ("SELECT 1; SELECT 2", "SELECT 1; SELECT 2"),
        (";;", ";;"),
        ("  \n", "  \n"),
        ("-- only a comment", "-- only a comment"),
    ] {
        assert_eq!(strip_trailing_terminators(sql), stripped, "{sql:?}");
    }
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
                .find(statement)
                .unwrap_or_else(|| panic!("{statement:?} is not an in-order slice of {sql:?}"));
            cursor += found + statement.len();
        }
    }
}

#[test]
fn extracts_named_parameters() {
    for (sql, params) in [
        ("SELECT @a, @b FROM t WHERE @a > 0", &["a", "b"][..]),
        ("@b @a @a @b @c", &["b", "a", "c"]),
        ("SELECT 1", &[]),
        // Literals, quoted identifiers and comments.
        ("SELECT '@x', `@col`, @y -- @z\n", &["y"]),
        ("SELECT @y /* @z */, @w # @v", &["y", "w"]),
        ("SELECT '''@x''', \"\"\"it's @x\"\"\", '', @y", &["y"]),
        // `\'` doesn't end a raw literal, so `@p` is inside it; `\\'` does.
        (r"SELECT r'\', @p", &[]),
        (r"SELECT rb'@x\\', @p", &["p"]),
        (r"SELECT xr'\' @a ', @b", &["b"]),
        // Statement and table hints, and system variables.
        (
            "@{JOIN_METHOD=HASH_JOIN} SELECT * FROM t@{FORCE_INDEX=i} WHERE id = @id",
            &["id"],
        ),
        ("@ {JOIN_METHOD=HASH_JOIN} SELECT @id", &["id"]),
        ("SELECT @@rows", &[]),
    ] {
        assert_eq!(named_parameters(sql), params, "{sql:?}");
    }
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
    // Escaped, empty and unterminated quoted names are skipped, as are names starting with a
    // digit.
    for sql in [
        r"SELECT @`a\`b`",
        "SELECT @``",
        "SELECT @`",
        "SELECT @`θ",
        "SELECT @1",
    ] {
        assert_eq!(named_parameters(sql), Vec::<String>::new(), "for {sql:?}");
    }
}

#[test]
fn named_parameters_dedupe_case_insensitively() {
    // Spanner rejects two spellings of one name as "Duplicate parameter name p". The first
    // spelling wins, matching what the query planner reports.
    assert_eq!(
        named_parameters("UPDATE t SET s = @v WHERE id = @V"),
        vec!["v"]
    );
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
fn quote_ident_accepts_only_names_backticks_can_hold() {
    // The emulator accepts all of these as column names.
    for ident in ["plain", "create", "spaced name", "é", "a.b", "x-y", "1"] {
        assert_eq!(quote_ident(ident).unwrap(), format!("`{ident}`"));
    }
    // As an ingest column name, this could inject an INTERLEAVE clause with cascading delete.
    let payload = "k` INT64, Id INT64 NOT NULL) PRIMARY KEY (Id), \
                   INTERLEAVE IN PARENT parent ON DELETE CASCADE -- ";
    for ident in [
        payload, "a`b", r"a\b", r"a\`b", "a\nb", "a\rb", "a\tb", "a\0b", "",
    ] {
        assert_eq!(
            quote_ident(ident).unwrap_err().status,
            Status::InvalidArguments,
            "should be rejected: {ident:?}"
        );
    }
}
