//! GoogleSQL lexing, statement classification/splitting, identifier quoting and parameter names.
//!
//! All SQL inspection shares one lexer so quoted literals and comments have consistent boundaries.
//! Classification routes DDL to the admin API and DML to read/write transactions; splitting lets
//! each API accept a batch. DDL batches are not transactional.

use crate::error::invalid_argument;
use adbc_core::error::Result;

/// Leading keywords that identify a Spanner DDL statement.
const DDL_KEYWORDS: &[&str] = &[
    "CREATE", "DROP", "ALTER", "RENAME", "GRANT", "REVOKE", "ANALYZE",
];

/// Leading keywords that identify a Spanner DML statement.
const DML_KEYWORDS: &[&str] = &["INSERT", "UPDATE", "DELETE"];

/// Whether the first nonempty statement is DDL, ignoring comments and statement hints.
pub(crate) fn is_ddl(sql: &str) -> bool {
    first_keyword(sql).is_some_and(|kw| DDL_KEYWORDS.iter().any(|k| kw.eq_ignore_ascii_case(k)))
}

/// Whether the first nonempty statement is DML (`INSERT`/`UPDATE`/`DELETE`).
pub(crate) fn is_dml(sql: &str) -> bool {
    first_keyword(sql).is_some_and(|kw| DML_KEYWORDS.iter().any(|k| kw.eq_ignore_ascii_case(k)))
}

/// Whether `sql` contains a top-level `THEN RETURN` clause, making it DML that returns rows.
///
/// `RETURN` is not reserved, so in `CASE WHEN c THEN return END` it may be a column; `THEN` only
/// counts outside `CASE ... END`. An unbalanced `END` saturates the depth at zero.
pub(crate) fn is_dml_returning(sql: &str) -> bool {
    let mut previous_was_then = false;
    let mut case_depth = 0usize;
    for lexeme in lex(sql) {
        match lexeme {
            Lexeme::Word(word) => {
                if previous_was_then && case_depth == 0 && word.eq_ignore_ascii_case("RETURN") {
                    return true;
                }
                previous_was_then = word.eq_ignore_ascii_case("THEN");
                if word.eq_ignore_ascii_case("CASE") {
                    case_depth += 1;
                } else if word.eq_ignore_ascii_case("END") {
                    case_depth = case_depth.saturating_sub(1);
                }
            }
            // A literal between the keywords breaks the clause; comments and punctuation don't.
            Lexeme::Quoted(_) => previous_was_then = false,
            Lexeme::Comment(_) | Lexeme::Other(_) => {}
        }
    }
    false
}

/// A GoogleSQL token. Tokens partition the input with no gaps or overlaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lexeme<'a> {
    /// A run of `[A-Za-z0-9_]`: a keyword, identifier or number.
    Word(&'a str),
    /// A literal or quoted identifier, delimiters included.
    Quoted(&'a str),
    /// A line comment up to its line break, or a block comment. Unterminated comments run to end
    /// of input.
    Comment(&'a str),
    /// Any other single character.
    Other(char),
}

/// Tokenize `sql`. A literal prefix (`r`, `b`, `rb`, `br`) lexes as a separate [`Lexeme::Word`]
/// because it does not change where the literal ends.
fn lex(sql: &str) -> Lexer<'_> {
    Lexer {
        input: sql,
        chars: sql.chars().peekable(),
        pos: 0,
    }
}

struct Lexer<'a> {
    input: &'a str,
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    /// Byte offset of the next character in `input`.
    pos: usize,
}

impl Lexer<'_> {
    fn bump(&mut self) -> Option<char> {
        let c = self.chars.next()?;
        self.pos += c.len_utf8();
        Some(c)
    }

    fn bump_if(&mut self, c: char) -> bool {
        let matched = self.chars.peek() == Some(&c);
        if matched {
            self.bump();
        }
        matched
    }

    fn bump_while(&mut self, accept: fn(char) -> bool) {
        while self.chars.peek().is_some_and(|&c| accept(c)) {
            self.bump();
        }
    }

    /// Raw literals still escape quotes with backslashes, so `r'x\'; DROP TABLE t'` is one string.
    /// Scanning past invalid newlines can hide a statement boundary but cannot introduce one.
    fn consume_quoted(&mut self, quote: char) {
        // A second quote closes an empty literal unless a third opens a triple-quoted one.
        let triple = quote != '`' && self.bump_if(quote);
        if triple && !self.bump_if(quote) {
            return;
        }
        let mut closing_run = 0;
        while let Some(ch) = self.bump() {
            if ch == '\\' {
                self.bump();
                closing_run = 0;
            } else if ch != quote {
                closing_run = 0;
            } else {
                closing_run += 1;
                if !triple || closing_run == 3 {
                    return;
                }
            }
        }
    }
}

fn is_ident_char(c: char) -> bool {
    c == '_' || c.is_ascii_alphanumeric()
}

/// GoogleSQL ends a line comment at `\r` as well as `\n`.
fn is_not_line_break(c: char) -> bool {
    c != '\n' && c != '\r'
}

impl<'a> Iterator for Lexer<'a> {
    type Item = Lexeme<'a>;

    fn next(&mut self) -> Option<Lexeme<'a>> {
        let start = self.pos;
        let c = self.bump()?;
        let lexeme = match c {
            '\'' | '"' | '`' => {
                self.consume_quoted(c);
                Lexeme::Quoted(&self.input[start..self.pos])
            }
            '#' => {
                self.bump_while(is_not_line_break);
                Lexeme::Comment(&self.input[start..self.pos])
            }
            '-' if self.bump_if('-') => {
                self.bump_while(is_not_line_break);
                Lexeme::Comment(&self.input[start..self.pos])
            }
            '/' if self.bump_if('*') => {
                let mut prev = '\0';
                while let Some(ch) = self.bump() {
                    if prev == '*' && ch == '/' {
                        break;
                    }
                    prev = ch;
                }
                Lexeme::Comment(&self.input[start..self.pos])
            }
            _ if is_ident_char(c) => {
                self.bump_while(is_ident_char);
                Lexeme::Word(&self.input[start..self.pos])
            }
            _ => Lexeme::Other(c),
        };
        Some(lexeme)
    }
}

/// Lexemes other than comments and whitespace, with one lexeme of lookahead.
///
/// GoogleSQL lexes `@`, `{` and a parameter name as separate tokens, so Spanner accepts
/// `@ {HINT=1} UPDATE ...` and `@/*c*/p`.
fn significant_lexemes(sql: &str) -> std::iter::Peekable<impl Iterator<Item = Lexeme<'_>>> {
    lex(sql)
        .filter(|lexeme| {
            !matches!(lexeme, Lexeme::Comment(_))
                && !matches!(lexeme, Lexeme::Other(c) if c.is_whitespace())
        })
        .peekable()
}

/// Borrow trimmed statements separated by `;` outside literals, identifiers and comments.
/// Comment-only segments are omitted so they do not count toward single-statement limits.
pub(crate) fn split_statements(sql: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut lexer = lex(sql);
    let mut start = 0;
    let mut has_statement = false;
    while let Some(lexeme) = lexer.next() {
        match lexeme {
            Lexeme::Other(';') => {
                if has_statement {
                    statements.push(sql[start..lexer.pos - 1].trim());
                }
                start = lexer.pos;
                has_statement = false;
            }
            Lexeme::Comment(_) => {}
            Lexeme::Other(c) if c.is_whitespace() => {}
            _ => has_statement = true,
        }
    }
    if has_statement {
        statements.push(sql[start..].trim());
    }
    statements
}

/// Spanner's query API rejects trailing `;`, so return the single statement without terminators.
/// Leave empty input and multiple statements unchanged for Spanner to reject.
pub(crate) fn strip_trailing_terminators(sql: &str) -> &str {
    match split_statements(sql).as_slice() {
        [statement] => statement,
        _ => sql,
    }
}

/// The first keyword, skipping empty statements, comments and `@{...}` hints. A quoted `}` does
/// not close a hint, and an unterminated hint consumes the rest. Leading punctuation or quoted
/// text yields `None`. Returns the word's leading ASCII letters in their original case, so
/// `CREATE2` routes as `CREATE`.
pub(crate) fn first_keyword(sql: &str) -> Option<&str> {
    let mut lexemes = significant_lexemes(sql);
    while let Some(lexeme) = lexemes.next() {
        match lexeme {
            Lexeme::Comment(_) | Lexeme::Other(';') => {}
            // A statement hint. `significant_lexemes` dropped anything between `@` and `{`.
            Lexeme::Other('@') if lexemes.peek() == Some(&Lexeme::Other('{')) => {
                for hint_lexeme in lexemes.by_ref() {
                    if hint_lexeme == Lexeme::Other('}') {
                        break;
                    }
                }
            }
            Lexeme::Word(word) => {
                let end = word
                    .find(|c: char| !c.is_ascii_alphabetic())
                    .unwrap_or(word.len());
                return (end > 0).then(|| &word[..end]);
            }
            Lexeme::Quoted(_) | Lexeme::Other(_) => return None,
        }
    }
    None
}

/// Distinct `@name` parameters in order of first appearance, which binding and
/// `get_parameter_schema` follow.
///
/// Literals, comments, hints (`@{...}`) and system variables (`@@var`) are skipped. Spanner allows
/// whitespace or a comment after `@`, and backtick-quoted names: `@ p`, `@/*c*/p` and `` @`p` ``
/// all name `p`. A quoted name containing a backslash escape is skipped rather than guessed at.
///
/// Spanner matches names case-insensitively and rejects two spellings of one name as "Duplicate
/// parameter name", so names are deduplicated case-insensitively, keeping the first spelling.
pub(crate) fn named_parameters(sql: &str) -> Vec<String> {
    let mut params: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut lexemes = significant_lexemes(sql);
    while let Some(lexeme) = lexemes.next() {
        if lexeme != Lexeme::Other('@') {
            continue;
        }
        // `.copied()` releases the peek borrow so the matched arm can advance `lexemes`.
        let name = match lexemes.peek().copied() {
            // `@@var` or `@{...}`: skip the second marker. A hint body is scanned normally.
            Some(Lexeme::Other('@')) | Some(Lexeme::Other('{')) => {
                lexemes.next();
                continue;
            }
            // A word may start with a digit, but `@1` is not a parameter.
            Some(Lexeme::Word(name))
                if name.starts_with(|ch: char| ch == '_' || ch.is_ascii_alphabetic()) =>
            {
                lexemes.next();
                name
            }
            // An unterminated quoted name lacks a closing backtick and may end mid-character, so
            // use `strip_suffix` instead of slicing.
            Some(Lexeme::Quoted(quoted)) => {
                lexemes.next();
                let Some(inner) = quoted
                    .strip_prefix('`')
                    .and_then(|rest| rest.strip_suffix('`'))
                else {
                    continue;
                };
                if inner.is_empty() || inner.contains('\\') {
                    continue;
                }
                inner
            }
            _ => continue,
        };
        if seen.insert(name.to_ascii_lowercase()) {
            params.push(name.to_string());
        }
    }
    params
}

/// Whether `sql` has a `?` outside literals, quoted identifiers and comments: a positional
/// placeholder from another SQL dialect, which GoogleSQL doesn't accept.
pub(crate) fn has_positional_placeholder(sql: &str) -> bool {
    lex(sql).any(|lexeme| lexeme == Lexeme::Other('?'))
}

/// Backtick-quote a Spanner identifier, rejecting empty names and unsafe characters.
/// DDL treats backslashes literally while queries interpret them as escapes, so escaping cannot
/// safely represent backticks or backslashes on both paths.
pub(crate) fn quote_ident(ident: &str) -> Result<String> {
    if ident.is_empty() {
        return Err(invalid_argument(
            "invalid SQL identifier: the name is empty, which Spanner rejects as an invalid empty \
             identifier",
        ));
    }
    if let Some(bad) = ident
        .chars()
        .find(|&c| c == '`' || c == '\\' || c.is_control())
    {
        return Err(invalid_argument(format!(
            "invalid SQL identifier {ident:?}: a name cannot contain {bad:?}, because Spanner's \
             DDL parser does not honour backslash escapes inside backticks and would read the \
             remainder of the name as SQL"
        )));
    }
    Ok(format!("`{ident}`"))
}

/// Quote a table name, qualified by `db_schema` unless it is empty (Spanner's default schema).
/// Named schemas are single-level, so `a.b` quotes to one identifier, `` `a.b` ``.
pub(crate) fn qualified_table(db_schema: Option<&str>, table_name: &str) -> Result<String> {
    match db_schema.filter(|s| !s.is_empty()) {
        Some(schema) => Ok(format!(
            "{}.{}",
            quote_ident(schema)?,
            quote_ident(table_name)?
        )),
        None => quote_ident(table_name),
    }
}

#[cfg(test)]
mod tests;
