/// Split a `;`-separated SQL batch into statements, honoring quotes and comments.
pub fn split_statements(sql: &str) -> Vec<String> {
    crate::sql::split_statements(sql)
        .into_iter()
        .map(str::to_owned)
        .collect()
}
/// Whether the SQL begins with a DDL statement.
pub fn is_ddl(sql: &str) -> bool {
    crate::sql::is_ddl(sql)
}
/// Whether the SQL contains a top-level `THEN RETURN` clause.
pub fn is_dml_returning(sql: &str) -> bool {
    crate::sql::is_dml_returning(sql)
}
/// Parse a Spanner `DATE` string into Arrow `Date32` days.
pub fn parse_date_days(s: &str) -> Option<i32> {
    crate::conversion::parse_date_days(s)
}
/// Parse a Spanner `TIMESTAMP` string into epoch nanoseconds.
pub fn parse_timestamp_nanos(s: &str) -> Option<i64> {
    crate::conversion::parse_timestamp_nanos(s)
}
/// Parse a Spanner `NUMERIC` string into an unscaled `i128` (scale 9).
pub fn parse_numeric_i128(s: &str) -> Option<i128> {
    crate::conversion::parse_numeric_i128(s)
}
/// Match an ADBC `LIKE` pattern against a value.
pub fn like_match(pattern: &str, value: &str) -> bool {
    crate::metadata::like_match(pattern, value)
}
/// The first SQL keyword, uppercased, after whitespace, comments and `@{...}` statement hints.
pub fn first_keyword(sql: &str) -> Option<String> {
    crate::sql::first_keyword(sql).map(str::to_ascii_uppercase)
}
/// Whether the SQL begins with a DML statement (`INSERT`/`UPDATE`/`DELETE`).
pub fn is_dml(sql: &str) -> bool {
    crate::sql::is_dml(sql)
}
/// Strip trailing top-level `;` terminators from a single-statement query.
pub fn strip_trailing_terminators(sql: &str) -> String {
    crate::sql::strip_trailing_terminators(sql).to_owned()
}
/// The distinct `@name` bind parameters of a query, in first-appearance order.
pub fn named_parameters(sql: &str) -> Vec<String> {
    crate::sql::named_parameters(sql)
}
/// Backtick-quote a Spanner identifier, or reject a name backticks cannot safely contain.
pub fn quote_ident(ident: &str) -> Option<String> {
    crate::sql::quote_ident(ident).ok()
}
/// Pair the parameters of `sql` with the columns of a batch named `column_names` (typed
/// `Int64`; pairing ignores types). Returns `None` after asserting that a rejection is
/// `InvalidArguments`.
pub fn resolve_parameter_names(
    sql: &str,
    column_names: &[String],
    bind_by_name: bool,
) -> Option<Vec<String>> {
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    let fields: Vec<Field> = column_names
        .iter()
        .map(|name| Field::new(name, DataType::Int64, true))
        .collect();
    let batch = RecordBatch::new_empty(std::sync::Arc::new(Schema::new(fields)));
    match crate::bind::resolve_parameter_names(sql, &batch, bind_by_name) {
        Ok(names) => Some(names),
        Err(e) => {
            assert_eq!(
                e.status,
                adbc_core::error::Status::InvalidArguments,
                "resolve_parameter_names must reject with InvalidArguments: {e:?}"
            );
            None
        }
    }
}
/// Decode an opaque partition descriptor, returning whether it decoded.
///
/// A rejected descriptor must fail cleanly with `InvalidArguments`. An accepted one must
/// reach a byte-stable fixed point under the driver's encoder. The check starts from the
/// encoder's own output because serde does not preserve every input form: an integer literal
/// too large for `i64`/`u64` parses to an imprecise `f64`. One normalizing pass suffices.
pub fn decode_partition(descriptor: &[u8]) -> bool {
    match crate::connection::decode_partition(descriptor) {
        Ok(partition) => {
            use crate::connection::{decode_partition, encode_partition};
            let first = encode_partition(&partition).expect("a decoded partition re-encodes");
            let normalized =
                decode_partition(&first).expect("a re-encoded partition descriptor decodes");
            let bytes = encode_partition(&normalized).expect("a decoded partition re-encodes");
            let again =
                decode_partition(&bytes).expect("a re-encoded partition descriptor decodes");
            assert_eq!(
                bytes,
                encode_partition(&again).expect("a decoded partition re-encodes"),
                "enveloped partition descriptor round-trip changed the bytes"
            );
            true
        }
        Err(e) => {
            assert_eq!(
                e.status,
                adbc_core::error::Status::InvalidArguments,
                "decode_partition must reject with InvalidArguments: {e:?}"
            );
            assert!(
                e.message.contains("invalid partition descriptor")
                    || e.message.contains("not supported by this driver"),
                "unexpected rejection message: {}",
                e.message
            );
            false
        }
    }
}
/// Normalize an emulator endpoint by adding an `http://` scheme when absent.
pub fn ensure_scheme(host: &str) -> String {
    crate::driver::ensure_scheme(host)
}

/// Parse a `spanner.read.staleness` value into client `TimestampBound`s.
pub fn parse_read_staleness(value: &str) {
    use adbc_core::options::OptionValue;
    let mut staleness = crate::staleness::ReadStaleness::default();
    if staleness
        .set_staleness(OptionValue::String(value.to_string()))
        .is_ok()
    {
        assert_eq!(
            staleness.timestamp_bound().is_some(),
            staleness.multi_use_timestamp_bound().is_some()
        );
    }
}

/// Parse a duration in the grammar shared by `spanner.read.staleness` and
/// `spanner.commit.max_delay`.
pub fn parse_duration(value: &str) {
    let _ = crate::staleness::parse_duration(value, crate::OPTION_READ_STALENESS);
}

/// Parse a `spanner.directed_read` value.
pub fn parse_directed_read(value: &str) {
    let _ = crate::directed_read::parse(value);
}

/// An arbitrary [`OptionValue`](adbc_core::options::OptionValue).
#[derive(arbitrary::Arbitrary, Debug)]
pub enum OptValue {
    Str(String),
    Int(i64),
    Double(f64),
    Bytes(Vec<u8>),
}

/// A new database from a driver built once per process, so that runtime construction does
/// not dominate fuzzing throughput.
fn new_database() -> Option<crate::SpannerDatabase> {
    use adbc_core::Driver;
    use std::sync::{Mutex, OnceLock};

    static DRIVER: OnceLock<Mutex<crate::SpannerDriver>> = OnceLock::new();
    let driver = DRIVER.get_or_init(|| {
        Mutex::new(crate::SpannerDriver::try_new().expect("driver construction is infallible"))
    });
    driver.lock().unwrap().new_database().ok()
}

/// Set and read back arbitrary database options, as a driver manager forwarding untrusted
/// strings would. Stops before `connect()`, so there is no network I/O.
pub fn exercise_database_options(ops: Vec<(String, OptValue)>) {
    use adbc_core::Optionable;
    use adbc_core::options::{OptionDatabase, OptionValue};

    let Some(mut database) = new_database() else {
        return;
    };
    for (key, value) in ops {
        let value = match value {
            OptValue::Str(s) => OptionValue::String(s),
            OptValue::Int(i) => OptionValue::Int(i),
            OptValue::Double(d) => OptionValue::Double(d),
            OptValue::Bytes(b) => OptionValue::Bytes(b),
        };
        let _ = database.set_option(OptionDatabase::Other(key.clone()), value);
        let _ = database.get_option_string(OptionDatabase::Other(key));
    }
}

/// Set an arbitrary `uri` option, which [`exercise_database_options`] cannot reach because it
/// only sets `Other(key)`. No network I/O.
pub fn expand_connection_uri(uri: &str) {
    use adbc_core::Optionable;
    use adbc_core::options::{OptionDatabase, OptionValue};

    let Some(mut database) = new_database() else {
        return;
    };
    let _ = database.set_option(OptionDatabase::Uri, OptionValue::String(uri.to_string()));
    let _ = database.get_option_string(OptionDatabase::Uri);
}

#[cfg(test)]
mod tests;
