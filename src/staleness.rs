//! The [`OPTION_READ_STALENESS`](crate::OPTION_READ_STALENESS) read bound for read-only queries,
//! mapped to the client's [`TimestampBound`]. Unset means a strong read.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use chrono::{DateTime, Utc};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::transaction::{
    MultiUseReadOnlyTransaction, SingleUseReadOnlyTransaction, TimestampBound,
};

use crate::error::{from_spanner, invalid_argument};
use crate::options::RawParsed;

/// Build a single-use read-only transaction; `None` means a strong read.
pub(crate) fn single_use(
    client: &DatabaseClient,
    bound: Option<TimestampBound>,
) -> SingleUseReadOnlyTransaction {
    let builder = client.single_use();
    match bound {
        Some(b) => builder.set_timestamp_bound(b).build(),
        None => builder.build(),
    }
}

/// Build a multi-use read-only transaction with a bound from
/// [`ReadStaleness::multi_use_timestamp_bound`]; `None` means a strong read. No RPC is issued
/// until the first query, which begins the transaction inline.
pub(crate) async fn multi_use(
    client: &DatabaseClient,
    bound: Option<TimestampBound>,
) -> Result<MultiUseReadOnlyTransaction> {
    let builder = client.read_only_transaction();
    match bound {
        Some(b) => builder.set_timestamp_bound(b),
        None => builder,
    }
    .build()
    .await
    .map_err(from_spanner)
}

/// A parsed read bound. Unlike [`TimestampBound`], it can be inspected for pinning and tests.
#[derive(Debug, Clone, PartialEq)]
enum ReadBound {
    /// `exact:<duration>`
    ExactStaleness(Duration),
    /// `max:<duration>`
    MaxStaleness(Duration),
    /// `read:<rfc3339>` or a bare timestamp
    ReadTimestamp(DateTime<Utc>),
    /// `min:<rfc3339>`
    MinReadTimestamp(DateTime<Utc>),
}

impl ReadBound {
    /// The bound to use for a multi-use read-only transaction.
    ///
    /// `BeginTransaction` rejects the bounded-staleness kinds, so they are pinned to the most stale
    /// timestamp they allow, which satisfies the original bound.
    fn pinned_for_multi_use(&self) -> ReadBound {
        match self {
            ReadBound::MaxStaleness(d) => ReadBound::ExactStaleness(*d),
            ReadBound::MinReadTimestamp(t) => ReadBound::ReadTimestamp(*t),
            other => other.clone(),
        }
    }

    fn to_timestamp_bound(&self) -> Result<TimestampBound> {
        match self {
            ReadBound::ExactStaleness(d) => TimestampBound::try_exact_staleness(*d)
                .map_err(|e| invalid_argument(format!("read staleness out of range: {e}"))),
            ReadBound::MaxStaleness(d) => TimestampBound::try_max_staleness(*d)
                .map_err(|e| invalid_argument(format!("read staleness out of range: {e}"))),
            ReadBound::ReadTimestamp(t) => TimestampBound::try_read_timestamp(to_system_time(*t))
                .map_err(|e| invalid_argument(format!("read timestamp out of range: {e}"))),
            ReadBound::MinReadTimestamp(t) => {
                TimestampBound::try_min_read_timestamp(to_system_time(*t))
                    .map_err(|e| invalid_argument(format!("read timestamp out of range: {e}")))
            }
        }
    }
}

/// The `spanner.read.staleness` setting of a connection or statement.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadStaleness {
    bound: RawParsed<ReadBound>,
}

impl ReadStaleness {
    pub(crate) fn set_staleness(&mut self, value: OptionValue) -> Result<()> {
        self.bound
            .set(value, crate::OPTION_READ_STALENESS, parse_read_bound)
    }

    pub(crate) fn staleness_string(&self) -> Option<&str> {
        self.bound.raw()
    }

    /// The bound for a single-use read, or `None` for a strong read.
    pub(crate) fn timestamp_bound(&self) -> Result<Option<TimestampBound>> {
        self.bound
            .parsed()
            .map(ReadBound::to_timestamp_bound)
            .transpose()
    }

    /// The bound for a multi-use read-only transaction (see
    /// [`ReadBound::pinned_for_multi_use`]), or `None` for a strong read.
    pub(crate) fn multi_use_timestamp_bound(&self) -> Result<Option<TimestampBound>> {
        self.bound
            .parsed()
            .map(|b| b.pinned_for_multi_use().to_timestamp_bound())
            .transpose()
    }
}

const GRAMMAR_FORMS: &str = "\"exact:<duration>\" (e.g. \"exact:10s\"), \"max:<duration>\" \
     (e.g. \"max:500ms\"), \"read:<rfc3339>\" (e.g. \"read:2026-07-07T00:00:00Z\") or \
     \"min:<rfc3339>\" (e.g. \"min:2026-07-07T00:00:00+02:00\")";

fn grammar_err(value: &str) -> adbc_core::error::Error {
    invalid_argument(format!(
        "option {}: {value:?} is not a valid read bound; expected {GRAMMAR_FORMS}",
        crate::OPTION_READ_STALENESS
    ))
}

/// Parse a `spanner.read.staleness` value. Prefixes are case-sensitive, like other ADBC option
/// values.
fn parse_read_bound(value: &str) -> Result<ReadBound> {
    // A timestamp's colons all follow its date, so the first colon ends any prefix. For a bare
    // timestamp the "prefix" is `2026-07-07T00`, which matches no kind.
    if let Some((kind, arg)) = value.split_once(':') {
        match kind.trim() {
            "exact" => return Ok(ReadBound::ExactStaleness(parse_duration(arg.trim())?)),
            "max" => return Ok(ReadBound::MaxStaleness(parse_duration(arg.trim())?)),
            // With an explicit prefix, chrono's diagnosis of the timestamp is useful.
            "read" => return parse_rfc3339(value, arg.trim()).map(ReadBound::ReadTimestamp),
            "min" => return parse_rfc3339(value, arg.trim()).map(ReadBound::MinReadTimestamp),
            _ => {}
        }
    }
    // Without a known prefix, the prefix is the likely mistake, so report the grammar instead of
    // chrono's error.
    DateTime::parse_from_rfc3339(value)
        .map(|t| ReadBound::ReadTimestamp(t.with_timezone(&Utc)))
        .map_err(|_| grammar_err(value))
}

/// Parse the timestamp of a `read:` or `min:` value, quoting the whole value in errors.
fn parse_rfc3339(value: &str, timestamp: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(timestamp)
        .map_err(|e| {
            invalid_argument(format!(
                "option {}: {value:?} does not carry a valid RFC 3339 timestamp ({e}); expected \
                 {GRAMMAR_FORMS}",
                crate::OPTION_READ_STALENESS
            ))
        })?
        .with_timezone(&Utc))
}

/// [`parse_duration_for`] with `spanner.read.staleness` named in errors.
pub(crate) fn parse_duration(value: &str) -> Result<Duration> {
    parse_duration_for(value, crate::OPTION_READ_STALENESS)
}

/// Parse a non-negative duration with an optional unit suffix (`s` by default, `ms`, `us`/`µs`,
/// `ns`, `m`, `h`), naming option `key` in errors.
pub(crate) fn parse_duration_for(value: &str, key: &str) -> Result<Duration> {
    let bad = || {
        invalid_argument(format!(
            "option {key}: {value:?} is not a valid duration; expected a number with an optional \
             unit suffix (s [default], ms, us/µs, ns, m, h), e.g. \"500ms\""
        ))
    };
    // Check two-letter suffixes before `s`.
    let (number, unit_secs): (&str, f64) = if let Some(n) = value.strip_suffix("ms") {
        (n, 1e-3)
    } else if let Some(n) = value
        .strip_suffix("us")
        .or_else(|| value.strip_suffix("µs"))
    {
        (n, 1e-6)
    } else if let Some(n) = value.strip_suffix("ns") {
        (n, 1e-9)
    } else if let Some(n) = value.strip_suffix('s') {
        (n, 1.0)
    } else if let Some(n) = value.strip_suffix('m') {
        (n, 60.0)
    } else if let Some(n) = value.strip_suffix('h') {
        (n, 3600.0)
    } else {
        (value, 1.0)
    };
    let magnitude: f64 = number.trim().parse().map_err(|_| bad())?;
    let seconds = magnitude * unit_secs;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(bad());
    }
    // Unlike `from_secs_f64`, this does not panic on overflow such as "exact:1e20".
    Duration::try_from_secs_f64(seconds).map_err(|_| bad())
}

/// Convert to [`SystemTime`], including timestamps before the Unix epoch.
fn to_system_time(dt: DateTime<Utc>) -> SystemTime {
    let secs = dt.timestamp();
    // Non-negative even before the epoch: an offset from the floor second.
    let nanos = dt.timestamp_subsec_nanos();
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nanos)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs.unsigned_abs()) + Duration::from_nanos(nanos as u64)
    }
}

#[cfg(test)]
mod tests;
