//! The [`OPTION_READ_STALENESS`](crate::OPTION_READ_STALENESS) read bound for read-only queries,
//! mapped to the client's [`TimestampBound`]. Unset means a strong read.

use std::time::Duration;

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::transaction::{
    MultiUseReadOnlyTransaction, SingleUseReadOnlyTransaction, TimestampBound,
};
use google_cloud_wkt::Timestamp;

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

/// A parsed read bound, for single-use reads and for transactions that begin explicitly or
/// serve several reads.
#[derive(Debug, Clone, PartialEq)]
struct Bounds {
    single_use: TimestampBound,
    /// `BeginTransaction` rejects the bounded-staleness kinds, so they are pinned to the most
    /// stale timestamp they allow, which satisfies the original bound.
    multi_use: TimestampBound,
}

/// The `spanner.read.staleness` setting of a connection or statement.
#[derive(Debug, Clone, Default)]
pub(crate) struct ReadStaleness {
    bound: RawParsed<Bounds>,
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
    pub(crate) fn timestamp_bound(&self) -> Option<TimestampBound> {
        self.bound.parsed().map(|b| b.single_use.clone())
    }

    /// The bound for a multi-use or batch read-only transaction, or `None` for a strong read.
    pub(crate) fn multi_use_timestamp_bound(&self) -> Option<TimestampBound> {
        self.bound.parsed().map(|b| b.multi_use.clone())
    }
}

const GRAMMAR_FORMS: &str = "\"exact:<duration>\" (e.g. \"exact:10s\"), \"max:<duration>\" \
     (e.g. \"max:500ms\"), \"read:<rfc3339>\" (e.g. \"read:2026-07-07T00:00:00Z\") or \
     \"min:<rfc3339>\" (e.g. \"min:2026-07-07T00:00:00+02:00\")";

/// Parse a `spanner.read.staleness` value. Prefixes are case-sensitive, like other ADBC option
/// values.
fn parse_read_bound(value: &str) -> Result<Bounds> {
    // A timestamp's colons all follow its date, so the first colon ends any prefix. For a bare
    // timestamp the "prefix" is `2026-07-07T00`, which matches no kind.
    if let Some((kind, arg)) = value.split_once(':') {
        let arg = arg.trim();
        let staleness = || {
            let duration = duration(arg).ok_or_else(|| {
                invalid_argument(format!(
                    "option {}: {value:?} does not carry a valid duration; expected \
                     {DURATION_FORM}",
                    crate::OPTION_READ_STALENESS
                ))
            })?;
            // The client would clamp a duration beyond the wire format's 10,000 years.
            match google_cloud_wkt::Duration::try_from(duration) {
                Ok(_) => Ok(duration),
                Err(_) => Err(invalid_argument(format!(
                    "option {}: {value:?} exceeds 10,000 years",
                    crate::OPTION_READ_STALENESS
                ))),
            }
        };
        let timestamp = || {
            Timestamp::try_from(arg).map_err(|e| {
                invalid_argument(format!(
                    "option {}: {value:?} does not carry a valid RFC 3339 timestamp ({e}); \
                     expected {GRAMMAR_FORMS}",
                    crate::OPTION_READ_STALENESS
                ))
            })
        };
        match kind.trim() {
            "exact" => return Ok(both(TimestampBound::exact_staleness(staleness()?))),
            "max" => {
                let d = staleness()?;
                return Ok(Bounds {
                    single_use: TimestampBound::max_staleness(d),
                    multi_use: TimestampBound::exact_staleness(d),
                });
            }
            "read" => return Ok(both(TimestampBound::read_timestamp(timestamp()?))),
            "min" => {
                let t = timestamp()?;
                return Ok(Bounds {
                    single_use: TimestampBound::min_read_timestamp(t),
                    multi_use: TimestampBound::read_timestamp(t),
                });
            }
            _ => {}
        }
    }
    // Without a known prefix, the prefix is the likely mistake, so report the grammar instead of
    // the timestamp parser's error.
    Timestamp::try_from(value)
        .map(|t| both(TimestampBound::read_timestamp(t)))
        .map_err(|_| {
            invalid_argument(format!(
                "option {}: {value:?} is not a valid read bound; expected {GRAMMAR_FORMS}",
                crate::OPTION_READ_STALENESS
            ))
        })
}

fn both(bound: TimestampBound) -> Bounds {
    Bounds {
        single_use: bound.clone(),
        multi_use: bound,
    }
}

const DURATION_FORM: &str = "a number with an optional unit suffix (s [default], ms, us/µs, \
     ns, m, h), e.g. \"500ms\"";

/// Parse a duration such as `500ms`, naming option `key` in errors.
pub(crate) fn parse_duration(value: &str, key: &str) -> Result<Duration> {
    duration(value).ok_or_else(|| {
        invalid_argument(format!(
            "option {key}: {value:?} is not a valid duration; expected {DURATION_FORM}"
        ))
    })
}

/// A non-negative number of seconds, or of the unit its suffix names.
fn duration(value: &str) -> Option<Duration> {
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
    let seconds = number.trim().parse::<f64>().ok()? * unit_secs;
    // Unlike `from_secs_f64`, this rejects negative and non-finite values and overflow such as
    // "1e20" instead of panicking.
    Duration::try_from_secs_f64(seconds).ok()
}

#[cfg(test)]
mod tests;
