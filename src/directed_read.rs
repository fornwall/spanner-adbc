//! The [`OPTION_DIRECTED_READ`](crate::OPTION_DIRECTED_READ) replica selection, whose grammar
//! is documented on that constant. Spanner rejects directed reads in read/write transactions, so
//! it is applied only to read-only queries.

use adbc_core::error::Result;
use adbc_core::options::OptionValue;
use google_cloud_spanner::model::DirectedReadOptions;
use google_cloud_spanner::model::directed_read_options::replica_selection::Type;
use google_cloud_spanner::model::directed_read_options::{
    ExcludeReplicas, IncludeReplicas, ReplicaSelection, Replicas,
};
use google_cloud_spanner::statement::StatementBuilder;

use crate::error::invalid_argument;
use crate::options::RawParsed;

/// The `spanner.directed_read` setting of a connection or statement.
#[derive(Debug, Clone, Default)]
pub(crate) struct DirectedRead(RawParsed<DirectedReadOptions>);

impl DirectedRead {
    pub(crate) fn set(&mut self, value: OptionValue) -> Result<()> {
        self.0.set(value, crate::OPTION_DIRECTED_READ, parse)
    }

    pub(crate) fn option_string(&self) -> Option<&str> {
        self.0.raw()
    }

    /// Apply the selection to a read-only query.
    #[must_use]
    pub(crate) fn apply_to_statement(&self, builder: StatementBuilder) -> StatementBuilder {
        match self.0.parsed() {
            Some(options) => builder.set_directed_read_options(options.clone()),
            None => builder,
        }
    }
}

/// Parse a `spanner.directed_read` value.
pub(crate) fn parse(value: &str) -> Result<DirectedReadOptions> {
    let bad = |problem: String| {
        invalid_argument(format!(
            "option {}: {problem} in {value:?}",
            crate::OPTION_DIRECTED_READ
        ))
    };
    // Locations never contain ';'.
    let (main, auto_failover_disabled) = match value.split_once(';') {
        Some((main, flag)) if flag.trim() == "auto_failover_disabled" => (main.trim(), true),
        Some((_, flag)) => {
            return Err(bad(format!(
                "unknown flag {:?}; the only flag is \"auto_failover_disabled\"",
                flag.trim()
            )));
        }
        None => (value.trim(), false),
    };
    let (mode, list) = match main.split_once(':') {
        Some((mode, list)) => (mode.trim(), Some(list)),
        None => (main, None),
    };
    let include = match mode {
        "include" => true,
        "exclude" => false,
        other => {
            return Err(bad(format!(
                "unknown mode {other:?}; expected \"include\" or \"exclude\""
            )));
        }
    };
    let selections = match list {
        Some(list) => list
            .split(',')
            .map(|item| parse_selection(item.trim()).map_err(bad))
            .collect::<Result<Vec<_>>>()?,
        None => Vec::new(),
    };
    if auto_failover_disabled && !include {
        return Err(bad(
            "auto_failover_disabled is only valid with \"include\"".to_string()
        ));
    }
    // Without selections the value does nothing, unless it disables auto-failover.
    if selections.is_empty() && !auto_failover_disabled {
        return Err(bad("no replica selection".to_string()));
    }
    let replicas = if include {
        Replicas::IncludeReplicas(Box::new(
            IncludeReplicas::new()
                .set_replica_selections(selections)
                .set_auto_failover_disabled(auto_failover_disabled),
        ))
    } else {
        Replicas::ExcludeReplicas(Box::new(
            ExcludeReplicas::new().set_replica_selections(selections),
        ))
    };
    Ok(DirectedReadOptions::new().set_replicas(replicas))
}

/// Parse `<location>`, `<location>:<type>` or `:<type>`; no type matches every replica type.
fn parse_selection(item: &str) -> std::result::Result<ReplicaSelection, String> {
    if item.is_empty() {
        return Err("empty replica selection (check for a stray comma)".to_string());
    }
    let (location, kind) = match item.split_once(':') {
        Some((location, kind)) => (location.trim(), kind.trim()),
        None => (item, "any"),
    };
    let kind = match kind {
        "read_write" => Type::ReadWrite,
        "read_only" => Type::ReadOnly,
        "any" if location.is_empty() => {
            return Err(format!(
                "replica selection {item:?} names neither a location nor a replica type"
            ));
        }
        "any" => Type::Unspecified,
        other => {
            return Err(format!(
                "unknown replica type {other:?}; expected \"read_write\", \"read_only\" or \
                 \"any\""
            ));
        }
    };
    Ok(ReplicaSelection::new()
        .set_location(location)
        .set_type(kind))
}

#[cfg(test)]
mod tests;
