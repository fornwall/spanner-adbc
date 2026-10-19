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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Include,
    Exclude,
}

/// A replica type; `Any` leaves it unspecified, matching every type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicaType {
    ReadWrite,
    ReadOnly,
    Any,
}

/// A replica selection; an empty `location` matches any location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Selection {
    location: String,
    kind: ReplicaType,
}

/// A parsed `spanner.directed_read` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectedReadSpec {
    mode: Mode,
    selections: Vec<Selection>,
    auto_failover_disabled: bool,
}

impl DirectedReadSpec {
    fn to_options(&self) -> DirectedReadOptions {
        let selections: Vec<ReplicaSelection> = self
            .selections
            .iter()
            .map(|s| {
                let sel = ReplicaSelection::new().set_location(s.location.clone());
                match s.kind {
                    ReplicaType::ReadWrite => sel.set_type(Type::ReadWrite),
                    ReplicaType::ReadOnly => sel.set_type(Type::ReadOnly),
                    ReplicaType::Any => sel,
                }
            })
            .collect();
        let replicas = match self.mode {
            Mode::Include => Replicas::IncludeReplicas(Box::new(
                IncludeReplicas::new()
                    .set_replica_selections(selections)
                    .set_auto_failover_disabled(self.auto_failover_disabled),
            )),
            Mode::Exclude => Replicas::ExcludeReplicas(Box::new(
                ExcludeReplicas::new().set_replica_selections(selections),
            )),
        };
        DirectedReadOptions::new().set_replicas(replicas)
    }
}

/// The `spanner.directed_read` setting of a connection or statement.
#[derive(Debug, Clone, Default)]
pub(crate) struct DirectedRead(RawParsed<DirectedReadSpec>);

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
            Some(spec) => builder.set_directed_read_options(spec.to_options()),
            None => builder,
        }
    }
}

/// Parse a `spanner.directed_read` value.
pub(crate) fn parse(value: &str) -> Result<DirectedReadSpec> {
    // Locations never contain ';'.
    let (main, auto_failover_disabled) = match value.split_once(';') {
        Some((main, flag)) => {
            let flag = flag.trim();
            if flag != "auto_failover_disabled" {
                return Err(invalid_argument(format!(
                    "unknown spanner.directed_read flag {flag:?}; the only supported flag is \
                     \";auto_failover_disabled\""
                )));
            }
            (main.trim(), true)
        }
        None => (value.trim(), false),
    };

    let (mode_str, selections_str) = match main.split_once(':') {
        Some((mode, rest)) => (mode.trim(), Some(rest.trim())),
        None => (main, None),
    };
    let mode = match mode_str {
        "include" => Mode::Include,
        "exclude" => Mode::Exclude,
        other => {
            return Err(invalid_argument(format!(
                "unknown spanner.directed_read mode {other:?}; expected \"include\" or \"exclude\""
            )));
        }
    };

    let mut selections = Vec::new();
    if let Some(selections_str) = selections_str {
        for item in selections_str.split(',') {
            let item = item.trim();
            if item.is_empty() {
                return Err(invalid_argument(
                    "empty replica selection in spanner.directed_read (check for a stray comma)",
                ));
            }
            let selection = match item.split_once(':') {
                Some((location, type_str)) => Selection {
                    location: location.trim().to_string(),
                    kind: parse_type(type_str.trim())?,
                },
                None => Selection {
                    location: item.to_string(),
                    kind: ReplicaType::Any,
                },
            };
            if selection.location.is_empty() && selection.kind == ReplicaType::Any {
                return Err(invalid_argument(
                    "a spanner.directed_read selection needs a location and/or a replica type",
                ));
            }
            selections.push(selection);
        }
    }

    if auto_failover_disabled && mode == Mode::Exclude {
        return Err(invalid_argument(
            "auto_failover_disabled is only valid with an \"include\" directed read",
        ));
    }
    // Without selections the value does nothing, unless it disables auto-failover.
    if selections.is_empty() && !(mode == Mode::Include && auto_failover_disabled) {
        return Err(invalid_argument(
            "spanner.directed_read needs at least one replica selection",
        ));
    }

    Ok(DirectedReadSpec {
        mode,
        selections,
        auto_failover_disabled,
    })
}

fn parse_type(value: &str) -> Result<ReplicaType> {
    match value {
        "read_write" => Ok(ReplicaType::ReadWrite),
        "read_only" => Ok(ReplicaType::ReadOnly),
        "any" => Ok(ReplicaType::Any),
        other => Err(invalid_argument(format!(
            "unknown replica type {other:?}; expected \"read_write\", \"read_only\" or \"any\""
        ))),
    }
}

#[cfg(test)]
mod tests;
