//! Directed-read replica-selection options for read-only queries.
//!
//! Spanner's [`DirectedReadOptions`](google_cloud_spanner::model::DirectedReadOptions) let a
//! read-only query steer where it is served: either an ordered **include** list of replica
//! selections (Spanner tries them in order) or an **exclude** list (Spanner routes around them).
//! Directed reads apply **only** to read-only queries — Spanner returns `INVALID_ARGUMENT` if they
//! are attached to a read/write transaction — so this module's option is applied at the driver's
//! read-only query sites only. It parses [`OPTION_DIRECTED_READ`](crate::OPTION_DIRECTED_READ)
//! (`spanner.directed_read`) into a driver-owned value ([`DirectedReadSpec`]) that is unit-testable
//! offline, then builds the client [`DirectedReadOptions`] on demand.
//!
//! # Grammar
//!
//! ```text
//! <value>      ::= <mode> [ ":" <selections> ] [ ";auto_failover_disabled" ]
//! <mode>       ::= "include" | "exclude"            (exact lowercase)
//! <selections> ::= <selection> ("," <selection>)*
//! <selection>  ::= <location> [ ":" <type> ]        (at least one of location / type)
//! <type>       ::= "read_write" | "read_only" | "any"   (exact lowercase)
//! ```
//!
//! A `<selection>` may omit the location (`:<type>` — any location of that type) or the type (which
//! then defaults to `any`, the unspecified replica type, matching every type). The optional
//! `;auto_failover_disabled` suffix is valid only with `include` (the exclude message has no such
//! field) and stops Spanner falling back to a replica outside the list.
//!
//! Examples: `include:us-east1`, `include:us-east1:read_only,us-east4:read_write`,
//! `exclude:us-central1`, `include:us-east1;auto_failover_disabled`, `include::read_only`.
//!
//! An empty string unsets the option; malformed input is rejected with `InvalidArguments`.

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

/// The replica-selection mode: an ordered *include* preference or an *exclude* set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Include,
    Exclude,
}

/// A replica type. A driver-owned enum (rather than the client's non-exhaustive
/// [`Type`]) so parsing is unit-testable offline; `Any` leaves the type unspecified (matches all).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicaType {
    ReadWrite,
    ReadOnly,
    Any,
}

/// One parsed replica selection: an optional location and a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Selection {
    location: String,
    kind: ReplicaType,
}

/// A parsed `spanner.directed_read` value, before it is turned into the client
/// [`DirectedReadOptions`]. Kept as a small, pure value so the option parsing is offline-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirectedReadSpec {
    mode: Mode,
    selections: Vec<Selection>,
    auto_failover_disabled: bool,
}

impl DirectedReadSpec {
    /// Build the client [`DirectedReadOptions`] for this spec.
    fn to_options(&self) -> DirectedReadOptions {
        let selections: Vec<ReplicaSelection> = self
            .selections
            .iter()
            .map(|s| {
                let sel = ReplicaSelection::new().set_location(s.location.clone());
                match s.kind {
                    ReplicaType::ReadWrite => sel.set_type(Type::ReadWrite),
                    ReplicaType::ReadOnly => sel.set_type(Type::ReadOnly),
                    // `Any` leaves the type unspecified, matching every replica type.
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

/// The directed-read configuration held by a connection or statement.
///
/// Stores the raw (trimmed) option string so `get_option` round-trips what was set, alongside the
/// parsed spec. A connection's value is cloned into each statement it creates (which may override
/// it), mirroring [`ReadStaleness`](crate::staleness::ReadStaleness) and
/// [`RequestConfig`](crate::request::RequestConfig).
#[derive(Debug, Clone, Default)]
pub(crate) struct DirectedRead(RawParsed<DirectedReadSpec>);

impl DirectedRead {
    /// Handle a `set_option` for `spanner.directed_read`. An empty value unsets it.
    pub(crate) fn set(&mut self, value: OptionValue) -> Result<()> {
        self.0.set(value, crate::OPTION_DIRECTED_READ, parse)
    }

    /// The raw `spanner.directed_read` value, for `get_option` round-trip.
    pub(crate) fn option_string(&self) -> Option<&str> {
        self.0.raw()
    }

    /// Apply the directed-read options to a read-only query statement builder. A no-op when unset.
    #[must_use]
    pub(crate) fn apply_to_statement(&self, builder: StatementBuilder) -> StatementBuilder {
        match self.0.parsed() {
            Some(spec) => builder.set_directed_read_options(spec.to_options()),
            None => builder,
        }
    }
}

/// Parse a `spanner.directed_read` value per the module grammar.
pub(crate) fn parse(value: &str) -> Result<DirectedReadSpec> {
    // Split off the optional ";auto_failover_disabled" suffix first (locations never contain ';').
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

    // `main` is `<mode>` or `<mode>:<selections>`.
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
            // A selection must constrain something — either a location or a specific type.
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
    // An empty selection list is only meaningful for `include;auto_failover_disabled`; otherwise it
    // is a no-op the user almost certainly did not intend, so reject it.
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

/// Parse a replica type: exactly `read_write` / `read_only` / `any` (lowercase, like every option
/// value in this driver).
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
