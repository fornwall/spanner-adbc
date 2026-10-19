//! Query optimizer options, passed to Spanner unchanged.

use google_cloud_spanner::model::execute_sql_request::QueryOptions;
use google_cloud_spanner::statement::StatementBuilder;

/// [`OPTION_QUERY_OPTIMIZER_VERSION`](crate::OPTION_QUERY_OPTIMIZER_VERSION) and
/// [`OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE`](crate::OPTION_QUERY_OPTIMIZER_STATISTICS_PACKAGE),
/// set through [`SharedConfig`](crate::options::SharedConfig).
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryOptionsConfig {
    pub(crate) optimizer_version: Option<String>,
    pub(crate) optimizer_statistics_package: Option<String>,
}

impl QueryOptionsConfig {
    /// Apply the options to a query. When neither is set the request carries no `QueryOptions`,
    /// since an empty one is not the same as none.
    #[must_use]
    pub(crate) fn apply_to_statement(&self, builder: StatementBuilder) -> StatementBuilder {
        if self.optimizer_version.is_none() && self.optimizer_statistics_package.is_none() {
            return builder;
        }
        let mut options = QueryOptions::default();
        if let Some(version) = &self.optimizer_version {
            options = options.set_optimizer_version(version.as_str());
        }
        if let Some(package) = &self.optimizer_statistics_package {
            options = options.set_optimizer_statistics_package(package.as_str());
        }
        builder.set_query_options(options)
    }
}

#[cfg(test)]
mod tests;
