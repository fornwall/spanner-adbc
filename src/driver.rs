//! [`SpannerDriver`] and [`SpannerDatabase`], the top two levels of the ADBC hierarchy.

mod admin;
mod credentials;
mod uri;

pub(crate) use uri::ensure_scheme;

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::client_builder::internal::ClientConfig;
use google_cloud_spanner::client::{DatabaseAdmin, DatabaseClient, Spanner};
use google_cloud_spanner::omni::{InstanceType, SpannerBuilderExt};

use crate::connection::SpannerConnection;
use crate::error::{
    err, from_builder, from_spanner, invalid_argument, invalid_state, option_not_set,
    unknown_option,
};
use crate::options::impl_typed_option_getters;
use crate::runtime::{SharedRuntime, new_runtime};
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_EMULATOR, OPTION_ENDPOINT, OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES, OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_INSTANCE_TYPE, OPTION_KEYFILE, OPTION_KEYFILE_JSON, OPTION_QUOTA_PROJECT,
};
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// The Spanner ADBC driver, which creates [`SpannerDatabase`]s.
///
/// The driver owns the Tokio runtime that drives the asynchronous Spanner client, so reuse one
/// driver for the lifetime of the application.
#[derive(Debug)]
pub struct SpannerDriver {
    runtime: SharedRuntime,
}

impl SpannerDriver {
    /// Create a new driver, initialising its Tokio runtime.
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            runtime: new_runtime()?,
        })
    }
}

impl Default for SpannerDriver {
    /// Create a driver with a fresh runtime.
    ///
    /// Panics if the Tokio runtime cannot be created. Prefer [`SpannerDriver::try_new`] to
    /// handle initialization errors; the C ABI uses that fallible constructor too.
    fn default() -> Self {
        Self::try_new().expect("failed to initialize the Spanner ADBC driver Tokio runtime")
    }
}

impl Driver for SpannerDriver {
    type DatabaseType = SpannerDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(SpannerDatabase::new(self.runtime.clone()))
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = SpannerDatabase::new(self.runtime.clone());
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

/// A configured Spanner database that creates [`SpannerConnection`]s.
///
/// The client stack (gRPC channels, credentials and the [`DatabaseClient`] with its multiplexed
/// session) is built lazily on the first connection and shared by every later one. Setting any
/// database option discards it, so the next connection rebuilds it; `SPANNER_EMULATOR_HOST` is
/// therefore read when the stack is built, not per connection.
///
/// [`Debug`] shows only whether each credential field is set.
pub struct SpannerDatabase {
    runtime: SharedRuntime,
    /// The `uri` option as set, for `get_option`; the fields below hold what it expanded into.
    uri: Option<String>,
    database: Option<String>,
    endpoint: Option<String>,
    emulator: bool,
    instance_type: InstanceType,
    keyfile: Option<String>,
    keyfile_json: Option<String>,
    impersonate_target_principal: Option<String>,
    /// Empty means no delegation chain.
    impersonate_delegates: Vec<String>,
    /// Empty means the auth crate's default `cloud-platform` scope.
    impersonate_scopes: Vec<String>,
    impersonate_lifetime_secs: Option<u64>,
    access_token: Option<String>,
    /// `None` bills the credential's own project.
    quota_project: Option<String>,
    /// The shared client stack, built by the first connection and cleared by `set_option`.
    connected: Mutex<Option<Connected>>,
}

impl std::fmt::Debug for SpannerDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redact = |value: &Option<String>| value.as_ref().map(|_| "<redacted>");
        f.debug_struct("SpannerDatabase")
            .field("runtime", &self.runtime)
            .field("uri", &self.uri)
            .field("database", &self.database)
            .field("endpoint", &self.endpoint)
            .field("emulator", &self.emulator)
            .field("instance_type", &self.instance_type)
            .field("keyfile", &redact(&self.keyfile))
            .field("keyfile_json", &redact(&self.keyfile_json))
            .field(
                "impersonate_target_principal",
                &self.impersonate_target_principal,
            )
            .field("impersonate_delegates", &self.impersonate_delegates)
            .field("impersonate_scopes", &self.impersonate_scopes)
            .field("impersonate_lifetime_secs", &self.impersonate_lifetime_secs)
            .field("access_token", &redact(&self.access_token))
            .field("quota_project", &self.quota_project)
            // The client types' own `Debug` could expose endpoint or credential internals.
            .field(
                "connected",
                &self
                    .connected
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|_| "<client stack>"),
            )
            .finish()
    }
}

impl SpannerDatabase {
    pub(crate) fn new(runtime: SharedRuntime) -> Self {
        Self {
            runtime,
            uri: None,
            database: None,
            endpoint: None,
            emulator: false,
            instance_type: InstanceType::Cloud,
            keyfile: None,
            keyfile_json: None,
            impersonate_target_principal: None,
            impersonate_delegates: Vec::new(),
            impersonate_scopes: Vec::new(),
            impersonate_lifetime_secs: None,
            access_token: None,
            quota_project: None,
            connected: Mutex::new(None),
        }
    }

    /// Build the client stack from the current options.
    ///
    /// Outside Omni, a non-empty `SPANNER_EMULATOR_HOST` enables emulator mode and supplies the
    /// endpoint unless one was set explicitly.
    pub(crate) fn connect(&self) -> Result<Connected> {
        let database = self.database.clone().ok_or_else(|| {
            invalid_state(
                "Spanner database path is not set; provide the `uri` option \
                 (projects/<p>/instances/<i>/databases/<d>)",
            )
        })?;

        let mut endpoint = self.endpoint.clone();
        let mut emulator = self.emulator;
        let omni = self.instance_type == InstanceType::Omni;
        if omni {
            if emulator {
                return Err(invalid_state(
                    "spanner.instance_type=omni cannot be combined with spanner.emulator=true",
                ));
            }
            if endpoint
                .as_ref()
                .is_none_or(|endpoint| endpoint.trim().is_empty())
            {
                return Err(invalid_state(
                    "spanner.instance_type=omni requires an explicit spanner.endpoint URL",
                ));
            }
        }
        if !omni
            && let Ok(host) = std::env::var("SPANNER_EMULATOR_HOST")
            && !host.is_empty()
        {
            if endpoint.is_none() {
                endpoint = Some(host);
            }
            emulator = true;
        }
        // tonic cannot dial a bare `host:port`, such as a URI authority or the variable above.
        let endpoint = endpoint.map(|endpoint| ensure_scheme(&endpoint));

        // Emulator mode uses anonymous plaintext credentials. Silently dropping explicitly
        // configured credentials would let a stray `SPANNER_EMULATOR_HOST` redirect real traffic,
        // unauthenticated, to another endpoint, so refuse the combination. Ambient ADC is not an
        // explicit option and does not count. A quota project would also be silently ignored.
        if emulator {
            let cause = if self.emulator {
                "the `spanner.emulator` option"
            } else {
                "the `SPANNER_EMULATOR_HOST` environment variable"
            };
            if let Some(option) = self.explicit_credential_option() {
                return Err(invalid_state(format!(
                    "emulator mode (enabled by {cause}) forces anonymous plaintext credentials \
                     and would silently ignore the configured `{option}` option; unset the \
                     credential option(s) or disable emulator mode"
                )));
            }
            if self.quota_project.is_some() {
                return Err(invalid_state(format!(
                    "emulator mode (enabled by {cause}) forces anonymous plaintext credentials \
                     and would silently ignore the configured `{OPTION_QUOTA_PROJECT}` option; \
                     unset it or disable emulator mode"
                )));
            }
        }

        // An access token is a complete credential, so a keyfile or impersonation target alongside
        // it is ambiguous.
        if self.access_token.is_some()
            && let Some(conflict) = self.conflicting_credential_with_access_token()
        {
            return Err(invalid_state(format!(
                "the `{OPTION_ACCESS_TOKEN}` option supplies a complete OAuth2 credential and \
                 cannot be combined with the `{conflict}` option; set only one"
            )));
        }

        // Blocking file I/O, so read the key file before entering the runtime.
        let credentials_json = self.credentials_json()?;

        self.runtime.block_on(async move {
            let mut builder = Spanner::builder().with_instance_type(self.instance_type);
            if let Some(endpoint) = endpoint.clone() {
                builder = builder.with_endpoint(endpoint);
            }
            // With no configured credential, Omni and plaintext endpoints connect anonymously
            // instead of through ADC.
            let credentials = self
                .build_credentials(emulator, credentials_json)?
                .or_else(|| {
                    (omni
                        || endpoint.as_deref().is_some_and(|value| {
                            value
                                .trim()
                                .get(..7)
                                .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
                        }))
                    .then(|| google_cloud_auth::credentials::anonymous::Builder::new().build())
                });
            if let Some(credentials) = &credentials {
                builder = builder.with_credentials(credentials.clone());
            }
            let mut admin_config = ClientConfig::default();
            admin_config.endpoint = endpoint;
            admin_config.cred = credentials;
            let spanner = builder.build().await.map_err(from_builder)?;
            let client = spanner
                .database_client(database.clone())
                .build()
                .await
                .map_err(from_spanner)?;
            Ok(Connected {
                client,
                database,
                admin: Arc::new(DatabaseAdminState {
                    client: OnceCell::new(),
                    config: admin_config,
                }),
            })
        })
    }

    /// Return the shared client stack, building it on first use. A failed build is not cached.
    fn connect_shared(&self) -> Result<Connected> {
        // Holding the lock across the build stops concurrent connections from building twice.
        // It cannot deadlock: only caller threads take it, never tasks on the runtime that
        // `connect` blocks on.
        let mut cached = self.connected.lock().unwrap();
        if let Some(connected) = cached.as_ref() {
            return Ok(connected.clone());
        }
        let connected = self.connect()?;
        *cached = Some(connected.clone());
        Ok(connected)
    }
}

/// The data client, database path and lazily built admin client. Clones share channels and the
/// session.
#[derive(Clone, Debug)]
pub(crate) struct Connected {
    pub(crate) client: DatabaseClient,
    pub(crate) database: String,
    pub(crate) admin: SharedDatabaseAdmin,
}

/// The [`DatabaseAdmin`] client for DDL, built on the first DDL statement so that workloads
/// without DDL never open an admin connection. It lives in [`Connected`], so it is rebuilt along
/// with the data client when database options change.
pub(crate) type SharedDatabaseAdmin = Arc<DatabaseAdminState>;

pub(crate) struct DatabaseAdminState {
    client: OnceCell<DatabaseAdmin>,
    config: ClientConfig<Credentials>,
}

impl std::fmt::Debug for DatabaseAdminState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseAdminState")
            .field("initialized", &self.client.initialized())
            .finish()
    }
}

impl DatabaseAdminState {
    pub(crate) async fn client(&self) -> Result<&DatabaseAdmin> {
        self.client
            .get_or_try_init(|| async {
                admin::database_admin(self.config.clone())
                    .await
                    .map_err(from_builder)
            })
            .await
    }
}

impl Optionable for SpannerDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        // Any option may change what the cached stack was built from. Dropping it even when the
        // set fails below only costs a rebuild.
        *self.connected.get_mut().unwrap() = None;
        match &key {
            OptionDatabase::Uri => {
                let value = string_value(&key, value)?;
                self.set_uri_option(value)?
            }
            OptionDatabase::Other(name) if name == OPTION_ENDPOINT => {
                self.endpoint = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_EMULATOR => {
                self.emulator = bool_value(&key, value)?
            }
            OptionDatabase::Other(name) if name == OPTION_INSTANCE_TYPE => {
                self.instance_type = match string_value(&key, value)?.as_str() {
                    "cloud" => InstanceType::Cloud,
                    "omni" => InstanceType::Omni,
                    _ => {
                        return Err(invalid_argument(
                            "spanner.instance_type expects cloud or omni",
                        ));
                    }
                };
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE => {
                self.keyfile = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE_JSON => {
                self.keyfile_json = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_TARGET_PRINCIPAL => {
                self.impersonate_target_principal = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_DELEGATES => {
                self.impersonate_delegates = comma_separated(&string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_SCOPES => {
                self.impersonate_scopes = comma_separated(&string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_LIFETIME => {
                self.impersonate_lifetime_secs = Some(u64_seconds_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_ACCESS_TOKEN => {
                self.access_token = Some(string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_QUOTA_PROJECT => {
                let project = string_value(&key, value)?;
                self.quota_project = (!project.is_empty()).then_some(project);
            }
            other => {
                return Err(unknown_option("database", other.as_ref()));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        // Secrets are write-only, so tools that dump options cannot print a credential.
        if let OptionDatabase::Other(name) = &key
            && (name == OPTION_KEYFILE_JSON || name == OPTION_ACCESS_TOKEN)
        {
            return Err(err(
                format!("option {name} is write-only (it holds a secret) and cannot be read back"),
                Status::NotFound,
            ));
        }
        let value = match &key {
            // adbc.h: `GetOption` returns the option value, so the URI comes back as set.
            OptionDatabase::Uri => self.uri.clone(),
            OptionDatabase::Other(name) if name == OPTION_ENDPOINT => self.endpoint.clone(),
            OptionDatabase::Other(name) if name == OPTION_EMULATOR => {
                Some(self.emulator.to_string())
            }
            OptionDatabase::Other(name) if name == OPTION_INSTANCE_TYPE => Some(
                if self.instance_type == InstanceType::Omni {
                    "omni"
                } else {
                    "cloud"
                }
                .to_string(),
            ),
            OptionDatabase::Other(name) if name == OPTION_KEYFILE => self.keyfile.clone(),
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_TARGET_PRINCIPAL => {
                self.impersonate_target_principal.clone()
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_DELEGATES => {
                (!self.impersonate_delegates.is_empty())
                    .then(|| self.impersonate_delegates.join(","))
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_SCOPES => {
                (!self.impersonate_scopes.is_empty()).then(|| self.impersonate_scopes.join(","))
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_LIFETIME => {
                self.impersonate_lifetime_secs.map(|secs| secs.to_string())
            }
            OptionDatabase::Other(name) if name == OPTION_QUOTA_PROJECT => {
                self.quota_project.clone()
            }
            _ => None,
        };
        value.ok_or_else(|| option_not_set(key.as_ref()))
    }

    impl_typed_option_getters!();
}

impl Database for SpannerDatabase {
    type ConnectionType = SpannerConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(SpannerConnection::new(
            self.runtime.clone(),
            self.connect_shared()?,
        ))
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (adbc_core::options::OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

fn string_value(key: &OptionDatabase, value: OptionValue) -> Result<String> {
    crate::options::string_option(value, &format!("option {}", key.as_ref()))
}

/// Split a comma-separated value, trimming entries and dropping empty ones.
fn comma_separated(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Parse a non-negative whole number of seconds, given as an integer or a numeric string.
fn u64_seconds_value(key: &OptionDatabase, value: OptionValue) -> Result<u64> {
    match value {
        OptionValue::Int(seconds) if seconds >= 0 => Ok(seconds as u64),
        OptionValue::String(seconds) => seconds.trim().parse::<u64>().map_err(|_| {
            invalid_argument(format!(
                "option {} expects a non-negative integer number of seconds, got {seconds:?}",
                key.as_ref()
            ))
        }),
        _ => Err(invalid_argument(format!(
            "option {} expects a non-negative integer number of seconds",
            key.as_ref()
        ))),
    }
}

fn bool_value(key: &OptionDatabase, value: OptionValue) -> Result<bool> {
    crate::options::bool_option(value, &format!("option {}", key.as_ref()))
}

#[cfg(test)]
mod tests;
