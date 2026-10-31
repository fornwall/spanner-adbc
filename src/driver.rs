//! [`SpannerDriver`] and [`SpannerDatabase`], the top two levels of the ADBC hierarchy.

mod admin;
mod credentials;
mod uri;

use credentials::{CredentialChoice, first_token};
pub(crate) use uri::ensure_scheme;
use uri::{URI_FORM, is_plaintext_endpoint};

use adbc_core::error::{Result, Status};
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::{Database, Driver, Optionable};
use google_cloud_auth::credentials::Credentials;
use google_cloud_gax::client_builder::internal::ClientConfig;
use google_cloud_spanner::client::{DatabaseAdmin, DatabaseClient, Spanner, SpannerBuilderExt};
use google_cloud_spanner::omni::InstanceType;

use crate::connection::SpannerConnection;
use crate::error::{
    err, from_builder, from_spanner, invalid_argument, invalid_state, option_not_set,
    unknown_option, unsupported,
};
use crate::options::{
    F64Range, f64_option, impl_typed_option_getters, non_empty_string_option, shown,
};
use crate::runtime::{SharedRuntime, block_on, new_runtime};
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_EMULATOR, OPTION_ENDPOINT, OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES, OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_INSTANCE_TYPE, OPTION_KEYFILE, OPTION_KEYFILE_JSON, OPTION_QUOTA_PROJECT,
    OPTION_RPC_TIMEOUT_CONNECT,
};
use std::sync::{Arc, Mutex, TryLockError};
use std::time::Duration;
use tokio::sync::OnceCell;

/// The [`OPTION_RPC_TIMEOUT_CONNECT`] default. A healthy connect takes well under a second; this
/// leaves room for a slow first token fetch and several retries of a transient failure.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

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

    /// The runtime for a new database. A forked child, such as a Python multiprocessing worker,
    /// gets a new one, since the parent's runtime threads do not exist there.
    fn database_runtime(&mut self) -> Result<SharedRuntime> {
        if self.runtime.forked() {
            self.runtime = new_runtime()?;
        }
        Ok(self.runtime.clone())
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
        Ok(SpannerDatabase::new(self.database_runtime()?))
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = self.new_database()?;
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
    /// [`OPTION_RPC_TIMEOUT_CONNECT`] as set; `None` means the default and `0` no deadline.
    connect_timeout_secs: Option<f64>,
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
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            // The client types' own `Debug` could expose endpoint or credential internals.
            .field(
                "connected",
                // `connect_shared` holds the lock while connecting; don't wait for it.
                &match self.connected.try_lock() {
                    Ok(cached) => cached.as_ref().map(|_| "<client stack>"),
                    Err(TryLockError::WouldBlock) => Some("<connecting>"),
                    Err(TryLockError::Poisoned(_)) => Some("<poisoned>"),
                },
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
            connect_timeout_secs: None,
            connected: Mutex::new(None),
        }
    }

    /// Build the client stack from the current options, within [`OPTION_RPC_TIMEOUT_CONNECT`].
    ///
    /// Outside Omni, a non-empty `SPANNER_EMULATOR_HOST` enables emulator mode and supplies the
    /// endpoint unless one was set explicitly.
    pub(crate) fn connect(&self) -> Result<Connected> {
        self.connect_with(std::env::var("SPANNER_EMULATOR_HOST").ok().as_deref())
    }

    /// [`Self::connect`] with `emulator_host` as the value of `SPANNER_EMULATOR_HOST`.
    fn connect_with(&self, emulator_host: Option<&str>) -> Result<Connected> {
        let Target {
            database,
            endpoint,
            endpoint_from_env,
            emulator,
            anonymous_default,
        } = self.resolve_target(emulator_host)?;
        let choice = self.credential_choice(emulator, anonymous_default);

        // Blocking file I/O, so read the key file before entering the runtime.
        let credentials_json = self.credentials_json()?;

        // Named in the timeout error, since a stray `SPANNER_EMULATOR_HOST` is a common cause.
        let target = match &endpoint {
            Some(endpoint) if endpoint_from_env => {
                format!("{database} at {endpoint} (from SPANNER_EMULATOR_HOST)")
            }
            Some(endpoint) => format!("{database} at {endpoint}"),
            None => format!("{database} at the default Spanner endpoint"),
        };

        let mut token_failure = None;
        let token_failure_slot = &mut token_failure;
        let build = async move {
            // One credential, and so one token cache, for every channel and the admin client.
            let credentials = self.build_credentials(choice, credentials_json)?;
            first_token(&credentials, token_failure_slot).await?;
            let mut builder = Spanner::builder()
                .with_instance_type(self.instance_type)
                .with_credentials(credentials.clone());
            if let Some(endpoint) = endpoint.clone() {
                builder = builder.with_endpoint(endpoint);
            }
            let mut admin_config = ClientConfig::default();
            admin_config.endpoint = endpoint;
            admin_config.cred = Some(credentials);
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
        };
        // The client retries session creation without limit while the endpoint is unreachable,
        // so only this deadline ends such a connect. Expiry drops, and so cancels, the build.
        let Some(limit) = self.connect_timeout() else {
            return block_on(&self.runtime, build);
        };
        // The timer must be created inside the runtime.
        let Ok(connected) = block_on(&self.runtime, async {
            Ok(tokio::time::timeout(limit, build).await)
        })?
        else {
            let expired = format!(
                "connecting to {target} timed out after {}s ({OPTION_RPC_TIMEOUT_CONNECT})",
                limit.as_secs_f64()
            );
            let Some(cause) = token_failure else {
                return Err(err(
                    format!(
                        "{expired}; check that the endpoint is reachable and serves Spanner, or \
                         raise the timeout"
                    ),
                    Status::Timeout,
                ));
            };
            let (source, advice) = if choice == CredentialChoice::Adc {
                (
                    "Application Default Credentials",
                    format!(
                        "outside Google Cloud, run `gcloud auth application-default login` or \
                         set GOOGLE_APPLICATION_CREDENTIALS or {OPTION_KEYFILE}"
                    ),
                )
            } else {
                (
                    "the configured credentials",
                    "check access to Google's authentication endpoints".to_owned(),
                )
            };
            return Err(err(
                format!("{expired}: {source} supplied no access token ({cause}); {advice}"),
                Status::Unauthenticated,
            ));
        };
        connected
    }

    /// The [`OPTION_RPC_TIMEOUT_CONNECT`] deadline; `None` when disabled with `0`.
    fn connect_timeout(&self) -> Option<Duration> {
        match self.connect_timeout_secs {
            None => Some(DEFAULT_CONNECT_TIMEOUT),
            seconds => crate::timeout::as_duration(seconds),
        }
    }

    /// Validate the options and resolve the endpoint and credential mode. `emulator_host` is the
    /// value of `SPANNER_EMULATOR_HOST`.
    fn resolve_target(&self, emulator_host: Option<&str>) -> Result<Target> {
        let database = self.database.clone().ok_or_else(|| {
            invalid_state(format!(
                "the `uri` option is not set; set it to a URI such as `{URI_FORM}`"
            ))
        })?;

        let mut endpoint = self
            .endpoint
            .clone()
            .filter(|endpoint| !endpoint.trim().is_empty());
        let mut emulator = self.emulator;
        let mut endpoint_from_env = false;
        let omni = self.instance_type == InstanceType::Omni;
        if omni {
            if emulator {
                return Err(invalid_state(
                    "spanner.instance_type=omni cannot be combined with spanner.emulator=true",
                ));
            }
            if endpoint.is_none() {
                return Err(invalid_state(
                    "spanner.instance_type=omni requires an explicit spanner.endpoint URL",
                ));
            }
        }
        if !omni && let Some(host) = emulator_host.filter(|host| !host.is_empty()) {
            if endpoint.is_none() {
                endpoint = Some(host.to_owned());
                endpoint_from_env = true;
            }
            emulator = true;
        }
        // tonic cannot dial a bare `host:port`, such as a URI authority or the variable above.
        let endpoint = endpoint.map(|endpoint| ensure_scheme(&endpoint));

        // Emulator mode uses anonymous plaintext credentials. Silently dropping explicitly
        // configured credentials would let a stray `SPANNER_EMULATOR_HOST` redirect real traffic,
        // unauthenticated, to another endpoint, so refuse the combination. Ambient ADC is not an
        // explicit option and does not count. A quota project would also be silently ignored.
        if emulator
            && let Some(option) = self
                .explicit_credential_option()
                .or(self.quota_project.is_some().then_some(OPTION_QUOTA_PROJECT))
        {
            let (cause, disable) = if self.emulator {
                ("the `spanner.emulator` option", "set that option to false")
            } else {
                (
                    "the `SPANNER_EMULATOR_HOST` environment variable",
                    "unset the variable",
                )
            };
            return Err(invalid_state(format!(
                "emulator mode (enabled by {cause}) forces anonymous plaintext credentials and \
                 would silently ignore the configured `{option}` option; unset it or {disable}"
            )));
        }
        // Emulator mode has no default address. Without one the client dials production
        // anonymously and fails with a confusing authentication error.
        if emulator && endpoint.is_none() {
            return Err(invalid_state(
                "spanner.emulator=true needs an endpoint: set spanner.endpoint, a host:port in \
                 the URI, or SPANNER_EMULATOR_HOST",
            ));
        }

        // Without a credential option, Omni and plaintext endpoints connect anonymously instead
        // of through ADC. Building ADC just to carry a quota project would send Google tokens to
        // a non-Google or unencrypted host, so refuse the quota project there as well.
        let anonymous_default = omni || endpoint.as_deref().is_some_and(is_plaintext_endpoint);
        if !emulator
            && anonymous_default
            && self.explicit_credential_option().is_none()
            && self.quota_project.is_some()
        {
            let mode = if omni {
                "Spanner Omni (`spanner.instance_type=omni`)"
            } else {
                "a plaintext (non-https) endpoint"
            };
            return Err(invalid_state(format!(
                "{mode} without explicit credentials connects anonymously and would silently \
                 ignore the configured `{OPTION_QUOTA_PROJECT}` option; unset it or configure \
                 explicit credentials"
            )));
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

        Ok(Target {
            database,
            endpoint,
            endpoint_from_env,
            emulator,
            anonymous_default,
        })
    }

    /// Return the shared client stack, building it on first use. A failed build is not cached.
    fn connect_shared(&self) -> Result<Connected> {
        // Holding the lock through the build avoids duplicate client stacks. Only caller threads
        // take it, so the build cannot deadlock with tasks on the driver's runtime.
        let mut cached = self.connected.lock().unwrap();
        if let Some(connected) = cached.as_ref() {
            return Ok(connected.clone());
        }
        let connected = self.connect()?;
        *cached = Some(connected.clone());
        Ok(connected)
    }
}

/// What [`SpannerDatabase::connect`] builds from.
#[derive(Debug)]
struct Target {
    database: String,
    endpoint: Option<String>,
    /// The endpoint came from `SPANNER_EMULATOR_HOST`.
    endpoint_from_env: bool,
    /// From the option or `SPANNER_EMULATOR_HOST`.
    emulator: bool,
    /// Omni or a plaintext endpoint, where no credential option means anonymous instead of ADC.
    anonymous_default: bool,
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
                let endpoint = string_value(&key, value)?;
                self.endpoint = (!endpoint.trim().is_empty()).then_some(endpoint);
            }
            OptionDatabase::Other(name) if name == OPTION_EMULATOR => {
                self.emulator = bool_value(&key, value)?
            }
            OptionDatabase::Other(name) if name == OPTION_INSTANCE_TYPE => {
                self.instance_type = match string_value(&key, value)?.as_str() {
                    "cloud" => InstanceType::Cloud,
                    "omni" => InstanceType::Omni,
                    other => {
                        return Err(invalid_argument(format!(
                            "option {name} expects \"cloud\" or \"omni\", got {other:?}"
                        )));
                    }
                };
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE => {
                self.keyfile = non_empty_string_option(value, name)?
            }
            OptionDatabase::Other(name) if name == OPTION_KEYFILE_JSON => {
                self.keyfile_json = non_empty_string_option(value, name)?
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_TARGET_PRINCIPAL => {
                self.impersonate_target_principal = non_empty_string_option(value, name)?
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_DELEGATES => {
                self.impersonate_delegates = comma_separated(&string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_SCOPES => {
                self.impersonate_scopes = comma_separated(&string_value(&key, value)?)
            }
            OptionDatabase::Other(name) if name == OPTION_IMPERSONATE_LIFETIME => {
                let seconds = u64_seconds_value(name, value)?;
                if seconds.is_some_and(|seconds| !(1..=43200).contains(&seconds)) {
                    return Err(invalid_argument(format!(
                        "the `{name}` option must be between 1 and 43200 seconds"
                    )));
                }
                self.impersonate_lifetime_secs = seconds;
            }
            OptionDatabase::Other(name) if name == OPTION_ACCESS_TOKEN => {
                self.access_token = non_empty_string_option(value, name)?
            }
            OptionDatabase::Other(name) if name == OPTION_QUOTA_PROJECT => {
                self.quota_project = non_empty_string_option(value, name)?
            }
            OptionDatabase::Other(name) if name == OPTION_RPC_TIMEOUT_CONNECT => {
                self.connect_timeout_secs = f64_option(value, name, F64Range::NonNegativeSeconds)?
            }
            OptionDatabase::Username | OptionDatabase::Password => {
                return Err(unsupported(format!(
                    "Spanner does not use the {} option; it authenticates with Application \
                     Default Credentials unless {OPTION_KEYFILE}, {OPTION_KEYFILE_JSON}, \
                     {OPTION_ACCESS_TOKEN} or {OPTION_IMPERSONATE_TARGET_PRINCIPAL} is set",
                    key.as_ref()
                )));
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
            OptionDatabase::Other(name) if name == OPTION_RPC_TIMEOUT_CONNECT => {
                self.connect_timeout_secs.map(|secs| secs.to_string())
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
    crate::options::string_option(value, key.as_ref())
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

/// Parse a non-negative whole number of seconds from an integer or a numeric string; a blank
/// string unsets.
fn u64_seconds_value(key: &str, value: OptionValue) -> Result<Option<u64>> {
    let seconds = match &value {
        OptionValue::Int(seconds) => u64::try_from(*seconds).ok(),
        OptionValue::String(s) if s.trim().is_empty() => return Ok(None),
        OptionValue::String(s) => s.trim().parse::<u64>().ok(),
        _ => None,
    };
    seconds.map(Some).ok_or_else(|| {
        invalid_argument(format!(
            "option {key} expects a non-negative integer number of seconds, got {}",
            shown(&value)
        ))
    })
}

fn bool_value(key: &OptionDatabase, value: OptionValue) -> Result<bool> {
    crate::options::bool_option(value, key.as_ref())
}

#[cfg(test)]
mod tests;
