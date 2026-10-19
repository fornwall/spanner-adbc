//! Credential selection and construction for a [`SpannerDatabase`].

use adbc_core::error::{Result, Status};
use google_cloud_auth::credentials::Builder as AdcCredentials;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_auth::credentials::external_account::Builder as ExternalAccountCredentials;
use google_cloud_auth::credentials::impersonated::Builder as ImpersonatedCredentials;
use google_cloud_auth::credentials::service_account::Builder as ServiceAccountCredentials;
use google_cloud_auth::credentials::user_account::Builder as UserAccountCredentials;
use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use http::{Extensions, HeaderMap};

use super::SpannerDatabase;
use crate::error::{err, from_builder, invalid_argument};
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_IMPERSONATE_TARGET_PRINCIPAL, OPTION_KEYFILE, OPTION_KEYFILE_JSON,
    OPTION_QUOTA_PROJECT,
};
use std::time::Duration;

/// The header `google-cloud-auth` sends for `with_quota_project_id`; the access-token path adds it
/// by hand.
const QUOTA_PROJECT_HEADER: &str = "x-goog-user-project";

/// Matches the default of the `google-cloud-auth` impersonated builder and gcloud's `--lifetime`.
pub(super) const DEFAULT_IMPERSONATION_LIFETIME_SECS: u64 = 3600;

/// The credential flow selected by [`SpannerDatabase::credential_choice`], in precedence order.
///
/// Separating selection from construction lets the precedence rules be tested offline. Variants
/// carry no data, so a bearer token never ends up in a `Debug`-derived type.
#[derive(Debug, PartialEq, Eq)]
enum CredentialChoice {
    /// Emulator mode, or Omni or a plaintext endpoint without a credential option. `connect`
    /// refuses emulator mode alongside any credential option, and every anonymous mode alongside
    /// a quota project.
    Anonymous,
    /// [`OPTION_ACCESS_TOKEN`]. `connect` refuses it alongside the options below.
    AccessToken,
    /// [`OPTION_IMPERSONATE_TARGET_PRINCIPAL`], using a keyfile or ADC as the source credential.
    Impersonate,
    /// [`OPTION_KEYFILE_JSON`] or [`OPTION_KEYFILE`].
    Keyfile,
    /// No credential option is set, on Cloud Spanner over TLS.
    Adc,
}

impl SpannerDatabase {
    /// The configured credential JSON: [`OPTION_KEYFILE_JSON`], else the contents of
    /// [`OPTION_KEYFILE`].
    pub(super) fn credentials_json(&self) -> Result<Option<String>> {
        if let Some(json) = &self.keyfile_json {
            Ok(Some(json.clone()))
        } else if let Some(path) = &self.keyfile {
            let json = std::fs::read_to_string(path).map_err(|e| {
                err(
                    format!("failed to read keyfile {path:?}: {e}"),
                    Status::InvalidArguments,
                )
            })?;
            Ok(Some(json))
        } else {
            Ok(None)
        }
    }

    /// The first credential option that is set. Ambient ADC does not count, so it cannot block
    /// emulator use. [`OPTION_ACCESS_TOKEN`] comes last so that
    /// [`conflicting_credential_with_access_token`](Self::conflicting_credential_with_access_token)
    /// can filter it out.
    pub(super) fn explicit_credential_option(&self) -> Option<&'static str> {
        if self.keyfile_json.is_some() {
            Some(OPTION_KEYFILE_JSON)
        } else if self.keyfile.is_some() {
            Some(OPTION_KEYFILE)
        } else if self.impersonate_target_principal.is_some() {
            Some(OPTION_IMPERSONATE_TARGET_PRINCIPAL)
        } else if self.access_token.is_some() {
            Some(OPTION_ACCESS_TOKEN)
        } else {
            None
        }
    }

    /// A keyfile or impersonation option that conflicts with [`OPTION_ACCESS_TOKEN`].
    pub(super) fn conflicting_credential_with_access_token(&self) -> Option<&'static str> {
        self.explicit_credential_option()
            .filter(|option| *option != OPTION_ACCESS_TOKEN)
    }

    /// `emulator` is the resolved mode: the option or `SPANNER_EMULATOR_HOST`.
    /// `anonymous_default` replaces ADC for Omni and plaintext endpoints.
    fn credential_choice(&self, emulator: bool, anonymous_default: bool) -> CredentialChoice {
        if emulator {
            CredentialChoice::Anonymous
        } else if self.access_token.is_some() {
            CredentialChoice::AccessToken
        } else if self.impersonate_target_principal.is_some() {
            CredentialChoice::Impersonate
        } else if self.keyfile_json.is_some() || self.keyfile.is_some() {
            CredentialChoice::Keyfile
        } else if anonymous_default {
            CredentialChoice::Anonymous
        } else {
            CredentialChoice::Adc
        }
    }

    /// Build the selected credential, or `None` to let the client resolve ADC itself.
    ///
    /// Must run inside the Tokio runtime, since the auth builders spawn token-cache tasks. No
    /// token is fetched until first use.
    pub(super) fn build_credentials(
        &self,
        emulator: bool,
        anonymous_default: bool,
        credentials_json: Option<String>,
    ) -> Result<Option<Credentials>> {
        let quota_project = self.quota_project.as_deref();
        match self.credential_choice(emulator, anonymous_default) {
            CredentialChoice::Anonymous => Ok(Some(AnonymousCredentials::new().build())),
            CredentialChoice::AccessToken => self
                .access_token
                .as_deref()
                .map(|token| build_static_token_credentials(token, quota_project))
                .transpose(),
            CredentialChoice::Impersonate => self
                .impersonate_target_principal
                .as_deref()
                .map(|target| {
                    // The quota project goes on the impersonated credential, not the source.
                    let source = match &credentials_json {
                        Some(json) => build_credentials_from_json(json, None)?,
                        None => AdcCredentials::default().build().map_err(|e| {
                            err(
                                format!(
                                    "failed to build Application Default Credentials to \
                                     impersonate {target:?}: {}",
                                    scrub_credential_error(&e)
                                ),
                                Status::InvalidArguments,
                            )
                        })?,
                    };
                    build_impersonated_credentials(
                        source,
                        target,
                        &self.impersonate_delegates,
                        &self.impersonate_scopes,
                        Duration::from_secs(
                            self.impersonate_lifetime_secs
                                .unwrap_or(DEFAULT_IMPERSONATION_LIFETIME_SECS),
                        ),
                        quota_project,
                    )
                })
                .transpose(),
            CredentialChoice::Keyfile => credentials_json
                .map(|json| build_credentials_from_json(&json, quota_project))
                .transpose(),
            // ADC is only built here when a quota project must be attached to it.
            CredentialChoice::Adc => quota_project
                .map(|project| {
                    AdcCredentials::default()
                        .with_quota_project_id(project)
                        .build()
                        .map_err(|e| {
                            err(
                                format!(
                                    "failed to build Application Default Credentials with quota \
                                     project {project:?}: {}",
                                    scrub_credential_error(&e)
                                ),
                                Status::InvalidArguments,
                            )
                        })
                })
                .transpose(),
        }
    }
}

const SUPPORTED_CREDENTIAL_TYPES: &str =
    "service_account, authorized_user, impersonated_service_account, external_account";

/// Build credentials from JSON, choosing the flow from its `"type"` field as gcloud does.
///
/// `google-cloud-auth` only dispatches on `"type"` for credentials it loads from the environment,
/// so inline JSON is dispatched here.
fn build_credentials_from_json(json: &str, quota_project: Option<&str>) -> Result<Credentials> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        err(
            format!("invalid credential JSON key: {e}"),
            Status::InvalidArguments,
        )
    })?;

    let credential_type = value
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            invalid_argument(format!(
                "credential JSON is missing a string `type` field; expected one of \
                 {SUPPORTED_CREDENTIAL_TYPES}"
            ))
        })?
        .to_owned();

    // The builders share `with_quota_project_id` but no trait.
    macro_rules! with_quota_project {
        ($builder:expr) => {{
            let builder = $builder;
            match quota_project {
                Some(project) => builder.with_quota_project_id(project),
                None => builder,
            }
        }};
    }
    let result = match credential_type.as_str() {
        "service_account" => with_quota_project!(ServiceAccountCredentials::new(value)).build(),
        "authorized_user" => with_quota_project!(UserAccountCredentials::new(value)).build(),
        "impersonated_service_account" => {
            with_quota_project!(ImpersonatedCredentials::new(value)).build()
        }
        "external_account" => with_quota_project!(ExternalAccountCredentials::new(value)).build(),
        other => {
            return Err(invalid_argument(format!(
                "unsupported credential `type` {other:?}; expected one of \
                 {SUPPORTED_CREDENTIAL_TYPES}"
            )));
        }
    };

    result.map_err(|e| {
        err(
            format!(
                "failed to build {credential_type} credentials: {}",
                scrub_credential_error(&e)
            ),
            Status::InvalidArguments,
        )
    })
}

/// Translate a client builder error. A client that cannot load ADC, for example from a missing
/// `GOOGLE_APPLICATION_CREDENTIALS` file, gets the status of the driver's own ADC builds above
/// instead of `Internal`.
pub(super) fn from_client_builder(
    error: google_cloud_gax::client_builder::Error,
) -> adbc_core::error::Error {
    if !error.is_default_credentials() {
        return from_builder(error);
    }
    let detail = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<google_cloud_auth::build_errors::Error>())
        .map_or("the credentials could not be built", scrub_credential_error);
    err(
        format!("failed to build Application Default Credentials: {detail}"),
        Status::InvalidArguments,
    )
}

/// Describe a credential-builder error with a fixed phrase.
///
/// The error's `Display` can include `serde_json` fragments of the credential JSON, such as a
/// private key or refresh token, so it must never reach an ADBC error message.
fn scrub_credential_error(error: &google_cloud_auth::build_errors::Error) -> &'static str {
    if error.is_missing_field() {
        "a required field is missing or has the wrong type"
    } else if error.is_parsing() {
        "the credential JSON could not be parsed"
    } else if error.is_unknown_type() {
        "the credential type is unknown or invalid"
    } else if error.is_not_supported() {
        "the credential type is not supported for this use"
    } else if error.is_loading() {
        "the credentials could not be loaded"
    } else {
        "the credentials could not be built"
    }
}

/// Wrap `source` so it mints short-lived tokens for `target_principal`. Empty `delegates` or
/// `scopes` keep the builder's defaults.
fn build_impersonated_credentials(
    source: Credentials,
    target_principal: &str,
    delegates: &[String],
    scopes: &[String],
    lifetime: Duration,
    quota_project: Option<&str>,
) -> Result<Credentials> {
    let mut builder = ImpersonatedCredentials::from_source_credentials(source)
        .with_target_principal(target_principal)
        .with_lifetime(lifetime);
    if !delegates.is_empty() {
        builder = builder.with_delegates(delegates.iter().cloned());
    }
    if !scopes.is_empty() {
        builder = builder.with_scopes(scopes.iter().cloned());
    }
    // The auth crate prefers this over any quota project on the source.
    if let Some(project) = quota_project {
        builder = builder.with_quota_project_id(project);
    }
    builder.build().map_err(|e| {
        err(
            format!(
                "failed to build impersonated credentials for {target_principal:?}: {}",
                scrub_credential_error(&e)
            ),
            Status::InvalidArguments,
        )
    })
}

/// Credentials for a fixed bearer token, which the pinned auth crate has no builder for.
#[derive(Debug)]
struct StaticTokenCredentials {
    headers: HeaderMap,
    /// Constant, since the token never changes.
    entity_tag: EntityTag,
}

impl CredentialsProvider for StaticTokenCredentials {
    async fn headers(
        &self,
        extensions: Extensions,
    ) -> std::result::Result<
        CacheableResource<HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        match extensions.get::<EntityTag>() {
            Some(tag) if self.entity_tag.eq(tag) => Ok(CacheableResource::NotModified),
            _ => Ok(CacheableResource::New {
                data: self.headers.clone(),
                entity_tag: self.entity_tag.clone(),
            }),
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        // The default `googleapis.com` universe.
        None
    }
}

/// Build credentials that send a fixed bearer token and optional quota project. An invalid token
/// is rejected here, without echoing it in the error.
fn build_static_token_credentials(token: &str, quota_project: Option<&str>) -> Result<Credentials> {
    let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
        invalid_argument(format!(
            "the `{OPTION_ACCESS_TOKEN}` option contains characters that are not valid in an HTTP \
             Authorization header value"
        ))
    })?;
    // Keeps the token out of `Debug` output and header logging.
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, value);
    if let Some(project) = quota_project {
        let project = HeaderValue::from_str(project).map_err(|_| {
            invalid_argument(format!(
                "the `{OPTION_QUOTA_PROJECT}` option contains characters that are not valid in an \
                 HTTP header value"
            ))
        })?;
        headers.insert(HeaderName::from_static(QUOTA_PROJECT_HEADER), project);
    }
    Ok(Credentials::from(StaticTokenCredentials {
        headers,
        entity_tag: EntityTag::new(),
    }))
}

#[cfg(test)]
mod tests;
