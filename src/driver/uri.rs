//! Parse `spanner://` connection URIs and apply their database options atomically.

use adbc_core::Optionable;
use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};

use super::SpannerDatabase;
use crate::error::invalid_argument;
use crate::{
    OPTION_ACCESS_TOKEN, OPTION_EMULATOR, OPTION_ENDPOINT, OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_LIFETIME, OPTION_IMPERSONATE_SCOPES, OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_KEYFILE, OPTION_KEYFILE_JSON, OPTION_QUOTA_PROJECT,
};

impl SpannerDatabase {
    /// Validate before applying any fields; keep the accepted URI verbatim for `get_option`.
    /// Options apply in path, authority, query order. Fields absent from the URI are unchanged,
    /// and subsequent explicit options override the URI's values.
    pub(super) fn set_uri_option(&mut self, value: String) -> Result<()> {
        let remainder = connection_uri_remainder(&value).ok_or_else(|| {
            // Never quote an invalid URI: it may contain credentials we have not validated.
            invalid_argument(
                "the `uri` option requires a `spanner://` connection URI; use \
                 `spanner:///projects/<project>/instances/<instance>/databases/<database>` \
                 (three slashes when no endpoint host is intended)",
            )
        })?;
        let parsed = parse_connection_uri(remainder)?;
        // Validate values against a scratch database before touching the real configuration.
        let mut scratch = SpannerDatabase::new(self.runtime.clone());
        for (key, value) in &parsed.params {
            scratch.set_option(
                OptionDatabase::Other(key.clone()),
                OptionValue::String(value.clone()),
            )?;
        }

        self.database = Some(parsed.database);
        if let Some(endpoint) = parsed.endpoint {
            self.endpoint = Some(endpoint);
        }
        for (key, value) in parsed.params {
            // These identical calls succeeded on the scratch database.
            self.set_option(OptionDatabase::Other(key), OptionValue::String(value))?;
        }
        self.uri = Some(value);
        Ok(())
    }
}

/// Supply the transport scheme for a bare emulator `host:port` address.
pub(crate) fn ensure_scheme(host: &str) -> String {
    if host.starts_with("http://") || host.starts_with("https://") {
        host.to_string()
    } else {
        format!("http://{host}")
    }
}

/// Database options allowed in URI query parameters; the URI path supplies the database.
const URI_QUERY_OPTIONS: [&str; 8] = [
    OPTION_ENDPOINT,
    OPTION_EMULATOR,
    OPTION_KEYFILE,
    OPTION_IMPERSONATE_TARGET_PRINCIPAL,
    OPTION_IMPERSONATE_DELEGATES,
    OPTION_IMPERSONATE_SCOPES,
    OPTION_IMPERSONATE_LIFETIME,
    OPTION_QUOTA_PROJECT,
];

/// Secret values must use dedicated, write-only options, since URIs are commonly logged.
const URI_SECRET_OPTIONS: [&str; 2] = [OPTION_KEYFILE_JSON, OPTION_ACCESS_TOKEN];

/// URI schemes are ASCII case-insensitive (RFC 3986).
fn connection_uri_remainder(value: &str) -> Option<&str> {
    const SCHEME: &str = "spanner:";
    value
        .get(..SCHEME.len())
        .filter(|prefix| prefix.eq_ignore_ascii_case(SCHEME))
        .map(|_| &value[SCHEME.len()..])
}

struct ParsedConnectionUri {
    database: String,
    endpoint: Option<String>,
    params: Vec<(String, String)>,
}

/// Parse the portion after `spanner:`. Require `//`, a full database path and known query keys.
/// Empty authority means no endpoint; query parameters retain their source order.
fn parse_connection_uri(remainder: &str) -> Result<ParsedConnectionUri> {
    if remainder.contains('#') {
        return Err(invalid_argument(
            "connection URI must not carry a #fragment",
        ));
    }
    let (before_query, query) = remainder.split_once('?').unwrap_or((remainder, ""));
    let after_authority = before_query.strip_prefix("//").ok_or_else(|| {
        invalid_argument(
            "the `uri` option requires two slashes after `spanner:`; use \
             `spanner:///projects/<project>/instances/<instance>/databases/<database>` \
             (three slashes when no endpoint host is intended)",
        )
    })?;
    let (authority, path) = after_authority
        .split_once('/')
        .unwrap_or((after_authority, ""));
    if authority.contains('@') {
        return Err(invalid_argument(
            "connection URI userinfo is not supported; pass credentials through the \
             `spanner.auth.*` database options instead",
        ));
    }
    let endpoint = (!authority.is_empty()).then(|| authority.to_owned());

    let database = match path.split('/').collect::<Vec<_>>().as_slice() {
        ["projects", p, "instances", i, "databases", d]
            if !p.is_empty() && !i.is_empty() && !d.is_empty() =>
        {
            path.to_owned()
        }
        _ => {
            return Err(invalid_argument(
                "connection URI database path must be \
                 projects/<project>/instances/<instance>/databases/<database>; \
                 `spanner://projects/...` treats `projects` as a host authority, so use \
                 `spanner:///projects/...` (three slashes) when no endpoint host is intended",
            ));
        }
    };

    let mut params = Vec::new();
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(key)?;
        if URI_SECRET_OPTIONS.contains(&key.as_str()) {
            return Err(invalid_argument(format!(
                "connection URI query parameter {key:?} contains a secret; \
                 set the `{key}` database option directly instead"
            )));
        }
        if !URI_QUERY_OPTIONS.contains(&key.as_str()) {
            return Err(invalid_argument(format!(
                "unknown connection URI query parameter {key:?}; supported parameters: {}",
                URI_QUERY_OPTIONS.join(", ")
            )));
        }
        params.push((key, percent_decode(value)?));
    }

    Ok(ParsedConnectionUri {
        database,
        endpoint,
        params,
    })
}

/// Decode RFC 3986 `%XX` escapes, preserving literal `+`. Reject malformed escapes and UTF-8.
fn percent_decode(s: &str) -> Result<String> {
    if !s.contains('%') {
        return Ok(s.to_owned());
    }
    let malformed = || invalid_argument("malformed percent-encoding in connection URI component");
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3).ok_or_else(malformed)?;
            // `from_str_radix` accepts a leading sign, which is not valid percent-encoding.
            if !hex.iter().all(u8::is_ascii_hexdigit) {
                return Err(malformed());
            }
            let hex = std::str::from_utf8(hex).map_err(|_| malformed())?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| malformed())?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out)
        .map_err(|_| invalid_argument("connection URI component percent-decodes to invalid UTF-8"))
}

#[cfg(test)]
mod tests;
