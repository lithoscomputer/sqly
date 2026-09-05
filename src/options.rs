use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use percent_encoding::percent_decode_str;
use url::{Url, form_urlencoded};

use crate::{Error, Result};

/// Connection configuration without implicit environment or credential-file
/// input.
#[derive(Clone)]
pub struct ConnectOptions {
    pub(crate) inner: Options,
}
#[derive(Clone)]
pub(crate) enum Options {
    Sqlite(SqliteOptions),
    Postgres(PostgresOptions),
}
/// SQLite file and connection settings. File creation and WAL are opt-in.
#[derive(Clone)]
#[must_use]
pub struct SqliteOptions {
    pub(crate) filename:     PathBuf,
    pub(crate) memory:       bool,
    pub(crate) create:       bool,
    pub(crate) read_only:    bool,
    pub(crate) wal:          bool,
    pub(crate) busy_timeout: Duration,
}
impl ConnectOptions {
    pub fn as_sqlite(&self) -> Option<&SqliteOptions> {
        match &self.inner {
            Options::Sqlite(options) => Some(options),
            Options::Postgres(_) => None,
        }
    }
    pub fn as_postgres(&self) -> Option<&PostgresOptions> {
        match &self.inner {
            Options::Postgres(options) => Some(options),
            Options::Sqlite(_) => None,
        }
    }
}
impl SqliteOptions {
    /// The application-owned file path, or None for managed in-memory storage.
    /// Applications can use it to apply their own parent-directory policy.
    pub fn filename(&self) -> Option<&Path> {
        (!self.memory).then_some(self.filename.as_path())
    }

    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            filename:     path.as_ref().to_owned(),
            memory:       false,
            create:       false,
            read_only:    false,
            wal:          false,
            busy_timeout: Duration::from_secs(5),
        }
    }
    pub fn in_memory() -> Self {
        Self {
            memory: true,
            ..Self::new(":memory:")
        }
    }
    pub fn create_if_missing(mut self, enabled: bool) -> Self {
        self.create = enabled;
        self
    }
    pub fn read_only(mut self, enabled: bool) -> Self {
        self.read_only = enabled;
        self
    }
    pub fn wal(mut self, enabled: bool) -> Self {
        self.wal = enabled;
        self
    }
    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = timeout;
        self
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.filename.as_os_str().is_empty() {
            return Err(Error::config("SQLite filename is required"));
        }
        if !self.memory
            && (self.filename == Path::new(":memory:")
                || self.filename.to_string_lossy().starts_with("file:"))
        {
            return Err(Error::config(
                "use in_memory for memory databases; file URIs are reserved",
            ));
        }
        if (self.memory && (self.read_only || self.wal || self.create))
            || (self.read_only && (self.create || self.wal))
        {
            return Err(Error::config("conflicting SQLite options"));
        }
        if self.busy_timeout.as_millis() > i32::MAX as u128 {
            return Err(Error::config("SQLite busy timeout is too large"));
        }
        Ok(())
    }
}
/// TLS policy. Server identity verification is the default.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlsMode {
    #[default]
    VerifyFull,
    Disable,
    /// Try plaintext first; allow TLS when the server requires it.
    Allow,
    /// Prefer TLS but permit plaintext fallback.
    Prefer,
    /// Require encryption without verifying server identity.
    Require,
    /// Verify the certificate authority without checking the host name.
    VerifyCa,
}
/// PostgreSQL configuration. Host, database, and username are explicit.
/// A host beginning with `/` names a Unix-socket directory.
#[derive(Clone)]
#[must_use]
pub struct PostgresOptions {
    pub(crate) host:             String,
    pub(crate) port:             u16,
    pub(crate) database:         String,
    pub(crate) username:         String,
    pub(crate) password:         String,
    pub(crate) tls:              TlsMode,
    pub(crate) root_cert:        Option<PathBuf>,
    pub(crate) application_name: String,
}
impl PostgresOptions {
    pub fn new(
        host: impl Into<String>,
        database: impl Into<String>,
        username: impl Into<String>,
    ) -> Self {
        Self {
            host:             host.into(),
            port:             5432,
            database:         database.into(),
            username:         username.into(),
            password:         String::new(),
            tls:              TlsMode::VerifyFull,
            root_cert:        None,
            application_name: "sqly".into(),
        }
    }
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }
    pub fn tls_mode(mut self, mode: TlsMode) -> Self {
        self.tls = mode;
        self
    }
    pub fn root_certificate(mut self, path: impl AsRef<Path>) -> Self {
        self.root_cert = Some(path.as_ref().to_owned());
        self
    }
    pub fn application_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = name.into();
        self
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.host.is_empty()
            || self.database.is_empty()
            || self.username.is_empty()
            || self.port == 0
        {
            return Err(Error::config(
                "PostgreSQL requires a host or socket directory, nonzero port, database, and username",
            ));
        }
        Ok(())
    }
}
macro_rules! redacted_debug {
    ($($ty:ty),+ $(,)?) => { $(impl fmt::Debug for $ty {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(concat!(stringify!($ty), "(<redacted>)")) }
    })+ };
}
redacted_debug!(ConnectOptions, SqliteOptions, PostgresOptions);
impl From<SqliteOptions> for ConnectOptions {
    fn from(v: SqliteOptions) -> Self {
        Self {
            inner: Options::Sqlite(v),
        }
    }
}
impl From<PostgresOptions> for ConnectOptions {
    fn from(v: PostgresOptions) -> Self {
        Self {
            inner: Options::Postgres(v),
        }
    }
}
fn decoded(text: &str) -> Result<String> {
    percent_decode_str(text)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| Error::config("URL text must be UTF-8"))
}
impl FromStr for ConnectOptions {
    type Err = Error;
    /// SQLite URLs support only `mode=ro|rw|rwc|memory`. PostgreSQL URLs
    /// support `sslmode=verify-full|verify-ca|require|prefer|allow|disable`,
    /// `sslrootcert`, and
    /// `application_name`. Unknown and duplicate query options fail.
    /// Credentials are never echoed.
    fn from_str(input: &str) -> Result<Self> {
        if let Some(input) = input.strip_prefix("sqlite:") {
            if input.contains('#') {
                return Err(Error::config("URL fragments are unsupported"));
            }
            let (path, query) = input.split_once('?').unwrap_or((input, ""));
            let path = decoded(path.strip_prefix("//").unwrap_or(path))?;
            let mut options = if path == ":memory:" {
                SqliteOptions::in_memory()
            } else {
                SqliteOptions::new(path)
            };
            let mut seen = false;
            for (key, value) in form_urlencoded::parse(query.as_bytes()) {
                if key != "mode" || seen {
                    return Err(Error::config("unknown or duplicate SQLite URL option"));
                }
                seen = true;
                match value.as_ref() {
                    "ro" => options.read_only = true,
                    "rw" => {}
                    "rwc" => options.create = true,
                    "memory" => options.memory = true,
                    _ => return Err(Error::config("invalid SQLite mode")),
                }
            }
            options.validate()?;
            return Ok(options.into());
        }
        let url = Url::parse(input).map_err(|_| Error::config("invalid database URL"))?;
        if !matches!(url.scheme(), "postgres" | "postgresql") {
            return Err(Error::config("unsupported URL scheme"));
        }
        if url.fragment().is_some() {
            return Err(Error::config("URL fragments are unsupported"));
        }
        let host = url
            .host_str()
            .ok_or_else(|| Error::config("PostgreSQL host is required"))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let mut options = PostgresOptions::new(
            decoded(host)?,
            decoded(url.path().trim_start_matches('/'))?,
            decoded(url.username())?,
        )
        .port(url.port().unwrap_or(5432))
        .password(decoded(url.password().unwrap_or(""))?);
        let mut seen = HashSet::new();
        for (key, value) in url.query_pairs() {
            if !seen.insert(key.clone()) {
                return Err(Error::config("duplicate PostgreSQL URL option"));
            }
            match key.as_ref() {
                "sslmode" => {
                    options.tls = match value.as_ref() {
                        "verify-full" => TlsMode::VerifyFull,
                        "disable" => TlsMode::Disable,
                        "allow" => TlsMode::Allow,
                        "prefer" => TlsMode::Prefer,
                        "require" => TlsMode::Require,
                        "verify-ca" => TlsMode::VerifyCa,
                        _ => return Err(Error::config("unsupported TLS mode")),
                    }
                }
                "sslrootcert" => options.root_cert = Some(PathBuf::from(value.as_ref())),
                "application_name" => options.application_name = value.into_owned(),
                _ => return Err(Error::config("unknown PostgreSQL URL option")),
            }
        }
        options.validate()?;
        Ok(options.into())
    }
}
impl TryFrom<&str> for ConnectOptions {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self> {
        s.parse()
    }
}

impl TryFrom<String> for ConnectOptions {
    type Error = Error;
    fn try_from(input: String) -> Result<Self> {
        input.parse()
    }
}
impl TryFrom<&String> for ConnectOptions {
    type Error = Error;
    fn try_from(input: &String) -> Result<Self> {
        input.parse()
    }
}
