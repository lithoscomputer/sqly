use std::convert::Infallible;
use std::error::Error as StdError;
use std::{fmt, result};

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sqlx::error::ErrorKind as DriverErrorKind;

/// The result of a sqly operation.
pub type Result<T> = result::Result<T, Error>;
/// A diagnostic cause. Formatting omits its contents; `Error::source` retains
/// it.
pub struct Cause(pub(crate) Box<dyn StdError + Send + Sync>);
impl fmt::Debug for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Cause(<redacted>)")
    }
}
impl Cause {
    fn new(source: impl StdError + Send + Sync + 'static) -> Self {
        Self(Box::new(source))
    }
}
/// Portable constraint classifications.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConstraintKind {
    Unique,
    ForeignKey,
    NotNull,
    Check,
}
/// Why a stored value cannot be decoded.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DecodeKind {
    MissingColumn,
    DuplicateColumn,
    UnexpectedNull,
    TypeMismatch,
    InvalidValue,
    OutOfRange,
}
/// Typed errors. Diagnostic sources can contain server data and must not be
/// exposed directly to clients.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    Configuration {
        reason: &'static str,
    },
    UnsupportedBackend {
        backend: &'static str,
    },
    BindCount {
        expected: Option<usize>,
        actual:   usize,
    },
    Encode {
        parameter: Option<usize>,
        source:    Cause,
    },
    Decode {
        column: Option<String>,
        kind:   DecodeKind,
        source: Option<Cause>,
    },
    RowNotFound,
    InvalidLock {
        reason: &'static str,
    },
    /// A required row lock matched no row.
    LockNotFound,
    NonUniqueLock,
    TransactionAborted,
    #[cfg(feature = "ambient")]
    NoActiveWriteScope,
    #[cfg(feature = "ambient")]
    NestedWriteScope,
    #[cfg(feature = "ambient")]
    ScopeDatabaseMismatch,
    #[cfg(feature = "ambient")]
    ActiveStream,
    /// The server did not confirm whether COMMIT completed.
    CommitUnknown {
        source: Cause,
    },
    Constraint {
        kind:   ConstraintKind,
        source: Cause,
    },
    Contention {
        source: Cause,
    },
    InvalidSql {
        source: Cause,
    },
    PoolClosed,
    PoolTimedOut,
    DatabaseLost,
    Connection {
        source: Cause,
    },
    Database {
        source: Cause,
    },
}
impl Error {
    pub fn encode(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Encode {
            parameter: None,
            source:    Cause::new(source),
        }
    }
    pub fn decode_value(source: impl StdError + Send + Sync + 'static) -> Self {
        Self::Decode {
            column: None,
            kind:   DecodeKind::InvalidValue,
            source: Some(Cause::new(source)),
        }
    }
    pub fn decode(
        column: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::decode_value(source).at_column(column.into())
    }
    pub fn is_unique_violation(&self) -> bool {
        matches!(self, Self::Constraint {
            kind: ConstraintKind::Unique,
            ..
        })
    }
    /// The constraint name, when supplied by the backend.
    pub fn constraint_name(&self) -> Option<&str> {
        if let Self::Constraint { source, .. } = self {
            source
                .0
                .downcast_ref::<sqlx::Error>()?
                .as_database_error()?
                .constraint()
        } else {
            None
        }
    }
    pub(crate) fn config(reason: &'static str) -> Self {
        Self::Configuration { reason }
    }
    pub(crate) fn invalid(kind: DecodeKind) -> Self {
        Self::Decode {
            column: None,
            kind,
            source: None,
        }
    }
    pub(crate) fn at_column(self, column: String) -> Self {
        match self {
            Self::Decode { kind, source, .. } => Self::Decode {
                column: Some(column),
                kind,
                source,
            },
            other => Self::decode(column, other),
        }
    }
    pub(crate) fn at_parameter(self, parameter: usize) -> Self {
        match self {
            Self::Encode { source, .. } => Self::Encode {
                parameter: Some(parameter),
                source,
            },
            other => Self::Encode {
                parameter: Some(parameter),
                source:    Cause::new(other),
            },
        }
    }
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) fn driver(error: sqlx::Error) -> Self {
        match error {
            sqlx::Error::PoolClosed => Self::PoolClosed,
            sqlx::Error::PoolTimedOut => Self::PoolTimedOut,
            sqlx::Error::RowNotFound => Self::RowNotFound,
            sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::Protocol(_) => {
                Self::Connection {
                    source: Cause::new(error),
                }
            }
            sqlx::Error::Database(ref db) => {
                let kind = match db.kind() {
                    DriverErrorKind::UniqueViolation => Some(ConstraintKind::Unique),
                    DriverErrorKind::ForeignKeyViolation => Some(ConstraintKind::ForeignKey),
                    DriverErrorKind::NotNullViolation => Some(ConstraintKind::NotNull),
                    DriverErrorKind::CheckViolation => Some(ConstraintKind::Check),
                    _ => None,
                };
                if let Some(kind) = kind {
                    return Self::Constraint {
                        kind,
                        source: Cause::new(error),
                    };
                }
                let code = db.code();
                if code.as_deref().is_some_and(|c| {
                    c.starts_with("40") || matches!(c, "55P03" | "5" | "6" | "261" | "262" | "517")
                }) {
                    Self::Contention {
                        source: Cause::new(error),
                    }
                } else if code
                    .as_deref()
                    .is_some_and(|c| c.starts_with("42") || c == "1")
                {
                    Self::InvalidSql {
                        source: Cause::new(error),
                    }
                } else {
                    Self::Database {
                        source: Cause::new(error),
                    }
                }
            }
            _ => Self::Database {
                source: Cause::new(error),
            },
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration { reason } => {
                write!(f, "invalid connection configuration: {reason}")
            }
            Self::UnsupportedBackend { backend } => {
                write!(f, "backend feature is disabled: {backend}")
            }
            Self::BindCount { expected, actual } => write!(
                f,
                "binding count mismatch (expected {expected:?}, received {actual})"
            ),
            Self::Encode { parameter, .. } => {
                write!(f, "parameter encoding failed at {parameter:?}")
            }
            Self::Decode { kind, .. } => write!(f, "row decoding failed: {kind:?}"),
            Self::RowNotFound => f.write_str("query returned no rows"),
            Self::InvalidLock { reason } => write!(f, "invalid lock selector: {reason}"),
            Self::LockNotFound => f.write_str("required lock row was not found"),
            Self::NonUniqueLock => f.write_str("lock selector matched multiple rows"),
            #[cfg(feature = "ambient")]
            Self::NoActiveWriteScope => f.write_str("an active write scope is required"),
            #[cfg(feature = "ambient")]
            Self::NestedWriteScope => f.write_str("write scopes cannot be nested"),
            #[cfg(feature = "ambient")]
            Self::ScopeDatabaseMismatch => {
                f.write_str("scope database or task identity does not match")
            }
            #[cfg(feature = "ambient")]
            Self::ActiveStream => f.write_str("a stream retains the scope connection"),
            Self::TransactionAborted => f.write_str("transaction must be rolled back"),
            Self::CommitUnknown { .. } => f.write_str("commit outcome is unknown"),
            Self::Constraint { kind, .. } => write!(f, "constraint violation: {kind:?}"),
            Self::Contention { .. } => f.write_str("database contention"),
            Self::InvalidSql { .. } => f.write_str("invalid SQL"),
            Self::PoolClosed => f.write_str("database pool is closed"),
            Self::PoolTimedOut => f.write_str("database acquisition timed out"),
            Self::DatabaseLost => f.write_str("in-memory database keeper was lost"),
            Self::Connection { .. } => f.write_str("database connection failed"),
            Self::Database { .. } => f.write_str("database operation failed"),
        }
    }
}
impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Encode { source, .. }
            | Self::Constraint { source, .. }
            | Self::Contention { source }
            | Self::InvalidSql { source }
            | Self::CommitUnknown { source }
            | Self::Connection { source }
            | Self::Database { source }
            | Self::Decode {
                source: Some(source),
                ..
            } => Some(source.0.as_ref()),
            _ => None,
        }
    }
}
impl From<Infallible> for Error {
    fn from(value: Infallible) -> Self {
        match value {}
    }
}
