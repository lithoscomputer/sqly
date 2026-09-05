//! Namespaced, transactional migrations and explicit legacy-ledger adoption.
//! Scripts are trusted application SQL. They must not contain transaction
//! control or nontransactional DDL. Quiesce old runners before ledger adoption.
//!
//! ```no_run
//! # async fn example(db: &sqly::Database) -> sqly::migrate::MigrationResult<()> {
//! use sqly::migrate::{Migration, Migrator};
//! let migrations = [Migration::new(
//!     1,
//!     "create widgets",
//!     "CREATE TABLE widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL)",
//! )];
//! Migrator::try_new("widgets", &migrations)?.run(db).await?;
//! # Ok(()) }
//! ```
use std::error::Error as StdError;
use std::{fmt, result};

use sha2::{Digest as _, Sha256};
use sqlx::{AssertSqlSafe, SqlSafeStr as _};

use crate::lock::identifier;
use crate::query::Mode;
use crate::{Database, Error, FromRow, Row, Sql, Transaction};

/// One application-owned migration. Checksums cover both dialects in every
/// build.
#[derive(Clone)]
pub struct Migration {
    version:     i64,
    description: String,
    sql:         Sql,
    checksum:    String,
}
impl Migration {
    pub fn new(version: i64, description: impl Into<String>, sql: impl Into<Sql>) -> Self {
        let description = description.into();
        let sql = sql.into();
        let mut hash = Sha256::new();
        hash.update(version.to_string());
        hash.update(b"\0");
        hash.update(&description);
        hash.update(b"\0");
        hash.update(sql.sqlite);
        hash.update(b"\0");
        hash.update(sql.postgres);
        Self {
            version,
            description,
            sql,
            checksum: format!("{:x}", hash.finalize()),
        }
    }
    pub fn version(&self) -> i64 {
        self.version
    }
    pub fn description(&self) -> &str {
        &self.description
    }
    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}
impl fmt::Debug for Migration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Migration")
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}
/// A legacy table with `version BIGINT`, `description TEXT`, `checksum TEXT`,
/// and `applied_at TEXT` columns. A missing table means a fresh database.
/// Existing rows must be a complete, matching prefix of this migration set.
#[derive(Clone)]
pub struct LegacyLedger {
    table: String,
}
impl LegacyLedger {
    pub fn try_new(table: impl Into<String>) -> MigrationResult<Self> {
        let table = table.into();
        identifier(&table).map_err(|_| MigrationError::Configuration {
            reason: "invalid legacy table identifier",
        })?;
        if table == "_sqly_migrations" {
            return Err(MigrationError::Configuration {
                reason: "legacy and canonical ledgers must differ",
            });
        }
        Ok(Self { table })
    }
}
impl fmt::Debug for LegacyLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LegacyLedger(<redacted>)")
    }
}
/// Migration runner. Construction validates definitions before database I/O.
#[derive(Debug)]
pub struct Migrator<'a> {
    namespace:  String,
    migrations: &'a [Migration],
    target:     i64,
    minimum:    i64,
    legacy:     Option<LegacyLedger>,
}
impl<'a> Migrator<'a> {
    pub fn try_new(
        namespace: impl Into<String>,
        migrations: &'a [Migration],
    ) -> MigrationResult<Self> {
        let namespace = namespace.into();
        if namespace.is_empty() || namespace.len() > 255 || namespace.contains('\0') {
            return Err(MigrationError::Configuration {
                reason: "namespace must contain 1 to 255 bytes without NUL",
            });
        }
        if migrations.first().is_some_and(|m| m.version <= 0)
            || migrations
                .windows(2)
                .any(|pair| pair[0].version >= pair[1].version)
        {
            return Err(MigrationError::Unordered);
        }
        if migrations.iter().any(|m| m.description.contains('\0')) {
            return Err(MigrationError::Configuration {
                reason: "migration descriptions must not contain NUL",
            });
        }
        Ok(Self {
            namespace,
            migrations,
            target: migrations.last().map_or(0, |m| m.version),
            minimum: 0,
            legacy: None,
        })
    }
    /// Limit the target and reject nonempty histories below the minimum.
    /// Zero is reserved for a fresh database. Target must name a supplied
    /// version.
    pub fn with_compatibility(
        mut self,
        target: i64,
        minimum_upgradeable: i64,
    ) -> MigrationResult<Self> {
        if target < 0
            || minimum_upgradeable < 0
            || minimum_upgradeable > target
            || (target != 0 && !self.migrations.iter().any(|m| m.version == target))
        {
            return Err(MigrationError::Configuration {
                reason: "invalid target or minimum upgradeable version",
            });
        }
        self.target = target;
        self.minimum = minimum_upgradeable;
        Ok(self)
    }
    /// Validate and adopt the legacy ledger within the same transaction as the
    /// pending batch. Matching repeated adoption is idempotent.
    #[must_use]
    pub fn adopt_legacy(mut self, legacy: LegacyLedger) -> Self {
        self.legacy = Some(legacy);
        self
    }
    pub async fn run(&self, db: &Database) -> MigrationResult<()> {
        let mut tx = db.begin_write().await?;
        let result = self.apply(&mut tx).await;
        match result {
            Ok(()) => {
                tx.commit().await?;
                Ok(())
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
    async fn apply(&self, tx: &mut Transaction) -> MigrationResult<()> {
        if tx.is_postgres() {
            // Signed first 8 bytes of SHA-256. Prefixes separate ledger and
            // namespace keys from application advisory-lock conventions.
            for key in [
                advisory_key(b"sqly:migrations:ledger:v1", ""),
                advisory_key(b"sqly:migrations:namespace:v1", &self.namespace),
            ] {
                tx.query("SELECT pg_advisory_xact_lock($1)")
                    .bind(key)
                    .execute()
                    .await?;
            }
        }
        tx.batch(Sql::from("CREATE TABLE IF NOT EXISTS _sqly_migrations (namespace TEXT NOT NULL, version BIGINT NOT NULL, description TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL, PRIMARY KEY (namespace, version))")).await?;
        let mut rows = tx.query_as::<LedgerRow>("SELECT version, description, checksum, applied_at FROM _sqly_migrations WHERE namespace = $1 ORDER BY version").bind(self.namespace.as_str()).fetch_all().await?;
        self.validate(&rows)?;
        if let Some(legacy) = &self.legacy {
            let table = if tx.is_postgres() {
                format!("\"{}\"", legacy.table)
            } else {
                legacy.table.clone()
            };
            let exists: bool = tx.query(Sql::dialects("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = $1) AS present", "SELECT to_regclass($1) IS NOT NULL AS present"))
                .bind(table).fetch_one().await?.try_get("present")?;
            if exists {
                let sql = AssertSqlSafe(format!("SELECT version, description, checksum, applied_at FROM \"{}\" ORDER BY version", legacy.table)).into_sql_str();
                let source = tx
                    .run_statement(sql, Vec::new(), Mode::All)
                    .await?
                    .rows
                    .iter()
                    .map(LedgerRow::from_row)
                    .collect::<crate::Result<Vec<_>>>()?;
                self.validate(&source)?;
                for (index, row) in source.into_iter().enumerate() {
                    if let Some(canonical) = rows.get(index) {
                        if row != *canonical {
                            return Err(MigrationError::LegacyConflict {
                                version: row.version,
                            });
                        }
                    } else {
                        tx.query("INSERT INTO _sqly_migrations (namespace, version, description, checksum, applied_at) VALUES ($1, $2, $3, $4, $5)")
                            .bind(self.namespace.as_str()).bind(row.version).bind(row.description.as_str()).bind(row.checksum.as_str()).bind(row.applied_at.as_str()).execute().await?;
                        rows.push(row);
                    }
                }
            }
        }
        let applied = rows.last().map_or(0, |row| row.version);
        if applied > 0 && applied < self.minimum {
            return Err(MigrationError::TooOld {
                version: applied,
                minimum: self.minimum,
            });
        }
        for migration in self
            .migrations
            .iter()
            .skip(rows.len())
            .take_while(|m| m.version <= self.target)
        {
            tx.batch(migration.sql).await?;
            tx.query("INSERT INTO _sqly_migrations (namespace, version, description, checksum, applied_at) VALUES ($1, $2, $3, $4, CAST(CURRENT_TIMESTAMP AS TEXT))")
                .bind(self.namespace.as_str()).bind(migration.version).bind(migration.description.as_str()).bind(migration.checksum.as_str()).execute().await?;
        }
        Ok(())
    }
    fn validate(&self, rows: &[LedgerRow]) -> MigrationResult<()> {
        let applied = rows.last().map_or(0, |r| r.version);
        if applied > self.target {
            return Err(MigrationError::Newer {
                version: applied,
                target:  self.target,
            });
        }
        for (index, row) in rows.iter().enumerate() {
            let migration = self
                .migrations
                .get(index)
                .ok_or(MigrationError::UnknownVersion {
                    version: row.version,
                })?;
            if row.version != migration.version {
                return Err(MigrationError::LedgerVersionMismatch {
                    expected: migration.version,
                    found:    row.version,
                });
            }
            if row.description != migration.description {
                return Err(MigrationError::DescriptionMismatch {
                    version: row.version,
                });
            }
            if row.checksum != migration.checksum {
                return Err(MigrationError::ChecksumMismatch {
                    version: row.version,
                });
            }
            if row.applied_at.is_empty() {
                return Err(MigrationError::Configuration {
                    reason: "ledger applied_at must not be empty",
                });
            }
        }
        Ok(())
    }
}
fn advisory_key(prefix: &[u8], namespace: &str) -> i64 {
    let mut hash = Sha256::new();
    hash.update(prefix);
    hash.update(b"\0");
    hash.update(namespace);
    let bytes = hash.finalize();
    i64::from_be_bytes(
        bytes[..8]
            .try_into()
            .expect("SHA-256 contains at least 8 bytes"),
    )
}
#[derive(PartialEq)]
struct LedgerRow {
    version:     i64,
    description: String,
    checksum:    String,
    applied_at:  String,
}
impl FromRow for LedgerRow {
    fn from_row(row: &Row) -> crate::Result<Self> {
        Ok(Self {
            version:     row.try_get("version")?,
            description: row.try_get("description")?,
            checksum:    row.try_get("checksum")?,
            applied_at:  row.try_get("applied_at")?,
        })
    }
}
pub type MigrationResult<T> = result::Result<T, MigrationError>;
/// Branch-oriented migration errors. Database causes preserve sqly's redaction.
#[derive(Debug)]
#[non_exhaustive]
pub enum MigrationError {
    Configuration { reason: &'static str },
    Unordered,
    Newer { version: i64, target: i64 },
    TooOld { version: i64, minimum: i64 },
    UnknownVersion { version: i64 },
    LedgerVersionMismatch { expected: i64, found: i64 },
    ChecksumMismatch { version: i64 },
    DescriptionMismatch { version: i64 },
    LegacyConflict { version: i64 },
    Database(Error),
}
impl From<Error> for MigrationError {
    fn from(error: Error) -> Self {
        Self::Database(error)
    }
}
impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration { reason } => {
                write!(f, "invalid migration configuration: {reason}")
            }
            Self::Unordered => {
                f.write_str("migration versions must be positive and strictly increasing")
            }
            Self::Newer { version, target } => {
                write!(f, "migration {version} is newer than target {target}")
            }
            Self::TooOld { version, minimum } => {
                write!(f, "migration {version} is older than minimum {minimum}")
            }
            Self::UnknownVersion { version } => write!(f, "unknown migration {version}"),
            Self::LedgerVersionMismatch { expected, found } => {
                write!(f, "ledger expected migration {expected}, found {found}")
            }
            Self::ChecksumMismatch { version } => {
                write!(f, "migration {version} checksum mismatch")
            }
            Self::DescriptionMismatch { version } => {
                write!(f, "migration {version} description mismatch")
            }
            Self::LegacyConflict { version } => {
                write!(f, "migration {version} conflicts with legacy ledger")
            }
            Self::Database(error) => write!(f, "migration database operation failed: {error}"),
        }
    }
}
impl StdError for MigrationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Database(error) => Some(error),
            _ => None,
        }
    }
}
