//! Async SQL for SQLite and PostgreSQL. Applications own the Tokio runtime.
//!
//! Enable `sqlite`, `postgres`, or both. Value integrations `uuid`, `time`, and
//! `json` are optional. Explicit transactions and row locks are available.
//! Enable `ambient` for task-local write scopes and `migrate` for migrations.
//! Query streams implement `futures_core::Stream` and start on first poll.
//!
//! ```no_run
//! # #[cfg(feature = "sqlite")]
//! # async fn example() -> sqly::Result<()> {
//! use sqly::{Database, SqliteOptions};
//! let db = Database::connect(SqliteOptions::in_memory()).await?;
//! db.query("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
//!     .execute()
//!     .await?;
//! db.query("INSERT INTO users VALUES ($1, $2)")
//!     .bind(1_i64)
//!     .bind("Ada")
//!     .execute()
//!     .await?;
//! let row = db
//!     .query("SELECT name FROM users WHERE id = $1")
//!     .bind(1_i64)
//!     .fetch_one()
//!     .await?;
//! assert_eq!(row.try_get::<String>("name")?, "Ada");
//! db.close().await;
//! # Ok(()) }
//! ```
mod database;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod driver;
mod error;
mod lock;
mod mapped;
#[cfg(feature = "migrate")]
pub mod migrate;
mod options;
mod query;
mod row;
#[cfg(feature = "ambient")]
mod scoped;
mod sql;
mod stream;
mod transaction;
mod value;

pub use database::{Database, DatabaseBuilder};
pub use error::{Cause, ConstraintKind, DecodeKind, Error, Result};
pub use lock::Lock;
pub use mapped::MappedQuery;
pub use options::{ConnectOptions, PostgresOptions, SqliteOptions, TlsMode};
pub use query::{ExecuteResult, Query};
pub use row::{FromRow, Row};
#[cfg(feature = "ambient")]
pub use scoped::{ReadQuery, ScopedDatabase};
pub use sql::Sql;
pub use stream::RowStream;
pub use transaction::Transaction;
pub use value::{Decode, Encode, SqlValue};
