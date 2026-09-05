use std::{fmt, result};

use sqlx::SqlStr;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sqlx::pool::PoolConnection;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::database::Inner;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::driver;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::error::Cause;
use crate::query::{Mode, Output};
use crate::stream::Cursor;
use crate::value::private::Value;
use crate::{Database, Error, FromRow, Lock, Query, Result, Row, Sql};

/// An owned write transaction with exclusive query access.
///
/// Drop discards an unfinished connection; closing it rolls back its work.
/// Database errors and cancelled operations make the transaction rollback-only.
/// Commit and rollback consume this handle. Cancellation during COMMIT can
/// leave the outcome unknown. No operation is retried automatically.
///
/// ```no_run
/// # async fn rename(db: &sqly::Database, id: i64, name: &str) -> sqly::Result<bool> {
/// let mut tx = db.begin_write().await?;
/// if !tx.lock(sqly::Lock::row("users").key("id", id)).await? {
///     tx.rollback().await?;
///     return Ok(false);
/// }
/// tx.query("UPDATE users SET name = $1 WHERE id = $2")
///     .bind(name)
///     .bind(id)
///     .execute()
///     .await?;
/// tx.commit().await?;
/// # Ok(true) }
/// ```
///
/// ```compile_fail
/// # async fn example(db: sqly::Database) -> sqly::Result<()> {
/// let mut tx = db.begin_write().await?;
/// let first = tx.query("SELECT 1");
/// let second = tx.query("SELECT 2");
/// first.execute().await?;
/// second.execute().await?;
/// # Ok(()) }
/// ```
#[must_use = "a transaction rolls back unless committed"]
pub struct Transaction {
    db:         Database,
    connection: Connection,
    aborted:    bool,
    finished:   bool,
}
enum Connection {
    #[cfg(feature = "sqlite")]
    Sqlite(PoolConnection<sqlx::Sqlite>),
    #[cfg(feature = "postgres")]
    Postgres(PoolConnection<sqlx::Postgres>),
}
impl Transaction {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) async fn begin(db: &Database) -> Result<Self> {
        db.check().await?;
        let (connection, begin) = match db.inner.as_ref() {
            #[cfg(feature = "sqlite")]
            Inner::Sqlite { pool, .. } => (
                Connection::Sqlite(pool.acquire().await.map_err(Error::driver)?),
                "BEGIN IMMEDIATE",
            ),
            #[cfg(feature = "postgres")]
            Inner::Postgres(pool) => (
                Connection::Postgres(pool.acquire().await.map_err(Error::driver)?),
                "BEGIN ISOLATION LEVEL READ COMMITTED",
            ),
        };
        let mut tx = Self {
            db: db.clone(),
            connection,
            aborted: false,
            finished: false,
        };
        tx.control(begin).await.map_err(driver_error)?;
        Ok(tx)
    }
    #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
    pub(crate) async fn begin(_db: &Database) -> Result<Self> {
        Err(Error::UnsupportedBackend {
            backend: "sqlite or postgres",
        })
    }
    pub fn query(&mut self, sql: impl Into<Sql>) -> Query<'_, Row> {
        self.query_as(sql)
    }
    pub fn query_as<T: FromRow>(&mut self, sql: impl Into<Sql>) -> Query<'_, T> {
        Query::transaction(self, sql.into())
    }
    /// Require an existing row lock, returning `LockNotFound` if absent.
    /// Absence alone does not make the transaction rollback-only. Acquisition
    /// can wait; dependent reads must follow it. Other lock errors are
    /// preserved.
    pub async fn require_lock(&mut self, lock: Lock) -> Result<()> {
        if self.lock(lock).await? {
            Ok(())
        } else {
            Err(Error::LockNotFound)
        }
    }
    /// Acquire an existing row by a primary or unique key, then read dependent
    /// data in a separate query. A missing row returns false.
    pub async fn lock(&mut self, lock: Lock) -> Result<bool> {
        let postgres = match &self.connection {
            #[cfg(feature = "sqlite")]
            Connection::Sqlite(_) => false,
            #[cfg(feature = "postgres")]
            Connection::Postgres(_) => true,
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => false,
        };
        let (sql, values) = lock.statement(postgres)?;
        let output = self.run_statement(sql, values, Mode::All).await?;
        match output.rows.len() {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::NonUniqueLock),
        }
    }
    /// Commit all changes. Transport failures return `CommitUnknown`.
    /// Cancellation during COMMIT can also leave the outcome unknown.
    /// A rollback-only transaction rolls back and returns `TransactionAborted`.
    pub async fn commit(mut self) -> Result<()> {
        if self.aborted {
            let _ = self.control("ROLLBACK").await;
            return Err(Error::TransactionAborted);
        }
        self.db.check_lease().await?;
        self.control("COMMIT").await.map_err(commit_error)?;
        self.finished = true;
        Ok(())
    }
    /// Roll back and consume the transaction. Dropping an unfinished rollback
    /// discards its connection; cancellation must not be treated as completion.
    pub async fn rollback(mut self) -> Result<()> {
        self.control("ROLLBACK").await.map_err(driver_error)?;
        self.finished = true;
        Ok(())
    }
    pub(crate) async fn run(&mut self, sql: Sql, values: Vec<Value>, mode: Mode) -> Result<Output> {
        let _ = sql;
        let text = match &self.connection {
            #[cfg(feature = "sqlite")]
            Connection::Sqlite(_) => sql.sqlite,
            #[cfg(feature = "postgres")]
            Connection::Postgres(_) => sql.postgres,
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => "",
        };
        self.run_statement(SqlStr::from_static(text), values, mode)
            .await
    }
    pub(crate) async fn run_statement(
        &mut self,
        sql: SqlStr,
        values: Vec<Value>,
        mode: Mode,
    ) -> Result<Output> {
        if self.aborted {
            return Err(Error::TransactionAborted);
        }
        // Set before the first await. Dropping the future leaves this flag set.
        self.aborted = true;
        self.db.check_lease().await?;
        let result = match &mut self.connection {
            #[cfg(feature = "sqlite")]
            Connection::Sqlite(connection) => {
                driver::sqlite_on(connection, sql, values, mode).await
            }
            #[cfg(feature = "postgres")]
            Connection::Postgres(connection) => {
                driver::postgres_on(connection, sql, values, mode).await
            }
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => {
                let _ = (sql, values, mode);
                Err(Error::UnsupportedBackend {
                    backend: "sqlite or postgres",
                })
            }
        };
        if result.is_ok() || matches!(&result, Err(Error::BindCount { .. } | Error::Encode { .. }))
        {
            self.aborted = false;
        }
        result
    }
    #[cfg(feature = "migrate")]
    pub(crate) fn is_postgres(&self) -> bool {
        match &self.connection {
            #[cfg(feature = "postgres")]
            Connection::Postgres(_) => true,
            #[cfg(feature = "sqlite")]
            Connection::Sqlite(_) => false,
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => false,
        }
    }
    #[cfg(feature = "migrate")]
    pub(crate) async fn batch(&mut self, sql: Sql) -> Result<()> {
        if self.aborted {
            return Err(Error::TransactionAborted);
        }
        self.aborted = true;
        self.db.check_lease().await?;
        let text = if self.is_postgres() {
            sql.postgres
        } else {
            sql.sqlite
        };
        self.control(text).await.map_err(driver_error)?;
        self.aborted = false;
        Ok(())
    }
    pub(crate) fn rows(&mut self, sql: Sql, values: Vec<Value>) -> Cursor<'_> {
        Box::pin(async_stream::try_stream! {
            if self.aborted { Err(Error::TransactionAborted)?; }
            self.aborted = true;
            self.db.check_lease().await?;
            let mut rows: Cursor<'_> = match &mut self.connection {
                #[cfg(feature = "sqlite")]
                Connection::Sqlite(connection) => driver::sqlite_rows(connection, SqlStr::from_static(sql.sqlite), values),
                #[cfg(feature = "postgres")]
                Connection::Postgres(connection) => driver::postgres_rows(connection, SqlStr::from_static(sql.postgres), values, true),
                #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
                _ => { let _ = (sql, values); Box::pin(futures_util::stream::empty()) },
            };
            while let Some(row) = futures_util::StreamExt::next(&mut rows).await {
                if matches!(&row, Err(Error::BindCount { .. } | Error::Encode { .. })) { self.aborted = false; }
                yield row?;
            }
            drop(rows);
            self.aborted = false;
        })
    }
    async fn control(&mut self, sql: &'static str) -> result::Result<(), sqlx::Error> {
        match &mut self.connection {
            #[cfg(feature = "sqlite")]
            Connection::Sqlite(connection) => {
                // A cancelled SQLite statement leaves an interruption callback.
                // Clear it before rollback so cleanup can complete.
                connection.lock_handle().await?.remove_progress_handler();
                sqlx::raw_sql(sql).execute(&mut **connection).await?;
            }
            #[cfg(feature = "postgres")]
            Connection::Postgres(connection) => {
                sqlx::raw_sql(sql).execute(&mut **connection).await?;
            }
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => {
                let _ = sql;
            }
        }
        Ok(())
    }
}
impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            match &mut self.connection {
                #[cfg(feature = "sqlite")]
                Connection::Sqlite(connection) => connection.close_on_drop(),
                #[cfg(feature = "postgres")]
                Connection::Postgres(connection) => connection.close_on_drop(),
                #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
                _ => {}
            }
        }
    }
}
impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("aborted", &self.aborted)
            .finish_non_exhaustive()
    }
}
fn driver_error(error: sqlx::Error) -> Error {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    {
        Error::driver(error)
    }
    #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
    {
        let _ = error;
        Error::UnsupportedBackend {
            backend: "sqlite or postgres",
        }
    }
}
fn commit_error(error: sqlx::Error) -> Error {
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    if !error.as_database_error().is_some_and(|db| {
        db.code().as_deref().is_some_and(|code| {
            code.starts_with("23")
                || (code.starts_with("40") && code != "40003")
                || matches!(
                    code,
                    "5" | "6" | "19" | "787" | "1299" | "1555" | "2067" | "275"
                )
        })
    }) {
        return Error::CommitUnknown {
            source: Cause(Box::new(error)),
        };
    }
    driver_error(error)
}
