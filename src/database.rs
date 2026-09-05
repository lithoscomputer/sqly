#[cfg(feature = "postgres")]
use std::env;
use std::fmt;
#[cfg(feature = "sqlite")]
use std::process;
use std::sync::Arc;
#[cfg(feature = "sqlite")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sqlx::ConnectOptions as _;
#[cfg(feature = "sqlite")]
use sqlx::Connection as _;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgConnectOptions;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPoolOptions;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgSslMode;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::SqliteConnectOptions;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::SqliteJournalMode;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::SqlitePoolOptions;
#[cfg(feature = "sqlite")]
use tokio::sync::Mutex;

use crate::options::Options;
use crate::{ConnectOptions, Error, FromRow, Query, Result, Row, Sql};

/// A cheap-to-clone pool owner. The caller supplies a Tokio runtime.
#[derive(Clone)]
pub struct Database {
    pub(crate) inner: Arc<Inner>,
}
pub(crate) enum Inner {
    #[cfg(feature = "sqlite")]
    Sqlite {
        pool:   sqlx::SqlitePool,
        keeper: Option<Arc<Keeper>>,
    },
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
}
#[cfg(feature = "sqlite")]
pub(crate) struct Keeper {
    connection: Mutex<Option<sqlx::SqliteConnection>>,
    lost:       AtomicBool,
}
#[cfg(feature = "sqlite")]
impl Keeper {
    async fn check(&self) -> Result<()> {
        if self.lost.load(Ordering::Acquire) {
            return Err(Error::DatabaseLost);
        }
        let mut guard = self.connection.lock().await;
        let Some(connection) = guard.as_mut() else {
            self.lost.store(true, Ordering::Release);
            return Err(Error::DatabaseLost);
        };
        if connection.ping().await.is_err() {
            self.lost.store(true, Ordering::Release);
            return Err(Error::DatabaseLost);
        }
        Ok(())
    }
}
/// Pool settings shared by the backends.
#[derive(Clone, Debug)]
#[must_use]
pub struct DatabaseBuilder {
    max_connections: Option<u32>,
    acquire_timeout: Duration,
}
impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self {
            max_connections: None,
            acquire_timeout: Duration::from_secs(30),
        }
    }
}
impl DatabaseBuilder {
    pub fn max_connections(mut self, count: u32) -> Self {
        self.max_connections = Some(count);
        self
    }
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }
    pub async fn connect<O>(self, options: O) -> Result<Database>
    where
        O: TryInto<ConnectOptions>,
        Error: From<O::Error>,
    {
        let options = options.try_into().map_err(Error::from)?;
        if self.max_connections == Some(0)
            || self.acquire_timeout.is_zero()
            || Instant::now().checked_add(self.acquire_timeout).is_none()
        {
            return Err(Error::config(
                "pool size and acquisition timeout must be positive",
            ));
        }
        match options.inner {
            Options::Sqlite(options) => {
                options.validate()?;
                if options.memory && self.max_connections.is_some_and(|n| n != 1) {
                    return Err(Error::config(
                        "in-memory SQLite requires one query connection",
                    ));
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    Err(Error::UnsupportedBackend { backend: "sqlite" })
                }
                #[cfg(feature = "sqlite")]
                {
                    let mut driver = SqliteConnectOptions::new()
                        .filename(&options.filename)
                        .create_if_missing(options.create)
                        .read_only(options.read_only)
                        .foreign_keys(true)
                        .busy_timeout(options.busy_timeout)
                        .disable_statement_logging();
                    if options.wal {
                        driver = driver.journal_mode(SqliteJournalMode::Wal);
                    }
                    let keeper = if options.memory {
                        static NEXT: AtomicU64 = AtomicU64::new(1);
                        let id = NEXT.fetch_add(1, Ordering::Relaxed);
                        driver = driver
                            .filename(format!("file:sqly-{}-{id}", process::id()))
                            .in_memory(true)
                            .shared_cache(true);
                        let connection = sqlx::SqliteConnection::connect_with(&driver)
                            .await
                            .map_err(Error::driver)?;
                        Some(Arc::new(Keeper {
                            connection: Mutex::new(Some(connection)),
                            lost:       AtomicBool::new(false),
                        }))
                    } else {
                        None
                    };
                    // The pool retains the keeper through its outstanding leases and
                    // return/cleanup tasks, even after the last Database is dropped.
                    let retained_keeper = keeper.clone();
                    let pool = SqlitePoolOptions::new()
                        .max_connections(if options.memory {
                            1
                        } else {
                            self.max_connections.unwrap_or(5)
                        })
                        .acquire_timeout(self.acquire_timeout)
                        .after_release(move |_, _| {
                            let retained_keeper = retained_keeper.clone();
                            Box::pin(async move {
                                drop(retained_keeper);
                                Ok(true)
                            })
                        })
                        .connect_with(driver)
                        .await
                        .map_err(Error::driver)?;
                    Ok(Database {
                        inner: Arc::new(Inner::Sqlite { pool, keeper }),
                    })
                }
            }
            Options::Postgres(options) => {
                options.validate()?;
                #[cfg(not(feature = "postgres"))]
                {
                    Err(Error::UnsupportedBackend {
                        backend: "postgres",
                    })
                }
                #[cfg(feature = "postgres")]
                {
                    // SQLx has no setters that clear these inherited fields. Fail
                    // closed rather than merge them or mutate process environment.
                    for name in ["PGOPTIONS", "PGSSLROOTCERT", "PGSSLCERT", "PGSSLKEY"] {
                        if env::var_os(name).is_some() {
                            return Err(Error::config(
                                "inherited PGOPTIONS or TLS certificate environment is unsupported; use explicit options",
                            ));
                        }
                    }
                    let mut driver = PgConnectOptions::new_without_pgpass()
                        .host(&options.host)
                        .port(options.port)
                        .database(&options.database)
                        .username(&options.username)
                        .password(&options.password)
                        .application_name(&options.application_name)
                        .statement_cache_capacity(0)
                        .ssl_mode(match options.tls {
                            crate::TlsMode::VerifyFull => PgSslMode::VerifyFull,
                            crate::TlsMode::Disable => PgSslMode::Disable,
                            crate::TlsMode::Allow => PgSslMode::Allow,
                            crate::TlsMode::Prefer => PgSslMode::Prefer,
                            crate::TlsMode::Require => PgSslMode::Require,
                            crate::TlsMode::VerifyCa => PgSslMode::VerifyCa,
                        })
                        .disable_statement_logging();
                    if options.host.starts_with('/') {
                        driver = driver.socket(&options.host);
                    }
                    if let Some(path) = options.root_cert {
                        driver = driver.ssl_root_cert(path);
                    }
                    let pool = PgPoolOptions::new()
                        .max_connections(self.max_connections.unwrap_or(5))
                        .acquire_timeout(self.acquire_timeout)
                        .connect_with(driver)
                        .await
                        .map_err(Error::driver)?;
                    Ok(Database {
                        inner: Arc::new(Inner::Postgres(pool)),
                    })
                }
            }
        }
    }
}
impl Database {
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::default()
    }
    pub async fn connect<O>(options: O) -> Result<Self>
    where
        O: TryInto<ConnectOptions>,
        Error: From<O::Error>,
    {
        Self::builder().connect(options).await
    }
    /// Begin a write transaction. SQLite reserves the writer immediately;
    /// PostgreSQL uses READ COMMITTED.
    pub async fn begin_write(&self) -> Result<crate::Transaction> {
        crate::Transaction::begin(self).await
    }
    pub(crate) async fn check_lease(&self) -> Result<()> {
        #[cfg(feature = "sqlite")]
        if let Inner::Sqlite {
            keeper: Some(keeper),
            ..
        } = self.inner.as_ref()
        {
            keeper.check().await?;
        }
        Ok(())
    }
    pub fn query(&self, sql: impl Into<Sql>) -> Query<'_, Row> {
        self.query_as(sql)
    }
    pub fn query_as<T: FromRow>(&self, sql: impl Into<Sql>) -> Query<'_, T> {
        Query::new(self, sql.into())
    }
    /// Close all clones and drain checked-out connections before closing the
    /// keeper. Cancelling this future keeps the pool closed; calling close
    /// again finishes it.
    pub async fn close(&self) {
        match self.inner.as_ref() {
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => match *self.inner {},
            #[cfg(feature = "sqlite")]
            Inner::Sqlite { pool, keeper } => {
                pool.close().await;
                if let Some(keeper) = keeper {
                    let mut guard = keeper.connection.lock().await;
                    if let Some(connection) = guard.take() {
                        let _ = connection.close().await;
                    }
                }
            }
            #[cfg(feature = "postgres")]
            Inner::Postgres(pool) => pool.close().await,
        }
    }
    pub(crate) async fn check(&self) -> Result<()> {
        #[cfg(feature = "sqlite")]
        if let Inner::Sqlite {
            pool,
            keeper: Some(keeper),
        } = self.inner.as_ref()
        {
            if keeper.lost.load(Ordering::Acquire) {
                return Err(Error::DatabaseLost);
            }
            if pool.is_closed() {
                return Err(Error::PoolClosed);
            }
            keeper.check().await?;
        }
        Ok(())
    }
}
impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Database(<redacted>)")
    }
}

#[cfg(test)]
#[cfg(feature = "sqlite")]
mod tests {
    use tokio::task;

    use super::*;
    use crate::SqliteOptions;

    fn sqlite_pool(db: &Database) -> &sqlx::SqlitePool {
        match db.inner.as_ref() {
            Inner::Sqlite { pool, .. } => pool,
            #[cfg(feature = "postgres")]
            Inner::Postgres(_) => panic!("expected SQLite fixture"),
        }
    }

    #[tokio::test]
    async fn keeper_preserves_data_after_query_connection_replacement() -> Result<()> {
        let db = Database::connect(SqliteOptions::in_memory()).await?;
        db.query("CREATE TABLE items (id INTEGER)")
            .execute()
            .await?;
        db.query("INSERT INTO items VALUES (7)").execute().await?;
        let pool = sqlite_pool(&db);
        pool.acquire()
            .await
            .map_err(Error::driver)?
            .close()
            .await
            .map_err(Error::driver)?;
        assert_eq!(
            db.query("SELECT id FROM items")
                .fetch_one()
                .await?
                .try_get::<i64>("id")?,
            7
        );
        db.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn keeper_loss_is_terminal() -> Result<()> {
        let db = Database::connect(SqliteOptions::in_memory()).await?;
        let Inner::Sqlite {
            keeper: Some(keeper),
            ..
        } = db.inner.as_ref()
        else {
            unreachable!()
        };
        // Simulate unexpected loss of the private keeper.
        keeper
            .connection
            .lock()
            .await
            .take()
            .expect("keeper")
            .close()
            .await
            .map_err(Error::driver)?;
        assert!(matches!(
            db.query("SELECT 1").fetch_one().await,
            Err(Error::DatabaseLost)
        ));
        assert!(matches!(
            db.query("SELECT 1").fetch_one().await,
            Err(Error::DatabaseLost)
        ));
        db.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn active_transaction_rejects_keeper_loss() -> Result<()> {
        let db = Database::connect(SqliteOptions::in_memory()).await?;
        let mut tx = db.begin_write().await?;
        let Inner::Sqlite {
            keeper: Some(keeper),
            ..
        } = db.inner.as_ref()
        else {
            unreachable!()
        };
        keeper
            .connection
            .lock()
            .await
            .take()
            .expect("keeper")
            .close()
            .await
            .map_err(Error::driver)?;
        assert!(matches!(
            tx.query("SELECT 1").execute().await,
            Err(Error::DatabaseLost)
        ));
        tx.rollback().await?;
        assert!(matches!(db.begin_write().await, Err(Error::DatabaseLost)));
        db.close().await;
        Ok(())
    }
    #[tokio::test]
    async fn transaction_retains_keeper_after_cancelled_shutdown_and_last_handle_drop() -> Result<()>
    {
        use tokio::time::timeout;
        let db = Database::connect(SqliteOptions::in_memory()).await?;
        let mut tx = db.begin_write().await?;
        let Inner::Sqlite {
            keeper: Some(keeper),
            ..
        } = db.inner.as_ref()
        else {
            unreachable!()
        };
        let weak = Arc::downgrade(keeper);
        assert!(
            timeout(Duration::from_millis(20), db.close())
                .await
                .is_err()
        );
        drop(db);
        assert!(weak.upgrade().is_some());
        tx.query("CREATE TABLE retained_by_tx (id BIGINT)")
            .execute()
            .await?;
        tx.commit().await?;
        timeout(Duration::from_secs(5), async {
            while weak.upgrade().is_some() {
                task::yield_now().await;
            }
        })
        .await
        .expect("cleanup releases the keeper");
        Ok(())
    }
    #[tokio::test]
    async fn cancelled_close_retains_keeper_until_pool_is_drained() -> Result<()> {
        use tokio::time::timeout;
        let db = Database::builder()
            .acquire_timeout(Duration::from_millis(20))
            .connect(SqliteOptions::in_memory())
            .await?;
        let Inner::Sqlite {
            pool,
            keeper: Some(keeper),
        } = db.inner.as_ref()
        else {
            unreachable!()
        };
        let connection = pool.acquire().await.map_err(Error::driver)?;
        assert!(matches!(
            db.query("SELECT 1").fetch_one().await,
            Err(Error::PoolTimedOut)
        ));
        assert!(
            timeout(Duration::from_millis(20), db.close())
                .await
                .is_err()
        );
        assert!(keeper.connection.lock().await.is_some());
        drop(connection);
        timeout(Duration::from_secs(5), db.close())
            .await
            .expect("close completes");
        assert!(keeper.connection.lock().await.is_none());
        Ok(())
    }
    #[tokio::test]
    async fn cancelled_statement_replaces_connection_without_losing_data() -> Result<()> {
        use tokio::task;
        use tokio::time::timeout;
        let db = Database::connect(SqliteOptions::in_memory()).await?;
        db.query("CREATE TABLE retained (value BIGINT)")
            .execute()
            .await?;
        db.query("INSERT INTO retained VALUES (42)")
            .execute()
            .await?;
        let pool = sqlite_pool(&db);
        timeout(Duration::from_secs(5), async {
            while pool.num_idle() != 1 {
                task::yield_now().await;
            }
        })
        .await
        .expect("pool is ready");
        let query_db = db.clone();
        let task = tokio::spawn(async move {
            query_db.query("WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 1000000000) SELECT sum(x) AS total FROM n").fetch_one().await
        });
        let pool = sqlite_pool(&db);
        timeout(Duration::from_secs(5), async {
            while pool.num_idle() != 0 {
                task::yield_now().await;
            }
        })
        .await
        .expect("query acquired connection");
        task.abort();
        assert!(task.await.expect_err("cancelled").is_cancelled());
        let row = timeout(
            Duration::from_secs(5),
            db.query("SELECT value FROM retained").fetch_one(),
        )
        .await
        .expect("cleanup releases connection")?;
        assert_eq!(row.try_get::<i64>("value")?, 42);
        db.close().await;
        Ok(())
    }
}
