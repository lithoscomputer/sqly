#![cfg(all(feature = "ambient", any(feature = "sqlite", feature = "postgres")))]
#[cfg(feature = "postgres")]
use std::env;
use std::future::{self, Future as _};
use std::task::Poll;
use std::time::Duration;

use sqly::{Database, Error, Lock, Result, ScopedDatabase, Sql};
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::timeout;

#[derive(Clone)]
struct Store {
    db: ScopedDatabase,
}
impl Store {
    async fn insert(&self, id: i64) -> Result<()> {
        self.db
            .query("INSERT INTO sqly_ambient_items VALUES ($1)")
            .bind(id)
            .execute()
            .await?;
        Ok(())
    }
    async fn count(&self) -> Result<i64> {
        self.db
            .read("SELECT count(*) AS n FROM sqly_ambient_items")
            .fetch_one()
            .await?
            .try_get("n")
    }
}
#[derive(Debug)]
enum AppError {
    Database(Error),
    Rejected(u32),
}
impl From<Error> for AppError {
    fn from(error: Error) -> Self {
        Self::Database(error)
    }
}
async fn contract(db: Database, other: Database) -> Result<()> {
    db.query("DROP TABLE IF EXISTS sqly_ambient_items")
        .execute()
        .await?;
    db.query("CREATE TABLE sqly_ambient_items (id BIGINT PRIMARY KEY)")
        .execute()
        .await?;
    let scoped = db.scoped();
    let first = Store { db: scoped.clone() };
    let second = first.clone();
    assert!(matches!(
        first.insert(1).await,
        Err(Error::NoActiveWriteScope)
    ));
    assert!(matches!(
        scoped.query("SELECT 1").fetch_optional().await,
        Err(Error::NoActiveWriteScope)
    ));
    assert!(matches!(
        scoped.query("SELECT 1").fetch_one().await,
        Err(Error::NoActiveWriteScope)
    ));
    assert!(matches!(
        scoped.query("SELECT 1").fetch_all().await,
        Err(Error::NoActiveWriteScope)
    ));
    assert!(matches!(
        scoped
            .query("INSERT INTO sqly_ambient_items VALUES (1) RETURNING id")
            .fetch_one()
            .await,
        Err(Error::NoActiveWriteScope)
    ));
    assert!(matches!(
        scoped
            .lock(Lock::row("sqly_ambient_items").key("id", 1_i64))
            .await,
        Err(Error::NoActiveWriteScope)
    ));
    let built_outside = scoped.query("INSERT INTO sqly_ambient_items VALUES (3)");
    scoped
        .write(|| async {
            first.insert(1).await?;
            second.insert(2).await?;
            built_outside.execute().await?;
            assert_eq!(second.count().await?, 3);
            assert!(
                scoped
                    .lock(Lock::row("sqly_ambient_items").key("id", 1_i64))
                    .await?
            );
            assert!(matches!(
                scoped.write(|| async { Ok::<_, Error>(()) }).await,
                Err(Error::NestedWriteScope)
            ));
            assert!(matches!(
                other.scoped().write(|| async { Ok::<_, Error>(()) }).await,
                Err(Error::NestedWriteScope)
            ));
            assert!(matches!(
                other.scoped().read("SELECT 1").fetch_one().await,
                Err(Error::ScopeDatabaseMismatch)
            ));
            assert!(matches!(
                other.scoped().query("SELECT 1").fetch_one().await,
                Err(Error::ScopeDatabaseMismatch)
            ));
            let spawned = first.clone();
            assert!(matches!(
                task::spawn(async move { spawned.insert(9).await })
                    .await
                    .expect("spawned store"),
                Err(Error::NoActiveWriteScope)
            ));
            let (a, b) = tokio::join!(first.insert(4), second.insert(5));
            a?;
            b?;
            Ok::<_, Error>(())
        })
        .await?;
    assert_eq!(first.count().await?, 5);
    let builder = scoped
        .write(|| async { Ok::<_, Error>(scoped.query("SELECT 1")) })
        .await?;
    assert!(matches!(
        builder.fetch_one().await,
        Err(Error::NoActiveWriteScope)
    ));
    let rejected = scoped
        .write(|| async {
            first.insert(6).await?;
            Err::<(), _>(AppError::Rejected(42))
        })
        .await
        .expect_err("business error");
    assert!(matches!(rejected, AppError::Rejected(42)));
    assert_eq!(first.count().await?, 5);
    let error = scoped
        .write(|| async {
            first.insert(6).await?;
            let _ = first.insert(1).await;
            Ok::<_, AppError>(())
        })
        .await
        .expect_err("caught database error cannot commit partial work");
    assert!(matches!(
        error,
        AppError::Database(Error::TransactionAborted)
    ));
    assert_eq!(first.count().await?, 5);
    scoped
        .write(|| async {
            assert!(matches!(
                scoped
                    .query("SELECT $1 IS NULL")
                    .bind(None::<String>)
                    .bind(2_i64)
                    .fetch_one()
                    .await,
                Err(Error::BindCount { .. })
            ));
            assert!(matches!(
                scoped.query("SELECT $1").bind(f64::NAN).fetch_one().await,
                Err(Error::Encode { .. })
            ));
            assert!(matches!(
                scoped.lock(Lock::row("sqly_ambient_items")).await,
                Err(Error::InvalidLock { .. })
            ));
            first.insert(6).await
        })
        .await?;
    assert!(matches!(
        scoped
            .write_locking(
                Lock::row("sqly_ambient_items").key("id", 999_i64),
                || async {
                    panic!("missing lock must not enter closure");
                    #[expect(unreachable_code, reason = "closure must not execute")]
                    Ok::<_, Error>(())
                }
            )
            .await,
        Err(Error::RowNotFound)
    ));
    scoped
        .write_locking(Lock::row("sqly_ambient_items").key("id", 6_i64), || async {
            first.insert(7).await
        })
        .await?;
    assert_eq!(first.count().await?, 7);

    let task_store = first.clone();
    let task_scope = scoped.clone();
    let panic = task::spawn(async move {
        task_scope
            .write(|| async {
                task_store.insert(8).await?;
                panic!("application panic");
                #[expect(unreachable_code, reason = "intentional application panic")]
                Ok::<_, Error>(())
            })
            .await
    })
    .await
    .expect_err("panic unwinds");
    assert!(panic.is_panic());
    assert_eq!(
        timeout(Duration::from_secs(5), first.count())
            .await
            .expect("panic cleanup")?,
        7
    );
    let (started, ready) = oneshot::channel();
    let task_store = first.clone();
    let task_scope = scoped.clone();
    let running = task::spawn(async move {
        task_scope
            .write(|| async {
                task_store.insert(8).await?;
                started.send(()).expect("observer");
                future::pending::<()>().await;
                Ok::<_, Error>(())
            })
            .await
    });
    ready.await.expect("scope started");
    running.abort();
    assert!(running.await.expect_err("cancelled scope").is_cancelled());
    assert_eq!(
        timeout(Duration::from_secs(5), first.count())
            .await
            .expect("cancellation cleanup")?,
        7
    );
    let cancelled = scoped.write(|| async {
        first.insert(8).await?;
        let slow = Sql::dialects("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < 1000000000) SELECT sum(x) FROM n", "SELECT pg_sleep(0.2)");
        assert!(timeout(Duration::from_millis(50), scoped.query(slow).execute()).await.is_err());
        Ok::<_, Error>(())
    }).await;
    assert!(matches!(cancelled, Err(Error::TransactionAborted)));
    assert_eq!(first.count().await?, 7);
    let mut retained_operation = None;
    let result = scoped.write(|| async {
        first.insert(8).await?;
        let slow = Sql::dialects("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x < 1000000000) SELECT sum(x) FROM n", "SELECT pg_sleep(0.2)");
        let mut operation = Box::pin(scoped.query(slow).execute());
        future::poll_fn(|cx| {
            assert!(operation.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        }).await;
        retained_operation = Some(operation);
        Ok::<_, Error>(())
    }).await;
    assert!(matches!(result, Err(Error::TransactionAborted)));
    assert!(matches!(
        retained_operation.expect("retained operation").await,
        Err(Error::ScopeDatabaseMismatch)
    ));
    assert_eq!(first.count().await?, 7);
    db.close().await;
    other.close().await;
    Ok(())
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_ambient_contract() -> Result<()> {
    contract(
        Database::connect(sqly::SqliteOptions::in_memory()).await?,
        Database::connect(sqly::SqliteOptions::in_memory()).await?,
    )
    .await
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_ambient_contract() -> Result<()> {
    let url = env::var("SQLY_TEST_POSTGRES_URL").expect("PostgreSQL fixture");
    contract(
        Database::connect(url.as_str()).await?,
        Database::connect(url.as_str()).await?,
    )
    .await
}
