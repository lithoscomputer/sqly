#![cfg(any(feature = "sqlite", feature = "postgres"))]
#[cfg(feature = "postgres")]
use std::env;
use std::time::Duration;

#[cfg(feature = "sqlite")]
use sqly::SqliteOptions;
use sqly::{Database, Error, Lock, Result, Sql};
use tokio::time::timeout;

async fn count(db: &Database) -> Result<i64> {
    db.query("SELECT count(*) AS n FROM sqly_tx_contract")
        .fetch_one()
        .await?
        .try_get("n")
}
async fn contract(db: Database) -> Result<()> {
    db.query("DROP TABLE IF EXISTS sqly_tx_contract")
        .execute()
        .await?;
    db.query("CREATE TABLE sqly_tx_contract (id BIGINT PRIMARY KEY, category BIGINT NOT NULL)")
        .execute()
        .await?;
    let mut tx = db.begin_write().await?;
    tx.query("INSERT INTO sqly_tx_contract VALUES (1, 7)")
        .execute()
        .await?;
    tx.commit().await?;
    assert_eq!(count(&db).await?, 1);
    let mut tx = db.begin_write().await?;
    tx.query("INSERT INTO sqly_tx_contract VALUES (2, 7)")
        .execute()
        .await?;
    tx.rollback().await?;
    assert_eq!(count(&db).await?, 1);
    let mut tx = db.begin_write().await?;
    tx.query("INSERT INTO sqly_tx_contract VALUES (3, 7)")
        .execute()
        .await?;
    drop(tx);
    assert_eq!(
        timeout(Duration::from_secs(5), count(&db))
            .await
            .expect("drop releases connection")?,
        1
    );

    let mut tx = db.begin_write().await?;
    // PostgreSQL needs the supplied type for this expression. Its internal
    // untyped probe must not leave the surrounding transaction aborted.
    tx.query("SELECT $1 IS NULL AS missing")
        .bind(None::<String>)
        .fetch_one()
        .await?;
    assert!(matches!(
        tx.query("SELECT $1 IS NULL AS missing").fetch_one().await,
        Err(Error::BindCount { .. })
    ));
    assert!(matches!(
        tx.query("SELECT $1 IS NULL AS missing")
            .bind(None::<String>)
            .bind(2_i64)
            .fetch_one()
            .await,
        Err(Error::BindCount { .. })
    ));
    assert!(matches!(
        tx.query("SELECT $1 AS value")
            .bind(f64::NAN)
            .execute()
            .await,
        Err(Error::Encode { .. })
    ));
    assert!(matches!(
        tx.query("SELECT 1 AS value WHERE 1 = 0").fetch_one().await,
        Err(Error::RowNotFound)
    ));
    assert!(
        tx.query("SELECT 1 AS value")
            .fetch_one()
            .await?
            .try_get::<Vec<u8>>("value")
            .is_err()
    );
    for lock in [
        Lock::row("sqly_tx_contract"),
        Lock::row("bad;table").key("id", 1_i64),
        Lock::row("sqly_tx_contract").key("id", None::<i64>),
        Lock::row("sqly_tx_contract")
            .key("id", 1_i64)
            .key("id", 1_i64),
    ] {
        assert!(matches!(
            tx.lock(lock).await,
            Err(Error::InvalidLock { .. })
        ));
    }
    assert!(
        tx.lock(
            Lock::row("sqly_tx_contract")
                .key("id", 1_i64)
                .key("category", 7_i64)
        )
        .await?
    );
    assert!(
        !tx.lock(Lock::row("sqly_tx_contract").key("id", 999_i64))
            .await?
    );
    tx.query("INSERT INTO sqly_tx_contract VALUES (4, 7)")
        .execute()
        .await?;
    assert!(matches!(
        tx.lock(Lock::row("sqly_tx_contract").key("category", 7_i64))
            .await,
        Err(Error::NonUniqueLock)
    ));
    tx.commit().await?;
    assert_eq!(count(&db).await?, 2);

    let mut tx = db.begin_write().await?;
    tx.query("INSERT INTO sqly_tx_contract VALUES (5, 7)")
        .execute()
        .await?;
    assert!(
        tx.query("INSERT INTO sqly_tx_contract VALUES (1, 7)")
            .execute()
            .await
            .expect_err("duplicate")
            .is_unique_violation()
    );
    assert!(matches!(
        tx.query("SELECT 1").execute().await,
        Err(Error::TransactionAborted)
    ));
    assert!(matches!(tx.commit().await, Err(Error::TransactionAborted)));
    assert_eq!(count(&db).await?, 2);

    let mut tx = db.begin_write().await?;
    assert!(matches!(
        tx.query("THIS IS INVALID SQL").execute().await,
        Err(Error::InvalidSql { .. })
    ));
    tx.rollback().await?;

    let mut tx = db.begin_write().await?;
    tx.query("INSERT INTO sqly_tx_contract VALUES (6, 7)")
        .execute()
        .await?;
    let slow = Sql::dialects(
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM n WHERE x < 1000000000) SELECT sum(x) FROM n",
        "SELECT pg_sleep(0.2)",
    );
    assert!(
        timeout(Duration::from_millis(50), tx.query(slow).execute())
            .await
            .is_err()
    );
    assert!(matches!(
        tx.query("SELECT 1").execute().await,
        Err(Error::TransactionAborted)
    ));
    timeout(Duration::from_secs(5), tx.rollback())
        .await
        .expect("cancelled query cleanup")?;
    assert_eq!(count(&db).await?, 2);

    // A checked-out transaction can finish while pool shutdown waits for it.
    let mut tx = db.begin_write().await?;
    let close = db.close();
    tokio::pin!(close);
    assert!(
        timeout(Duration::from_millis(20), &mut close)
            .await
            .is_err()
    );
    tx.query("INSERT INTO sqly_tx_contract VALUES (7, 7)")
        .execute()
        .await?;
    tx.commit().await?;
    timeout(Duration::from_secs(5), close)
        .await
        .expect("shutdown drains transaction");
    Ok(())
}

async fn deferred_commit(db: Database) -> Result<()> {
    db.query("DROP TABLE IF EXISTS sqly_tx_deferred")
        .execute()
        .await?;
    db.query("CREATE TABLE sqly_tx_deferred (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES sqly_tx_deferred(id) DEFERRABLE INITIALLY DEFERRED)").execute().await?;
    let mut tx = db.begin_write().await?;
    tx.query("INSERT INTO sqly_tx_deferred VALUES (1, 99)")
        .execute()
        .await?;
    assert!(matches!(tx.commit().await, Err(Error::Constraint { .. })));
    let row = timeout(
        Duration::from_secs(5),
        db.query("SELECT count(*) AS n FROM sqly_tx_deferred")
            .fetch_one(),
    )
    .await
    .expect("failed commit releases connection")?;
    assert_eq!(row.try_get::<i64>("n")?, 0);
    db.close().await;
    Ok(())
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_transaction_contract() -> Result<()> {
    contract(Database::connect(SqliteOptions::in_memory()).await?).await
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_transaction_contract() -> Result<()> {
    contract(
        Database::builder()
            .max_connections(1)
            .connect(
                env::var("SQLY_TEST_POSTGRES_URL")
                    .expect("PostgreSQL fixture")
                    .as_str(),
            )
            .await?,
    )
    .await
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_deferred_commit_failure() -> Result<()> {
    deferred_commit(Database::connect(SqliteOptions::in_memory()).await?).await
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_deferred_commit_failure() -> Result<()> {
    deferred_commit(
        Database::builder()
            .max_connections(1)
            .connect(
                env::var("SQLY_TEST_POSTGRES_URL")
                    .expect("PostgreSQL fixture")
                    .as_str(),
            )
            .await?,
    )
    .await
}
#[test]
fn lock_debug_redacts_names_and_values() {
    let lock = Lock::row("private_table").key("private_column", "secret");
    let debug = format!("{lock:?}");
    for private in ["private_table", "private_column", "secret"] {
        assert!(!debug.contains(private));
    }
}
