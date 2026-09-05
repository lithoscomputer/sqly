#![cfg(any(feature = "sqlite", feature = "postgres"))]
#[cfg(feature = "postgres")]
use std::env;
#[cfg(feature = "ambient")]
use std::future;
use std::io;
use std::time::Duration;

use futures_util::TryStreamExt as _;
use sqly::{Database, Error, FromRow, Result, Row};
use tokio::time::timeout;

struct Limited(i64);
impl FromRow for Limited {
    fn from_row(row: &Row) -> Result<Self> {
        let id = row.try_get("id")?;
        if id == 3 {
            return Err(Error::decode_value(io::Error::other("rejected row")));
        }
        Ok(Self(id))
    }
}
async fn contract(db: Database) -> Result<()> {
    db.query("DROP TABLE IF EXISTS sqly_stream_items")
        .execute()
        .await?;
    db.query("CREATE TABLE sqly_stream_items (id BIGINT PRIMARY KEY)")
        .execute()
        .await?;
    let unpolled = db
        .query("INSERT INTO sqly_stream_items VALUES (99) RETURNING id")
        .fetch();
    drop(unpolled);
    db.query("INSERT INTO sqly_stream_items VALUES (1), (2), (3), (4)")
        .execute()
        .await?;
    let mut rows = db
        .query_as::<Limited>("SELECT id FROM sqly_stream_items ORDER BY id")
        .fetch();
    assert_eq!(rows.try_next().await?.expect("first row").0, 1);
    assert_eq!(rows.try_next().await?.expect("second row").0, 2);
    assert!(matches!(rows.try_next().await, Err(Error::Decode { .. })));
    assert!(rows.try_next().await?.is_none());
    drop(rows);
    let mut rows = db
        .query("SELECT id FROM sqly_stream_items ORDER BY id")
        .fetch();
    assert_eq!(
        rows.try_next()
            .await?
            .expect("first row")
            .try_get::<i64>("id")?,
        1
    );
    drop(rows);
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            db.query("SELECT count(*) AS n FROM sqly_stream_items")
                .fetch_one()
        )
        .await
        .expect("early drop frees connection")?
        .try_get::<i64>("n")?,
        4
    );
    let mut bad = db.query("SELECT $1 AS id").bind(1_i64).bind(2_i64).fetch();
    assert!(matches!(bad.try_next().await, Err(Error::BindCount { .. })));
    assert!(bad.try_next().await?.is_none());
    drop(bad);
    let mut bad = db.query("SELECT $1 AS id").bind(f64::NAN).fetch();
    assert!(matches!(bad.try_next().await, Err(Error::Encode { .. })));
    assert!(bad.try_next().await?.is_none());
    drop(bad);
    let sql = sqly::Sql::dialects(
        "SELECT CASE WHEN id = 3 THEN abs(-9223372036854775805 - id) ELSE id END AS id FROM sqly_stream_items",
        "SELECT CASE WHEN id = 3 THEN 1 / (3 - id) ELSE id END AS id FROM sqly_stream_items",
    );
    let mut failing = db.query(sql).fetch();
    failing
        .try_next()
        .await?
        .expect("first row before driver failure");
    failing
        .try_next()
        .await?
        .expect("second row before driver failure");
    assert!(failing.try_next().await.is_err());
    assert!(failing.try_next().await?.is_none());
    drop(failing);
    let mut tx = db.begin_write().await?;
    let rows = tx
        .query("INSERT INTO sqly_stream_items VALUES (5) RETURNING id")
        .fetch()
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(rows.len(), 1);
    tx.commit().await?;
    let mut tx = db.begin_write().await?;
    let mut rows = tx
        .query("SELECT id FROM sqly_stream_items ORDER BY id")
        .fetch();
    rows.try_next().await?.expect("first row");
    drop(rows);
    assert!(matches!(
        tx.query("SELECT 1").execute().await,
        Err(Error::TransactionAborted)
    ));
    tx.rollback().await?;
    db.close().await;
    Ok(())
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_stream_contract() -> Result<()> {
    contract(Database::connect(sqly::SqliteOptions::in_memory()).await?).await
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_stream_contract() -> Result<()> {
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

#[cfg(feature = "ambient")]
async fn ambient(db: Database) -> Result<()> {
    use sqly::Lock;
    use tokio::sync::oneshot;
    use tokio::task;
    db.query("DROP TABLE IF EXISTS sqly_ambient_stream")
        .execute()
        .await?;
    db.query("CREATE TABLE sqly_ambient_stream (id BIGINT PRIMARY KEY)")
        .execute()
        .await?;
    db.query("INSERT INTO sqly_ambient_stream VALUES (1), (2)")
        .execute()
        .await?;
    let scoped = db.scoped();
    let mut unscoped = scoped.query("SELECT 1").fetch();
    assert!(matches!(
        unscoped.try_next().await,
        Err(Error::NoActiveWriteScope)
    ));
    assert!(unscoped.try_next().await?.is_none());
    let outside = scoped
        .query("SELECT id FROM sqly_ambient_stream ORDER BY id")
        .fetch();
    scoped
        .write(|| async {
            let mut rows = outside;
            rows.try_next().await?.expect("first row");
            assert!(matches!(
                scoped.query("SELECT 1").execute().await,
                Err(Error::ActiveStream)
            ));
            assert!(matches!(
                scoped.read("SELECT 1").fetch_all().await,
                Err(Error::ActiveStream)
            ));
            assert!(matches!(
                scoped
                    .lock(Lock::row("sqly_ambient_stream").key("id", 1_i64))
                    .await,
                Err(Error::ActiveStream)
            ));
            let mut competitor = scoped.read("SELECT 1").fetch();
            assert!(matches!(
                competitor.try_next().await,
                Err(Error::ActiveStream)
            ));
            assert!(competitor.try_next().await?.is_none());
            rows.try_next().await?.expect("second row");
            assert!(rows.try_next().await?.is_none());
            scoped
                .query("INSERT INTO sqly_ambient_stream VALUES (3)")
                .execute()
                .await?;
            Ok::<_, Error>(())
        })
        .await?;
    let result = scoped
        .write(|| async {
            scoped
                .query("INSERT INTO sqly_ambient_stream VALUES (4)")
                .execute()
                .await?;
            let mut rows = scoped
                .query("SELECT id FROM sqly_ambient_stream ORDER BY id")
                .fetch();
            rows.try_next().await?.expect("first row");
            Ok::<_, Error>(rows)
        })
        .await;
    assert!(matches!(result, Err(Error::ActiveStream)));
    let mut escaped = None;
    let result = scoped
        .write(|| async {
            let mut rows = scoped
                .read("SELECT id FROM sqly_ambient_stream ORDER BY id")
                .fetch();
            rows.try_next().await?.expect("first row");
            escaped = Some(rows);
            Err::<(), _>(Error::RowNotFound)
        })
        .await;
    assert!(matches!(result, Err(Error::RowNotFound)));
    let mut escaped = escaped.expect("escaped cursor handle");
    assert!(matches!(
        escaped.try_next().await,
        Err(Error::ScopeDatabaseMismatch)
    ));
    assert!(escaped.try_next().await?.is_none());
    let result = scoped
        .write(|| async {
            let mut rows = scoped
                .query("SELECT id FROM sqly_ambient_stream ORDER BY id")
                .fetch();
            rows.try_next().await?.expect("first row");
            drop(rows);
            assert!(matches!(
                scoped.query("SELECT 1").execute().await,
                Err(Error::TransactionAborted)
            ));
            Ok::<_, Error>(())
        })
        .await;
    assert!(matches!(result, Err(Error::TransactionAborted)));
    let (send_rows, receive_rows) = oneshot::channel();
    let mut pending_scope = Box::pin(scoped.write(|| async {
        scoped
            .query("INSERT INTO sqly_ambient_stream VALUES (4)")
            .execute()
            .await?;
        let mut rows = scoped
            .query("SELECT id FROM sqly_ambient_stream ORDER BY id")
            .fetch();
        rows.try_next().await?.expect("first row");
        send_rows.send(rows).expect("retained handle observer");
        future::pending::<()>().await;
        Ok::<_, Error>(())
    }));
    let mut retained = tokio::select! {
        result = &mut pending_scope => panic!("scope ended unexpectedly: {result:?}"),
        rows = receive_rows => rows.expect("retained handle"),
    };
    drop(pending_scope);
    scoped
        .write(|| async {
            assert!(matches!(
                retained.try_next().await,
                Err(Error::ScopeDatabaseMismatch)
            ));
            Ok::<_, Error>(())
        })
        .await?;
    assert!(retained.try_next().await?.is_none());
    let (started, ready) = oneshot::channel();
    let task_scoped = scoped.clone();
    let running = task::spawn(async move {
        task_scoped
            .write(|| async {
                task_scoped
                    .query("INSERT INTO sqly_ambient_stream VALUES (4)")
                    .execute()
                    .await?;
                let mut rows = task_scoped
                    .query("SELECT id FROM sqly_ambient_stream ORDER BY id")
                    .fetch();
                rows.try_next().await?.expect("first row");
                started.send(()).expect("observer");
                future::pending::<()>().await;
                Ok::<_, Error>(())
            })
            .await
    });
    ready.await.expect("cursor active");
    running.abort();
    assert!(running.await.expect_err("cancelled scope").is_cancelled());
    assert_eq!(
        timeout(
            Duration::from_secs(5),
            scoped
                .read("SELECT count(*) AS n FROM sqly_ambient_stream")
                .fetch_one()
        )
        .await
        .expect("scope cleanup releases connection")?
        .try_get::<i64>("n")?,
        3
    );
    db.close().await;
    Ok(())
}
#[cfg(all(feature = "sqlite", feature = "ambient"))]
#[tokio::test]
async fn sqlite_ambient_stream_contract() -> Result<()> {
    ambient(Database::connect(sqly::SqliteOptions::in_memory()).await?).await
}
#[cfg(all(feature = "postgres", feature = "ambient"))]
#[tokio::test]
async fn postgres_ambient_stream_contract() -> Result<()> {
    ambient(
        Database::connect(
            env::var("SQLY_TEST_POSTGRES_URL")
                .expect("PostgreSQL fixture")
                .as_str(),
        )
        .await?,
    )
    .await
}
