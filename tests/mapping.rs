#[cfg(any(feature = "sqlite", feature = "postgres"))]
async fn exercise(db: sqly::Database) -> sqly::Result<()> {
    use std::cell::Cell;
    use std::io;

    use futures_util::TryStreamExt as _;
    use sqly::{Error, Row};

    const SQL: &str = "SELECT 1 AS value UNION ALL SELECT 2 AS value ORDER BY value";
    let calls = Cell::new(0);
    let map = |row: &Row| {
        calls.set(calls.get() + 1);
        row.try_get::<i64>("value")
    };
    assert_eq!(db.query(SQL).try_map(map).fetch_one().await?, 1);
    assert_eq!(calls.get(), 1);
    assert_eq!(db.query(SQL).try_map(map).fetch_all().await?, vec![1, 2]);
    assert_eq!(calls.get(), 3);
    let mut rows = db.query(SQL).try_map(map).fetch();
    assert_eq!(calls.get(), 3);
    assert_eq!(rows.try_next().await?, Some(1));
    assert_eq!(calls.get(), 4);
    assert_eq!(rows.try_next().await?, Some(2));
    assert_eq!(rows.try_next().await?, None);
    drop(rows);
    assert_eq!(
        db.query("SELECT $1 AS value")
            .try_map(map)
            .bind(7_i64)
            .fetch_one()
            .await?,
        7
    );
    assert_eq!(
        db.query("SELECT 1 AS value WHERE 1 = 0")
            .try_map(map)
            .fetch_optional()
            .await?,
        None
    );
    assert!(matches!(
        db.query("SELECT 1 AS value WHERE 1 = 0")
            .try_map(map)
            .fetch_one()
            .await,
        Err(Error::RowNotFound)
    ));

    let fail = |_: &Row| -> sqly::Result<i64> {
        Err(Error::decode_value(io::Error::other("mapping failed")))
    };
    let mut tx = db.begin_write().await?;
    assert!(matches!(
        tx.query(SQL).try_map(fail).fetch_all().await,
        Err(Error::Decode { .. })
    ));
    assert_eq!(tx.query(SQL).try_map(map).fetch_one().await?, 1);
    let mut rows = tx.query(SQL).try_map(map).fetch();
    while rows.try_next().await?.is_some() {}
    drop(rows);
    tx.commit().await?;

    let mut tx = db.begin_write().await?;
    let mut rows = tx.query(SQL).try_map(fail).fetch();
    assert!(matches!(rows.try_next().await, Err(Error::Decode { .. })));
    assert_eq!(rows.try_next().await?, None);
    drop(rows);
    assert!(matches!(tx.commit().await, Err(Error::TransactionAborted)));

    #[cfg(feature = "ambient")]
    {
        use futures_util::FutureExt as _;
        let scoped = db.scoped();
        assert!(matches!(
            scoped.query(SQL).try_map(map).fetch_one().await,
            Err(Error::NoActiveWriteScope)
        ));
        assert_eq!(scoped.read(SQL).try_map(map).fetch_all().await?, vec![1, 2]);
        scoped
            .write(|| async {
                let mut rows = scoped
                    .read(SQL)
                    .try_map(|row| {
                        // Re-enter scope operations during conversion. Holding the
                        // cursor mutex here would deadlock this synchronous poll.
                        assert!(matches!(
                            scoped.read(SQL).fetch_one().now_or_never(),
                            Some(Err(Error::ActiveStream))
                        ));
                        row.try_get::<i64>("value")
                    })
                    .fetch();
                assert_eq!(rows.try_next().await?, Some(1));
                assert_eq!(rows.try_next().await?, Some(2));
                assert_eq!(rows.try_next().await?, None);
                drop(rows);
                Ok::<_, Error>(())
            })
            .await?;
        let escaped = scoped
            .write(|| async {
                let mut rows = scoped.read(SQL).try_map(map).fetch();
                assert_eq!(rows.try_next().await?, Some(1));
                Ok::<_, Error>(rows)
            })
            .await;
        assert!(matches!(escaped, Err(Error::ActiveStream)));
    }
    db.close().await;
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_mapping_contract() -> sqly::Result<()> {
    exercise(sqly::Database::connect(sqly::SqliteOptions::in_memory()).await?).await
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_mapping_contract() -> sqly::Result<()> {
    use std::env;
    let url = env::var("SQLY_TEST_POSTGRES_URL").expect("disposable PostgreSQL fixture");
    exercise(sqly::Database::connect(url.as_str()).await?).await
}
