#![cfg(feature = "sqlite")]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{env, fs, process};

use sqly::{Database, DecodeKind, Error, Result, SqliteOptions};

struct TempDirectory(PathBuf);
impl TempDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = env::temp_dir().join(format!(
            "sqly-storage-{}-{}",
            process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("test directory");
        Self(path)
    }
}
impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn file_creation_wal_and_read_only_are_explicit() -> Result<()> {
    let directory = TempDirectory::new();
    let path = directory.0.join("database.sqlite");
    assert!(Database::connect(SqliteOptions::new(&path)).await.is_err());
    assert!(!path.exists());
    let db = Database::connect(SqliteOptions::new(&path).create_if_missing(true)).await?;
    let mode: String = db
        .query("PRAGMA journal_mode")
        .fetch_one()
        .await?
        .try_get("journal_mode")?;
    assert_eq!(mode, "delete");
    db.query("CREATE TABLE persistent (value INTEGER)")
        .execute()
        .await?;
    db.close().await;
    let db = Database::connect(SqliteOptions::new(&path).wal(true)).await?;
    let mode: String = db
        .query("PRAGMA journal_mode")
        .fetch_one()
        .await?
        .try_get("journal_mode")?;
    assert_eq!(mode, "wal");
    db.close().await;
    let db = Database::connect(SqliteOptions::new(&path).read_only(true)).await?;
    assert!(
        db.query("INSERT INTO persistent VALUES (1)")
            .execute()
            .await
            .is_err()
    );
    db.close().await;
    assert!(
        Database::connect(
            SqliteOptions::new(directory.0.join("missing/database.sqlite")).create_if_missing(true)
        )
        .await
        .is_err()
    );
    assert!(!directory.0.join("missing").exists());
    Ok(())
}
#[tokio::test]
async fn invalid_sqlite_boolean_is_a_decode_error() -> Result<()> {
    let db = Database::connect(SqliteOptions::in_memory()).await?;
    let row = db.query("SELECT 2 AS value").fetch_one().await?;
    assert!(matches!(
        row.try_get::<bool>("value"),
        Err(Error::Decode {
            kind: DecodeKind::InvalidValue,
            ..
        })
    ));
    db.close().await;
    Ok(())
}
#[cfg(feature = "time")]
#[tokio::test]
async fn timestamp_storage_is_fixed_width_and_orders_instants() -> Result<()> {
    let db = Database::connect(SqliteOptions::in_memory()).await?;
    db.query("CREATE TABLE moments (value TEXT)")
        .execute()
        .await?;
    let first = time::OffsetDateTime::from_unix_timestamp(1_750_000_000).expect("timestamp");
    let second = first.replace_microsecond(1).expect("precision");
    db.query("INSERT INTO moments VALUES ($1), ($2)")
        .bind(second)
        .bind(first)
        .execute()
        .await?;
    let rows = db
        .query("SELECT value FROM moments ORDER BY value")
        .fetch_all()
        .await?;
    assert!(rows[0].try_get::<String>("value")?.ends_with(".000000Z"));
    assert!(rows[1].try_get::<String>("value")?.ends_with(".000001Z"));
    assert_eq!(rows[0].try_get::<time::OffsetDateTime>("value")?, first);
    assert_eq!(rows[1].try_get::<time::OffsetDateTime>("value")?, second);
    db.close().await;
    Ok(())
}
