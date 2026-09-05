#![cfg(feature = "sqlite")]
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::{env, fs, process};

use sqly::{Database, Lock, Result, SqliteOptions};
use tokio::time::timeout;

struct TempDirectory(PathBuf);
impl TempDirectory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = env::temp_dir().join(format!(
            "sqly-tx-{}-{}",
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
async fn begin_immediate_serializes_writers_and_cancelled_begin_releases_its_connection()
-> Result<()> {
    let directory = TempDirectory::new();
    let options = SqliteOptions::new(directory.0.join("db.sqlite"))
        .create_if_missing(true)
        .wal(true)
        .busy_timeout(Duration::from_secs(2));
    let db = Database::builder()
        .max_connections(1)
        .connect(options.clone())
        .await?;
    let other = Database::builder()
        .max_connections(1)
        .connect(options)
        .await?;
    db.query("CREATE TABLE revision (id BIGINT PRIMARY KEY, value BIGINT NOT NULL)")
        .execute()
        .await?;
    db.query("INSERT INTO revision VALUES (1, 1)")
        .execute()
        .await?;
    let mut first = db.begin_write().await?;
    assert!(first.lock(Lock::row("revision").key("id", 1_i64)).await?);
    first
        .query("UPDATE revision SET value = 2 WHERE id = 1")
        .execute()
        .await?;
    assert_eq!(
        other
            .query("SELECT value FROM revision WHERE id = 1")
            .fetch_one()
            .await?
            .try_get::<i64>("value")?,
        1
    );
    // Cancellation while BEGIN waits must discard that connection, even if
    // SQLite acquires the writer reservation just as cancellation occurs.
    assert!(
        timeout(Duration::from_millis(50), other.begin_write())
            .await
            .is_err()
    );
    let second = other.begin_write();
    tokio::pin!(second);
    assert!(
        timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err()
    );
    first.commit().await?;
    let mut second = timeout(Duration::from_secs(5), second)
        .await
        .expect("writer reservation released")?;
    assert!(second.lock(Lock::row("revision").key("id", 1_i64)).await?);
    assert_eq!(
        second
            .query("SELECT value FROM revision WHERE id = 1")
            .fetch_one()
            .await?
            .try_get::<i64>("value")?,
        2
    );
    second.rollback().await?;
    db.close().await;
    other.close().await;
    Ok(())
}
