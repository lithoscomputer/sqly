#![cfg(feature = "postgres")]
use std::env;
use std::process::{self, Command};
use std::time::Duration;

use sqly::{Database, Error, Result};
use tokio::task;
use tokio::time::timeout;

fn url() -> String {
    env::var("SQLY_TEST_POSTGRES_URL").expect("use mise run test to provision PostgreSQL")
}

#[tokio::test]
async fn cancellation_discards_the_busy_connection_and_allows_reuse() -> Result<()> {
    let url = url();
    let application_name = format!("sqly_cancel_{}", process::id());
    let query_url = format!("{url}&application_name={application_name}");
    let db = Database::builder()
        .max_connections(1)
        .connect(query_url.as_str())
        .await?;
    let observer = Database::connect(url.as_str()).await?;
    let owner = db.clone();
    let running = task::spawn(async move { owner.query("SELECT pg_sleep(10)").execute().await });
    timeout(Duration::from_secs(5), async {
        loop {
            let row = observer.query("SELECT count(*) AS count FROM pg_stat_activity WHERE application_name = $1 AND state = 'active' AND query = 'SELECT pg_sleep(10)'").bind(application_name.as_str()).fetch_one().await?;
            if row.try_get::<i64>("count")? == 1 { break; }
            task::yield_now().await;
        }
        Ok::<_, Error>(())
    }).await.expect("statement becomes active")?;
    running.abort();
    assert!(running.await.expect_err("cancelled").is_cancelled());
    let row = timeout(
        Duration::from_secs(5),
        db.query("SELECT 42 AS value").fetch_one(),
    )
    .await
    .expect("connection replacement")?;
    assert_eq!(row.try_get::<i64>("value")?, 42);
    db.close().await;
    observer.close().await;
    Ok(())
}
#[tokio::test]
async fn tls_does_not_silently_fall_back_to_plaintext() {
    // The disposable fixture has no TLS server configuration.
    let url = url().replace("sslmode=disable", "sslmode=verify-full");
    let error = Database::connect(url.as_str())
        .await
        .expect_err("TLS is required");
    assert!(matches!(error, Error::Connection { .. }));
    assert!(!format!("{error:?} {error}").contains("sqly_test"));
}
#[test]
fn ambient_settings_are_rejected_in_an_isolated_process() {
    let status = Command::new(env::current_exe().expect("test binary"))
        .args(["--exact", "ambient_settings_child", "--nocapture"])
        .env("SQLY_ENV_CHILD", "1")
        .env("PGOPTIONS", "-c search_path=unexpected")
        .status()
        .expect("child process");
    assert!(status.success());
}
#[tokio::test]
async fn ambient_settings_child() {
    if env::var_os("SQLY_ENV_CHILD").is_none() {
        return;
    }
    assert!(matches!(
        Database::connect("postgres://user:password@localhost/db?sslmode=disable").await,
        Err(Error::Configuration { .. })
    ));
}
