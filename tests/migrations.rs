#![cfg(all(feature = "migrate", any(feature = "sqlite", feature = "postgres")))]
use std::env;
#[cfg(feature = "sqlite")]
use std::{fs, process, time::Duration};

use sqly::migrate::{LegacyLedger, Migration, MigrationError, MigrationResult, Migrator};
use sqly::{Database, Sql};

async fn ledger_count(db: &Database, namespace: &str) -> sqly::Result<i64> {
    db.query("SELECT count(*) AS n FROM _sqly_migrations WHERE namespace = $1")
        .bind(namespace)
        .fetch_one()
        .await?
        .try_get("n")
}
async fn contract(db: Database, other: Database) -> MigrationResult<()> {
    for sql in [
        "DROP TABLE IF EXISTS _sqly_migrations",
        "DROP TABLE IF EXISTS migration_items",
        "DROP TABLE IF EXISTS migration_other",
        "DROP TABLE IF EXISTS migration_failed",
        "DROP TABLE IF EXISTS adopted_items",
        "DROP TABLE IF EXISTS legacy_history",
    ] {
        db.query(sql).execute().await?;
    }
    let migrations = [
        Migration::new(
            1,
            "create items",
            "CREATE TABLE migration_items (id BIGINT PRIMARY KEY); INSERT INTO migration_items VALUES (1)",
        ),
        Migration::new(3, "third item", "INSERT INTO migration_items VALUES (3)"),
        Migration::new(5, "fifth item", "INSERT INTO migration_items VALUES (5)"),
    ];
    let runner = Migrator::try_new("items", &migrations)?.with_compatibility(3, 0)?;
    let (a, b) = tokio::join!(runner.run(&db), runner.run(&other));
    a?;
    b?;
    assert_eq!(ledger_count(&db, "items").await?, 2);
    assert_eq!(
        db.query("SELECT count(*) AS n FROM migration_items")
            .fetch_one()
            .await?
            .try_get::<i64>("n")?,
        2
    );
    let other_set = [Migration::new(
        1,
        "other schema",
        "CREATE TABLE migration_other (id BIGINT)",
    )];
    Migrator::try_new("other", &other_set)?.run(&db).await?;
    assert_eq!(ledger_count(&db, "other").await?, 1);
    assert!(matches!(
        Migrator::try_new("items", &migrations)?
            .with_compatibility(1, 0)?
            .run(&db)
            .await,
        Err(MigrationError::Newer {
            version: 3,
            target:  1,
        })
    ));
    assert!(matches!(
        Migrator::try_new("items", &migrations)?
            .with_compatibility(5, 5)?
            .run(&db)
            .await,
        Err(MigrationError::TooOld {
            version: 3,
            minimum: 5,
        })
    ));
    let corrupt = [
        Migration::new(1, "create items", "SELECT 0"),
        migrations[1].clone(),
    ];
    assert!(matches!(
        Migrator::try_new("items", &corrupt)?.run(&db).await,
        Err(MigrationError::ChecksumMismatch { version: 1 })
    ));
    let renamed = [
        Migration::new(1, "renamed", "SELECT 0"),
        migrations[1].clone(),
    ];
    assert!(matches!(
        Migrator::try_new("items", &renamed)?.run(&db).await,
        Err(MigrationError::DescriptionMismatch { version: 1 })
    ));
    db.query("UPDATE _sqly_migrations SET version = 2 WHERE namespace = 'items' AND version = 3")
        .execute()
        .await?;
    assert!(matches!(
        runner.run(&db).await,
        Err(MigrationError::LedgerVersionMismatch {
            expected: 3,
            found:    2,
        })
    ));
    db.query("UPDATE _sqly_migrations SET version = 3 WHERE namespace = 'items' AND version = 2")
        .execute()
        .await?;
    Migrator::try_new("items", &migrations)?.run(&db).await?;
    assert_eq!(ledger_count(&db, "items").await?, 3);
    let failing = [
        Migration::new(
            1,
            "create rollback fixture",
            "CREATE TABLE migration_failed (id BIGINT); INSERT INTO migration_failed VALUES (1)",
        ),
        Migration::new(2, "fail batch", "THIS IS INVALID SQL"),
    ];
    assert!(matches!(
        Migrator::try_new("failed", &failing)?.run(&db).await,
        Err(MigrationError::Database(_))
    ));
    assert_eq!(ledger_count(&db, "failed").await?, 0);
    let exists: bool = db
        .query(Sql::dialects(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'migration_failed') AS present",
            "SELECT to_regclass('migration_failed') IS NOT NULL AS present",
        ))
        .fetch_one()
        .await?
        .try_get("present")?;
    assert!(!exists, "DDL and ledger must roll back together");

    let historical = [
        Migration::new(
            1,
            "historical schema",
            "CREATE TABLE adopted_items (id BIGINT PRIMARY KEY)",
        ),
        Migration::new(3, "historical row", "INSERT INTO adopted_items VALUES (3)"),
        Migration::new(5, "new row", "INSERT INTO adopted_items VALUES (5)"),
    ];
    db.query("CREATE TABLE adopted_items (id BIGINT PRIMARY KEY)")
        .execute()
        .await?;
    db.query("INSERT INTO adopted_items VALUES (3)")
        .execute()
        .await?;
    db.query("CREATE TABLE legacy_history (version BIGINT PRIMARY KEY, description TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)").execute().await?;
    let timestamp = "2025-01-02T03:04:05.123456789Z";
    for migration in &historical[..2] {
        db.query("INSERT INTO legacy_history VALUES ($1, $2, $3, $4)")
            .bind(migration.version())
            .bind(migration.description())
            .bind(migration.checksum())
            .bind(timestamp)
            .execute()
            .await?;
    }
    let legacy = LegacyLedger::try_new("legacy_history")?;
    let adopted = Migrator::try_new("adopted", &historical)?.adopt_legacy(legacy.clone());
    db.query("UPDATE legacy_history SET checksum = 'invalid' WHERE version = 3")
        .execute()
        .await?;
    assert!(matches!(
        adopted.run(&db).await,
        Err(MigrationError::ChecksumMismatch { version: 3 })
    ));
    assert_eq!(ledger_count(&db, "adopted").await?, 0);
    db.query("UPDATE legacy_history SET checksum = $1 WHERE version = 3")
        .bind(historical[1].checksum())
        .execute()
        .await?;
    db.query("UPDATE legacy_history SET version = 2 WHERE version = 3")
        .execute()
        .await?;
    assert!(matches!(
        adopted.run(&db).await,
        Err(MigrationError::LedgerVersionMismatch {
            expected: 3,
            found:    2,
        })
    ));
    db.query("UPDATE legacy_history SET version = 3 WHERE version = 2")
        .execute()
        .await?;
    // Matching partial copies can be completed only after validating all source
    // history. Conflicting metadata must fail without advancing the ledger.
    db.query("INSERT INTO _sqly_migrations SELECT 'adopted', version, description, checksum, 'conflicting timestamp' FROM legacy_history WHERE version = 1").execute().await?;
    assert!(matches!(
        adopted.run(&db).await,
        Err(MigrationError::LegacyConflict { version: 1 })
    ));
    db.query(
        "UPDATE _sqly_migrations SET applied_at = $1 WHERE namespace = 'adopted' AND version = 1",
    )
    .bind(timestamp)
    .execute()
    .await?;
    let (a, b) = tokio::join!(adopted.run(&db), adopted.run(&other));
    a?;
    b?;
    assert_eq!(ledger_count(&db, "adopted").await?, 3);
    assert_eq!(
        db.query("SELECT count(*) AS n FROM adopted_items")
            .fetch_one()
            .await?
            .try_get::<i64>("n")?,
        2
    );
    assert_eq!(
        db.query(
            "SELECT applied_at FROM _sqly_migrations WHERE namespace = 'adopted' AND version = 3"
        )
        .fetch_one()
        .await?
        .try_get::<String>("applied_at")?,
        timestamp
    );
    // A stale legacy prefix does not lower the already-adopted schema version.
    Migrator::try_new("adopted", &historical)?
        .with_compatibility(5, 5)?
        .adopt_legacy(legacy.clone())
        .run(&db)
        .await?;
    let failure = [
        historical[0].clone(),
        historical[1].clone(),
        Migration::new(
            5,
            "injected failure",
            "CREATE TABLE migration_failed (id BIGINT); THIS IS INVALID SQL",
        ),
    ];
    assert!(matches!(
        Migrator::try_new("adoption_failure", &failure)?
            .adopt_legacy(legacy)
            .run(&db)
            .await,
        Err(MigrationError::Database(_))
    ));
    assert_eq!(ledger_count(&db, "adoption_failure").await?, 0);
    let fresh = [Migration::new(1, "empty migration", "SELECT 1")];
    Migrator::try_new("fresh", &fresh)?
        .adopt_legacy(LegacyLedger::try_new("missing_legacy")?)
        .run(&db)
        .await?;
    assert_eq!(ledger_count(&db, "fresh").await?, 1);
    db.close().await;
    other.close().await;
    Ok(())
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_migration_contract() -> MigrationResult<()> {
    let url = env::var("SQLY_TEST_POSTGRES_URL").expect("PostgreSQL fixture");
    contract(
        Database::connect(url.as_str()).await?,
        Database::connect(url.as_str()).await?,
    )
    .await
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_migration_contract() -> MigrationResult<()> {
    let directory = env::temp_dir().join(format!("sqly-migrations-{}", process::id()));
    fs::create_dir(&directory).expect("fixture directory");
    let options = sqly::SqliteOptions::new(directory.join("db.sqlite"))
        .create_if_missing(true)
        .wal(true)
        .busy_timeout(Duration::from_secs(10));
    let result = contract(
        Database::connect(options.clone()).await?,
        Database::connect(options).await?,
    )
    .await;
    fs::remove_dir_all(directory).expect("remove fixture");
    result
}
#[test]
fn definitions_and_legacy_identifiers_validate_without_io() {
    assert!(LegacyLedger::try_new("x; DROP TABLE x").is_err());
    assert!(LegacyLedger::try_new("_sqly_migrations").is_err());
    assert!(matches!(
        Migrator::try_new("x", &[Migration::new(0, "zero", "SELECT 1")]),
        Err(MigrationError::Unordered)
    ));
    assert!(matches!(
        Migrator::try_new("x", &[
            Migration::new(2, "two", "SELECT 1"),
            Migration::new(1, "one", "SELECT 1")
        ]),
        Err(MigrationError::Unordered)
    ));
    assert!(Migrator::try_new("", &[]).is_err());
}

#[test]
fn checksums_preserve_both_dialects_and_exact_bytes() {
    let migration = Migration::new(7, "first", Sql::dialects("\nSELECT 1;\n", "\nSELECT 2;\n"));
    assert_eq!(
        migration.checksum(),
        "617926e0cc1322e646cfa6dadd6f2622d745124778eea53b03c6240b3d1338d7"
    );
    assert_ne!(
        migration.checksum(),
        Migration::new(7, "first", Sql::dialects("SELECT 1;\n", "\nSELECT 2;\n")).checksum()
    );
}
