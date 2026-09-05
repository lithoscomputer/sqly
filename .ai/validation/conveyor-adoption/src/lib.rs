#[cfg(test)]
mod tests {
    use sqly::{Database, Sql};
    use sqly::migrate::{Migration, Migrator, LegacyLedger};
    fn definitions() -> Vec<Migration> { vec![
        Migration::new(1, "create feed definitions and events", Sql::dialects(include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0001_create_feed_definitions_and_events.sqlite.sql"), include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0001_create_feed_definitions_and_events.postgres.sql"))),
        Migration::new(2, "create refresh requests", Sql::dialects(include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0002_create_refresh_requests.sqlite.sql"), include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0002_create_refresh_requests.postgres.sql"))),
        Migration::new(3, "create refresh source context", Sql::dialects(include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0003_create_refresh_source_context.sqlite.sql"), include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0003_create_refresh_source_context.postgres.sql"))),
        Migration::new(4, "create source attempts", Sql::dialects(include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0004_create_source_attempts.sqlite.sql"), include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0004_create_source_attempts.postgres.sql"))),
        Migration::new(5, "create cards and source records", Sql::dialects(include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0005_create_cards_and_source_records.sqlite.sql"), include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0005_create_cards_and_source_records.postgres.sql"))),
        Migration::new(6, "create card review decisions", Sql::dialects(include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0006_create_card_review_decisions.sqlite.sql"), include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0006_create_card_review_decisions.postgres.sql"))),
        Migration::new(7, "create actions", Sql::dialects(include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0007_create_actions.sqlite.sql"), include_str!("/private/tmp/sqly-conveyor-integration/crates/schema/migrations/0007_create_actions.postgres.sql"))),
        Migration::new(8, "cutover probe", "CREATE TABLE adoption_probe (id BIGINT PRIMARY KEY)"),
    ] }
    async fn adopt(url: &str) {
        let old = persistence::Database::connect(url).await.expect("old Conveyor connection");
        persistence::MigrationRunner::new(schema::migrations()).run(&old).await.expect("old Conveyor migrations");
        old.execute("INSERT INTO feeds (id, public_id, lifecycle_state) VALUES ('00000000-0000-0000-0000-000000000001', 'feed_fixture', 'active')").await.expect("historical application row");
        old.close().await; // Quiesce the old runner before cutover.
        let db = Database::connect(url).await.expect("new sqly connection");
        let before = db.query("SELECT version, description, checksum, applied_at FROM _conveyor_migrations ORDER BY version").fetch_all().await.expect("legacy metadata");
        let migrations = definitions();
        let old_definitions = schema::migrations();
        for (new, old) in migrations.iter().zip(old_definitions.iter()) { assert_eq!(new.checksum(), old.checksum); }
        let migrator = Migrator::try_new("conveyor", &migrations).expect("definitions").with_compatibility(8, 1).expect("compatibility").adopt_legacy(LegacyLedger::try_new("_conveyor_migrations").expect("legacy table"));
        migrator.run(&db).await.expect("atomic adoption and pending migration");
        migrator.run(&db).await.expect("repeat startup");
        let after = db.query("SELECT version, description, checksum, applied_at FROM _sqly_migrations WHERE namespace = 'conveyor' ORDER BY version").fetch_all().await.expect("canonical metadata");
        assert_eq!(before.len(), 7);
        assert_eq!(after.len(), 8);
        for (a,b) in before.iter().zip(after.iter()) {
            assert_eq!(a.try_get::<i64>("version").unwrap(), b.try_get::<i64>("version").unwrap());
            for column in ["description", "checksum", "applied_at"] { assert_eq!(a.try_get::<String>(column).unwrap(), b.try_get::<String>(column).unwrap()); }
        }
        assert_eq!(db.query("SELECT public_id FROM feeds").fetch_one().await.unwrap().try_get::<String>("public_id").unwrap(), "feed_fixture");
        db.close().await;
    }
    #[tokio::test]
    async fn sqlite_existing_conveyor_database() {
        let path = std::env::temp_dir().join(format!("sqly-conveyor-adopt-{}.sqlite", std::process::id()));
        adopt(&format!("sqlite:{}?mode=rwc", path.display())).await;
        std::fs::remove_file(path).expect("remove fixture");
    }
    #[tokio::test]
    async fn postgres_existing_conveyor_database() { adopt(&std::env::var("SQLY_TEST_POSTGRES_URL").expect("disposable fixture")).await; }
}
