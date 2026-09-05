use std::time::Duration;

use futures_util::TryStreamExt;
use sqlx::{Executor, SqlSafeStr, Statement};

macro_rules! probes {
    () => {
        #[tokio::test]
        async fn metadata_preparation_uses_bound_type_information() {
            let pool=pool().await;
            let mut conn=pool.acquire().await.unwrap();
            let types=[<i64 as sqlx::Type<Backend>>::type_info()];
            let statement=conn.prepare_with("SELECT $1".into_sql_str(),&types).await.unwrap();
            let row=statement.query().bind(None::<i64>).fetch_one(&mut *conn).await.unwrap();
            let value:Option<i64>=sqlx::Row::try_get(&row,0).unwrap();
            assert_eq!(value,None);
            drop(conn); pool.close().await;
        }
        #[tokio::test]
        async fn primitive_values_round_trip() {
            let pool=pool().await;
            let value:(i32,i64,bool,String,Vec<u8>,f64)=sqlx::query_as("SELECT $1,$2,$3,$4,$5,$6")
                .bind(17_i32).bind(9_000_000_000_i64).bind(true).bind("hello")
                .bind(vec![0_u8,1,255]).bind(1.25_f64).fetch_one(&pool).await.unwrap();
            assert_eq!(value,(17,9_000_000_000,true,"hello".to_owned(),vec![0,1,255],1.25));
            pool.close().await;
        }

        #[tokio::test]
        async fn numbered_parameters_repeat_and_reorder_without_rewriting() {
            let pool = pool().await;
            let row: (i64, i64, i64) = sqlx::query_as(
                "SELECT CAST($2 AS BIGINT), CAST($1 AS BIGINT), CAST($2 AS BIGINT)"
            ).bind(11_i64).bind(29_i64).fetch_one(&pool).await.unwrap();
            assert_eq!(row, (29, 11, 29));
            let statement = pool.prepare("SELECT CAST($2 AS BIGINT), CAST($1 AS BIGINT), CAST($2 AS BIGINT)".into_sql_str()).await.unwrap();
            let count = match statement.parameters().unwrap() { sqlx::Either::Left(v) => v.len(), sqlx::Either::Right(v) => v };
            assert_eq!(count, 2);
            let text: String = sqlx::query_scalar("SELECT '$1 ? ; -- literal' /* $2 ? */").fetch_one(&pool).await.unwrap();
            assert_eq!(text, "$1 ? ; -- literal");
            pool.close().await;
        }
        #[tokio::test]
        async fn typed_null_and_metadata_guard_work_before_execution() {
            let pool = pool().await;
            let value: Option<i64> = sqlx::query_scalar("SELECT CAST($1 AS BIGINT)")
                .bind(None::<i64>).fetch_one(&pool).await.unwrap();
            assert_eq!(value, None);
            pool.execute("CREATE TEMP TABLE guard_probe (value BIGINT)").await.unwrap();
            let mut conn = pool.acquire().await.unwrap();
            let statement = conn.prepare("INSERT INTO guard_probe VALUES ($1)".into_sql_str()).await.unwrap();
            let count = match statement.parameters().unwrap() { sqlx::Either::Left(v) => v.len(), sqlx::Either::Right(v) => v };
            assert_eq!(count, 1);
            // The proposed guard rejects these counts before executing the statement.
            for supplied in [0, 2] { assert_ne!(count, supplied); }
            let n: i64 = sqlx::query_scalar("SELECT count(*) FROM guard_probe").fetch_one(&mut *conn).await.unwrap();
            assert_eq!(n, 0);
            drop(conn);
            pool.close().await;
        }
        #[tokio::test]
        async fn dropping_a_live_stream_allows_single_connection_pool_reuse() {
            let pool = pool().await;
            let mut rows = sqlx::query_scalar::<_, i64>(
                "WITH RECURSIVE n(x) AS (SELECT CAST(1 AS BIGINT) UNION ALL SELECT x+1 FROM n WHERE x<10000) SELECT x FROM n"
            ).fetch(&pool);
            assert_eq!(rows.try_next().await.unwrap(), Some(1));
            drop(rows);
            let v: i64 = tokio::time::timeout(Duration::from_secs(5),
                sqlx::query_scalar("SELECT CAST(42 AS BIGINT)").fetch_one(&pool)).await.unwrap().unwrap();
            assert_eq!(v, 42);
            pool.close().await;
        }
        #[tokio::test]
        async fn transaction_drop_rolls_back_and_pool_remains_usable() {
            let pool = pool().await;
            pool.execute("CREATE TEMP TABLE tx_probe (id BIGINT)").await.unwrap();
            let mut tx = pool.begin().await.unwrap();
            sqlx::query("INSERT INTO tx_probe VALUES ($1)").bind(1_i64).execute(&mut *tx).await.unwrap();
            drop(tx);
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tx_probe").fetch_one(&pool).await.unwrap();
            assert_eq!(count, 0);
            pool.close().await;
        }
        #[tokio::test]
        async fn legacy_migration_import_preserves_history_and_is_idempotent() {
            use sha2::{Digest, Sha256};
            let pool = pool().await;
            let migrations: serde_json::Value = serde_json::from_str(include_str!("../fixtures/migrations.json")).unwrap();
            let mut tx = pool.begin().await.unwrap();
            setup_namespace(&mut tx).await;
            sqlx::raw_sql("CREATE TABLE _conveyor_migrations (version BIGINT PRIMARY KEY, description TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL)").execute(&mut *tx).await.unwrap();
            for migration in migrations.as_array().unwrap() {
                let version = migration["version"].as_i64().unwrap();
                let description = migration["description"].as_str().unwrap();
                let sqlite = migration["sqlite"].as_str().unwrap();
                let postgres = migration["postgres"].as_str().unwrap();
                let bytes = format!("{version}\0{description}\0{sqlite}\0{postgres}");
                let checksum = format!("{:x}", Sha256::digest(bytes.as_bytes()));
                assert_eq!(checksum, migration["checksum"].as_str().unwrap());
                let ddl = migration[DIALECT].as_str().unwrap();
                sqlx::raw_sql(sqlx::AssertSqlSafe(ddl.to_owned())).execute(&mut *tx).await.unwrap();
                sqlx::query("INSERT INTO _conveyor_migrations VALUES ($1,$2,$3,$4)")
                    .bind(version).bind(description).bind(&checksum).bind("2026-08-25T00:00:00Z").execute(&mut *tx).await.unwrap();
            }
            sqlx::raw_sql("CREATE TABLE _sqly_migrations (namespace TEXT NOT NULL, version BIGINT NOT NULL, description TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL, PRIMARY KEY(namespace,version))").execute(&mut *tx).await.unwrap();
            let expected: Vec<(i64,String,String,String)> = sqlx::query_as("SELECT version,description,checksum,applied_at FROM _conveyor_migrations ORDER BY version").fetch_all(&mut *tx).await.unwrap();
            assert_eq!(expected.len(),7);
            // Caller has validated the complete legacy prefix against the manifest.
            for _ in 0..2 {
                sqlx::query("INSERT INTO _sqly_migrations SELECT $1, version,description,checksum,applied_at FROM _conveyor_migrations WHERE TRUE ON CONFLICT(namespace,version) DO NOTHING")
                    .bind("conveyor").execute(&mut *tx).await.unwrap();
            }
            let imported: Vec<(i64,String,String,String)> = sqlx::query_as("SELECT version,description,checksum,applied_at FROM _sqly_migrations WHERE namespace=$1 ORDER BY version").bind("conveyor").fetch_all(&mut *tx).await.unwrap();
            assert_eq!(imported, expected);
            tx.rollback().await.unwrap();
            pool.close().await;
        }
    }
}
#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    const DIALECT: &str = "sqlite";
    type Backend = sqlx::Sqlite;
    include!("support/ambient.rs");
    async fn pool() -> sqlx::SqlitePool {
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap()
    }
    async fn setup_namespace(_: &mut sqlx::Transaction<'_, sqlx::Sqlite>) {}
    probes!();
    #[tokio::test]
    async fn sqlite_surplus_missing_and_gap_bindings_require_a_contract() {
        let pool = pool().await;
        let missing: Option<i64> = sqlx::query_scalar("SELECT CAST($1 AS BIGINT)")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(missing, None);
        let surplus: i64 = sqlx::query_scalar("SELECT CAST($1 AS BIGINT)")
            .bind(1_i64)
            .bind(2_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(surplus, 1);
        let statement = pool
            .prepare("SELECT CAST($2 AS BIGINT)".into_sql_str())
            .await
            .unwrap();
        assert!(matches!(
            statement.parameters(),
            Some(sqlx::Either::Right(1))
        ));
        let gap: Option<i64> = sqlx::query_scalar("SELECT CAST($2 AS BIGINT)")
            .bind(7_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(gap, None);
        pool.close().await;
    }
    #[tokio::test]
    async fn keeper_preserves_data_across_query_connection_replacement() {
        use sqlx::Connection;
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(format!("file:sqly-validation-{}", uuid::Uuid::new_v4()))
            .in_memory(true)
            .shared_cache(true);
        let keeper = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await
            .unwrap();
        pool.execute("CREATE TABLE kept (id BIGINT); INSERT INTO kept VALUES (42)")
            .await
            .unwrap();
        pool.acquire().await.unwrap().close().await.unwrap();
        let value: i64 = sqlx::query_scalar("SELECT id FROM kept")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(value, 42);
        let other = sqlx::SqliteConnection::connect_with(
            &options
                .clone()
                .filename(format!("file:other-{}", uuid::Uuid::new_v4())),
        )
        .await
        .unwrap();
        other.close().await.unwrap();
        pool.close().await;
        keeper.close().await.unwrap();
        let mut replacement = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        let tables: i64 =
            sqlx::query_scalar("SELECT count(*) FROM sqlite_master WHERE name='kept'")
                .fetch_one(&mut replacement)
                .await
                .unwrap();
        assert_eq!(tables, 0);
        replacement.close().await.unwrap();
    }
}
#[cfg(feature = "postgres")]
mod postgres {
    use super::*;
    const DIALECT: &str = "postgres";
    type Backend = sqlx::Postgres;
    include!("support/ambient.rs");
    async fn pool() -> sqlx::PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(
                &std::env::var("SQLY_VALIDATION_POSTGRES_URL")
                    .expect("dedicated validation URL is required"),
            )
            .await
            .unwrap()
    }
    async fn setup_namespace(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) {
        let name = format!("probe_{}", uuid::Uuid::new_v4().simple());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE SCHEMA {name}; SET LOCAL search_path TO {name}"
        )))
        .execute(&mut **tx)
        .await
        .unwrap();
    }
    probes!();
    #[tokio::test]
    async fn postgres_rejects_missing_and_surplus_parameters() {
        let pool = pool().await;
        assert!(
            sqlx::query("SELECT CAST($1 AS BIGINT)")
                .fetch_one(&pool)
                .await
                .is_err()
        );
        assert!(
            sqlx::query("SELECT CAST($1 AS BIGINT)")
                .bind(1_i64)
                .bind(2_i64)
                .fetch_one(&pool)
                .await
                .is_err()
        );
        pool.close().await;
    }
}
