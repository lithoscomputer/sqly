use std::path::Path;

#[cfg(any(not(feature = "sqlite"), not(feature = "postgres")))]
use sqly::Error;
#[cfg(feature = "sqlite")]
use sqly::Result;
use sqly::{ConnectOptions, Database, SqliteOptions};

#[cfg(any(feature = "sqlite", feature = "postgres"))]
mod behavior {
    #[cfg(feature = "postgres")]
    use std::env;
    use std::error::Error as _;
    use std::io;

    #[cfg(feature = "sqlite")]
    use sqly::SqliteOptions;
    use sqly::{Database, Decode, DecodeKind, Encode, Error, FromRow, Result, Row, Sql};
    #[derive(Debug, PartialEq)]
    struct UserId(i64);
    impl Encode for UserId {
        type Repr = i64;
        fn encode(self) -> Result<i64> {
            Ok(self.0)
        }
    }
    impl Encode for &UserId {
        type Repr = i64;
        fn encode(self) -> Result<i64> {
            Ok(self.0)
        }
    }
    impl Decode for UserId {
        type Repr = i64;
        fn decode(value: i64) -> Result<Self> {
            Ok(Self(value))
        }
    }
    #[derive(Debug, PartialEq)]
    struct User {
        id:   UserId,
        name: String,
    }
    impl FromRow for User {
        fn from_row(row: &Row) -> Result<Self> {
            Ok(Self {
                id:   row.try_get("id")?,
                name: row.try_get("name")?,
            })
        }
    }

    async fn crud(db: Database) -> Result<()> {
        db.query("DROP TABLE IF EXISTS sqly_core_users")
            .execute()
            .await?;
        db.query("CREATE TABLE sqly_core_users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, parent BIGINT REFERENCES sqly_core_users(id), CHECK (id > 0))").execute().await?;
        let id = UserId(7);
        let mut name = "Ada".to_owned();
        let query = db
            .query("INSERT INTO sqly_core_users (id, name) VALUES ($1, $2)")
            .bind(&id)
            .bind(name.as_str());
        name.clear(); // The query already owns its binding.
        assert_eq!(query.execute().await?.rows_affected(), 1);
        assert_eq!(
            db.query_as::<User>("SELECT id, name FROM sqly_core_users WHERE id = $1")
                .bind(id)
                .fetch_one()
                .await?,
            User {
                id:   UserId(7),
                name: "Ada".into(),
            }
        );
        assert_eq!(
            db.query_as::<User>("SELECT id, name FROM sqly_core_users")
                .fetch_all()
                .await?
                .len(),
            1
        );
        assert!(
            db.query("SELECT id FROM sqly_core_users WHERE id = $1")
                .bind(99_i64)
                .fetch_optional()
                .await?
                .is_none()
        );
        assert!(matches!(
            db.query("SELECT id FROM sqly_core_users WHERE id = $1")
                .bind(99_i64)
                .fetch_one()
                .await,
            Err(Error::RowNotFound)
        ));
        let error = db
            .query("INSERT INTO sqly_core_users (id, name) VALUES ($1, $2)")
            .bind(7_i64)
            .bind("duplicate")
            .execute()
            .await
            .expect_err("unique constraint");
        assert!(error.is_unique_violation());
        assert!(error.source().is_some());
        assert!(!format!("{error:?}").contains("duplicate"));
        assert!(matches!(
            db.query("INSERT INTO sqly_core_users (id, name) VALUES ($1, $2)")
                .bind(8_i64)
                .bind(None::<String>)
                .execute()
                .await,
            Err(Error::Constraint {
                kind: sqly::ConstraintKind::NotNull,
                ..
            })
        ));
        assert!(matches!(
            db.query("INSERT INTO sqly_core_users (id, name, parent) VALUES ($1, $2, $3)")
                .bind(9_i64)
                .bind("orphan")
                .bind(99_i64)
                .execute()
                .await,
            Err(Error::Constraint {
                kind: sqly::ConstraintKind::ForeignKey,
                ..
            })
        ));
        assert!(matches!(
            db.query("INSERT INTO sqly_core_users (id, name) VALUES ($1, $2)")
                .bind(-1_i64)
                .bind("invalid")
                .execute()
                .await,
            Err(Error::Constraint {
                kind: sqly::ConstraintKind::Check,
                ..
            })
        ));
        assert_eq!(
            db.query("UPDATE sqly_core_users SET name = $1 WHERE id = $2")
                .bind("Grace")
                .bind(7_i64)
                .execute()
                .await?
                .rows_affected(),
            1
        );
        assert_eq!(
            db.query("DELETE FROM sqly_core_users WHERE id = $1")
                .bind(7_i64)
                .execute()
                .await?
                .rows_affected(),
            1
        );
        let clone = db.clone();
        db.close().await;
        assert!(matches!(
            clone.query("SELECT 1").fetch_one().await,
            Err(Error::PoolClosed)
        ));
        Ok(())
    }

    async fn bindings(db: Database) -> Result<()> {
        let row = db
        .query(
            "SELECT $2 AS second, $1 AS first, $1 AS repeated, '$3 ?' AS literal /* $4 */ -- $5\n",
        )
        .bind(11_i64)
        .bind("value")
        .fetch_one()
        .await?;
        assert_eq!(row.try_get::<i64>("first")?, 11);
        assert_eq!(row.try_get::<i32>("repeated")?, 11);
        assert_eq!(row.try_get::<String>("second")?, "value");
        assert_eq!(row.try_get::<String>("literal")?, "$3 ?");
        assert!(matches!(
            db.query("SELECT $1 AS value")
                .bind(1_i64)
                .bind(2_i64)
                .fetch_one()
                .await,
            Err(Error::BindCount {
                expected: Some(1),
                actual:   2,
            })
        ));
        assert!(matches!(
            db.query("SELECT CAST($1 AS BIGINT) AS value")
                .fetch_one()
                .await,
            Err(Error::BindCount {
                expected: Some(1),
                actual:   0,
            })
        ));
        assert!(matches!(
            db.query("SELECT $1 IS NULL AS value")
                .bind(None::<i64>)
                .bind(2_i64)
                .fetch_one()
                .await,
            Err(Error::BindCount {
                expected: Some(1),
                actual:   2,
            })
        ));
        assert!(
            db.query("SELECT $1 IS NULL AS value")
                .bind(None::<i64>)
                .fetch_one()
                .await?
                .try_get::<bool>("value")?
        );
        assert!(matches!(
            db.query("SELECT 1 AS value").bind(1_i64).fetch_one().await,
            Err(Error::BindCount {
                expected: Some(0),
                actual:   1,
            })
        ));
        let row = db
            .query("SELECT $1 AS value")
            .bind(None::<UserId>)
            .fetch_one()
            .await?;
        assert_eq!(row.try_get::<Option<UserId>>("value")?, None);
        assert!(matches!(
            row.try_get::<UserId>("value"),
            Err(Error::Decode {
                kind: DecodeKind::UnexpectedNull,
                ..
            })
        ));
        let row = db
            .query("SELECT $1 AS value")
            .bind(Some(UserId(9)))
            .fetch_one()
            .await?;
        assert_eq!(row.try_get::<Option<UserId>>("value")?, Some(UserId(9)));
        db.close().await;
        Ok(())
    }

    async fn decoding(db: Database) -> Result<()> {
        struct CheckedText;
        impl Decode for CheckedText {
            type Repr = String;
            fn decode(_: String) -> Result<Self> {
                Err(Error::decode_value(io::Error::other("invalid domain text")))
            }
        }

        struct Invalid;
        impl Encode for Invalid {
            type Repr = String;
            fn encode(self) -> Result<String> {
                Err(Error::encode(io::Error::other("typed cause")))
            }
        }

        let row = db
            .query("SELECT $1 AS narrow, $2 AS wide, $3 AS flag, $4 AS bytes, $5 AS float")
            .bind(1_i32)
            .bind(i64::MAX)
            .bind(true)
            .bind(&b"abc"[..])
            .bind(1.5_f64)
            .fetch_one()
            .await?;
        assert_eq!(row.try_get::<i64>("narrow")?, 1);
        assert_eq!(row.try_get::<i64>("wide")?, i64::MAX);
        assert!(row.try_get::<bool>("flag")?);
        assert_eq!(row.try_get::<Vec<u8>>("bytes")?, b"abc");
        assert_eq!(row.try_get::<f64>("float")?.to_bits(), 1.5_f64.to_bits());
        assert!(matches!(
            row.try_get::<i32>("wide"),
            Err(Error::Decode {
                kind: DecodeKind::OutOfRange,
                ..
            })
        ));
        assert!(matches!(
            row.try_get::<String>("wide"),
            Err(Error::Decode {
                kind: DecodeKind::TypeMismatch,
                ..
            })
        ));
        assert!(matches!(
            row.try_get::<i64>("missing"),
            Err(Error::Decode {
                kind: DecodeKind::MissingColumn,
                ..
            })
        ));
        let row = db
            .query("SELECT 1 AS duplicate, 2 AS duplicate")
            .fetch_one()
            .await?;
        assert!(matches!(
            row.try_get::<i64>("duplicate"),
            Err(Error::Decode {
                kind: DecodeKind::DuplicateColumn,
                ..
            })
        ));
        let row = db
            .query(Sql::dialects(
                "SELECT 1 AS enabled",
                "SELECT TRUE AS enabled",
            ))
            .fetch_one()
            .await?;
        assert!(row.try_get::<bool>("enabled")?);
        assert!(matches!(
            db.query("this is not SQL").execute().await,
            Err(Error::InvalidSql { .. })
        ));
        let row = db
            .query("SELECT $1 AS checked")
            .bind("stored")
            .fetch_one()
            .await?;
        let Err(err) = row.try_get::<CheckedText>("checked") else {
            panic!("domain validation");
        };
        assert!(
            err.source()
                .expect("typed decode cause")
                .downcast_ref::<io::Error>()
                .is_some()
        );
        assert!(
            matches!(err, Error::Decode { column: Some(ref column), .. } if column == "checked")
        );
        // Invalid encoding must win even when acquisition would fail.
        db.close().await;
        for value in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert!(matches!(
                db.query("SELECT $1").bind(value).fetch_one().await,
                Err(Error::Encode {
                    parameter: Some(1),
                    ..
                })
            ));
        }
        let err = db
            .query("SELECT $1")
            .bind(Invalid)
            .fetch_one()
            .await
            .expect_err("encoding failure");
        assert!(
            err.source()
                .expect("cause")
                .downcast_ref::<io::Error>()
                .is_some()
        );
        assert!(!format!("{err:?}").contains("typed cause"));
        Ok(())
    }

    async fn integrations(db: Database) -> Result<()> {
        #[cfg(feature = "uuid")]
        {
            let id = uuid::Uuid::from_u128(123);
            let row = db
                .query("SELECT $1 AS value, $2 AS absent")
                .bind(id)
                .bind(None::<uuid::Uuid>)
                .fetch_one()
                .await?;
            assert_eq!(row.try_get::<uuid::Uuid>("value")?, id);
            assert_eq!(row.try_get::<Option<uuid::Uuid>>("absent")?, None);
        }
        #[cfg(feature = "json")]
        {
            let value = serde_json::json!({"items":[1, true, null, "text"]});
            let row = db
                .query("SELECT $1 AS value, $2 AS absent")
                .bind(&value)
                .bind(None::<serde_json::Value>)
                .fetch_one()
                .await?;
            assert_eq!(row.try_get::<serde_json::Value>("value")?, value);
            assert_eq!(row.try_get::<Option<serde_json::Value>>("absent")?, None);
        }
        #[cfg(feature = "time")]
        {
            let instant = time::OffsetDateTime::from_unix_timestamp(1_750_000_000)
                .expect("timestamp")
                .replace_microsecond(123_456)
                .expect("precision");
            let offset = instant.to_offset(time::UtcOffset::from_hms(5, 30, 0).expect("offset"));
            let row = db
                .query("SELECT $1 AS value, $2 AS absent")
                .bind(offset)
                .bind(None::<time::OffsetDateTime>)
                .fetch_one()
                .await?;
            let decoded = row.try_get::<time::OffsetDateTime>("value")?;
            assert_eq!(decoded, instant);
            assert_eq!(decoded.offset(), time::UtcOffset::UTC);
            assert_eq!(row.try_get::<Option<time::OffsetDateTime>>("absent")?, None);
            assert!(matches!(
                db.query("SELECT $1")
                    .bind(instant.replace_nanosecond(1).expect("precision"))
                    .fetch_one()
                    .await,
                Err(Error::Encode { .. })
            ));
        }
        db.close().await;
        Ok(())
    }

    macro_rules! backend_tests {
        ($name:ident, $connect:expr) => {
            mod $name {
                use super::*;
                async fn connect() -> Result<Database> {
                    $connect
                }
                #[tokio::test]
                async fn crud_and_constraints() -> Result<()> {
                    crud(connect().await?).await
                }
                #[tokio::test]
                async fn parameter_contract() -> Result<()> {
                    bindings(connect().await?).await
                }
                #[tokio::test]
                async fn row_and_encoding_errors() -> Result<()> {
                    decoding(connect().await?).await
                }
                #[tokio::test]
                async fn optional_value_integrations() -> Result<()> {
                    integrations(connect().await?).await
                }
            }
        };
    }
    #[cfg(feature = "sqlite")]
    backend_tests!(sqlite, Database::connect(SqliteOptions::in_memory()).await);
    #[cfg(feature = "postgres")]
    backend_tests!(
        postgres,
        Database::connect(
            env::var("SQLY_TEST_POSTGRES_URL")
                .expect("PostgreSQL tests require SQLY_TEST_POSTGRES_URL; use mise run test")
                .as_str()
        )
        .await
    );
}
#[test]
fn configuration_rejects_unknown_duplicate_and_invalid_options_without_echoing_credentials() {
    for url in [
        "postgres://user:secret@localhost/db?typo=yes",
        "postgres://user:secret@localhost/db?sslmode=disable&sslmode=verify-full",
        "postgres://user:secret@localhost/db?sslmode=unknown",
        "postgres://localhost/db",
        "sqlite://file?typo=yes",
        "sqlite::memory:?mode=rwc",
    ] {
        let error = url
            .parse::<ConnectOptions>()
            .expect_err("invalid configuration");
        assert!(!format!("{error:?} {error}").contains("secret"));
    }
    let options: ConnectOptions = "postgres://user:secret@localhost/db"
        .parse()
        .expect("valid URL");
    assert!(!format!("{options:?}").contains("secret"));
}
#[cfg(not(feature = "sqlite"))]
#[tokio::test]
async fn disabled_sqlite_fails_before_connection() {
    assert!(matches!(
        Database::connect(SqliteOptions::in_memory()).await,
        Err(Error::UnsupportedBackend { backend: "sqlite" })
    ));
}
#[cfg(not(feature = "postgres"))]
#[tokio::test]
async fn disabled_postgres_fails_before_connection() {
    assert!(matches!(
        Database::connect("postgres://user:secret@localhost/db").await,
        Err(Error::UnsupportedBackend {
            backend: "postgres",
        })
    ));
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn memory_instances_are_isolated_and_require_one_query_connection() -> Result<()> {
    assert!(
        Database::builder()
            .max_connections(2)
            .connect(SqliteOptions::in_memory())
            .await
            .is_err()
    );
    let first = Database::connect(SqliteOptions::in_memory()).await?;
    let second = Database::connect("sqlite::memory:").await?;
    first
        .query("CREATE TABLE isolated (value INTEGER)")
        .execute()
        .await?;
    assert!(
        second
            .query("SELECT value FROM isolated")
            .fetch_one()
            .await
            .is_err()
    );
    first.close().await;
    second.close().await;
    Ok(())
}

#[test]
fn application_connection_options_preserve_supported_transports_and_file_policy() {
    for mode in [
        "verify-full",
        "verify-ca",
        "require",
        "prefer",
        "allow",
        "disable",
    ] {
        let url = format!("postgres://user:password@%2Fvar%2Frun%2Fpostgresql/db?sslmode={mode}");
        assert!(
            ConnectOptions::try_from(url.as_str())
                .expect("supported transport and TLS mode")
                .as_postgres()
                .is_some()
        );
    }
    let options = ConnectOptions::try_from("sqlite:application%20data/db.sqlite?mode=rwc")
        .expect("file options");
    assert_eq!(
        options.as_sqlite().expect("SQLite").filename(),
        Some(Path::new("application data/db.sqlite"))
    );
    assert!(SqliteOptions::in_memory().filename().is_none());
}
