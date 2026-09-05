//! A separate application schema that consumes only sqly's public API.
use std::io;

use futures_util::TryStreamExt as _;
use sqly::migrate::{Migration, MigrationResult, Migrator};
use sqly::{Database, Decode, Encode, FromRow, Lock, Result, Row, ScopedDatabase};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sku(String);
impl Sku {
    pub fn parse(value: &str) -> Result<Self> {
        if value.is_empty()
            || !value
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err(sqly::Error::encode(io::Error::other("invalid SKU")));
        }
        Ok(Self(value.to_owned()))
    }
}
impl Encode for Sku {
    type Repr = String;
    fn encode(self) -> Result<String> {
        Ok(self.0)
    }
}
impl Encode for &Sku {
    type Repr = String;
    fn encode(self) -> Result<String> {
        Ok(self.0.clone())
    }
}
impl Decode for Sku {
    type Repr = String;
    fn decode(value: String) -> Result<Self> {
        Self::parse(&value).map_err(sqly::Error::decode_value)
    }
}
#[derive(Debug)]
pub struct Stock {
    pub sku:       Sku,
    pub available: i64,
}
impl FromRow for Stock {
    fn from_row(row: &Row) -> Result<Self> {
        Ok(Self {
            sku:       row.try_get("sku")?,
            available: row.try_get("available")?,
        })
    }
}
#[derive(Debug)]
pub enum ReserveError {
    Database(sqly::Error),
    InsufficientStock,
}
impl From<sqly::Error> for ReserveError {
    fn from(error: sqly::Error) -> Self {
        Self::Database(error)
    }
}
pub struct Warehouse {
    db:           ScopedDatabase,
    reservations: Reservations,
}
struct Reservations {
    db: ScopedDatabase,
}
impl Reservations {
    async fn insert(&self, id: i64, sku: &Sku, quantity: i64) -> Result<()> {
        self.db
            .query("INSERT INTO warehouse_reservations (id, sku, quantity) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(sku)
            .bind(quantity)
            .execute()
            .await?;
        Ok(())
    }
}
impl Warehouse {
    pub fn new(db: ScopedDatabase) -> Self {
        Self {
            reservations: Reservations { db: db.clone() },
            db,
        }
    }
    pub async fn reserve(
        &self,
        id: i64,
        sku: &Sku,
        quantity: i64,
    ) -> std::result::Result<(), ReserveError> {
        if quantity <= 0 {
            return Err(ReserveError::InsufficientStock);
        }
        self.db
            .write_locking(Lock::row("warehouse_stock").key("sku", sku), || async {
                let stock = self
                    .db
                    .read_as::<Stock>("SELECT sku, available FROM warehouse_stock WHERE sku = $1")
                    .bind(sku)
                    .fetch_one()
                    .await?;
                if stock.available < quantity {
                    return Err(ReserveError::InsufficientStock);
                }
                self.db
                    .query("UPDATE warehouse_stock SET available = available - $1 WHERE sku = $2")
                    .bind(quantity)
                    .bind(sku)
                    .execute()
                    .await?;
                self.reservations.insert(id, sku, quantity).await?;
                Ok(())
            })
            .await
    }
    pub async fn report(&self) -> Result<Vec<Stock>> {
        self.db
            .read_as::<Stock>("SELECT sku, available FROM warehouse_stock ORDER BY sku")
            .fetch()
            .try_collect()
            .await
    }
}
pub async fn initialize(db: &Database) -> MigrationResult<()> {
    let migrations = [Migration::new(
        1,
        "warehouse stock and reservations",
        "CREATE TABLE warehouse_stock (sku TEXT PRIMARY KEY, available BIGINT NOT NULL CHECK (available >= 0)); CREATE TABLE warehouse_reservations (id BIGINT PRIMARY KEY, sku TEXT NOT NULL REFERENCES warehouse_stock(sku), quantity BIGINT NOT NULL CHECK (quantity > 0))",
    )];
    Migrator::try_new("warehouse", &migrations)?.run(db).await
}

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
mod tests {
    use super::*;
    async fn exercise(db: Database) {
        initialize(&db).await.expect("application migrations");
        let sku = Sku::parse("BOLT-M8").expect("SKU");
        let mut tx = db.begin_write().await.expect("explicit seed transaction");
        tx.query("INSERT INTO warehouse_stock VALUES ($1, $2)")
            .bind(&sku)
            .bind(10_i64)
            .execute()
            .await
            .expect("seed stock");
        tx.commit().await.expect("commit seed");
        let warehouse = Warehouse::new(db.scoped());
        warehouse.reserve(1, &sku, 3).await.expect("reserve stock");
        assert!(matches!(
            warehouse.reserve(2, &sku, 20).await,
            Err(ReserveError::InsufficientStock)
        ));
        // The failed second store call must undo the first store's stock update.
        assert!(
            matches!(warehouse.reserve(1, &sku, 2).await, Err(ReserveError::Database(error)) if error.is_unique_violation())
        );
        let stocks = warehouse.report().await.expect("stream report");
        assert_eq!(stocks.len(), 1);
        assert_eq!(stocks[0].sku, sku);
        assert_eq!(stocks[0].available, 7);
        initialize(&db)
            .await
            .expect("idempotent application migrations");
        db.close().await;
    }
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_consumer() {
        exercise(
            Database::connect(sqly::SqliteOptions::in_memory())
                .await
                .expect("SQLite"),
        )
        .await;
    }
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_consumer() {
        let db = Database::connect(
            std::env::var("SQLY_TEST_POSTGRES_URL")
                .expect("disposable PostgreSQL fixture")
                .as_str(),
        )
        .await
        .expect("PostgreSQL");
        db.query("DROP TABLE IF EXISTS warehouse_reservations")
            .execute()
            .await
            .expect("reset reservations");
        db.query("DROP TABLE IF EXISTS warehouse_stock")
            .execute()
            .await
            .expect("reset stock");
        // The parent test suite creates the canonical ledger; this fixture also
        // supports a fresh service when run on its own.
        let exists: bool = db
            .query("SELECT to_regclass('_sqly_migrations') IS NOT NULL AS present")
            .fetch_one()
            .await
            .expect("ledger lookup")
            .try_get("present")
            .expect("boolean");
        if exists {
            db.query("DELETE FROM _sqly_migrations WHERE namespace = 'warehouse'")
                .execute()
                .await
                .expect("reset namespace");
        }
        exercise(db).await;
    }
}
