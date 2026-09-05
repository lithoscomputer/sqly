use futures_util::TryStreamExt;
use sqly_validation::{self as sqly, Database, Decode, Encode, FromRow, Row};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
struct UserId(Uuid);
impl Encode for UserId {
    type Repr = Uuid;
    fn encode(&self) -> sqly::Result<Uuid> {
        Ok(self.0)
    }
}
impl Decode for UserId {
    type Repr = Uuid;
    fn decode(v: Uuid) -> sqly::Result<Self> {
        Ok(Self(v))
    }
}
struct User {
    _id: UserId,
}
impl FromRow for User {
    fn from_row(row: &Row) -> sqly::Result<Self> {
        Ok(Self {
            _id: row.try_get("id")?,
        })
    }
}
struct Store {
    db: sqly::ScopedDatabase,
}
impl Store {
    async fn save(&self, id: &UserId) -> sqly::Result<()> {
        self.db
            .query("INSERT INTO users(id) VALUES ($1)")
            .bind(id)
            .execute()
            .await
    }
}
fn is_send<T: Send>(_: T) {}
fn is_send_unpin<T: Send + Unpin>() {}
#[allow(dead_code, reason = "compile-only API ergonomics probe")]
fn signatures(db: &Database, id: UserId) {
    is_send(
        db.query_as::<User>("SELECT id FROM users WHERE id=$1")
            .bind(&id)
            .fetch_optional(),
    );
    let scoped = db.scoped();
    let store = Store { db: scoped.clone() };
    is_send(scoped.write(|| async {
        store.save(&id).await?;
        Ok::<_, sqly::Error>(())
    }));
    is_send(async {
        let mut tx = db.begin_write().await?;
        let mut rows = tx.query_as::<User>("SELECT id FROM users").fetch();
        while let Some(_row) = rows.try_next().await? {}
        drop(rows);
        tx.commit().await
    });
    is_send(Database::connect("sqlite::memory:"));
    is_send(Database::connect(sqly::ConnectOptions));
    is_send(Database::connect(sqly::SqliteOptions));
    is_send(Database::connect(sqly::PostgresOptions));
    const PAIR: sqly::Sql = sqly::Sql::dialects("SELECT id FROM users", "SELECT id FROM users");
    is_send(db.query_as::<User>(PAIR).fetch_optional());
    is_send_unpin::<sqly::RowStream<'static, User>>();
}
#[test]
fn custom_and_nullable_codecs_work_in_a_downstream_crate() {
    let id = UserId(Uuid::new_v4());
    assert_eq!(sqly::round_trip(&id).unwrap(), id);
    assert_eq!(
        sqly::round_trip(&Some(id.clone())).unwrap(),
        Some(id.clone())
    );
    assert_eq!(sqly::round_trip(&None::<UserId>).unwrap(), None);
    assert_eq!((&id).encode().unwrap(), id.0);
    assert_eq!(None::<UserId>.encode().unwrap(), None);
}
