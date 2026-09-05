use std::fmt;
use std::marker::PhantomData;

#[cfg(feature = "ambient")]
use crate::ScopedDatabase;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::database::Inner;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::driver;
use crate::value::private::Value;
use crate::{Database, Encode, Error, FromRow, Result, Row, Sql, Transaction};

/// An owned set of bindings and a borrowed execution target.
#[must_use = "queries execute only when a terminal method is awaited"]
pub struct Query<'a, T> {
    pub(crate) target: Target<'a>,
    #[cfg(any(feature = "sqlite", feature = "postgres"))]
    pub(crate) sql:    Sql,
    pub(crate) values: Vec<Value>,
    pub(crate) error:  Option<Error>,
    marker:            PhantomData<fn() -> T>,
}
pub(crate) enum Target<'a> {
    Database(&'a Database),
    Transaction(&'a mut Transaction),
    #[cfg(feature = "ambient")]
    Scoped(&'a ScopedDatabase, bool),
}
/// Portable execution metadata. Trigger counts remain backend-specific.
#[derive(Clone, Copy, Debug)]
pub struct ExecuteResult {
    affected: u64,
}
impl ExecuteResult {
    pub fn rows_affected(&self) -> u64 {
        self.affected
    }
}
pub(crate) enum Mode {
    Execute,
    Optional,
    All,
}
pub(crate) struct Output {
    pub(crate) rows:     Vec<Row>,
    pub(crate) affected: u64,
}
impl<'a, T> Query<'a, T> {
    pub(crate) fn new(db: &'a Database, sql: Sql) -> Self {
        let _ = sql;
        Self {
            target: Target::Database(db),
            #[cfg(any(feature = "sqlite", feature = "postgres"))]
            sql,
            values: Vec::new(),
            error: None,
            marker: PhantomData,
        }
    }
    pub(crate) fn transaction(tx: &'a mut Transaction, sql: Sql) -> Self {
        let _ = sql;
        Self {
            target: Target::Transaction(tx),
            #[cfg(any(feature = "sqlite", feature = "postgres"))]
            sql,
            values: Vec::new(),
            error: None,
            marker: PhantomData,
        }
    }
    #[cfg(feature = "ambient")]
    pub(crate) fn scoped(db: &'a ScopedDatabase, sql: Sql, read: bool) -> Self {
        let mut query = Self::new(&db.db, sql);
        query.target = Target::Scoped(db, read);
        query
    }
    #[cfg(feature = "ambient")]
    pub(crate) fn values(db: &'a Database, sql: Sql, values: Vec<Value>) -> Self {
        let mut query = Self::new(db, sql);
        query.values = values;
        query
    }
    /// Convert and copy a parameter now; surface the first failure before I/O.
    pub fn bind(mut self, value: impl Encode) -> Self {
        if self.error.is_none() {
            match value
                .encode()
                .and_then(super::value::private::Representation::into_value)
            {
                Ok(v) => self.values.push(v),
                Err(e) => self.error = Some(e.at_parameter(self.values.len() + 1)),
            }
        }
        self
    }
    pub(crate) async fn run(self, mode: Mode) -> Result<Output> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let db = match self.target {
            Target::Database(db) => db,
            #[cfg(feature = "ambient")]
            Target::Scoped(db, read) => {
                #[cfg(any(feature = "sqlite", feature = "postgres"))]
                return Box::pin(db.run(self.sql, self.values, mode, read)).await;
                #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
                return Box::pin(db.run(Sql::from(""), self.values, mode, read)).await;
            }
            Target::Transaction(tx) => {
                #[cfg(any(feature = "sqlite", feature = "postgres"))]
                return tx.run(self.sql, self.values, mode).await;
                #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
                return tx.run(Sql::from(""), self.values, mode).await;
            }
        };
        db.check().await?;
        match db.inner.as_ref() {
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => {
                let _ = mode;
                match *db.inner {}
            }
            #[cfg(feature = "sqlite")]
            Inner::Sqlite { pool, .. } => {
                driver::sqlite(pool, self.sql.sqlite, self.values, mode).await
            }
            #[cfg(feature = "postgres")]
            Inner::Postgres(pool) => {
                driver::postgres(pool, self.sql.postgres, self.values, mode).await
            }
        }
    }
}
impl<T: FromRow> Query<'_, T> {
    pub async fn fetch_optional(self) -> Result<Option<T>> {
        self.run(Mode::Optional)
            .await?
            .rows
            .first()
            .map(T::from_row)
            .transpose()
    }
    pub async fn fetch_one(self) -> Result<T> {
        self.fetch_optional().await?.ok_or(Error::RowNotFound)
    }
    pub async fn fetch_all(self) -> Result<Vec<T>> {
        self.run(Mode::All)
            .await?
            .rows
            .iter()
            .map(T::from_row)
            .collect()
    }
}
impl Query<'_, Row> {
    pub async fn execute(self) -> Result<ExecuteResult> {
        Ok(ExecuteResult {
            affected: self.run(Mode::Execute).await?.affected,
        })
    }
}
impl<T> fmt::Debug for Query<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Query")
            .field("bindings", &self.values.len())
            .finish_non_exhaustive()
    }
}
