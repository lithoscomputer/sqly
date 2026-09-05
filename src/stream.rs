use std::fmt;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use futures_util::StreamExt as _;
use futures_util::stream::once;

#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::driver;
use crate::query::Target;
use crate::value::private::Value;
use crate::{FromRow, Query, Result, Row};

pub(crate) type Cursor<'a> = Pin<Box<dyn Stream<Item = Result<Row>> + Send + 'a>>;
/// An incremental result. Execution begins on first poll and stops after the
/// first error. Drop releases or discards the connection. An explicit stream
/// retains its mutable transaction borrow until dropped.
///
/// ```no_run
/// # async fn example(db: &sqly::Database) -> sqly::Result<()> {
/// use futures_util::TryStreamExt;
/// let mut rows = db.query("SELECT name FROM users ORDER BY name").fetch();
/// while let Some(row) = rows.try_next().await? {
///     let name: String = row.try_get("name")?;
///     // Export this row before polling the next one.
/// }
/// # Ok(()) }
/// ```
///
/// ```compile_fail
/// # async fn example(db: sqly::Database) -> sqly::Result<()> {
/// let mut tx = db.begin_write().await?;
/// let rows = tx.query("SELECT 1").fetch();
/// tx.commit().await?;
/// drop(rows);
/// # Ok(()) }
/// ```
#[must_use = "streams execute only when polled"]
pub struct RowStream<'a, T, F = fn(&Row) -> Result<T>> {
    rows:   Option<Cursor<'a>>,
    mapper: Box<F>,
    marker: PhantomData<fn() -> T>,
}
impl<'a, T: FromRow> Query<'a, T> {
    /// Begin an incremental query on first poll. The first database or mapping
    /// error ends the stream. Early drop conservatively aborts an explicit or
    /// ambient transaction. See `RowStream` for borrowing and cleanup behavior.
    pub fn fetch(self) -> RowStream<'a, T> {
        RowStream::mapped(self, T::from_row as fn(&Row) -> Result<T>)
    }
}
impl<'a, T> Query<'a, T> {
    pub(crate) fn into_cursor(self) -> Cursor<'a> {
        let rows: Cursor<'a> = if let Some(error) = self.error {
            Box::pin(once(async { Err(error) }))
        } else {
            #[cfg(any(feature = "sqlite", feature = "postgres"))]
            let sql = self.sql;
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            let sql = crate::Sql::from("");
            match self.target {
                Target::Database(db) => database(db, sql, self.values),
                Target::Transaction(tx) => tx.rows(sql, self.values),
                #[cfg(feature = "ambient")]
                Target::Scoped(db, read) => db.rows(sql, self.values, read),
            }
        };
        rows
    }
}
impl<'a, T, F> RowStream<'a, T, F> {
    pub(crate) fn mapped<U>(query: Query<'a, U>, mapper: F) -> Self {
        Self {
            rows:   Some(query.into_cursor()),
            mapper: Box::new(mapper),
            marker: PhantomData,
        }
    }
}
impl<T, F: FnMut(&Row) -> Result<T>> Stream for RowStream<'_, T, F> {
    type Item = Result<T>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(rows) = &mut this.rows else {
            return Poll::Ready(None);
        };
        let result = match rows.as_mut().poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(row) => row.map(|row| row.and_then(|row| (this.mapper)(&row))),
        };
        if result.is_none() || matches!(result, Some(Err(_))) {
            this.rows = None;
        }
        Poll::Ready(result)
    }
}
impl<T, F> fmt::Debug for RowStream<'_, T, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RowStream").finish_non_exhaustive()
    }
}
pub(crate) fn database(db: &crate::Database, sql: crate::Sql, values: Vec<Value>) -> Cursor<'_> {
    Box::pin(async_stream::try_stream! {
        db.check().await?;
        #[cfg(any(feature = "sqlite", feature = "postgres"))]
        {
            let mut rows = driver::rows(db, sql, values);
            while let Some(row) = rows.next().await { yield row?; }
        }
        #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
        { let _ = (sql, values); }
    })
}
