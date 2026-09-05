use std::fmt;
use std::marker::PhantomData;

use crate::{Encode, Error, Query, Result, Row, RowStream};

/// A query whose rows are converted by a caller-provided fallible closure.
/// Mapping runs outside ambient scope guards. Buffered mapping failures do not
/// abort the transaction; stream mapping failures end the stream and can make
/// the transaction rollback-only. Closures may borrow application state.
///
/// ```no_run
/// # async fn example(db: &sqly::Database) -> sqly::Result<()> {
/// let count: i64 = db
///     .query("SELECT COUNT(*) AS count FROM users")
///     .try_map(|row| row.try_get::<i64>("count"))
///     .fetch_one()
///     .await?;
/// # Ok(()) }
/// ```
///
/// ```compile_fail
/// # async fn example(db: &sqly::Database) -> sqly::Result<()> {
/// db.query("SELECT 1 AS value").try_map(|row| row.try_get::<i64>("value"))
///     .execute().await?;
/// # Ok(()) }
/// ```
#[must_use = "queries execute only when awaited or streamed"]
pub struct MappedQuery<'a, T, F> {
    query:  Query<'a, Row>,
    mapper: F,
    marker: PhantomData<fn() -> T>,
}
impl<'a> Query<'a, Row> {
    /// Convert each row using a fallible closure, including streamed results.
    /// Use `query_as` and `FromRow` for reusable record mappings.
    pub fn try_map<T, F: FnMut(&Row) -> Result<T>>(self, mapper: F) -> MappedQuery<'a, T, F> {
        MappedQuery {
            query: self,
            mapper,
            marker: PhantomData,
        }
    }
}
impl<'a, T, F: FnMut(&Row) -> Result<T>> MappedQuery<'a, T, F> {
    /// Bind a value before executing the mapped query.
    pub fn bind(self, value: impl Encode) -> Self {
        Self {
            query: self.query.bind(value),
            ..self
        }
    }
    /// Return the first mapped row, or None. This does not assert uniqueness.
    pub async fn fetch_optional(mut self) -> Result<Option<T>> {
        self.query
            .fetch_optional()
            .await?
            .as_ref()
            .map(&mut self.mapper)
            .transpose()
    }
    /// Return the first mapped row, or `RowNotFound`. Extra rows are ignored.
    pub async fn fetch_one(self) -> Result<T> {
        self.fetch_optional().await?.ok_or(Error::RowNotFound)
    }
    /// Buffer all rows and map them in order, stopping at the first error.
    pub async fn fetch_all(mut self) -> Result<Vec<T>> {
        self.query
            .fetch_all()
            .await?
            .iter()
            .map(&mut self.mapper)
            .collect()
    }
    /// Stream mapped rows. Execution and mapping begin on first poll.
    pub fn fetch(self) -> RowStream<'a, T, F> {
        RowStream::mapped(self.query, self.mapper)
    }
}
impl<T, F> fmt::Debug for MappedQuery<'_, T, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MappedQuery").finish_non_exhaustive()
    }
}
