use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::task::{Context, Poll};
use std::{fmt, result};

use futures_core::Stream;
use futures_util::StreamExt as _;
use tokio::sync::Mutex;

use crate::query::{Mode, Output};
use crate::stream::{self, Cursor};
use crate::value::private::Value;
use crate::{
    Database, Encode, Error, FromRow, Lock, Query, Result, Row, RowStream, Sql, Transaction,
};

tokio::task_local! { static ACTIVE: Arc<Scope>; }
struct Scope {
    db:          Database,
    transaction: Arc<Mutex<Option<Transaction>>>,
    cursor:      StdMutex<Option<ScopedCursor>>,
    operation:   StdMutex<Option<Operation>>,
    serial:      Mutex<()>,
    closed:      AtomicBool,
}
struct ScopedCursor {
    token: Arc<()>,
    rows:  Cursor<'static>,
}
struct ScopeOwner(Arc<Scope>);
impl Drop for ScopeOwner {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        self.0
            .cursor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        self.0
            .operation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Ok(mut transaction) = self.0.transaction.try_lock() {
            transaction.take();
        }
    }
}
impl Scope {
    fn available(&self) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::ScopeDatabaseMismatch);
        }
        if self
            .cursor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
        {
            return Err(Error::ActiveStream);
        }
        Ok(())
    }
}
/// A database handle whose writes require a task-local write scope.
/// Clones share database identity. Spawned tasks inherit no scope.
///
/// ```no_run
/// # async fn example(db: sqly::Database) -> sqly::Result<()> {
/// let store = db.scoped();
/// let id = 7_i64;
/// store
///     .write(|| async {
///         store
///             .query("INSERT INTO users (id) VALUES ($1)")
///             .bind(id)
///             .execute()
///             .await?;
///         Ok::<_, sqly::Error>(())
///     })
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub struct ScopedDatabase {
    pub(crate) db: Database,
}
/// A caller-declared read query, allowed to use the pool outside a write scope.
/// SQL must be read-only. Sqly does not inspect or police it.
///
/// ```compile_fail
/// # async fn example(db: sqly::ScopedDatabase) -> sqly::Result<()> {
/// db.read("SELECT 1").execute().await?;
/// # Ok(()) }
/// ```
#[must_use = "queries execute only when a terminal method is awaited"]
pub struct ReadQuery<'a, T>(Query<'a, T>);
impl<'a> ReadQuery<'a, Row> {
    /// Map rows with a fallible closure. The mapped query has no execute
    /// method.
    pub fn try_map<T, F: FnMut(&Row) -> Result<T>>(
        self,
        mapper: F,
    ) -> crate::MappedQuery<'a, T, F> {
        self.0.try_map(mapper)
    }
}
impl<T> ReadQuery<'_, T> {
    pub fn bind(self, value: impl Encode) -> Self {
        Self(self.0.bind(value))
    }
}
impl<'a, T: FromRow> ReadQuery<'a, T> {
    /// Stream rows on first poll. Early drop can make the scope rollback-only;
    /// a live ambient stream rejects competing operations with `ActiveStream`.
    pub fn fetch(self) -> RowStream<'a, T> {
        self.0.fetch()
    }
    /// Return the first row or `RowNotFound`; does not check uniqueness.
    /// Scope membership resolves when awaited. See `ScopedDatabase::read`.
    pub async fn fetch_one(self) -> Result<T> {
        self.0.fetch_one().await
    }
    /// Return the first row or None; additional rows are ignored.
    /// Scope membership resolves when awaited. See `ScopedDatabase::read`.
    pub async fn fetch_optional(self) -> Result<Option<T>> {
        self.0.fetch_optional().await
    }
    /// Buffer all rows. Callers must bound result size. Local conversion errors
    /// do not abort the scope; database failures do.
    pub async fn fetch_all(self) -> Result<Vec<T>> {
        self.0.fetch_all().await
    }
}
impl Database {
    /// Create a cloned ambient handle. Give stores this handle to keep
    /// transaction parameters out of their method signatures.
    pub fn scoped(&self) -> ScopedDatabase {
        ScopedDatabase { db: self.clone() }
    }
}
impl ScopedDatabase {
    /// Build a query that requires a matching active write scope at execution,
    /// including for SELECT. Builders do not capture scope membership.
    pub fn query(&self, sql: impl Into<Sql>) -> Query<'_, Row> {
        self.query_as(sql)
    }
    /// Build a typed query with the same scope requirements as `Self::query`.
    pub fn query_as<T: FromRow>(&self, sql: impl Into<Sql>) -> Query<'_, T> {
        Query::scoped(self, sql.into(), false)
    }
    /// Build caller-declared read-only SQL. At execution this joins a matching
    /// scope, or uses the pool outside one. Sqly does not inspect SQL.
    pub fn read(&self, sql: impl Into<Sql>) -> ReadQuery<'_, Row> {
        self.read_as(sql)
    }
    /// Build a typed read with the same scope behavior as `Self::read`.
    pub fn read_as<T: FromRow>(&self, sql: impl Into<Sql>) -> ReadQuery<'_, T> {
        ReadQuery(Query::scoped(self, sql.into(), true))
    }
    /// Run application work atomically, committing on success and rolling back
    /// on error, panic, or cancellation. Application errors retain precedence
    /// if rollback also fails. Caught database errors make the scope
    /// rollback-only.
    ///
    /// Nested scopes fail before acquisition. Spawned tasks inherit no scope.
    /// Concurrent buffered operations in one task serialize. An active stream
    /// rejects competitors with `ActiveStream`; successful return with an
    /// active stream invalidates it and rolls back. Early stream drop
    /// aborts the scope. Cancellation during COMMIT can leave its outcome
    /// unknown. Never retries.
    pub async fn write<T, E, F, Fut>(&self, f: F) -> result::Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = result::Result<T, E>>,
        E: From<Error>,
    {
        self.enter(None, f).await
    }
    /// Acquire a required owner-row lock before invoking application work.
    /// A missing row returns `LockNotFound` without invoking the closure.
    /// All other lifecycle and cancellation behavior follows `Self::write`.
    pub async fn write_locking<T, E, F, Fut>(&self, lock: Lock, f: F) -> result::Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = result::Result<T, E>>,
        E: From<Error>,
    {
        self.enter(Some(lock), f).await
    }
    async fn enter<T, E, F, Fut>(&self, lock: Option<Lock>, f: F) -> result::Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = result::Result<T, E>>,
        E: From<Error>,
    {
        if ACTIVE.try_with(|_| ()).is_ok() {
            return Err(Error::NestedWriteScope.into());
        }
        let mut tx = self.db.begin_write().await?;
        if let Some(lock) = lock
            && let Err(error) = tx.require_lock(lock).await
        {
            let _ = tx.rollback().await;
            return Err(error.into());
        }
        let state = Arc::new(Scope {
            db:          self.db.clone(),
            transaction: Arc::new(Mutex::new(Some(tx))),
            cursor:      StdMutex::new(None),
            operation:   StdMutex::new(None),
            serial:      Mutex::new(()),
            closed:      AtomicBool::new(false),
        });
        let _owner = ScopeOwner(state.clone());
        // Invoke the closure inside the scope, without holding the connection
        // mutex. This also covers closures that perform work before returning a future.
        let result = ACTIVE.scope(state.clone(), async { f().await }).await;
        state.closed.store(true, Ordering::Release);
        let active_stream = state
            .cursor
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .is_some();
        state
            .operation
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        let tx = state
            .transaction
            .lock()
            .await
            .take()
            .ok_or(Error::TransactionAborted)?;
        match result {
            Ok(_) if active_stream => {
                let _ = tx.rollback().await;
                Err(Error::ActiveStream.into())
            }
            Ok(value) => {
                tx.commit().await?;
                Ok(value)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
    fn active(&self) -> Result<Option<Arc<Scope>>> {
        let scope = ACTIVE.try_with(Arc::clone).ok();
        if scope
            .as_ref()
            .is_some_and(|scope| !Arc::ptr_eq(&scope.db.inner, &self.db.inner))
        {
            return Err(Error::ScopeDatabaseMismatch);
        }
        Ok(scope)
    }
    /// Require an existing row lock, returning `LockNotFound` if absent.
    /// Absence alone does not make the transaction rollback-only. Acquisition
    /// can wait; dependent reads must follow it. Other lock errors are
    /// preserved.
    pub async fn require_lock(&self, lock: Lock) -> Result<()> {
        if self.lock(lock).await? {
            Ok(())
        } else {
            Err(Error::LockNotFound)
        }
    }
    /// Acquire a row lock in the active scope, returning false if absent.
    /// Acquisition can wait. Read dependent rows after acquiring their owner.
    /// Without a scope this returns `NoActiveWriteScope`.
    pub async fn lock(&self, lock: Lock) -> Result<bool> {
        let scope = self.active()?.ok_or(Error::NoActiveWriteScope)?;
        let slot = scope.transaction.clone();
        let result = scope
            .operate(Box::pin(async move {
                let mut guard = slot.lock().await;
                let locked = guard
                    .as_mut()
                    .ok_or(Error::TransactionAborted)?
                    .lock(lock)
                    .await?;
                Ok(OperationOutput::Locked(locked))
            }))
            .await?;
        match result {
            OperationOutput::Locked(locked) => Ok(locked),
            OperationOutput::Query(_) => unreachable!("lock operation result"),
        }
    }
    pub(crate) fn rows(&self, sql: Sql, values: Vec<Value>, read: bool) -> Cursor<'_> {
        Box::pin(ScopedRows {
            db: self.clone(),
            input: Some((sql, values)),
            read,
            bound: None,
            pooled: None,
            done: false,
        })
    }
    pub(crate) async fn run(
        &self,
        sql: Sql,
        values: Vec<Value>,
        mode: Mode,
        read: bool,
    ) -> Result<Output> {
        if let Some(scope) = self.active()? {
            let slot = scope.transaction.clone();
            let result = scope
                .operate(Box::pin(async move {
                    let mut guard = slot.lock().await;
                    let output = guard
                        .as_mut()
                        .ok_or(Error::TransactionAborted)?
                        .run(sql, values, mode)
                        .await?;
                    Ok(OperationOutput::Query(output))
                }))
                .await?;
            match result {
                OperationOutput::Query(output) => Ok(output),
                OperationOutput::Locked(_) => unreachable!("query operation result"),
            }
        } else if read {
            Query::<Row>::values(&self.db, sql, values).run(mode).await
        } else {
            Err(Error::NoActiveWriteScope)
        }
    }
}
impl fmt::Debug for ScopedDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ScopedDatabase(<redacted>)")
    }
}
impl<T> fmt::Debug for ReadQuery<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReadQuery").field(&self.0).finish()
    }
}

struct ScopedRows {
    db:     ScopedDatabase,
    input:  Option<(Sql, Vec<Value>)>,
    read:   bool,
    bound:  Option<(Arc<Scope>, Arc<()>)>,
    pooled: Option<Cursor<'static>>,
    done:   bool,
}
impl ScopedRows {
    fn poll(&mut self, cx: &mut Context<'_>) -> Result<Poll<Option<Result<Row>>>> {
        let current = self.db.active()?;
        if let Some((bound, _)) = &self.bound {
            if bound.closed.load(Ordering::Acquire)
                || !current.as_ref().is_some_and(|s| Arc::ptr_eq(s, bound))
            {
                return Err(Error::ScopeDatabaseMismatch);
            }
        } else if self.pooled.is_some() {
            if current.is_some() {
                return Err(Error::ScopeDatabaseMismatch);
            }
        } else {
            let (sql, values) = self.input.take().expect("unstarted stream input");
            if let Some(scope) = current {
                scope.available()?;
                let token = Arc::new(());
                let slot = scope.transaction.clone();
                let rows = Box::pin(async_stream::try_stream! {
                    let mut guard = slot.lock_owned().await;
                    let tx = guard.as_mut().ok_or(Error::TransactionAborted)?;
                    let mut rows = tx.rows(sql, values);
                    while let Some(row) = rows.next().await { yield row?; }
                });
                *scope.cursor.lock().unwrap_or_else(PoisonError::into_inner) = Some(ScopedCursor {
                    token: token.clone(),
                    rows,
                });
                self.bound = Some((scope, token));
            } else if self.read {
                let db = self.db.db.clone();
                self.pooled = Some(Box::pin(async_stream::try_stream! {
                    let mut rows = stream::database(&db, sql, values);
                    while let Some(row) = rows.next().await { yield row?; }
                }));
            } else {
                return Err(Error::NoActiveWriteScope);
            }
        }
        if let Some((scope, token)) = &self.bound {
            let mut cursor = scope.cursor.lock().unwrap_or_else(PoisonError::into_inner);
            let cursor = cursor
                .as_mut()
                .filter(|c| Arc::ptr_eq(&c.token, token))
                .ok_or(Error::ScopeDatabaseMismatch)?;
            Ok(cursor.rows.as_mut().poll_next(cx))
        } else {
            Ok(self
                .pooled
                .as_mut()
                .expect("pooled stream")
                .as_mut()
                .poll_next(cx))
        }
    }
    fn finish(&mut self) {
        if let Some((scope, token)) = self.bound.take() {
            let mut cursor = scope.cursor.lock().unwrap_or_else(PoisonError::into_inner);
            if cursor
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(&c.token, &token))
            {
                cursor.take();
            }
        }
        self.pooled = None;
        self.done = true;
    }
}
impl Stream for ScopedRows {
    type Item = Result<Row>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let result = this
            .poll(cx)
            .unwrap_or_else(|error| Poll::Ready(Some(Err(error))));
        if matches!(result, Poll::Ready(None | Some(Err(_)))) {
            this.finish();
        }
        result
    }
}
impl Drop for ScopedRows {
    fn drop(&mut self) {
        self.finish();
    }
}

enum OperationOutput {
    Query(Output),
    Locked(bool),
}
type OperationFuture = Pin<Box<dyn Future<Output = Result<OperationOutput>> + Send>>;
struct Operation {
    token:  Arc<()>,
    future: OperationFuture,
}
struct OperationHandle {
    scope: Arc<Scope>,
    token: Arc<()>,
}
impl Drop for OperationHandle {
    fn drop(&mut self) {
        let mut slot = self
            .scope
            .operation
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if slot
            .as_ref()
            .is_some_and(|op| Arc::ptr_eq(&op.token, &self.token))
        {
            slot.take();
        }
    }
}
impl Scope {
    async fn operate(self: &Arc<Self>, future: OperationFuture) -> Result<OperationOutput> {
        self.available()?;
        let _serial = self.serial.lock().await;
        self.available()?;
        let token = Arc::new(());
        let _handle = OperationHandle {
            scope: self.clone(),
            token: token.clone(),
        };
        *self
            .operation
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Operation {
            token: token.clone(),
            future,
        });
        poll_fn(|cx| {
            if self.closed.load(Ordering::Acquire)
                || !ACTIVE
                    .try_with(|current| Arc::ptr_eq(current, self))
                    .unwrap_or(false)
            {
                return Poll::Ready(Err(Error::ScopeDatabaseMismatch));
            }
            let mut slot = self
                .operation
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match slot.as_mut().filter(|op| Arc::ptr_eq(&op.token, &token)) {
                Some(op) => op.future.as_mut().poll(cx),
                None => Poll::Ready(Err(Error::ScopeDatabaseMismatch)),
            }
        })
        .await
    }
}
