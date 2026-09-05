use std::sync::Arc;
#[cfg(feature = "sqlite")]
use std::sync::atomic::{AtomicBool, Ordering};

use sqlx::pool::PoolConnection;
#[cfg(feature = "postgres")]
use sqlx::postgres::{PgArguments, PgStatement, PgTypeInfo};
#[cfg(feature = "sqlite")]
use sqlx::sqlite::{SqliteArguments, SqliteTypeInfo};
use sqlx::{Arguments as _, Executor as _, Statement as _};

use crate::database::Inner;
use crate::error::Cause;
use crate::query::{Mode, Output};
use crate::row::Inner as RowInner;
use crate::stream::Cursor;
#[cfg(all(feature = "sqlite", feature = "time"))]
use crate::value::private::timestamp;
use crate::value::private::{Kind, Value};
use crate::{Error, Result, Row};

// A cancelled or failed driver operation discards its connection. Successful
// completion alone permits pool reuse; cleanup cannot leak an unknown state.
struct Lease<DB: sqlx::Database> {
    connection: PoolConnection<DB>,
    reusable:   bool,
}
impl<DB: sqlx::Database> Drop for Lease<DB> {
    fn drop(&mut self) {
        if !self.reusable {
            self.connection.close_on_drop();
        }
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_arguments(values: Vec<Value>) -> Result<(SqliteArguments, Vec<SqliteTypeInfo>)> {
    let mut args = <SqliteArguments>::default();
    let mut types = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let result = match value {
            Value::Text(v) => {
                types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v)
            }
            Value::I32(v) => {
                types.push(<i32 as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v)
            }
            Value::I64(v) => {
                types.push(<i64 as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v)
            }
            Value::Bool(v) => {
                types.push(<bool as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v)
            }
            Value::Bytes(v) => {
                types.push(<Vec<u8> as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v)
            }
            Value::Float(v) => {
                types.push(<f64 as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v)
            }
            #[cfg(feature = "uuid")]
            Value::Uuid(v) => {
                types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v.to_string())
            }
            #[cfg(feature = "time")]
            Value::Time(v) => {
                types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(timestamp(v))
            }
            #[cfg(feature = "json")]
            Value::Json(v) => {
                types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                args.add(v.to_string())
            }
            Value::Null(kind) => match kind {
                Kind::Text => {
                    types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<String>)
                }
                Kind::I32 => {
                    types.push(<i32 as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<i32>)
                }
                Kind::I64 => {
                    types.push(<i64 as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<i64>)
                }
                Kind::Bool => {
                    types.push(<bool as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<bool>)
                }
                Kind::Bytes => {
                    types.push(<Vec<u8> as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<Vec<u8>>)
                }
                Kind::Float => {
                    types.push(<f64 as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<f64>)
                }
                #[cfg(feature = "uuid")]
                Kind::Uuid => {
                    types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<String>)
                }
                #[cfg(feature = "time")]
                Kind::Time => {
                    types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<String>)
                }
                #[cfg(feature = "json")]
                Kind::Json => {
                    types.push(<String as sqlx::Type<sqlx::Sqlite>>::type_info());
                    args.add(None::<String>)
                }
            },
        };
        result.map_err(|source| Error::Encode {
            parameter: Some(index + 1),
            source:    Cause(source),
        })?;
    }
    Ok((args, types))
}

#[cfg(feature = "sqlite")]
pub(crate) async fn sqlite(
    pool: &sqlx::Pool<sqlx::Sqlite>,
    sql: &'static str,
    values: Vec<Value>,
    mode: Mode,
) -> Result<Output> {
    let (args, types) = sqlite_arguments(values)?;
    let mut lease = Lease {
        connection: pool.acquire().await.map_err(Error::driver)?,
        reusable:   false,
    };
    let result = sqlite_execute(
        &mut lease.connection,
        sqlx::SqlStr::from_static(sql),
        args,
        types,
        mode,
    )
    .await;
    lease.reusable = result.is_ok() || matches!(&result, Err(Error::BindCount { .. }));
    result
}
#[cfg(feature = "sqlite")]
pub(crate) async fn sqlite_on(
    connection: &mut sqlx::SqliteConnection,
    sql: sqlx::SqlStr,
    values: Vec<Value>,
    mode: Mode,
) -> Result<Output> {
    let (args, types) = sqlite_arguments(values)?;
    sqlite_execute(connection, sql, args, types, mode).await
}
#[cfg(feature = "sqlite")]
async fn sqlite_execute(
    connection: &mut sqlx::SqliteConnection,
    sql: sqlx::SqlStr,
    args: SqliteArguments,
    types: Vec<SqliteTypeInfo>,
    mode: Mode,
) -> Result<Output> {
    let actual = types.len();
    let cancelled = Arc::new(AtomicBool::new(false));
    let callback_flag = cancelled.clone();
    connection
        .lock_handle()
        .await
        .map_err(Error::driver)?
        .set_progress_handler(1000, move || !callback_flag.load(Ordering::Acquire));
    let mut cancellation = SqliteCancellation {
        cancelled,
        complete: false,
    };
    let statement = (&mut *connection)
        .prepare_with(sql.clone(), &types)
        .await
        .map_err(Error::driver)?;
    let expected = statement.parameters().map_or(0, |p| match p {
        sqlx::Either::Left(types) => types.len(),
        sqlx::Either::Right(count) => count,
    });
    if expected != actual {
        return Err(Error::BindCount {
            expected: Some(expected),
            actual,
        });
    }
    let query = statement.query_with(args);
    let mut output = Output {
        rows:     Vec::new(),
        affected: 0,
    };
    match mode {
        Mode::Execute => {
            output.affected = query
                .execute(&mut *connection)
                .await
                .map_err(Error::driver)?
                .rows_affected();
        }
        Mode::Optional => {
            if let Some(row) = query
                .fetch_optional(&mut *connection)
                .await
                .map_err(Error::driver)?
            {
                output.rows.push(Row {
                    inner: Arc::new(RowInner::Sqlite(row)),
                });
            }
        }
        Mode::All => {
            output.rows = query
                .fetch_all(&mut *connection)
                .await
                .map_err(Error::driver)?
                .into_iter()
                .map(|row| Row {
                    inner: Arc::new(RowInner::Sqlite(row)),
                })
                .collect();
        }
    }
    cancellation.complete = true;
    Ok(output)
}

#[cfg(feature = "postgres")]
async fn postgres_prepare(
    connection: &mut sqlx::PgConnection,
    sql: sqlx::SqlStr,
    types: &[PgTypeInfo],
    actual: usize,
    in_transaction: bool,
) -> Result<PgStatement> {
    let statement = match (&mut *connection).prepare_with(sql.clone(), types).await {
        Ok(statement) => statement,
        Err(error) if indeterminate_parameter(&error) => {
            return Err(Error::BindCount {
                expected: None,
                actual,
            });
        }
        Err(error) => return Err(Error::driver(error)),
    };
    let expected = statement.parameters().map_or(0, |p| match p {
        sqlx::Either::Left(types) => types.len(),
        sqlx::Either::Right(count) => count,
    });
    if expected != actual {
        return Err(Error::BindCount {
            expected: Some(expected),
            actual,
        });
    }
    // PostgreSQL includes unused trailing type hints in ParameterDescription.
    // Remove hints from the tail until the server either reports a referenced
    // parameter beyond the hints or requires its type to resolve the statement.
    // Full typed preparation already succeeded, so either outcome establishes
    // the last referenced parameter under the dense-number caller contract.
    let mut referenced = actual;
    for hint_count in (0..actual).rev() {
        match (&mut *connection)
            .prepare_with(sql.clone(), &types[..hint_count])
            .await
        {
            Ok(probe) => {
                let count = probe.parameters().map_or(0, |p| match p {
                    sqlx::Either::Left(types) => types.len(),
                    sqlx::Either::Right(n) => n,
                });
                if count > hint_count {
                    referenced = count;
                    break;
                }
                referenced = count;
            }
            Err(error) if parameter_resolution_error(&error) => {
                if in_transaction {
                    sqlx::raw_sql("ROLLBACK TO SAVEPOINT sqly_prepare")
                        .execute(&mut *connection)
                        .await
                        .map_err(Error::driver)?;
                }
                referenced = hint_count + 1;
                break;
            }
            Err(error) => return Err(Error::driver(error)),
        }
    }
    if referenced != actual {
        return Err(Error::BindCount {
            expected: Some(referenced),
            actual,
        });
    }
    Ok(statement)
}

#[cfg(feature = "postgres")]
fn postgres_arguments(values: Vec<Value>) -> Result<(PgArguments, Vec<PgTypeInfo>)> {
    let mut args = <PgArguments>::default();
    let mut types = Vec::with_capacity(values.len());
    for (index, value) in values.into_iter().enumerate() {
        let result = match value {
            Value::Text(v) => {
                types.push(<String as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            Value::I32(v) => {
                types.push(<i32 as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            Value::I64(v) => {
                types.push(<i64 as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            Value::Bool(v) => {
                types.push(<bool as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            Value::Bytes(v) => {
                types.push(<Vec<u8> as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            Value::Float(v) => {
                types.push(<f64 as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            #[cfg(feature = "uuid")]
            Value::Uuid(v) => {
                types.push(<uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            #[cfg(feature = "time")]
            Value::Time(v) => {
                types.push(<time::OffsetDateTime as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v.to_offset(time::UtcOffset::UTC))
            }
            #[cfg(feature = "json")]
            Value::Json(v) => {
                types.push(<serde_json::Value as sqlx::Type<sqlx::Postgres>>::type_info());
                args.add(v)
            }
            Value::Null(kind) => match kind {
                Kind::Text => {
                    types.push(<String as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<String>)
                }
                Kind::I32 => {
                    types.push(<i32 as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<i32>)
                }
                Kind::I64 => {
                    types.push(<i64 as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<i64>)
                }
                Kind::Bool => {
                    types.push(<bool as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<bool>)
                }
                Kind::Bytes => {
                    types.push(<Vec<u8> as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<Vec<u8>>)
                }
                Kind::Float => {
                    types.push(<f64 as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<f64>)
                }
                #[cfg(feature = "uuid")]
                Kind::Uuid => {
                    types.push(<uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<uuid::Uuid>)
                }
                #[cfg(feature = "time")]
                Kind::Time => {
                    types.push(<time::OffsetDateTime as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<time::OffsetDateTime>)
                }
                #[cfg(feature = "json")]
                Kind::Json => {
                    types.push(<serde_json::Value as sqlx::Type<sqlx::Postgres>>::type_info());
                    args.add(None::<serde_json::Value>)
                }
            },
        };
        result.map_err(|source| Error::Encode {
            parameter: Some(index + 1),
            source:    Cause(source),
        })?;
    }
    Ok((args, types))
}

#[cfg(feature = "postgres")]
pub(crate) async fn postgres(
    pool: &sqlx::Pool<sqlx::Postgres>,
    sql: &'static str,
    values: Vec<Value>,
    mode: Mode,
) -> Result<Output> {
    let (args, types) = postgres_arguments(values)?;
    let mut lease = Lease {
        connection: pool.acquire().await.map_err(Error::driver)?,
        reusable:   false,
    };
    let result = postgres_execute(
        &mut lease.connection,
        sqlx::SqlStr::from_static(sql),
        args,
        types,
        mode,
        false,
    )
    .await;
    lease.reusable = result.is_ok() || matches!(&result, Err(Error::BindCount { .. }));
    result
}
#[cfg(feature = "postgres")]
pub(crate) async fn postgres_on(
    connection: &mut sqlx::PgConnection,
    sql: sqlx::SqlStr,
    values: Vec<Value>,
    mode: Mode,
) -> Result<Output> {
    let (args, types) = postgres_arguments(values)?;
    postgres_execute(connection, sql, args, types, mode, true).await
}
#[cfg(feature = "postgres")]
async fn postgres_execute(
    connection: &mut sqlx::PgConnection,
    sql: sqlx::SqlStr,
    args: PgArguments,
    types: Vec<PgTypeInfo>,
    mode: Mode,
    in_transaction: bool,
) -> Result<Output> {
    let statement = postgres_statement(connection, sql, &types, in_transaction).await?;
    let query = statement.query_with(args);
    let mut output = Output {
        rows:     Vec::new(),
        affected: 0,
    };
    match mode {
        Mode::Execute => {
            output.affected = query
                .execute(&mut *connection)
                .await
                .map_err(Error::driver)?
                .rows_affected();
        }
        Mode::Optional => {
            if let Some(row) = query
                .fetch_optional(&mut *connection)
                .await
                .map_err(Error::driver)?
            {
                output.rows.push(Row {
                    inner: Arc::new(RowInner::Postgres(row)),
                });
            }
        }
        Mode::All => {
            output.rows = query
                .fetch_all(&mut *connection)
                .await
                .map_err(Error::driver)?
                .into_iter()
                .map(|row| Row {
                    inner: Arc::new(RowInner::Postgres(row)),
                })
                .collect();
        }
    }
    Ok(output)
}

#[cfg(feature = "postgres")]
fn indeterminate_parameter(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .is_some_and(|e| e.code().as_deref() == Some("42P18"))
}
#[cfg(feature = "postgres")]
fn parameter_resolution_error(error: &sqlx::Error) -> bool {
    // Removing a used type hint can make an expression ambiguous or choose an
    // incompatible inferred type. Unused trailing hints cannot affect resolution.
    error.as_database_error().is_some_and(|e| {
        e.code()
            .as_deref()
            .is_some_and(|c| matches!(c, "42P18" | "42P08" | "42725" | "42883" | "42804" | "42846"))
    })
}

#[cfg(feature = "sqlite")]
struct SqliteCancellation {
    cancelled: Arc<AtomicBool>,
    complete:  bool,
}
#[cfg(feature = "sqlite")]
impl Drop for SqliteCancellation {
    fn drop(&mut self) {
        if !self.complete {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}

pub(crate) fn rows(db: &crate::Database, sql: crate::Sql, values: Vec<Value>) -> Cursor<'_> {
    Box::pin(async_stream::try_stream! {
        match db.inner.as_ref() {
            #[cfg(feature = "sqlite")]
            Inner::Sqlite { pool, .. } => {
                let mut lease = Lease { connection: pool.acquire().await.map_err(Error::driver)?, reusable: false };
                {
                    let mut rows = sqlite_rows(&mut lease.connection, sqlx::SqlStr::from_static(sql.sqlite), values);
                    while let Some(row) = futures_util::StreamExt::next(&mut rows).await { yield row?; }
                }
                lease.reusable = true;
            }
            #[cfg(feature = "postgres")]
            Inner::Postgres(pool) => {
                let mut lease = Lease { connection: pool.acquire().await.map_err(Error::driver)?, reusable: false };
                {
                    let mut rows = postgres_rows(&mut lease.connection, sqlx::SqlStr::from_static(sql.postgres), values, false);
                    while let Some(row) = futures_util::StreamExt::next(&mut rows).await { yield row?; }
                }
                lease.reusable = true;
            }
        }
    })
}
#[cfg(feature = "sqlite")]
pub(crate) fn sqlite_rows(
    connection: &mut sqlx::SqliteConnection,
    sql: sqlx::SqlStr,
    values: Vec<Value>,
) -> Cursor<'_> {
    Box::pin(async_stream::try_stream! {
        let (args, types) = sqlite_arguments(values)?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let callback = cancelled.clone();
        connection.lock_handle().await.map_err(Error::driver)?.set_progress_handler(1000, move || !callback.load(Ordering::Acquire));
        let mut cancellation = SqliteCancellation { cancelled, complete: false };
        let statement = connection.prepare_with(sql, &types).await.map_err(Error::driver)?;
        let expected = statement.parameters().map_or(0, |p| match p { sqlx::Either::Left(t) => t.len(), sqlx::Either::Right(n) => n });
        if expected != types.len() { Err(Error::BindCount { expected: Some(expected), actual: types.len() })?; }
        let mut rows = statement.query_with(args).fetch(&mut *connection);
        while let Some(row) = futures_util::TryStreamExt::try_next(&mut rows).await.map_err(Error::driver)? {
            yield Row { inner: Arc::new(RowInner::Sqlite(row)) };
        }
        cancellation.complete = true;
    })
}
#[cfg(feature = "postgres")]
pub(crate) fn postgres_rows(
    connection: &mut sqlx::PgConnection,
    sql: sqlx::SqlStr,
    values: Vec<Value>,
    in_transaction: bool,
) -> Cursor<'_> {
    Box::pin(async_stream::try_stream! {
        let (args, types) = postgres_arguments(values)?;
        let statement = postgres_statement(connection, sql, &types, in_transaction).await?;
        let mut rows = statement.query_with(args).fetch(&mut *connection);
        while let Some(row) = futures_util::TryStreamExt::try_next(&mut rows).await.map_err(Error::driver)? {
            yield Row { inner: Arc::new(RowInner::Postgres(row)) };
        }
    })
}

#[cfg(feature = "postgres")]
async fn postgres_statement(
    connection: &mut sqlx::PgConnection,
    sql: sqlx::SqlStr,
    types: &[PgTypeInfo],
    in_transaction: bool,
) -> Result<PgStatement> {
    let actual = types.len();
    if in_transaction {
        sqlx::raw_sql("SAVEPOINT sqly_prepare")
            .execute(&mut *connection)
            .await
            .map_err(Error::driver)?;
    }
    let prepared = postgres_prepare(connection, sql, types, actual, in_transaction).await;
    if in_transaction {
        if matches!(&prepared, Err(Error::BindCount { .. })) {
            sqlx::raw_sql("ROLLBACK TO SAVEPOINT sqly_prepare")
                .execute(&mut *connection)
                .await
                .map_err(Error::driver)?;
        }
        if prepared.is_ok() || matches!(&prepared, Err(Error::BindCount { .. })) {
            sqlx::raw_sql("RELEASE SAVEPOINT sqly_prepare")
                .execute(&mut *connection)
                .await
                .map_err(Error::driver)?;
        }
    }
    prepared
}
