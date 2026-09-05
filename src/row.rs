use std::fmt;
use std::sync::Arc;

#[cfg(feature = "postgres")]
use sqlx::Column as _;
#[cfg(feature = "postgres")]
use sqlx::TypeInfo as _;
#[cfg(feature = "postgres")]
use sqlx::postgres::PgRow;
#[cfg(feature = "sqlite")]
use sqlx::sqlite::SqliteRow;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use sqlx::{Row as _, ValueRef as _};
#[cfg(all(feature = "time", feature = "sqlite"))]
use time::format_description::well_known::Rfc3339;

use crate::value::private::{Kind, Representation as _, Value};
use crate::{Decode, Result};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
use crate::{DecodeKind, Error};

/// An owned result row. Cloning shares immutable row storage.
#[derive(Clone)]
pub struct Row {
    pub(crate) inner: Arc<Inner>,
}
pub(crate) enum Inner {
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteRow),
    #[cfg(feature = "postgres")]
    Postgres(PgRow),
}
/// Map a row into an application record.
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self>;
}
impl FromRow for Row {
    fn from_row(row: &Self) -> Result<Self> {
        Ok(row.clone())
    }
}
impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Row(<redacted>)")
    }
}
impl Row {
    /// Decode a column by its unique result name. Use SQL aliases for joins.
    pub fn try_get<T: Decode>(&self, column: &str) -> Result<T> {
        self.value(column, T::Repr::KIND)
            .and_then(T::Repr::from_value)
            .and_then(T::decode)
            .map_err(|e| e.at_column(column.to_owned()))
    }
    fn value(&self, column: &str, kind: Kind) -> Result<Value> {
        match self.inner.as_ref() {
            #[cfg(not(any(feature = "sqlite", feature = "postgres")))]
            _ => {
                let _ = (column, kind);
                match *self.inner {}
            }
            #[cfg(feature = "sqlite")]
            Inner::Sqlite(row) => {
                let index = unique_column(row.columns().iter().map(sqlx::Column::name), column)?;
                if row.try_get_raw(index).map_err(decode_driver)?.is_null() {
                    return Ok(Value::Null(kind));
                }
                match kind {
                    Kind::I64 => Ok(Value::I64(row.try_get(index).map_err(decode_driver)?)),
                    Kind::I32 => Ok(Value::I32(
                        i32::try_from(row.try_get::<i64, _>(index).map_err(decode_driver)?)
                            .map_err(|_| Error::invalid(DecodeKind::OutOfRange))?,
                    )),
                    Kind::Text => Ok(Value::Text(row.try_get(index).map_err(decode_driver)?)),
                    Kind::Bytes => Ok(Value::Bytes(row.try_get(index).map_err(decode_driver)?)),
                    Kind::Float => Ok(Value::Float(row.try_get(index).map_err(decode_driver)?)),
                    Kind::Bool => match row.try_get::<i64, _>(index).map_err(decode_driver)? {
                        0 => Ok(Value::Bool(false)),
                        1 => Ok(Value::Bool(true)),
                        _ => Err(Error::invalid(DecodeKind::InvalidValue)),
                    },
                    #[cfg(feature = "uuid")]
                    Kind::Uuid => Ok(Value::Uuid(
                        uuid::Uuid::parse_str(
                            row.try_get::<&str, _>(index).map_err(decode_driver)?,
                        )
                        .map_err(Error::decode_value)?,
                    )),
                    #[cfg(feature = "json")]
                    Kind::Json => Ok(Value::Json(
                        serde_json::from_str(row.try_get::<&str, _>(index).map_err(decode_driver)?)
                            .map_err(Error::decode_value)?,
                    )),
                    #[cfg(feature = "time")]
                    Kind::Time => Ok(Value::Time(
                        time::OffsetDateTime::parse(
                            row.try_get::<&str, _>(index).map_err(decode_driver)?,
                            &Rfc3339,
                        )
                        .map_err(Error::decode_value)?
                        .checked_to_offset(time::UtcOffset::UTC)
                        .ok_or_else(|| Error::invalid(DecodeKind::OutOfRange))?,
                    )),
                }
            }
            #[cfg(feature = "postgres")]
            Inner::Postgres(row) => {
                let index = unique_column(row.columns().iter().map(sqlx::Column::name), column)?;
                if row.try_get_raw(index).map_err(decode_driver)?.is_null() {
                    return Ok(Value::Null(kind));
                }
                match kind {
                    Kind::I32 | Kind::I64 => {
                        let value = match row.columns()[index].type_info().name() {
                            "INT2" => {
                                i64::from(row.try_get::<i16, _>(index).map_err(decode_driver)?)
                            }
                            "INT4" => {
                                i64::from(row.try_get::<i32, _>(index).map_err(decode_driver)?)
                            }
                            "INT8" => row.try_get::<i64, _>(index).map_err(decode_driver)?,
                            _ => return Err(Error::invalid(DecodeKind::TypeMismatch)),
                        };
                        if matches!(kind, Kind::I32) {
                            Ok(Value::I32(
                                i32::try_from(value)
                                    .map_err(|_| Error::invalid(DecodeKind::OutOfRange))?,
                            ))
                        } else {
                            Ok(Value::I64(value))
                        }
                    }
                    Kind::Text => Ok(Value::Text(row.try_get(index).map_err(decode_driver)?)),
                    Kind::Bytes => Ok(Value::Bytes(row.try_get(index).map_err(decode_driver)?)),
                    Kind::Float => Ok(Value::Float(row.try_get(index).map_err(decode_driver)?)),
                    Kind::Bool => Ok(Value::Bool(row.try_get(index).map_err(decode_driver)?)),
                    #[cfg(feature = "uuid")]
                    Kind::Uuid => Ok(Value::Uuid(row.try_get(index).map_err(decode_driver)?)),
                    #[cfg(feature = "json")]
                    Kind::Json => Ok(Value::Json(row.try_get(index).map_err(decode_driver)?)),
                    #[cfg(feature = "time")]
                    Kind::Time => Ok(Value::Time(
                        row.try_get::<time::OffsetDateTime, _>(index)
                            .map_err(decode_driver)?
                            .checked_to_offset(time::UtcOffset::UTC)
                            .ok_or_else(|| Error::invalid(DecodeKind::OutOfRange))?,
                    )),
                }
            }
        }
    }
}
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn unique_column<'a>(names: impl Iterator<Item = &'a str>, name: &str) -> Result<usize> {
    let mut matches = names
        .enumerate()
        .filter_map(|(i, n)| (n == name).then_some(i));
    let first = matches
        .next()
        .ok_or_else(|| Error::invalid(DecodeKind::MissingColumn))?;
    if matches.next().is_some() {
        return Err(Error::invalid(DecodeKind::DuplicateColumn));
    }
    Ok(first)
}
#[cfg(any(feature = "sqlite", feature = "postgres"))]
fn decode_driver(error: sqlx::Error) -> Error {
    Error::Decode {
        column: None,
        kind:   DecodeKind::TypeMismatch,
        source: match Error::decode_value(error) {
            Error::Decode { source, .. } => source,
            _ => None,
        },
    }
}
