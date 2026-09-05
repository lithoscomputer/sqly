use std::fmt::{self, Write as _};

use sqlx::SqlSafeStr as _;

use crate::value::private::{Representation, Value};
use crate::{Encode, Error, Result};

/// Equality selector for an existing primary or unique key.
///
/// Applications acquire multiple locks in a consistent order and read dependent
/// rows only after acquisition. Missing PostgreSQL rows are not locked.
#[must_use]
pub struct Lock {
    table: String,
    keys:  Vec<(String, Value)>,
    error: Option<Error>,
}
impl Lock {
    pub fn row(table: impl Into<String>) -> Self {
        let table = table.into();
        let error = identifier(&table).err();
        Self {
            table,
            keys: Vec::new(),
            error,
        }
    }
    pub fn key(mut self, column: impl Into<String>, value: impl Encode) -> Self {
        if self.error.is_some() {
            return self;
        }
        let column = column.into();
        let validated = identifier(&column).and_then(|()| {
            if self.keys.iter().any(|(name, _)| name == &column) {
                return Err(Error::InvalidLock {
                    reason: "duplicate key column",
                });
            }
            let value = value
                .encode()
                .and_then(Representation::into_value)
                .map_err(|e| e.at_parameter(self.keys.len() + 1))?;
            if matches!(value, Value::Null(_)) {
                return Err(Error::InvalidLock { reason: "NULL key" });
            }
            Ok(value)
        });
        match validated {
            Ok(value) => self.keys.push((column, value)),
            Err(error) => self.error = Some(error),
        }
        self
    }
    pub(crate) fn statement(self, postgres: bool) -> Result<(sqlx::SqlStr, Vec<Value>)> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.keys.is_empty() {
            return Err(Error::InvalidLock {
                reason: "at least one key is required",
            });
        }
        let mut sql = format!("SELECT 1 AS sqly_lock_match FROM \"{}\" WHERE ", self.table);
        let mut values = Vec::with_capacity(self.keys.len());
        for (index, (column, value)) in self.keys.into_iter().enumerate() {
            if index != 0 {
                sql.push_str(" AND ");
            }
            // Identifier validation excludes quotes and delimiters. Only these
            // validated identifiers and generated placeholders enter the SQL.
            write!(sql, "\"{column}\" = ${}", index + 1).expect("writing to a String cannot fail");
            values.push(value);
        }
        sql.push_str(" LIMIT 2");
        if postgres {
            sql.push_str(" FOR UPDATE");
        }
        Ok((sqlx::AssertSqlSafe(sql).into_sql_str(), values))
    }
}
pub(crate) fn identifier(value: &str) -> Result<()> {
    let mut bytes = value.bytes();
    if value.len() > 63
        || !bytes
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        || !bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(Error::InvalidLock {
            reason: "identifiers must be simple ASCII names of at most 63 bytes",
        });
    }
    Ok(())
}
impl fmt::Debug for Lock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lock")
            .field("key_count", &self.keys.len())
            .finish_non_exhaustive()
    }
}
