//! Signature and trait-coherence probe, not a production implementation.
//! Database operations deliberately remain unimplemented; driver behavior is
//! exercised separately by integration tests against both real backends.
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use uuid::Uuid;

#[derive(Debug)]
pub struct Error;
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("probe conversion error")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;

mod sealed {
    use uuid::Uuid;

    use super::{Error, Result};
    #[derive(Clone)]
    pub enum Value {
        Uuid(Uuid),
        Integer(i64),
        Text(String),
        Null,
    }
    pub trait Repr: Sized {
        fn into_value(self) -> Value;
        fn from_value(value: Value) -> Result<Self>;
    }
    macro_rules! repr {
        ($ty:ty, $variant:ident) => {
            impl Repr for $ty {
                fn into_value(self) -> Value {
                    Value::$variant(self)
                }
                fn from_value(value: Value) -> Result<Self> {
                    match value {
                        Value::$variant(v) => Ok(v),
                        _ => Err(Error),
                    }
                }
            }
        };
    }
    repr!(Uuid, Uuid);
    repr!(i64, Integer);
    repr!(String, Text);
    impl<T: Repr> Repr for Option<T> {
        fn into_value(self) -> Value {
            self.map_or(Value::Null, Repr::into_value)
        }
        fn from_value(value: Value) -> Result<Self> {
            match value {
                Value::Null => Ok(None),
                v => T::from_value(v).map(Some),
            }
        }
    }
}
use sealed::Repr as _;
pub trait SqlValue: sealed::Repr {}
impl SqlValue for Uuid {}
impl SqlValue for i64 {}
impl SqlValue for String {}
impl<T: SqlValue> SqlValue for Option<T> {}
pub trait Encode {
    type Repr: SqlValue;
    fn encode(&self) -> Result<Self::Repr>;
}
pub trait Decode: Sized {
    type Repr: SqlValue;
    fn decode(value: Self::Repr) -> Result<Self>;
}
macro_rules! codec {
    ($ty:ty) => {
        impl Encode for $ty {
            type Repr = Self;
            fn encode(&self) -> Result<Self> {
                Ok(self.clone())
            }
        }
        impl Decode for $ty {
            type Repr = Self;
            fn decode(v: Self) -> Result<Self> {
                Ok(v)
            }
        }
    };
}
codec!(Uuid);
codec!(i64);
codec!(String);
impl Encode for str {
    type Repr = String;
    fn encode(&self) -> Result<String> {
        Ok(self.to_owned())
    }
}
impl<T: Encode + ?Sized> Encode for &T {
    type Repr = T::Repr;
    fn encode(&self) -> Result<Self::Repr> {
        T::encode(self)
    }
}
impl<T: Encode> Encode for Option<T> {
    type Repr = Option<T::Repr>;
    fn encode(&self) -> Result<Self::Repr> {
        self.as_ref().map(Encode::encode).transpose()
    }
}
impl<T: Decode> Decode for Option<T> {
    type Repr = Option<T::Repr>;
    fn decode(v: Self::Repr) -> Result<Self> {
        v.map(T::decode).transpose()
    }
}
pub fn round_trip<T: Encode + Decode<Repr = <T as Encode>::Repr>>(value: &T) -> Result<T> {
    let wire = value.encode()?.into_value();
    T::decode(<T as Decode>::Repr::from_value(wire)?)
}
pub struct Row;
impl Row {
    pub fn try_get<T: Decode>(&self, _: &str) -> Result<T> {
        todo!("signature probe")
    }
}
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self>;
}
impl FromRow for Row {
    fn from_row(_: &Row) -> Result<Self> {
        Ok(Row)
    }
}
#[derive(Clone)]
pub struct Database;
pub struct ConnectOptions;
pub struct SqliteOptions;
pub struct PostgresOptions;
impl TryFrom<&str> for ConnectOptions {
    type Error = Error;
    fn try_from(_: &str) -> Result<Self> {
        Ok(Self)
    }
}
impl From<SqliteOptions> for ConnectOptions {
    fn from(_: SqliteOptions) -> Self {
        Self
    }
}
impl From<PostgresOptions> for ConnectOptions {
    fn from(_: PostgresOptions) -> Self {
        Self
    }
}
impl From<std::convert::Infallible> for Error {
    fn from(v: std::convert::Infallible) -> Self {
        match v {}
    }
}
pub struct Sql;
impl From<&'static str> for Sql {
    fn from(_: &'static str) -> Self {
        Self
    }
}
impl Sql {
    pub const fn dialects(_: &'static str, _: &'static str) -> Self {
        Self
    }
}

#[derive(Clone)]
pub struct ScopedDatabase;
pub struct Transaction;
pub struct Query<'a, T> {
    _owner: PhantomData<&'a mut ()>,
    _row:   PhantomData<fn() -> T>,
    error:  Option<Error>,
}
impl<T> Query<'_, T> {
    pub fn bind(mut self, value: impl Encode) -> Self {
        if let Err(e) = value.encode() {
            self.error = Some(e);
        }
        self
    }
}
impl<'a, T: FromRow> Query<'a, T> {
    pub async fn fetch_optional(self) -> Result<Option<T>> {
        todo!("signature probe")
    }
    pub async fn fetch_all(self) -> Result<Vec<T>> {
        todo!("signature probe")
    }
    pub fn fetch(self) -> RowStream<'a, T> {
        todo!("signature probe")
    }
}
impl Query<'_, Row> {
    pub async fn execute(self) -> Result<()> {
        todo!("signature probe")
    }
}
pub struct RowStream<'a, T> {
    inner: Pin<Box<dyn Stream<Item = Result<T>> + Send + 'a>>,
}
impl<T> Stream for RowStream<'_, T> {
    type Item = Result<T>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
impl Database {
    pub async fn connect<O>(_: O) -> Result<Self>
    where
        O: TryInto<ConnectOptions>,
        Error: From<O::Error>,
    {
        todo!("signature probe")
    }

    pub fn query_as<T: FromRow>(&self, _: impl Into<Sql>) -> Query<'_, T> {
        todo!("signature probe")
    }
    pub fn query(&self, sql: impl Into<Sql>) -> Query<'_, Row> {
        self.query_as(sql)
    }
    pub async fn begin_write(&self) -> Result<Transaction> {
        todo!("signature probe")
    }
    pub fn scoped(&self) -> ScopedDatabase {
        ScopedDatabase
    }
}
impl Transaction {
    pub fn query_as<T: FromRow>(&mut self, _: impl Into<Sql>) -> Query<'_, T> {
        todo!("signature probe")
    }
    pub fn query(&mut self, sql: impl Into<Sql>) -> Query<'_, Row> {
        self.query_as(sql)
    }
    pub async fn commit(self) -> Result<()> {
        todo!("signature probe")
    }
}
impl ScopedDatabase {
    pub fn query_as<T: FromRow>(&self, _: impl Into<Sql>) -> Query<'_, T> {
        todo!("signature probe")
    }
    pub fn query(&self, sql: impl Into<Sql>) -> Query<'_, Row> {
        self.query_as(sql)
    }
    pub async fn write<T, E, F, Fut>(&self, _: F) -> std::result::Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: From<Error>,
    {
        todo!("signature probe")
    }
}
