use crate::{DecodeKind, Error, Result};

/// A built-in portable SQL representation. This trait is sealed.
///
/// ```compile_fail
/// struct DriverSpecific;
/// impl sqly::SqlValue for DriverSpecific {}
/// ```
pub trait SqlValue: private::Representation {}
/// Synchronously convert an application value into an owned SQL representation.
/// Owned values can move their storage; borrowed implementations copy into
/// owned storage. Implement `Encode` for `&YourType` when callers need borrowed
/// binding. Implementations must not perform database I/O.
pub trait Encode {
    type Repr: SqlValue;
    fn encode(self) -> Result<Self::Repr>;
}
/// Convert a built-in representation into an application value.
pub trait Decode: Sized {
    type Repr: SqlValue;
    fn decode(value: Self::Repr) -> Result<Self>;
}

pub(crate) mod private {
    use super::{DecodeKind, Error, Result};
    // These types belong to the sealed trait protocol, not the public facade.
    #[derive(Clone, Copy)]
    pub enum Kind {
        Text,
        I32,
        I64,
        Bool,
        Bytes,
        Float,
        #[cfg(feature = "uuid")]
        Uuid,
        #[cfg(feature = "time")]
        Time,
        #[cfg(feature = "json")]
        Json,
    }
    pub enum Value {
        Null(Kind),
        Text(String),
        I32(i32),
        I64(i64),
        Bool(bool),
        Bytes(Vec<u8>),
        Float(f64),
        #[cfg(feature = "uuid")]
        Uuid(uuid::Uuid),
        #[cfg(feature = "time")]
        Time(time::OffsetDateTime),
        #[cfg(feature = "json")]
        Json(serde_json::Value),
    }
    pub trait Representation: Sized {
        const KIND: Kind;
        fn into_value(self) -> Result<Value>;
        fn from_value(value: Value) -> Result<Self>;
    }
    macro_rules! repr {
        ($ty:ty, $variant:ident, $validate:expr) => {
            impl Representation for $ty {
                const KIND: Kind = Kind::$variant;
                fn into_value(self) -> Result<Value> {
                    ($validate)(&self)?;
                    Ok(Value::$variant(self))
                }
                fn from_value(value: Value) -> Result<Self> {
                    match value {
                        Value::$variant(value) => {
                            ($validate)(&value)?;
                            Ok(value)
                        }
                        Value::Null(_) => Err(Error::invalid(DecodeKind::UnexpectedNull)),
                        _ => Err(Error::invalid(DecodeKind::TypeMismatch)),
                    }
                }
            }
        };
    }
    #[expect(
        clippy::unnecessary_wraps,
        reason = "uniform validator signature used by the representation macro"
    )]
    fn valid<T>(_: &T) -> Result<()> {
        Ok(())
    }
    repr!(String, Text, valid);
    repr!(i32, I32, valid);
    repr!(i64, I64, valid);
    repr!(bool, Bool, valid);
    repr!(Vec<u8>, Bytes, valid);
    repr!(f64, Float, |v: &f64| if v.is_finite() {
        Ok(())
    } else {
        Err(Error::invalid(DecodeKind::InvalidValue))
    });
    #[cfg(feature = "uuid")]
    repr!(uuid::Uuid, Uuid, valid);
    #[cfg(feature = "json")]
    repr!(serde_json::Value, Json, valid);
    #[cfg(feature = "time")]
    repr!(time::OffsetDateTime, Time, validate_time);
    #[cfg(feature = "time")]
    pub(crate) fn validate_time(v: &time::OffsetDateTime) -> Result<()> {
        let utc = v
            .checked_to_offset(time::UtcOffset::UTC)
            .ok_or_else(|| Error::invalid(DecodeKind::OutOfRange))?;
        if !(1..=9999).contains(&utc.year()) {
            return Err(Error::invalid(DecodeKind::OutOfRange));
        }
        if !utc.nanosecond().is_multiple_of(1000) {
            return Err(Error::invalid(DecodeKind::InvalidValue));
        }
        Ok(())
    }
    #[cfg(all(feature = "time", feature = "sqlite"))]
    pub(crate) fn timestamp(v: time::OffsetDateTime) -> String {
        let v = v.to_offset(time::UtcOffset::UTC);
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            v.year(),
            u8::from(v.month()),
            v.day(),
            v.hour(),
            v.minute(),
            v.second(),
            v.microsecond()
        )
    }
    impl<T: Representation> Representation for Option<T> {
        const KIND: Kind = T::KIND;
        fn into_value(self) -> Result<Value> {
            self.map_or(Ok(Value::Null(T::KIND)), Representation::into_value)
        }
        fn from_value(value: Value) -> Result<Self> {
            match value {
                Value::Null(_) => Ok(None),
                value => T::from_value(value).map(Some),
            }
        }
    }
}
macro_rules! codec {
    ($($ty:ty),+ $(,)?) => { $(
        impl SqlValue for $ty {}
        impl Encode for $ty {
            type Repr = Self;
            fn encode(self) -> Result<Self> { Ok(self) }
        }
        impl Encode for &$ty {
            type Repr = $ty;
            fn encode(self) -> Result<$ty> { Ok(self.clone()) }
        }
        impl Decode for $ty {
            type Repr = Self;
            fn decode(value: Self) -> Result<Self> { Ok(value) }
        }
    )+ };
}
codec!(String, i32, i64, bool, Vec<u8>, f64);
#[cfg(feature = "uuid")]
codec!(uuid::Uuid);
#[cfg(feature = "time")]
codec!(time::OffsetDateTime);
#[cfg(feature = "json")]
codec!(serde_json::Value);
impl<T: SqlValue> SqlValue for Option<T> {}
impl Encode for &str {
    type Repr = String;
    fn encode(self) -> Result<String> {
        Ok(self.to_owned())
    }
}
impl Encode for &[u8] {
    type Repr = Vec<u8>;
    fn encode(self) -> Result<Vec<u8>> {
        Ok(self.to_owned())
    }
}
impl<T: Encode> Encode for Option<T> {
    type Repr = Option<T::Repr>;
    fn encode(self) -> Result<Self::Repr> {
        self.map(Encode::encode).transpose()
    }
}
impl<'a, T> Encode for &'a Option<T>
where
    &'a T: Encode,
{
    type Repr = Option<<&'a T as Encode>::Repr>;
    fn encode(self) -> Result<Self::Repr> {
        self.as_ref().map(Encode::encode).transpose()
    }
}
impl<T: Decode> Decode for Option<T> {
    type Repr = Option<T::Repr>;
    fn decode(value: Self::Repr) -> Result<Self> {
        value.map(T::decode).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::Encode;

    #[test]
    fn owned_encodings_retain_their_allocations() -> crate::Result<()> {
        let text = String::from("owned text");
        let text_ptr = text.as_ptr();
        assert_eq!(text.encode()?.as_ptr(), text_ptr);
        let bytes = vec![1_u8, 2, 3];
        let bytes_ptr = bytes.as_ptr();
        assert_eq!(bytes.encode()?.as_ptr(), bytes_ptr);
        Ok(())
    }

    #[test]
    fn borrowed_optional_values_encode_without_consuming_the_original() -> crate::Result<()> {
        let text = Some(String::from("borrowed"));
        assert_eq!((&text).encode()?, text);
        assert_eq!(text.as_deref(), Some("borrowed"));
        let absent: Option<String> = None;
        assert_eq!((&absent).encode()?, None);
        Ok(())
    }
}
