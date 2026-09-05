use std::fmt;
/// Static SQL using numbered `$1` through `$N` parameters.
/// Every number must occur; repeated and reordered references are supported.
#[derive(Clone, Copy)]
pub struct Sql {
    #[cfg(any(feature = "sqlite", feature = "migrate"))]
    pub(crate) sqlite:   &'static str,
    #[cfg(any(feature = "postgres", feature = "migrate"))]
    pub(crate) postgres: &'static str,
}
impl Sql {
    /// Declare a dialect pair with identical binding meanings and result shape.
    pub const fn dialects(sqlite: &'static str, postgres: &'static str) -> Self {
        let _ = (sqlite, postgres);
        Self {
            #[cfg(any(feature = "sqlite", feature = "migrate"))]
            sqlite,
            #[cfg(any(feature = "postgres", feature = "migrate"))]
            postgres,
        }
    }
}
impl From<&'static str> for Sql {
    fn from(sql: &'static str) -> Self {
        Self::dialects(sql, sql)
    }
}
impl fmt::Debug for Sql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Sql(<redacted>)")
    }
}
