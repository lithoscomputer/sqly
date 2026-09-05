# sqly public API

Status: Finalized on 2026-09-05. The eight technical-validation changes are incorporated, and the library implementation and independent-consumer checks are complete. This document remains the first-release interface and acceptance contract. Code examples are design illustrations; README and rustdoc contain maintained usage examples. See the [implementation plan](implementation.md) for current delivery status.

## Design

sqly is an async Rust library for applications that support SQLite and PostgreSQL. Applications write ordinary SQL, bind ordinary Rust values, and receive their own record types. One concrete `Database` selects the backend at runtime. SQLx stays inside the implementation.

Keep four concepts central: `Database`, `Query`, `Row`, and `Transaction`. Add small configuration, locking, and migration types where their contracts need to be explicit. Do not expose a generic backend parameter to application stores. Applications own the Tokio runtime, schema, configuration policy, business errors, and transaction boundaries. An optional `ambient` feature lets applications carry those boundaries through task-local scopes.

The promise is shared application queries over a documented SQL and value contract. It is not arbitrary SQL translation or identical database behavior.

The first release must fully replace Conveyor’s persistence layer, including all stores, ambient transaction integration, and migration execution with existing-database compatibility. A partial store adoption does not meet the release scope.

## Everyday use

```rust
use sqly::{Database, FromRow, Row};
use uuid::Uuid;

struct User {
    id: Uuid,
    email: String,
}

impl FromRow for User {
    fn from_row(row: &Row) -> sqly::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            email: row.try_get("email")?,
        })
    }
}

async fn find_user(db: &Database, id: Uuid) -> sqly::Result<Option<User>> {
    db.query_as::<User>("SELECT id, email FROM users WHERE id = $1")
        .bind(id)
        .fetch_optional()
        .await
}

async fn create_user(db: &Database, user: &User) -> sqly::Result<()> {
    db.query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(user.id)
        .bind(user.email.as_str())
        .execute()
        .await?;
    Ok(())
}
```

`db.query(sql)` returns `Query<'_, Row>`. `db.query_as::<T>(sql)` returns `Query<'_, T>`. Both capture the execution target when constructed. Callers do not need an `Executor` trait or a separate executor argument at the end. Explicit queries are consumed when executed. Bound data is owned internally in the first version; passing `&str` copies its contents. This keeps parameter lifetimes out of the API. Optimize storage later without changing callers.

| Operation | Result | Contract |
| --- | --- | --- |
| `bind(value)` | `Self` | Add one typed parameter |
| `execute().await` | `Result<ExecuteResult>` | Execute without returning rows |
| `fetch_one().await` | `Result<T>` | First row; `RowNotFound` when empty |
| `fetch_optional().await` | `Result<Option<T>>` | First row, if present |
| `fetch_all().await` | `Result<Vec<T>>` | All rows; caller bounds result size |

`execute` is available only on `Query<Row>`. `ExecuteResult` exposes `rows_affected() -> u64`; it does not promise identical trigger counts or provide a portable last-insert ID. Applications generate IDs or use explicit `RETURNING`. Fetch methods do not assert uniqueness. Queries that depend on uniqueness need a database constraint. Use `LIMIT 1` when only one row matters.

## Streaming results

Streaming is required in the first release. Both `Query<T>` and `ReadQuery<T>` provide `fetch(self) -> RowStream<'_, T>`. The concrete sqly-owned stream implements `futures_core::Stream<Item = Result<T>>`, with `Unpin` and `Send` when its captured state permits it. SQLx stream and executor types stay private. The public `Stream` trait is an intentional ecosystem dependency; callers can use `futures_util::TryStreamExt`.

```rust
use futures_util::TryStreamExt;

let mut users = db.query_as::<User>(
    "SELECT id, email FROM users ORDER BY id",
).fetch();

while let Some(user) = users.try_next().await? {
    export_user(user).await?;
}
```

`fetch` consumes the query but starts execution only when polled. Validation, acquisition, database, and decoding failures appear as stream errors. Stop on the first error; subsequent polls return `None`. Decode rows as they are consumed rather than collecting the complete result in sqly. Demand controls sqly's consumption, although drivers and database execution plans may buffer internally; streaming does not promise constant total server memory.

A pooled stream holds a query connection while the result is active. On completion or early drop, complete or cancel driver work before reuse; discard the connection if cleanup is uncertain. Dropping a stream is not proof that a write with `RETURNING` was rolled back. Explicit-transaction streams retain a mutable borrow of their transaction until released, so another transaction operation cannot overlap them through that handle.

The scope owns the active driver cursor, not just the public stream handle. A scoped stream is an identity-checked handle to that cursor. Store the transaction with the cursor during execution, return it to the scope on exhaustion, and let scope teardown invalidate handles and drop the cursor itself. This permits rollback when a live stream escapes or the scope is cancelled, without waiting for the caller to drop its handle. This ownership strategy passes the real-driver probes on both backends.

Ambient streams must resolve their execution target on first poll and stay bound to that scope identity once started. They must not escape into a spawned task, another scope, or pooled execution after their scope ends. Check scope identity on each poll and fail without further I/O on mismatch. An unstarted scoped query stream still follows the ordinary unscoped-write rejection rule. A read stream started outside a scope remains a pooled read; polling it inside a scope must fail rather than bypass that transaction.

While a stream retains an ambient transaction connection, another query, lock operation, or stream execution on that transaction fails immediately with ActiveStream. Never wait for the caller to advance or drop its own stream. This pre-execution error does not make the scope rollback-only. Consume the stream to completion or drop it before another operation. Early drop starts driver cleanup; reuse the transaction only if cleanup establishes a usable state, otherwise mark it rollback-only. A scope that returns success with an active stream must invalidate the stream, roll back, and return ActiveStream rather than commit or wait indefinitely. On closure error, invalidate the stream and roll back while preserving the original application error. Ordinary transaction rollback and cancellation guarantees still apply.

The stream protocol follows the standard [`Stream` trait](https://docs.rs/futures-core/latest/futures_core/stream/trait.Stream.html).

## Connections and ownership

```rust
let options: sqly::ConnectOptions = database_url.parse()?;
let db = Database::builder()
    .max_connections(8)
    .acquire_timeout(std::time::Duration::from_secs(5))
    .connect(options)
    .await?;

// Convenience form, with the same parser and documented defaults:
let db = Database::connect(database_url).await?;

// An isolated database with one query connection and a private keeper:
let db = Database::connect(sqly::SqliteOptions::in_memory()).await?;

db.close().await;
```

`Database::connect` and the builder accept `impl TryInto<ConnectOptions>` with conversion errors mapped to `sqly::Error`. Support `&str`, owned `ConnectOptions`, `SqliteOptions`, and `PostgresOptions`. The latter two are sqly-owned types with private fields. Backend-specific options live on these types, not in a universal builder full of irrelevant settings.

`Database` is cheap to clone, `Send + Sync`, and owns a shared pool. `close` takes `&self`, closes the pool for every clone, and waits for checked-out connections. Holding a transaction while awaiting close can therefore block shutdown. Dropping a handle is not the explicit shutdown protocol.

Defaults: five connections, a 30-second acquisition timeout, and SQLite foreign keys enabled on every connection. SQLite file creation and WAL are explicit `SqliteOptions` choices. In-memory databases use one pooled query connection plus a separate keeper connection; conflicting pool settings fail validation. Do not silently create parent directories or change persistent journal settings.

Sqly-managed in-memory SQLite databases use a unique named database shared by the query connection and a private keeper connection. The keeper is owned by the shared `Database` state. It is not checked out for queries, does not hold a transaction, is excluded from the query pool limit, and is never subject to idle timeout or connection lifetime recycling. Cloned handles share it; independent `Database` instances receive different names.

Construct the private shared database with a unique named `file:` URI, plus in-memory and shared-cache options. The keeper and every replacement query connection must use the same URI. The SQLx probe preserved data with this URI form; a plain filename with the flags did not.

Replacing a query connection must reconnect to that same named database. Committed schema and data therefore survive query cancellation cleanup, connection discard, and pool recycling. Uncommitted work still follows the normal rollback contract.

`Database::close()` first closes and drains the query pool, including active transactions and pending cleanup, then closes the keeper. Outstanding query and transaction leases must retain the shared lifetime state so dropping a handle or cancelling a close future cannot release the keeper prematurely. Resources are released after the last owner and outstanding operation finish; explicit `close` is the way to await shutdown. Losing the keeper unexpectedly makes the database handle terminally unusable with `DatabaseLost`; never silently create a fresh empty database as recovery.

URL parsing must document each supported option. Unknown options fail rather than disappear. No application environment variable name is built in. Ambient PostgreSQL settings and credential files are not implicitly merged; applications must opt into any future ambient-configuration helper. TLS verification is the PostgreSQL default; local non-TLS use is explicit. Options, query bindings, and connection errors must redact credentials in their own `Debug` and `Display` output.

## Values and records

`Row::try_get<T: Decode>(&self, column: &str) -> Result<T>` returns an owned value. Missing columns, unexpected NULLs, invalid representations, and range errors are distinct decoding failures. Duplicate result column names are an error for name lookup; callers should use aliases.

`FromRow` is a small, open application extension point:

```rust
pub trait FromRow: Sized {
    fn from_row(row: &Row) -> Result<Self>;
}
```

`Row` implements `FromRow`. Start with manual implementations; a derive macro can be added later without changing the contract. Application decoding failures can use `Error::decode(column, source)` to retain a typed cause.

Encode and Decode are public, unsealed application extension traits in the first release. Each declares a Repr associated type constrained by sqly’s sealed SqlValue trait. SqlValue identifies supported built-in representations; applications cannot implement it to add backend-specific driver types. bind accepts impl Encode, retaining any encoding failure and returning it before SQL execution. The backend-neutral value enum stays private. Applications may implement either direction independently; when both are implemented for a type, they should use the same representation and preserve round-trip meaning.

```rust
pub trait Encode {
    type Repr: SqlValue;

    fn encode(&self) -> Result<Self::Repr>;
}

pub trait Decode: Sized {
    type Repr: SqlValue;

    fn decode(value: Self::Repr) -> Result<Self>;
}
```

Applications map their own types to these representations once:

```rust
struct UserId(Uuid);

impl sqly::Encode for UserId {
    type Repr = Uuid;

    fn encode(&self) -> sqly::Result<Uuid> {
        Ok(self.0)
    }
}

impl sqly::Decode for UserId {
    type Repr = Uuid;

    fn decode(value: Uuid) -> sqly::Result<Self> {
        Ok(Self(value))
    }
}

// Application use:
db.query("DELETE FROM users WHERE id = $1")
    .bind(user_id)
    .execute()
    .await?;

let id: UserId = row.try_get("id")?;
```

Binding converts the application value into `Repr`, then sqly applies the backend's built-in binding. Reading performs the reverse: sqly decodes the built-in representation and passes it to the application's `decode` method. A validated text type can use `String` and reject invalid stored values. Neither method receives a connection, backend selector, SQL text, or SQLx type. Custom types cannot change query execution or inject SQL.

Provide implementations for supported built-in types, references used in binding, and `Option<T>` where the representation supports NULL. Thus `bind(&user_id)`, `bind(None::<UserId>)`, and `row.try_get::<Option<UserId>>("id")` work without application boilerplate. NULL binding obtains its backend type from `T::Repr`; it does not call `T::encode` on an absent value. NULL decoding into `Option<T>` returns `None` without calling `T::decode`. Nested optional values do not represent additional SQL NULL states and should not be used to model distinct states.

Conversion methods are synchronous and return errors for invalid values. Provide `Error::encode(source)` and `Error::decode_value(source)` constructors for application causes, accepting errors that are `Send + Sync + 'static`. The binding or row layer attaches the parameter index or column name; custom types do not need those details. Preserve the original cause through the standard error source chain. The existing `Error::decode(column, source)` remains useful for validation performed directly inside `FromRow`.

Encoding errors happen before I/O and do not poison an ambient transaction. For row conversion errors after execution begins, follow the result and stream cleanup rules; a decode failure alone is not evidence that the backend transaction aborted. A partially consumed write result must not be reported as successfully rolled back just because decoding failed.

| Rust type | SQLite storage | PostgreSQL storage |
| --- | --- | --- |
| `String`, `&str` for binding | TEXT | TEXT |
| `i64` | INTEGER | BIGINT |
| `i32` | INTEGER | INTEGER |
| `bool` | INTEGER, restricted to 0 or 1 | BOOLEAN |
| `Vec<u8>`, `&[u8]` for binding | BLOB | BYTEA |
| `f64` | REAL | DOUBLE PRECISION |
| `uuid::Uuid` with `uuid` feature | Canonical lowercase UUID text | UUID |
| `time::OffsetDateTime` with `time` feature | Fixed-width UTC text | TIMESTAMPTZ |
| `serde_json::Value` with `json` feature | JSON text | JSONB |
| `Option<T>` | Typed NULL or T | Typed NULL or T |

Keep integer binding types intentional: PostgreSQL INTEGER and BIGINT are different parameter types. Decoding integer widths uses checked conversion. Exclude unsigned integers, decimal, arrays, and custom PostgreSQL types from the initial portable contract. Reject non-finite floats on either backend. Booleans must reject SQLite integers other than 0 and 1.

Timestamps represent instants, not presentation strings. Normalize to UTC, use exactly six fractional digits in SQLite, and reject sub-microsecond precision rather than silently lose it. Restrict the portable range to UTC years 0001–9999. Applications choose display precision. JSON support means semantic values, not byte-preserved formatting or key order. PostgreSQL JSONB restrictions still apply; JSON operators are outside shared SQL.

Conveyor cutover needs a new application-owned data migration that normalizes historical SQLite timestamp columns to the canonical encoding before queries mix old and new values. Its current RFC 3339 formatting uses variable fractional width. A probe confirms that an older whole-second string can sort after a newer fractional-second string. Audit all temporal comparison columns and fail on unrepresentable values. Preserve historical migration SQL bytes and keep Conveyor API serialization and digest construction unchanged in its domain layer.

Typed nulls remain natural: .bind(None::\<Uuid\>), .bind(None::\<UserId\>), or .bind(optional\_email.as\_deref()). Bare None requires a type annotation.

## Transactions and locking

```rust
async fn rename_user(db: &Database, id: Uuid, email: &str) -> sqly::Result<bool> {
    let mut tx = db.begin_write().await?;

    if !tx.lock(sqly::Lock::row("users").key("id", id)).await? {
        tx.rollback().await?;
        return Ok(false);
    }

    tx.query("UPDATE users SET email = $1 WHERE id = $2")
        .bind(email)
        .bind(id)
        .execute()
        .await?;

    tx.commit().await?;
    Ok(true)
}
```

`begin_write` returns an owned `Transaction`. Its `query` and `query_as` methods match `Database` but borrow the transaction mutably. A query cannot outlive the transaction, and simultaneous queries on one transaction do not compile. Async I/O futures are `Send` where their inputs permit it.

Transactions may read and write. SQLite begins with `BEGIN IMMEDIATE`; PostgreSQL begins at READ COMMITTED. These are deliberate choices for read-modify-write operations, not a claim of equal isolation. Independent reads normally use the pool. A read-only snapshot API is deferred.

`Lock::row(table).key(column, value)` describes equality on an existing primary or unique key. Repeated `key` calls support composite keys. The library quotes validated identifier segments and binds every value. Initially accept simple table and column names only; reject empty key sets, duplicate columns, NULL keys, and invalid identifiers before I/O. Return `false` for a missing row and an error if the selector matches multiple rows.

On PostgreSQL, acquire the row with a separate `SELECT ... FOR UPDATE`. On SQLite, `BEGIN IMMEDIATE` already excludes other writers, and `lock` checks existence. Read dependent rows only after lock acquisition completes. This avoids reusing a joined read made before a PostgreSQL lock was granted. Missing rows are not locked on PostgreSQL. Use unique constraints for concurrent creation. Acquire multiple locks in a consistent application order.

`commit(self)` and `rollback(self)` consume the transaction. An unfinished transaction starts rollback on drop; its connection cannot return to the pool until cleanup completes. Cancellation during acquisition or a query must also leave no connection in an unknown reusable state.

An I/O failure or cancellation during COMMIT may leave the commit outcome unknown. Do not promise that every failed or cancelled commit rolled back. Do not automatically retry writes or whole transactions. Applications decide whether an operation is safe to repeat.

Queries through `db` always use the pool, even when the same task holds a transaction. This includes autocommit writes. These explicit handles never consult task-local state. The optional ambient API below provides task-local resolution through a separate handle. Neither API automatically retries closures or supplies savepoints initially.

## Optional ambient transactions

Enable the `ambient` feature and use `ScopedDatabase`. This is a distinct handle, so a store's field type states whether it uses explicit or ambient transaction semantics. It is not a process-global switch or a mutable mode on `Database`.

Stores receive a cloned `ScopedDatabase` once, during construction, and keep it as a field. Store methods accept only `&self` and application inputs. Neither a transaction handle nor a database handle needs to pass through method call chains. The stored handle resolves the current task's transaction when a statement executes.

```rust
struct UserStore {
    db: sqly::ScopedDatabase,
}

impl UserStore {
    async fn save_email(&self, id: Uuid, email: &str) -> sqly::Result<()> {
        self.db.query("UPDATE users SET email = $1 WHERE id = $2")
            .bind(email)
            .bind(id)
            .execute()
            .await?;
        Ok(())
    }

    async fn find(&self, id: Uuid) -> sqly::Result<Option<User>> {
        self.db.read_as::<User>("SELECT id, email FROM users WHERE id = $1")
            .bind(id)
            .fetch_optional()
            .await
    }
}

struct AccountStore {
    db: sqly::ScopedDatabase,
    users: UserStore,
    events: EventStore,
}

impl AccountStore {
    fn new(db: sqly::ScopedDatabase) -> Self {
        Self {
            users: UserStore { db: db.clone() },
            events: EventStore::new(db.clone()),
            db,
        }
    }

    async fn update_email(&self, id: Uuid, email: &str) -> sqly::Result<()> {
        self.db.write_locking(sqly::Lock::row("users").key("id", id), || async {
            self.users.save_email(id, email).await?;
            self.events.record_email_change(id).await?;
            Ok(())
        }).await
    }
}

let accounts = AccountStore::new(db.scoped());
accounts.update_email(id, "new@example.com").await?;
```

`EventStore` follows the same pattern: it holds a cloned `ScopedDatabase` and its `record_email_change(&self, id)` method executes a statement through that field. `AccountStore::update_email` establishes the transaction once. Both component stores join it without a transaction or database argument. Reads through `UserStore::find` join it too and see uncommitted writes; outside a scope, the same method reads through the pool.

Dependency injection happens at construction. Transaction propagation happens through task-local state. Holding a `ScopedDatabase` does not hold an open transaction, and cloning it does not create a new transaction.

Support additional locks within an existing ambient scope:

```rust
impl ScopedDatabase {
    pub async fn lock(&self, lock: Lock) -> Result<bool>;
}
```

Require an active matching scope and reject `ActiveStream` before acquisition. Use the same lock behavior as `Transaction::lock`. Applications acquire multiple locks in a consistent order and recheck soft-deletion and related-row conditions after acquisition. Opening a nested scope is not an alternative.

The scope entry signatures are:

```rust
impl ScopedDatabase {
    pub async fn write<T, E, F, Fut>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: From<sqly::Error>;

    pub async fn write_locking<T, E, F, Fut>(
        &self,
        lock: Lock,
        f: F,
    ) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: From<sqly::Error>;
}
```

Here `Result<T, E>` is `std::result::Result`, and `Future` is `std::future::Future`. Ordinary closures can borrow application arguments; no boxed future, transaction lifetime, or mandatory `'static` bound appears at the call site. Applications can return their own errors. An otherwise unconstrained closure may need `Ok::<_, AppError>(value)`.

| API | Inside the active scope | Outside a scope |
| --- | --- | --- |
| `read(sql)`, `read_as::<T>(sql)` | Read through its transaction | Read through a pooled connection; caller supplies read-only SQL |
| `query(sql)`, `query_as::<T>(sql)` | Execute through its transaction | Fail with `NoActiveWriteScope`, for every execution method |
| `lock(lock)` | Acquire an additional lock through its transaction | Fail with `NoActiveWriteScope` |
| `write(closure)` | Fail with `NestedWriteScope` | Begin a write transaction, then enter closure |
| `write_locking(lock, closure)` | Fail with `NestedWriteScope` | Begin, acquire lock, then enter closure |

The read builders return `ReadQuery<T>` with bindings and fetch methods, without `execute`. A query that can write, including `INSERT ... RETURNING`, must use `query`/`query_as` even when its terminal method is `fetch_one`. Do not infer read versus write from the terminal method or the first SQL keyword. `query` is deliberately scope-required even for SELECT; `read` expresses that an unscoped read is allowed.

The `read` and `read_as` methods trust the caller to provide read-only SQL. sqly does not inspect that SQL or add a read-only transaction or connection-setting guard merely to police it. Outside a scope, these methods read through the pool; inside a scope, they use its transaction. Applications must route all write-capable SQL, including functions with database side effects, through `query` or `query_as`. Scope checks enforce the chosen API path, not the meaning of arbitrary SQL; mislabeling a write as a read violates the caller contract.

A successful closure commits; a closure error rolls back and returns the original application error. If rollback fails, discard the connection and preserve the original error. The scoped API intentionally does not replace an application error with a cleanup error. Callers that need to observe rollback failure use the explicit transaction API.

A database error during preparation or execution marks the scope rollback-only on both backends. If the closure catches that error and returns success, scope exit rolls back and returns `TransactionAborted`; it must not commit partial work that SQLite would otherwise permit. Local encoding, scope, and lock-selector validation failures do not poison the scope. A bind-count mismatch found after successful preparation also does not poison it. A driver preparation error is a database error, even though statement execution has not begun. A cancelled in-flight statement also makes the scope rollback-only unless the whole scope is already being dropped.

A panic unwinds normally. Panic or cancellation drops the transaction and starts rollback. Cancellation during COMMIT still has the unknown-outcome limit described above. Scopes never retry the closure. `write_locking` returns `RowNotFound` without entering the closure when the row is absent.

The active scope belongs to one database identity and one task. Cloned handles share identity; separately connected pools do not, even for equal URLs. A mismatched scoped handle fails with `ScopeDatabaseMismatch`, for reads as well as writes. Reject nested entry before attempting acquisition, including entry for a different database. There is no implicit cross-database transaction.

Resolve scoped queries when they execute, not when their builders are constructed. A builder does not carry transaction authority outside the scope. Spawned tasks inherit no scope: their query calls fail, and their read calls use the pool. A future directly polled within the scope joins it; independently spawning that future does not. Concurrent futures in one task serialize statements over the one connection. Buffered operations hold the internal lock only during an individual database operation, never while calling user code. Streaming retains exclusive connection use while rows are consumed; its additional lifetime and contention rules are specified in Streaming results.

`ScopedDatabase` does not dereference into `Database` and exposes no raw pool or transaction. Applications adopting strict ambient policy should give stores only scoped handles. Code retaining an explicit `Database` can still write independently; optional ambient semantics are an application structure, not a capability boundary against other database credentials.

Optional ambient support is part of the first interface, including the behavior tests. It implements the reusable scope mechanics from Conveyor's accepted decision. Conveyor keeps its rule about which Store operations may open scopes and which business rows they must lock.

## SQL contract

The shared query API takes `&'static str`: literals, constants, and `include_str!` work. Applications supply one statement per query and bind all request values. sqly trusts application SQL. It does not classify statements, maintain a statement allowlist or denylist, or parse SQL to police transaction control and connection settings. Applications must use sqly's transaction and configuration APIs rather than issue control SQL through ordinary queries. These are caller obligations, not runtime-enforced restrictions; violating them can invalidate transaction and pool guarantees.

Static SQL is the accepted initial scope. Do not add runtime-generated SQL or a general query builder speculatively. The validation inventory found that Conveyor’s dynamic card-list query can use four static statement variants. Confirm that inventory during full replacement. If a required query cannot reasonably use static statements, design the smallest runtime-SQL extension needed for that concrete case, with explicit ownership and parameter binding. This is an exception for required compatibility, not a reason to broaden the initial API in advance.

Use shared numbered placeholders `$1` through `$N` on both backends, binding values in numeric order. Every number from 1 through N must occur at least once. References may repeat or appear out of order. This is a trusted-caller numbering contract, not a syntax policing rule. SQLx 0.9 runtime probes confirm this convention on SQLite and PostgreSQL, including typed NULLs.

No SQL scanner or tokenizer is required for the validated numbered-parameter design. Prepare the selected statement on the execution connection with encoded argument type information through SQLx `prepare_with`, then compare driver-reported parameter count with supplied arguments before execution. Cache preparation where appropriate. This requires preparation I/O; it is not validation before all I/O. SQLite count metadata cannot prove consecutive numbering, so malformed numbering remains a caller-contract violation. Missing and surplus counts for correctly numbered SQL must return a binding error on either backend.

Parameter handling does not prove semantic portability or enforce a SQL sandbox. Applications supply compatible statements and functions; application schema and tests enforce that contract. Collation, NULL ordering, case folding, JSON operations, and conflict handling can still differ. Document tested recipes rather than translating SQL ASTs.

For a required dialect difference, make the pair visible at the declaration:

```rust
const ACTIVE_USERS: sqly::Sql = sqly::Sql::dialects(
    "SELECT id, email FROM users WHERE active = 1",
    "SELECT id, email FROM users WHERE active = TRUE",
);

let users = db.query_as::<User>(ACTIVE_USERS).fetch_all().await?;
```

Query constructors accept `impl Into<Sql>`, including `&'static str`. `Sql::dialects(sqlite, postgres)` also requires static strings. Both arms use numbered placeholders and the same value and row contract. Authors must preserve parameter meanings and result shape across the pair. Parameter-count validation follows the preparation contract above; the pair does not introduce statement policing.

## Errors

Expose sqly::Result\<T\> and a non-exhaustive Error enum. Branch-oriented cases should cover configuration, missing backend features, invalid SQL, bind count/encoding, decoding, missing rows, non-unique lock selectors, constraint violations, contention, transaction aborts, pool closure/timeouts, connection failures, and remaining database errors. The ambient feature adds NoActiveWriteScope, NestedWriteScope, ScopeDatabaseMismatch, and ActiveStream. Known aborted scopes use TransactionAborted. DatabaseLost identifies unexpected loss of the in-memory keeper and requires creating and initializing a new database handle.

Constraint classification has `Unique`, `ForeignKey`, `NotNull`, and `Check` kinds. Offer `is_unique_violation()` for the common application branch. Preserve an optional constraint name when the backend provides one; never require that metadata for correctness. Keep original causes through `std::error::Error::source()` without public SQLx error fields.

Distinguish a known transaction abort from an unknown commit outcome. Contention does not imply a statement can be retried in place. No general `is_retryable()` promise. Diagnostic source chains can contain server data; applications must not expose them directly in client responses. sqly does not install logging subscribers or log SQL parameter values.

## Migrations

Provide an optional `migrate` module. Applications supply schema SQL:

```rust
use sqly::migrate::{Migration, Migrator};

let migrations = [Migration::new(
    1,
    "create users",
    sqly::Sql::dialects(
        include_str!("migrations/0001.sqlite.sql"),
        include_str!("migrations/0001.postgres.sql"),
    ),
)];

let migrator = Migrator::try_new("accounts", &migrations)?;
migrator.run(&db).await?;
```

The namespace separates migration sets in a shared database. A fixed `_sqly_migrations` table keys entries by namespace and version. Stable checksums include version, description, and both SQL texts. Versions must be positive and strictly increasing. Validate existing ledger entries, checksums, and unsupported newer versions before applying pending work.

Serialize migration runs before reading the ledger: SQLite's write lock; PostgreSQL transaction-scoped advisory locks with a documented, deterministic key. First-time shared ledger creation also needs a common lock before the namespace lock. Use one transaction for the pending batch and its ledger entries. Failure rolls the batch back. Migration scripts may contain multiple statements. Authors must omit transaction control and nontransactional DDL so the runner can own the transaction. This is a trusted-script contract, not a SQL parser or statement filter.

No down migrations or automatic schema discovery initially. Migration errors have their own typed enum. Full Conveyor replacement is required for the first release, including its migration runner. Provide an explicit, validated compatibility path from Conveyor's existing ledger to sqly's migration management. Preserve applied migration identity and byte-sensitive checksum evidence; do not rerun applied migrations, silently bless mismatches, or require database recreation. The adoption mechanism below must be implemented and tested on both backends before cutover. Conveyor-specific schema and historical metadata remain application-owned; retaining its old runner permanently does not satisfy replacement.

Provide explicit generic legacy-ledger adoption in the migration API, with application-supplied table identification and compatibility policy. For Conveyor, retain its checksum byte format: SHA-256 of decimal version, NUL, description, NUL, SQLite SQL, NUL, PostgreSQL SQL. Validate the complete legacy prefix, descriptions, and checksums; then atomically copy version, description, checksum, and applied timestamp into the `conveyor` namespace without replaying DDL. Reject conflicting canonical rows. Preserve target/minimum-upgradeable checks. Quiesce old binaries during cutover so old and new runners cannot advance separate ledgers. Test corruption, partial and repeated adoption, newer schemas, concurrent startup, and rollback before replacing the old runner.

## Packaging and compatibility

Use a single library crate. Default features are empty. `sqlite` and `postgres` independently enable drivers; `uuid`, `time`, `json`, and `migrate` are additive integrations. `ambient` enables `ScopedDatabase` and its task-local scope mechanics. A build without a requested backend returns a clear unsupported-backend error. Each backend alone and both together must compile and pass their applicable checks.

An application wanting Conveyor's value set would select:

```toml
[dependencies]
sqly = { version = "0.1", features = ["sqlite", "postgres", "uuid", "time", "json", "migrate", "ambient"] }
```

This is a proposed future dependency declaration, not a claim that this package/version is published. Check crates.io name availability before publication. The first version uses Tokio, Rust 2024, SQLx 0.9, and Rust 1.94 as its minimum supported compiler. The selected locked dependencies and validation probes compile and pass on Rust 1.94.0. Keep the development compiler and formatter pins, and update the library MSRV declaration and check when implementation adopts these dependencies.

Keep SQLx types out of the normal public API. The optional UUID, time, and JSON types and the futures\_core::Stream trait are intentional public dependency commitments. Streaming is included in the first release. No derive crate, ORM, general query builder, raw pool access, or alternative runtime initially. Consider those additions only from concrete consumer needs.

## Implementation sequence and acceptance checks

1. Convert the template to a library. Update the purpose, documentation, development tasks, and release workflow. Replace binary archives with library packaging checks. Do not enable automatic publication yet.
2. Carry the validated API signatures into the library and turn the probes into production conformance tests. Preserve mutable borrowing, typed NULLs, owned bindings, external `FromRow` and `Encode`/`Decode` implementations, borrowed custom bindings, optional custom values, representation trait bounds, and `Send` futures on Rust 1.94. Compile the public examples against the actual library.
3. Implement pools, numbered parameter handling with typed preparation and count checks, codecs, and row mapping. Run the same behavior suite against SQLite and PostgreSQL, including single-backend feature builds. Cover repeated parameters, typed NULLs, and missing or surplus bindings. Verify placeholder-like text in strings and comments passes unchanged to the driver. Do not add statement-policing tests. Do not treat SQLite-only results as portability evidence. Verify downstream-defined types on both backends, including encoding and validation errors with preserved sources, checked decoding, nullable custom values, and compile-time rejection of unsupported representation types.
4. Add explicit transactions, locks, and optional ambient scopes. Verify scoped reads/writes, rejection of every unscoped query/query\_as execution method (including RETURNING), unscoped read/read\_as pool routing, cross-store atomicity, nested-entry rejection before acquisition, database identity, spawned-task isolation, concurrent-future serialization, closure errors, panics, and cancellation on both backends. Also verify rollback, query cancellation, pool cleanup, concurrent writers, missing/duplicate lock targets, and commit uncertainty handling. Use a dependent-row revision test to catch PostgreSQL pre-lock snapshot mistakes. For in-memory SQLite, verify schema and committed data survive query-connection replacement after cancellation and recycling. Verify independent-instance isolation, shared clones, keeper lifetime during active transactions and cancelled shutdown, and terminal DatabaseLost behavior. Add streaming tests for incremental consumption, mid-result errors, early drop, connection cleanup, explicit-transaction borrow rules, ambient scope identity, attempted stream escape, and scope exit with an unfinished stream. On both backends, verify competing queries, locks, and streams return ActiveStream before I/O; the pre-execution rejection alone does not abort the transaction; reuse after stream drop waits only for driver cleanup; uncertain cleanup aborts the transaction; and successful closure return with a live stream rolls back with ActiveStream. Verify invalidated streams cannot resume database work.
5. Add namespaced migrations and generic legacy-ledger adoption. Verify concurrent first startup, rollback, checksum changes, namespace separation, target/minimum-upgradeable policy, and newer-schema rejection. Cover valid, corrupt, partial, repeated, and conflicting adoption on both backends. Preserve historical SQL bytes and ledger metadata.
6. Fully replace Conveyor's existing persistence layer with sqly in the first release. Migrate every store and persistence consumer, runtime connection setup, health checks, transaction/locking paths, migration execution, and affected test infrastructure. Adopt ScopedDatabase for Conveyor's ambient semantics and remove the replaced pool dispatch, codecs, query execution, and migration-runner implementations. Keep application schemas, domain conversions, and operation policies in Conveyor. Replace the dynamic card-list query with static variants. Add the SQLite timestamp normalization migration, preserve domain serialization and digests, and quiesce old binaries for ledger adoption. Verify fresh startup and upgrades of existing databases on both backends without data loss or migration replay. Run Conveyor's complete applicable database and application behavior suites. Also build a small second consumer with a different schema to check that sqly remains reusable. A one-store adapter is an intermediate implementation step, not the release acceptance boundary.

## Evidence and design limits

[Technical validation](../reviews/technical-validation.md) records 26 distinct passing runtime probes across the validation runs and two expected compile failures. The probes validate numbered parameters, typed preparation, custom codec signatures, transaction borrowing, scope-owned cursors, in-memory keeper naming, legacy-ledger transfer, and the Rust 1.94 dependency baseline. The [validation harness](../validation/) is a prototype, not the production library or a complete conformance suite.

Conveyor's `crates/components/persistence/src/enabled.rs` already wraps both pools. Its `seam.rs` supplies the starting point for value codecs, query execution, and transaction locking. Its migration runner and schema crate show the separation between execution and application-owned DDL. These are implementation references, not APIs to copy unchanged.

The accepted Conveyor ambient-write-scope.md decision specifies task-local scope behavior. The inspected store implementation still passes explicit transaction handles. The first release must supply the optional scope machinery and move Conveyor stores onto that API as part of full replacement. Neither the existing code nor these API sketches prove the ambient policy is implemented.

SQLx documents rollback on unfinished transaction drop and shared-pool shutdown. These are the implementation starting points, with the complete cancellation and cleanup contracts to be verified by production sqly tests. See [SQLx transactions](https://docs.rs/sqlx/0.9.0/sqlx/struct.Transaction.html) and [SQLx pools](https://docs.rs/sqlx/0.9.0/sqlx/struct.Pool.html). The locking design follows [SQLite transaction behavior](https://www.sqlite.org/lang_transaction.html) and [PostgreSQL row locks](https://www.postgresql.org/docs/18/explicit-locking.html).

## Unresolved questions

No unresolved user scope questions remain. Accepted scope includes full Conveyor replacement, static SQL initially, streaming, immediate ActiveStream errors for competing ambient operations, and public Encode/Decode traits over built-in representations. Runtime-generated SQL remains an exception only if full Conveyor replacement requires it.

Technical feasibility and library implementation checks are complete, including the full legacy-ledger validator and transaction/stream cleanup tests. Remaining delivery work includes the CI toolchain setup fix, Conveyor timestamp normalization, full Conveyor replacement, and application acceptance. License and registry-name selection remain separate release decisions.
