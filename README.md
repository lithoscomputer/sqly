# sqly

sqly is an async Rust library for applications that support SQLite and PostgreSQL. Applications own the Tokio runtime, schema, and transaction policy.

Connections, buffered and streaming queries, portable values, explicit transactions, row locks, optional ambient scopes, and migrations are implemented. A separate warehouse consumer exercises the public API on both backends. The crate is under development and is not published. Full Conveyor replacement remains release acceptance work.

## Querying

Enable `sqlite`, `postgres`, or both. Default features are empty.

```rust
use sqly::{Database, SqliteOptions};

async fn example() -> sqly::Result<()> {
    let db = Database::connect(SqliteOptions::in_memory()).await?;
    db.query("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .execute().await?;
    db.query("INSERT INTO users VALUES ($1, $2)")
        .bind(1_i64).bind("Ada").execute().await?;
    let row = db.query("SELECT name FROM users WHERE id = $1")
        .bind(1_i64).fetch_one().await?;
    assert_eq!(row.try_get::<String>("name")?, "Ada");
    db.close().await;
    Ok(())
}
```

`query_as::<T>` maps rows through your `FromRow` implementation. Implement `Encode` and `Decode` to map domain types to built-in representations. Bindings are owned. `Encode::encode(self)` moves owned representations into the query; borrowed strings and byte slices are copied when bound. Application types can implement `Encode` separately for their owned and borrowed forms.

For counts and one-off projections, use a fallible mapper. It works with buffered results and streams, and the closure can borrow application state:

```rust
async fn example(db: &sqly::Database) -> sqly::Result<()> {
let count: i64 = db.query("SELECT COUNT(*) AS count FROM users")
    .try_map(|row| row.try_get::<i64>("count"))
    .fetch_one().await?;
    Ok(())
}
```

`fetch_one` returns the first row or `RowNotFound`; `fetch_optional` returns the first row or `None`. Neither checks uniqueness. `try_map` is also available on `ScopedDatabase::read`; mapped queries have no `execute` method. Mapping errors follow the same buffered and streaming cleanup rules as `FromRow` errors.

SQL must be static and contain one statement. Use `$1` through `$N`, in numeric binding order, with every number present. References may repeat or appear out of order. Use `Sql::dialects(sqlite, postgres)` for a declared dialect difference. Sqly trusts application SQL and does not tokenize, rewrite, or police statements. Parameter count checks require preparation I/O. PostgreSQL preparation includes additional type-hint probes to reject unused trailing bindings. `BindCount` reports an unknown expected count when the server cannot infer a missing parameter's type.

`uuid`, `time`, and `json` enable value integrations. SQLite stores UUIDs as canonical text, instants as UTC text with six fractional digits, and JSON as text. PostgreSQL uses native UUID, TIMESTAMPTZ, and JSONB types. Non-finite floats, sub-microsecond timestamps, and instants outside UTC years 0001–9999 fail. `migrate` enables namespaced migrations and legacy-ledger adoption. `ambient` enables task-local write scopes.

## Transactions and row locks

```rust
use sqly::{Database, Lock};

async fn rename(db: &Database, id: i64, name: &str) -> sqly::Result<bool> {
    let mut tx = db.begin_write().await?;
    if !tx.lock(Lock::row("users").key("id", id)).await? {
        tx.rollback().await?;
        return Ok(false);
    }
    tx.query("UPDATE users SET name = $1 WHERE id = $2")
        .bind(name).bind(id).execute().await?;
    tx.commit().await?;
    Ok(true)
}
```

`begin_write` returns an owned transaction. Queries borrow it mutably, so simultaneous queries through one transaction do not compile. SQLite starts with `BEGIN IMMEDIATE`; PostgreSQL uses READ COMMITTED. Queries through `Database` still use the pool.

Locks select an existing primary or unique key. Repeated `key` calls support composite keys. Missing rows return `false`; multiple matches return `NonUniqueLock`. Use `require_lock` on a transaction or ambient handle to require a row and return `LockNotFound` if absent. A handled missing-row error alone does not abort the transaction. Identifiers must be simple ASCII names of at most 63 bytes. Empty selectors, duplicate columns, and NULL keys fail before I/O. PostgreSQL uses a separate `SELECT ... FOR UPDATE`; SQLite checks existence while holding its writer reservation. Read dependent rows after acquiring the lock. Acquire multiple locks in a consistent order. Missing PostgreSQL rows are not locked.

`commit` and `rollback` consume the transaction. Dropping an unfinished transaction discards its connection; connection closure rolls back the work. A database error or cancelled operation makes the transaction rollback-only. Subsequent queries return `TransactionAborted`; committing that state rolls back and returns the same error. Local encoding, selector validation, bind-count, and buffered row-conversion errors do not abort it. PostgreSQL preparation probes use an internal savepoint to preserve this behavior.

A lost COMMIT acknowledgement returns `CommitUnknown`; the write may have committed. Cancellation during COMMIT has the same uncertainty. Sqly never retries a write or transaction automatically. Application SQL must leave transaction control to sqly. This is a caller contract; sqly does not parse or police statements.

`Database::close` waits for active transactions. Finish or drop them before awaiting shutdown. Cancelling shutdown leaves the pool closed; an existing transaction can still finish, and calling `close` again drains cleanup.

## Ambient scopes

Enable `ambient` and give stores a cloned `ScopedDatabase` from `db.scoped()`. Store methods take application inputs; transaction handles stay out of their signatures.

```rust
async fn change_name(db: &sqly::ScopedDatabase, id: i64, name: &str) -> sqly::Result<()> {
    db.write_locking(sqly::Lock::row("users").key("id", id), || async {
        db.query("UPDATE users SET name = $1 WHERE id = $2")
            .bind(name).bind(id).execute().await?;
        Ok::<_, sqly::Error>(())
    }).await
}
```

`write_locking` returns `LockNotFound` without invoking the closure when its owner row is absent.

`write` and `write_locking` commit on closure success and roll back on error, panic, or cancellation. Application errors are preserved if rollback also fails. Caught database failures make the scope rollback-only. Nested scopes fail before acquisition. Separately connected databases cannot join one scope; clones share identity.

`query` and `query_as` require an active scope for every terminal method, including SELECT and RETURNING queries. `read` and `read_as` join an active scope and otherwise use the pool. Read builders provide fetch methods without `execute`; callers must supply read-only SQL. Builders resolve membership when executed. Spawned tasks inherit no scope. Concurrent buffered operations in one task serialize over the transaction.

## Streaming

`Query::fetch` and `ReadQuery::fetch` return `RowStream<T>`, which implements `futures_core::Stream<Item = sqly::Result<T>>`. Use `futures_util::TryStreamExt` to consume it. Execution begins on first poll; rows decode as consumed. The first error ends the stream. Drivers and server plans may buffer internally.

An explicit transaction stream keeps its mutable transaction borrow. Dropping a started stream early conservatively makes that transaction rollback-only. A pooled stream discards its connection when cleanup is uncertain. Dropping a stream from a write with RETURNING does not prove that the write rolled back.

Ambient scopes own their cursors. Competing queries, locks, and streams return `ActiveStream` without waiting or aborting the transaction. Exhaustion releases the connection for later operations. Early drop makes the transaction rollback-only. Returning success with an active stream invalidates it, rolls back, and returns `ActiveStream`. Polling a started stream in another scope or task fails before further I/O. Scope teardown also cancels retained buffered-operation futures.

## Migrations

Enable `migrate`. Build `Migration` values from a positive, strictly increasing version, description, and static `Sql`, then call `Migrator::try_new(namespace, &migrations)?.run(&db).await?`. Scripts may contain multiple statements; applications must omit transaction control and nontransactional DDL.

The `_sqly_migrations` ledger keys rows by namespace and version. A pending batch and its ledger entries commit together. SQLite reserves the writer; PostgreSQL takes transaction-scoped advisory locks before reading the ledger. Advisory keys are the signed first eight SHA-256 bytes of a prefix, NUL, and namespace: `sqly:migrations:ledger:v1` with an empty namespace, then `sqly:migrations:namespace:v1` with the selected namespace. The common lock remains held through the batch, so different namespaces also serialize.

`with_compatibility(target, minimum_upgradeable)` limits the target and rejects nonempty histories older than the supported minimum. Checksums preserve SHA-256 of decimal version, NUL, description, NUL, SQLite SQL, NUL, PostgreSQL SQL, including all SQL whitespace. Both dialects participate even in single-backend builds.

`adopt_legacy(LegacyLedger::try_new(table)?)` supports application-named legacy tables with `version`, `description`, `checksum`, and `applied_at` columns. The latter three columns contain text. It validates the complete prefix, preserves metadata bytes, completes matching partial copies, and rejects conflicts without replaying historical SQL. A missing legacy table permits fresh startup. Adoption and pending migrations share one transaction. Stop old runners before adoption; an old binary must not advance its separate ledger after cutover. Errors use `migrate::MigrationError`.

## Connections

Use a database URL (`&str`, `String`, or `&String`) or explicit `SqliteOptions` / `PostgresOptions`. `Database::builder()` configures pool size and acquisition timeout. Defaults are five connections and a 30-second acquisition timeout. Managed in-memory SQLite uses one query connection and a separate private keeper so connection replacement does not erase committed data. `ConnectOptions::as_sqlite` and `as_postgres` expose the sqly-owned settings. `SqliteOptions::filename` returns a file path, or `None` for in-memory storage, so applications can apply their own directory policy.

SQLite foreign keys are enabled. File creation, read-only mode, and WAL are explicit options. URLs support `mode=ro|rw|rwc|memory`; use option methods for WAL and busy timeout. `SqliteOptions::new(path)` accepts plain file paths; use `in_memory()` for isolated memory databases.

PostgreSQL requires an explicit host or Unix-socket directory, database, and username. A host starting with `/` selects a socket directory; URLs percent-encode that directory in the host field. Passwords come from the URL or options. TLS verifies the server identity by default; local plaintext connections must select `sslmode=disable`. Supported URL options are `sslmode=verify-full|verify-ca|require|prefer|allow|disable`, `sslrootcert`, and `application_name`. Unknown or duplicate URL options fail.

Sqly does not merge PostgreSQL environment settings or password files. `PGOPTIONS`, `PGSSLROOTCERT`, `PGSSLCERT`, and `PGSSLKEY` must be unset because SQLx cannot clear these inherited fields through its public API; sqly rejects such configuration before connecting. Use explicit options instead. Other SQLx environment defaults are overwritten with sqly's explicit values.

Options, queries, and top-level errors redact sensitive contents in formatting. Error source chains retain diagnostic causes and may contain server data. Do not expose those chains directly to clients.

## Development

Requires Rust 1.94 or later, using Rust 2024. Install the pinned tools:

```sh
mise trust
mise install --locked --jobs=1
mise run setup
mise run dev
mise run test
mise run check
```

The default test task provisions a disposable PostgreSQL container using Docker. See [DEVELOPING.md](DEVELOPING.md) for the feature matrix and test-service options. Release tags verify library packages; they do not publish a crate.
