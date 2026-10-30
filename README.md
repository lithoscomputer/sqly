# sqly

Async SQL for Rust applications that use SQLite and PostgreSQL through one API.

[![CI](https://github.com/lithoscomputer/sqly/actions/workflows/routine.yml/badge.svg)](https://github.com/lithoscomputer/sqly/actions/workflows/routine.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](#license)

```rust
use sqly::{Database, SqliteOptions};

#[tokio::main]
async fn main() -> sqly::Result<()> {
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

Part of [Lithos](#part-of-lithos), a family of Rust building blocks for AI tools.

## Why does this exist?

sqly provides shared query, value, and transaction types for applications that support both SQLite and PostgreSQL. It uses SQLx internally and keeps driver types out of its public API.

TODO(human): What application need led to sqly, and why did using SQLx directly not fit?

## Who is it for?

**Use it if** you are building a Tokio application that needs SQLite, PostgreSQL, or both, and you want to own the SQL and transaction boundaries.

**Look elsewhere if** you need compile-time SQL checking or MySQL support; [SQLx](https://github.com/transact-rs/sqlx) provides those capabilities.

## What does it do?

- Connect through a pool and run parameterized queries with buffered or streamed results.
- Map rows and application types through `FromRow`, `Encode`, `Decode`, or a fallible closure.
- Run explicit write transactions and lock existing rows.
- Share a write transaction across store methods with optional task-local scopes.
- Apply namespaced migrations and adopt an existing migration ledger.

**Out of scope:** sqly does not generate SQL or retry writes automatically. Your application supplies the Tokio runtime, schema, SQL, and transaction policy.

No features are enabled by default. Enable at least one database driver to connect.

| Feature | Default | Enables |
|---|---|---|
| `sqlite` | No | SQLite connections |
| `postgres` | No | PostgreSQL connections |
| `uuid` | No | UUID values |
| `time` | No | Timestamp values from the `time` crate |
| `json` | No | JSON values from `serde_json` |
| `migrate` | No | Migrations and legacy-ledger adoption |
| `ambient` | No | Task-local write scopes |

## How do I use it?

### Install

This library is under development and is not published to crates.io. From your application directory, add it from Git:

```sh
cargo add sqly --git https://github.com/lithoscomputer/sqly --features sqlite
cargo add tokio --features macros,rt-multi-thread
```

Use Rust 1.94 or later. The examples run with an in-memory SQLite database and need no database server. Copy the first example into `src/main.rs`, then run `cargo run`.

To use PostgreSQL, enable `postgres` and supply a connection URL or `PostgresOptions`. See [connection settings](https://github.com/lithoscomputer/sqly/blob/main/docs/usage.md#connections) for credentials and TLS configuration.

### Use it as a library

Lock an existing row, update it, and commit the transaction:

```rust
use sqly::{Database, Lock, SqliteOptions};

#[tokio::main]
async fn main() -> sqly::Result<()> {
    let db = Database::connect(SqliteOptions::in_memory()).await?;
    db.query("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL)")
        .execute().await?;
    db.query("INSERT INTO users VALUES ($1, $2)")
        .bind(1_i64).bind("Ada").execute().await?;
    let mut tx = db.begin_write().await?;
    tx.require_lock(Lock::row("users").key("id", 1_i64)).await?;
    tx.query("UPDATE users SET name = $1 WHERE id = $2")
        .bind("Grace").bind(1_i64).execute().await?;
    tx.commit().await?;
    let row = db.query("SELECT name FROM users WHERE id = $1")
        .bind(1_i64).fetch_one().await?;
    assert_eq!(row.try_get::<String>("name")?, "Grace");
    db.close().await;
    Ok(())
}
```

See the [usage guide](https://github.com/lithoscomputer/sqly/blob/main/docs/usage.md) for query mapping, transaction failures, ambient scopes, streaming, migrations, and connection settings. The [warehouse consumer](https://github.com/lithoscomputer/sqly/blob/main/consumers/warehouse/src/lib.rs) shows these APIs in a separate application schema.

Generate the API reference from your application with `cargo doc -p sqly --no-deps --open`. It includes the features you enabled for sqly.

## Status and compatibility

- **Maturity:** Under development, version 0.1.0; publication is disabled. The [release checklist](https://github.com/lithoscomputer/sqly/blob/main/DEVELOPING.md#continuous-integration-and-releases) tracks remaining acceptance work.
- **MSRV:** Rust 1.94, edition 2024.
- **CI platforms:** Linux x86_64 and arm64, and macOS arm64. Linux tests both databases; macOS runs SQLite tests and compiles both drivers.
- **Stored formats:** [Value encodings](https://github.com/lithoscomputer/sqly/blob/main/docs/usage.md#querying) and the [migration ledger and checksums](https://github.com/lithoscomputer/sqly/blob/main/docs/usage.md#migrations) matter beyond Rust types.
- **Change history:** No changelog yet; see [commits](https://github.com/lithoscomputer/sqly/commits/main/).

## Part of Lithos

| Crate | Role |
|---|---|
| [lithos-llm](https://github.com/lithoscomputer/lithos-llm) | Model access: one client and catalog across LLM providers |
| [pebble](https://github.com/lithoscomputer/pebble) | Agent loop, plus a coding agent and CLI built on it |
| [sandbox-driver](https://github.com/lithoscomputer/sandbox-driver) | Sandboxes (host, Docker, cloud VMs) behind one interface |
| [petri](https://github.com/lithoscomputer/petri) | Workflow engine that runs CI and agent workflows on the crates above |
| **[sqly](https://github.com/lithoscomputer/sqly)** | **Async SQL for SQLite and PostgreSQL** |

## Contributing

Development setup, tests, and conventions are in [DEVELOPING.md](https://github.com/lithoscomputer/sqly/blob/main/DEVELOPING.md).

## License

Licensed under the [MIT License](https://github.com/lithoscomputer/sqly/blob/main/LICENSE).
