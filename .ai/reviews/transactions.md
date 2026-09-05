# Explicit transactions and row locks

Status: Implemented and verified on 2026-09-05.

## Public interface

`Database::begin_write()` returns an owned `Transaction`. It provides `query`, `query_as`, `lock`, `commit`, and `rollback`. Queries borrow the transaction mutably. Commit and rollback consume it.

`Lock::row(table).key(column, value)` selects an existing primary or unique key. Repeated keys support composite selectors. Missing rows return `false`; multiple matches return `Error::NonUniqueLock`. Empty selectors, duplicate columns, NULL values, and invalid identifiers fail before I/O. Simple ASCII identifiers are quoted and limited to 63 bytes. Values use the existing binding API.

SQLite begins with `BEGIN IMMEDIATE`. PostgreSQL begins at READ COMMITTED and acquires a row lock in a separate `SELECT ... FOR UPDATE`. Applications read dependent rows after acquiring the lock. Queries through `Database` continue to use the pool.

## Ownership and failure behavior

Each transaction owns its pooled connection and a database handle. An unfinished transaction marks its connection for discard. SQLx closes that connection, which rolls back unfinished work. It cannot return to the pool with uncertain state. Sqly does not start detached cleanup tasks.

An operation marks the transaction rollback-only before its first await. Success clears that flag. Local encoding and bind-count errors also clear it. A database error or cancelled operation leaves it set. Subsequent queries fail with `TransactionAborted`. Commit then attempts rollback and returns `TransactionAborted`; explicit rollback can report cleanup failure. Lock validation and buffered row-conversion failures do not abort the transaction.

PostgreSQL parameter probes run inside an internal savepoint. Expected parameter-resolution failures roll back to that savepoint before releasing it. Bind-count errors also restore the savepoint. Actual database failures leave the outer transaction rollback-only. This preserves typed NULL and surplus-binding checks without adding a tokenizer or a public savepoint API.

Confirmed commit rejections retain their constraint or contention classification. Failures without a confirmed rejection return `CommitUnknown` and preserve the source. Cancellation during COMMIT can also leave the outcome unknown. No write or transaction is retried automatically.

A transaction retains the in-memory SQLite keeper through connection cleanup. An active transaction can finish after pool shutdown starts or its close future is cancelled. Unexpected keeper loss remains terminal and prevents further work or commit.

## Verification

`CARGO_NET_OFFLINE=true mise run check:nightly` passed. The gate covers formatting, Clippy, rustdoc, debug and release tests, all feature builds, Rust 1.94 builds, workflow checks, and package verification. Four rustdoc checks passed: two examples compile and two invalid programs fail to compile as intended.

| Runtime configuration | Debug | Release |
| --- | --- | --- |
| Both backends and all features | 31 passed | 31 passed |
| PostgreSQL with integrations | 15 passed | 15 passed |
| SQLite with integrations | 20 passed | 20 passed |
| No backend | 3 passed | 3 passed |

The tests cover:

- Commit, rollback, rollback on drop, deferred constraint failures, and connection reuse.
- Database errors, cancelled queries, rollback-only behavior, and local errors that leave the transaction usable.
- Typed parameter probes and bind-count failures inside PostgreSQL transactions.
- Composite selectors, missing and non-unique matches, selector validation, and redacted formatting.
- Competing SQLite writers, cancellation during `BEGIN IMMEDIATE`, and independent pooled reads.
- A PostgreSQL lock waiter that reads a dependent-row revision only after the first writer commits.
- In-memory schema survival, active-transaction keeper loss, cancelled shutdown, and keeper lifetime after the last external handle drops.
- A TCP proxy that withholds PostgreSQL's COMMIT acknowledgement after the server commits. One case drops the connection and returns `CommitUnknown`; another cancels the commit future. Both verify that the write persisted.
- Public transaction examples and compile-time rejection of overlapping mutable transaction queries. The PostgreSQL tasks also check that lock and commit futures can be spawned.

Evidence: [extended gate output](evidence/transactions-check.log). Nextest marked one existing PostgreSQL environment subprocess test as leaky in the release run because an output pipe remained open after exit. A [focused rerun](evidence/transactions-subprocess-check.log) of both subprocess tests passed without that flag. No functional test failed.

## Remaining work

The next implementation stage is the optional ambient API. It will route store calls through task-local write scopes without transaction handles in store method signatures. It can build on the owned transaction and rollback-only behavior implemented here. Streaming and migrations remain separate stages. Full Conveyor replacement and an independent second consumer remain release acceptance work.

The crate is still unpublished. The existing package warnings about missing license/metadata and the locked, yanked `chacha20 0.10.1` remain release follow-ups. This milestone adds no dependencies and changes no dependency versions.
