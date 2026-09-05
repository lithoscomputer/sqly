# Buffered query implementation verification

Date: 2026-09-05. Result: the first working database API is implemented.

## Delivered behavior

The library exposes `Database`, `DatabaseBuilder`, connection options, static
`Sql`, `Query`, `ExecuteResult`, `Row`, `FromRow`, typed errors, and public
`Encode`/`Decode` traits over sealed built-in representations.

It supports SQLite and PostgreSQL connections, owned bindings, buffered execute
and fetch methods, declared dialect pairs, custom application value types,
typed NULLs, and optional UUID/time/JSON integrations. Driver types stay behind
the public interface. Options, queries, rows, and top-level error formatting
redact their contents; diagnostic causes remain available through error sources.

SQLite memory databases retain committed state through query-connection
replacement using a private keeper. A pool callback retains keeper ownership
through outstanding leases and cleanup. Close drains the pool before closing
the keeper. Unexpected keeper loss is terminal. A per-operation SQLite progress
handler allows cancelled work to stop; uncertain pooled operations discard their
connection before reuse.

No explicit transactions, locks, ambient scopes, streaming, or migration APIs
are implemented yet. The accepted `ambient` and `migrate` feature names remain
reserved for subsequent milestones.

## Verification

`CARGO_NET_OFFLINE=true mise run check:nightly` passed on macOS arm64.
The [full gate log](evidence/buffered-queries-check.log) records formatting,
Clippy, workflow audit, feature builds, documentation checks, MSRV builds,
runtime tests, release-mode tests, and package verification.

The runtime matrix passed on both the development compiler and Rust 1.94.0:

| Feature configuration | Nextest test executions per run |
| --- | --- |
| All features | 21 passed |
| PostgreSQL with integrations | 10 passed |
| SQLite with integrations | 14 passed |
| Neither backend | 3 passed |

These counts overlap across builds and include the subprocess-test entry point;
they are not counts of distinct behaviors. Two rustdoc checks passed: the public
example compiles, and an unsupported external representation fails to compile.
The [MSRV runtime log](evidence/buffered-queries-msrv.log) records the separate
`mise exec -- bin/test +1.94.0` run.

The tests cover CRUD and result semantics, constraints, reordered and repeated
bindings, missing and surplus bindings, type-sensitive preparation, nullable
custom values, owned borrowed data, checked decoding, preserved custom error
sources, and optional codecs. Lifecycle tests cover keeper loss, replacement,
cancelled shutdown, acquisition timeout, query cancellation, TLS fallback
rejection, and inherited PostgreSQL setting rejection. SQLite tests also cover
file creation, WAL, read-only access, missing parent directories, invalid
booleans, and canonical timestamp storage and ordering.

`bin/test` and `bin/with-test-database` provision disposable PostgreSQL 18.4 by
the same fixed image digest used in technical validation. The image index was
checked for Linux amd64 and arm64 variants. Linux CI runs both databases; hosted
macOS CI runs SQLite and no-backend behavior tests and compiles both drivers.
Hosted CI itself has not run in this session. Required PostgreSQL tests fail
when their fixture is absent. Containers created for verification were removed.

All registry dependency versions and checksums remain within the validated
lockfile set. Cargo dependency resolution ran offline. No application database,
Conveyor source, commit, or publication was changed by this task.

## Findings beyond the original probes

### PostgreSQL parameter metadata

Typed PostgreSQL preparation reports supplied trailing type hints even when the
SQL never references those parameters. Comparing that count alone fails to
reject surplus bindings. The original probes separately tested untyped counts
and typed preparation; they did not establish their combined behavior.

The implementation prepares with the full type vector, then removes trailing
hints until the server reports a referenced parameter beyond the hints or
requires that parameter's type to resolve the statement. This establishes the
referenced count under the accepted consecutive-number contract. It does not
scan or rewrite SQL. Missing arguments whose types cannot be inferred produce
`BindCount` with `expected: None`. Other count mismatches report a known count.
Tests include unused hints on zero-parameter SQL and surplus hints on an
expression that requires an explicit parameter type.

These metadata probes currently run on pooled autocommit connections. Before
using them in explicit transactions, isolate speculative preparation errors
with an internal savepoint or an equivalent safe mechanism. An expected metadata
probe failure must not leave PostgreSQL's transaction aborted. Actual query
preparation failures still follow the finalized rollback-only contract.

SQLx also caches statements by SQL text rather than by sqly's argument types.
PostgreSQL statement caching is disabled to prevent stale type metadata when
identical SQL is used with different representations. Correctness currently
costs additional preparation round trips. Future caching must preserve the
SQL-and-type distinction and the transaction rules above.

### PostgreSQL environment defaults

SQLx provides no public way to clear inherited `PGOPTIONS`, `PGSSLROOTCERT`,
`PGSSLCERT`, or `PGSSLKEY` fields. Sqly rejects these variables before connecting
and documents explicit option alternatives. It constructs without password-file
lookup and overwrites the other inherited defaults. It does not mutate process
environment to configure a connection.

## Remaining delivery work

Proceed to explicit transactions and locks, including the preparation isolation
noted above. Then deliver ambient scopes, streaming, migration adoption, complete
Conveyor replacement, and a second consumer as required by the implementation
plan.

Package verification still reports the previously recorded yanked transitive
`chacha20 0.10.1` version and missing release metadata. Resolve those release
follow-ups, including license selection, before publication. Publication remains
disabled.
