# sqly technical validation

Date: 2026-09-05. Result: the core interface is feasible. The probes support
implementation, with the design changes below. This is not a completed sqly
library or a completed Conveyor migration.

## Evidence

The isolated crate is `.ai/validation/`. Its `run` script provisions its own
PostgreSQL container and runs the matrix. It uses an existing PostgreSQL
18.4 image by digest, with temporary storage. It does not access application
databases. The template source and Conveyor source were not changed.

Registry dependencies are pinned to Conveyor's 2026-08-25 lockfile, except
`async-stream` and `async-stream-impl` 0.3.6, both published 2024-10-01. All
Cargo builds ran offline. Dependency provenance is recorded in
`.ai/validation/evidence/dependency-provenance.json`.

| Check | Result |
| --- | --- |
| Both drivers, Rust 1.97.1 | 24 tests passed: 23 driver/ownership tests and one downstream codec test |
| Both drivers, Rust 1.94.0 | The same 24 tests passed; two additional typed-preparation probes also passed |
| SQLite feature alone | 13 tests passed |
| PostgreSQL feature alone | 12 tests passed |
| Neither backend enabled | Library compiles |
| External custom type, borrowed binding, nullable custom type | Compiles and conversion round trips pass |
| Query/stream lifetimes, Send futures, Unpin stream wrapper | Compile-time probes pass |
| Connection input conversions and static dialect pairs | Compile-time probes pass on Rust 1.94 |
| Overlapping explicit transaction queries | Rejected with E0499 |
| Unsupported custom representation | Rejected with E0277 |
| Historical migration checksum evidence | All five independently pinned historical checksums match; seven migrations captured |

Logs are under `.ai/validation/evidence/`. The normal query and connection
facade in `src/lib.rs` is a signature probe with deliberately unimplemented
I/O. It is not counted as working database code. The integration tests use
real SQLx SQLite and PostgreSQL connections. The ambient ownership prototype
also uses real driver transactions and streams.

## 1. Use shared numbered parameters; no SQL scanner is needed

SQLx 0.9 binds `$1`, `$2`, and repeated or reordered references correctly on
both databases. Quoted strings and comments containing parameter-like text
pass unchanged to the database. Typed NULL parameters work.

Recommendation: adopt `$1` through `$N` for the public SQL contract. Bind
values in numeric order. Every number from 1 through N must occur at least
once; references may repeat or appear in a different textual order. Update
the draft examples from `?` to this syntax. No statement policing is added.

Prepare the selected statement on the connection that will execute it, passing
the encoded argument type information through `prepare_with`. This avoids
PostgreSQL inferring the wrong type for an unconstrained `$1`. The additional
typed-preparation probes pass on both backends. Use
SQLx statement metadata to compare the expected count with the supplied
binding count before execution. This requires preparation I/O and schema
access; do not promise validation before all I/O. Preparation can be cached.

The consecutive-number rule is a caller contract. SQLite reports distinct
parameter slots, not the highest numeric name: `SELECT $2` reports one
parameter and binding one value leaves `$2` NULL. SQLite also accepts missing
bindings as NULL and silently ignores surplus bindings, whereas PostgreSQL
rejects those arity mismatches. Count checking makes correctly numbered SQL
consistent. It cannot validate arbitrary numbering without additional
inspection, which is intentionally outside scope.

Source: local SQLx 0.9 `sqlx-sqlite/src/arguments.rs` and
`sqlx-sqlite/src/statement/mod.rs`. Tests:
`numbered_parameters_repeat_and_reorder_without_rewriting`,
`typed_null_and_metadata_guard_work_before_execution`, and the backend-specific
missing/surplus/gap cases in `tests/drivers.rs`.

## 2. Set the implementation baseline to Rust 1.94

SQLx 0.9 declares Rust 1.94. The complete selected dependency set and probes
compile and run on the installed Rust 1.94.0 toolchain. The existing template
still declares 1.85; it must change when the library implementation adopts
these dependencies. This validation does not change the template manifest.

Keep Rust 1.97.1 as the development compiler and the pinned nightly formatter.
Change the library's MSRV check to 1.94.0. Choosing SQLx 0.8 instead would be a
separate dependency decision and is not needed for the validated approach.

## 3. The public conversion traits work

An external crate can implement `Encode<Repr = Uuid>` and
`Decode<Repr = Uuid>` for its own `UserId`. Blanket reference and `Option<T>`
implementations coexist without trait conflicts. A private representation
implementation and public sealed `SqlValue` marker prevent applications
from adding unsupported driver types. Custom and nullable conversions pass.

The signature probe also accepts `query_as::<User>(sql)` without an executor
type parameter, `&str` and typed connection options through `TryInto`, and
static `Sql::dialects` values. Closure-based writes can borrow application
stores and inputs. Futures remain Send with suitable captured types.

This validates the shape, not every planned representation. UUID conversion
is exercised in the portable codec probe; backend runtime tests cover
integers, boolean, text, bytes, floats, and integer NULLs. Full UUID/JSON/time
backend codecs and their error normalization still need implementation tests.

## 4. Ambient streams need scope-owned cursors

A stream wrapper that owns the only transaction guard and escapes its scope
can prevent scope exit from rolling back. The scope must retain the ability
to cancel and drop driver work itself.

The tested strategy stores a boxed driver cursor in scope-owned state. A
public stream handle carries identity and polls that cursor. The cursor's
async generator owns the driver transaction while streaming and returns it
to the scope when exhausted. The scope owner invalidates handles and drops
its cursor on cancellation or unsuccessful exit. No detached producer task
is needed. No mutex guard is held while running application callbacks.

Both backends pass tests for cross-store atomicity, unscoped and spawned-task
write rejection, nested scope rejection, database identity mismatch,
ActiveStream on competing writes or streams, reuse after exhaustion, rejection
of a returned live stream, and rollback after cancellation with a live cursor.

The small prototype conservatively aborts on early stream drop. Production
code may resume only after establishing that cleanup left the transaction
usable. Buffered-operation queuing, lock-operation contention, commit outcome
ambiguity, panic handling, all early-drop cleanup paths, and complete
scope-escape combinations still require production conformance tests.

## 5. The keeper requires a named SQLite URI

A named `file:` URI with in-memory and shared-cache options lets a separate
keeper retain schema and data while the sole pooled query connection is
closed and replaced. A plain filename with the flags did not preserve the
shared database in the initial probe; adding the URI prefix fixed it.

The test confirms data survives query connection replacement and disappears
after both pool and keeper close. Use a unique URI per Database identity.
The final implementation must additionally test keeper loss, cancellation of
shutdown, and active-lease lifetime retention. These ownership guarantees are
not supplied automatically by SQLx's pool.

## 6. Conveyor replacement is feasible but needs concrete compatibility work

### Static SQL

The production store search found runtime SQL assembly in
`cards-store/src/store.rs::list_cards`. Its filters and ordering have four
fixed shapes: no state filter, review, superseded, and another supplied
state. Static statements can cover these cases while binding state values.
No runtime-SQL API requirement was established. Other matches were fixture
construction, URLs, hashing, or internal dialect rendering.

### Additional locks

Conveyor calls `lock_owner` after a transaction has begun, including in the
refresh checkpoint path. It also locks both a source and its feed. The draft
only shows `write_locking` at scope entry and `Transaction::lock` for explicit
transactions.

Add `ScopedDatabase::lock(Lock) -> Result<bool>` for an existing scope. It
must reject unscoped use and ActiveStream before acquisition. It uses the
same lock representation and backend behavior as explicit transactions.
Applications can then acquire multiple row locks in a consistent order and
recheck soft-deletion and related-row conditions after acquisition. A nested
write scope is not the replacement for an additional lock.

### Existing migration ledger

All seven current Conveyor migration files execute on both databases. An
atomic copy from `_conveyor_migrations` into a namespaced `_sqly_migrations`
preserves version, description, checksum, and applied timestamp. Repeating
the copy does not duplicate rows. The five independent historical checksum
constants match the exact captured file bytes.

Recommended implementation path: keep Conveyor's existing checksum byte
format for these migration definitions; validate the complete legacy ledger
prefix against the supplied manifest, including descriptions and checksums;
then adopt the rows atomically into the `conveyor` namespace without running
old DDL. A conflicting canonical row must fail, not be silently skipped.
The probe verifies transfer mechanics, not the complete adoption validator.

Expose a generic explicit legacy-ledger adoption option in the migration API,
with application-supplied table identification and compatibility policy.
Preserve Conveyor's target/minimum-upgradeable checks. Quiesce old binaries
for cutover so old and new migration runners cannot advance separate ledgers.
Test corrupted, partial, already-adopted, and newer ledgers, concurrent startup,
and failure recovery before replacing Conveyor's runner.

### Timestamp storage

Conveyor's SQLite timestamp encoding uses RFC 3339 with variable fractional
width. The draft's proposed fixed six-digit UTC encoding is different.
The local SQLite probe shows `2026-08-18T12:34:56Z` sorts after
`2026-08-18T12:34:56.000001Z`, despite being the earlier instant.

Do not mix historical and new encodings in columns used for time comparisons
or ordering. Add a new application-owned data migration that parses existing
timestamp values and normalizes them to the selected canonical format before
cutover. Audit all temporal columns; fail with useful diagnostics on values
that cannot be represented. Leave historical migration bytes unchanged.
Keep Conveyor's API serialization and digest construction in its domain types;
changing database encoding must not change published representations or hashes.

## Scope of completion

Technical feasibility and the principal ownership strategy are validated.
The remaining work is to accept the proposed document changes and implement
sqly with the production conformance checks above. Full Conveyor replacement
remains the first-release acceptance requirement; it has not been performed.

No new user scope decision is required by these findings. The proposals retain
static SQL, avoid statement policing, include streaming and custom types, and
support optional ambient transactions.
