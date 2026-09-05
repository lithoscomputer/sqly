# Public API review before adoption

Status: Accepted in full and implemented on 2026-09-05. Encoding ownership, fallible query mapping, owned URL conversions, required-lock semantics, and method documentation are implemented in separate commits. The rationale and tradeoffs below are retained as the original review; its proposed signatures now describe the implemented direction. The full local verification gate passed; see the implementation evidence below.

## Recommendation

Make one focused API refinement pass before Conveyor starts depending on sqly. Keep the existing architecture: concrete database handles, private drivers, static SQL, optional ambient scopes, explicit transactions, and owned query bindings. The highest-value breaking decision is ownership in `Encode`. Query mapping and method documentation would improve everyday use. Avoid expanding into an ORM, SQL parser, or general query builder.

| Priority | Recommendation | Compatibility and cost |
| --- | --- | --- |
| Before adoption | Let encoding consume owned inputs | Breaks downstream `Encode` implementations; best decided now |
| Before adoption | Document terminal and ambient method contracts in rustdoc | No runtime change; protects callers from surprises |
| Recommended | Add one fallible query mapping adapter | Additive API; requires careful buffered and streaming integration |
| Small improvement | Accept owned connection URL strings | Additive conversion implementations |
| Optional | Give required row locks an explicit API and error | Additive helper; changing the existing missing-row error is breaking |

## 1. Let owned bindings move their data

[Encode](../../src/value.rs) currently requires `fn encode(&self)`, while [Query::bind](../../src/query.rs) takes its argument by value. Built-in `String`, `Vec<u8>`, and JSON encoding clones the representation. Consequently, `.bind(owned_string)` consumes the caller's string but copies its contents into the query. The same issue applies to large byte arrays and JSON trees. This is established by source inspection; no performance claim has been benchmarked.

Recommended shape:

```rust
pub trait Encode {
    type Repr: SqlValue;
    fn encode(self) -> Result<Self::Repr>;
}
```

Implement owned inputs by moving their representation. Implement borrowed inputs such as `&str`, `&String`, and `&[u8]` by copying into owned storage. Preserve immediate encoding at `bind`, typed NULLs, and error reporting before database I/O.

For application types, implement `Encode` for the owned type and, when needed, its reference type. An owned SKU can move its inner string; `&Sku` can copy it. Do not promise the current blanket reference implementation will survive unchanged: a consuming method cannot be called through an arbitrary shared reference. Explicit reference implementations avoid requiring every domain type to implement `Clone`.

Tradeoff: some domain types need two small implementations. The alternative is retaining `encode(&self)` and adding an overridable consuming method with a default implementation. That preserves a blanket reference path but gives the trait two encoding routes to explain and maintain. Prefer the single consuming method while there are no established external implementors.

Acceptance: exercise owned and borrowed built-ins, non-Clone domain types, optional domain values, borrowed lifetimes ending before execution, and both backend bindings. Confirm the owned representations move without the extra sqly encoding copy.

## 2. Put contracts where callers use them

The README explains important behavior that the public methods in [query.rs](../../src/query.rs) and [scoped.rs](../../src/scoped.rs) do not document individually.

Document these directly on the relevant methods, with maintained examples:

- `fetch_one` returns the first row and does not assert that exactly one row matched. `fetch_optional` means zero or one returned value, not a uniqueness check. Preserve the accepted behavior.
- `ScopedDatabase::query` requires an active scope even for SELECT. `read` joins a scope or uses the pool outside it; read-only SQL remains a caller contract.
- Scope membership resolves at execution. Spawned tasks do not inherit it. Nested scopes reject before acquisition.
- `write_locking` does not invoke the closure when its row is missing, and currently returns `RowNotFound` in that case.
- Caught database errors make a transaction rollback-only. Local buffered conversion errors do not. Stream conversion failure ends the stream and early termination can make a transaction rollback-only.
- Query execution starts on awaiting a buffered terminal method or polling a stream. Cancellation during commit can leave the outcome unknown.

Recommend concise method documentation with links to shared transaction/stream sections, rather than copying the full README into every method. Do not change the accepted SQL or transaction semantics as part of this documentation work.

## 3. Add one fallible mapping adapter

[FromRow](../../src/row.rs) works well for reusable records. A count, existence check, or one-off projection currently needs either a named record with a trait implementation or manual conversion after fetching a `Row`. There is no mapping adapter or scalar query API. Keeping conversion in the query would also give buffered results and streams a consistent usage pattern.

Proposed API sketch, not implemented:

```rust
let count: i64 = db
    .query("SELECT COUNT(*) AS count FROM users")
    .try_map(|row| row.try_get::<i64>("count"))
    .fetch_one()
    .await?;

let names = scoped
    .read("SELECT name FROM users ORDER BY name")
    .try_map(|row| row.try_get::<String>("name"))
    .fetch();
```

Keep `query_as::<Record>` and `FromRow` for reusable mappings. Add `try_map` to raw `Query<Row>` and `ReadQuery<Row>`. Use a mapper taking `&Row` and returning `sqly::Result<T>`; do not require a `'static` closure. Permit repeated calls, including across a stream, and define conversion failures consistently with existing `FromRow` behavior.

Internally, share mapping logic between buffered and streaming execution. Application closures must run outside ambient mutex guards. Derive `Send` from what the adapter actually captures rather than forcing it on every caller. Mapped read queries must still have no `execute` method.

Tradeoff: this adds a mapper type/lifetime to query internals and meaningful stream tests. It is more useful than adding tuple support, derive macros, scalar builders, and mapping closures together. A dedicated scalar method can be added later if usage justifies it. This recommendation is additive and can be deferred without trapping the API.

## 4. Accept an owned URL directly

[ConnectOptions](../../src/options.rs) implements `TryFrom<&str>` and `FromStr`, but not `TryFrom<String>` or `TryFrom<&String>`. Thus callers cannot pass the owned result of reading an environment variable directly to `Database::connect`.

Add both conversions, delegating to the existing parser. Preserve the existing explicit credential policy and redacted parse errors. This is a small ergonomic change with no new configuration policy or database behavior.

## 5. Consider explicit required-lock semantics

[Transaction::lock](../../src/transaction.rs) and `ScopedDatabase::lock` return a boolean for row presence. `write_locking` treats absence as `Error::RowNotFound`, which is also used by unrelated fetches inside the closure. Callers cannot distinguish the two causes from that error alone.

Optional improvement: add `require_lock(lock) -> Result<()>` on explicit transactions and ambient handles, and a distinct `LockNotFound` error. Have `write_locking` use the same required-lock contract. Keep boolean `lock` for applications that handle missing owners as a domain outcome. Do not rename it `try_lock`: acquisition can wait, so that name could suggest nonblocking behavior incorrectly.

Tradeoff: a helper and error variant enlarge the API, and the changed `write_locking` error must be reflected in tests and documentation. Applications can already handle domain-specific absence by calling boolean `lock` inside `write`. This is less urgent than encoding ownership and method documentation.

## Keep and defer

Keep transaction handles out of ambient store signatures. Keep the explicit transaction API for callers that want visible ownership. Do not add a generic executor trait solely to unify these two calling styles.

Keep static SQL and explicit dialect pairs. Do not reopen statement policing, automatic retries, nested scope semantics, or driver exposure. Keep first-row fetch behavior; adding strict cardinality checks would be a separate API decision.

Defer migration reports, extra pool configuration, scalar conveniences, derive macros, and a prelude until real usage shows their value. They can be added later. Focus the pre-adoption pass on contracts that are difficult to change and friction demonstrated by current code.

## Evidence and limits

The original review inspected public declarations, implementations, the warehouse consumer, and behavioral tests. All five recommendations are now implemented. `CARGO_NET_OFFLINE=true mise run check:nightly` passed after the changes: both backends/all features 49 tests, PostgreSQL 27, SQLite 32, neither backend 7, and the independent consumer 2, in both debug and release. Feature builds, Rust 1.94, Clippy, formatting, public documentation examples, rustdoc, workflow audit, and package verification also passed. See [verification evidence](evidence/api-refinement-check.log).

New checks cover moving owned string/byte allocations, borrowed optional inputs, non-Clone domain encoders, owned/borrowed URL parsing and connection, lazy fallible mapping, borrowed closures, buffered and stream error behavior, ambient re-entry during mapping, escaped streams, and required-lock absence without aborting usable transactions. No performance benchmark was run. Ordinary row streams use the default mapper type; closure-mapped streams carry their closure type and inherit its Send requirements.

## Accepted decisions

1. Adopt consuming `Encode` with explicit borrowed implementations.
2. Include `try_map` in the pre-adoption pass.
3. Add required-lock helpers and a distinct `LockNotFound` error.

All review decisions are resolved. Changes were committed separately by recommendation, with an additional test-only correction to keep allocation-check values alive. Conveyor integration remains the next acceptance task.
