# sqly implementation plan

Status: Updated on 2026-09-05. Milestones 1–5 and the independent consumer checks are complete. The library is pushed to private `lithoscomputer/sqly` at `3f4e52d`; the dependency fix passed the full local verification gate. CI passed on all three runners after the toolchain setup fix. Full Conveyor replacement and first-release acceptance remain open.

## Outcome and authority

Deliver a reusable Rust library for SQLite and PostgreSQL, including optional ambient transactions, streaming, custom value conversions, and migrations. The first release must fully replace Conveyor's persistence layer and work with a second application schema.

The [finalized public API](public-api.md) owns the interface and behavior contract. This plan orders implementation and defines milestone completion. The [technical validation](../reviews/technical-validation.md) and [probe harness](../validation/) provide starting evidence. The harness contains signature stubs and partial ownership implementations; it is not production code to copy wholesale.

The repository is now a library. Connections, buffered queries, explicit transactions, ambient scopes, streaming, and migrations are implemented. Remaining work is integration and release verification. This plan covers sqly work and Conveyor integration acceptance; it is not a Conveyor Work Order Implementation Plan. Conveyor work must enter its own required Work Order and Fabro workflow.

## Delivery order

| Milestone | Deliverable | Depends on | Status |
| --- | --- | --- | --- |
| 1 | Library structure and verification tasks | Finalized API | Complete; CI passes on all three runners |
| 2 | Connections, values, and buffered queries | 1 | Complete |
| 3 | Explicit transactions and row locks | 2 | Complete |
| 4 | Ambient scopes and streaming | 3 | Complete |
| 5 | Migrations and legacy-ledger adoption | 3 | Complete |
| 6 | Complete Conveyor replacement | 4 and 5 | Not started |
| 7 | Independent consumer and release preparation | 6 | Consumer and local package checks complete; release acceptance open |

The sections below retain the agreed delivery requirements and completion checks. The independent consumer was completed before Conveyor integration because it could validate the public API separately. An intermediate store adapter does not satisfy milestone 6.

## 1. Establish the library and verification tasks

Replace `src/main.rs` with the library entry point. Update `Cargo.toml`, the lockfile, README, DEVELOPING, repository purpose instructions, mise tasks, GitHub workflows, and binary release tooling to describe and check a library. Remove the binary-name setting and archive packaging path. Keep publication disabled while implementation and consumer verification are incomplete.

Use Rust 2024 and Rust 1.94 as the MSRV. Retain the development compiler and nightly formatter pins. Start from the dependency versions verified by the probe harness. Check release age before installing any additional package; never install a package published less than 24 hours ago.

Use empty default features. Make `sqlite` and `postgres` independent backend features, with additive `uuid`, `time`, `json`, `migrate`, and `ambient` features. Keep SQLx driver types private. Introduce modules around connections, queries, values/rows, transactions/locks, scopes, and migrations as each implementation lands; avoid empty abstractions for future backends.

Before Rust, configuration, or project-structure changes, prepare and read the repository's pinned Rust style guide and the pages it requires.

Completion checks:

- The library builds with neither backend, each backend alone, and both.
- A disabled backend produces the documented typed configuration error.
- `mise run dev` provides a useful library development task. Test, lint, MSRV, documentation-example, and packaging checks have explicit tasks.
- Routine and extended checks use the library targets. PostgreSQL tests use a disposable service and fail clearly when a required service is unavailable.

## 2. Implement connections, values, and buffered queries

Implement connection parsing, backend options, shared pool ownership, close, and typed errors. Test defaults and option validation, explicit SQLite file creation/WAL choices, PostgreSQL TLS behavior, and credential redaction. Keep test credentials and databases isolated from application data.

Implement the in-memory SQLite database with one query connection and a private keeper using the same unique named `file:` URI. Retain the keeper through clones, outstanding leases, connection replacement, and cancelled shutdown. Unexpected keeper loss makes the handle terminally unusable.

Implement static `Sql`, dialect pairs, owned bindings, sealed `SqlValue`, open `Encode`/`Decode`, `Row`, `FromRow`, and the buffered query methods. Use shared `$1` through `$N` placeholders with the documented consecutive-number caller contract. Prepare with encoded argument types on the execution connection, then compare driver parameter count before execution. Do not add a tokenizer, statement classification, or transaction-control policing.

Completion checks:

- One behavior suite passes against both real backends for CRUD, fetch result semantics, repeated/reordered parameters, typed NULLs, missing/surplus bindings, and static dialect pairs.
- Built-in and downstream custom types round trip. Checked integer decoding, bool validation, non-finite float rejection, UUID/JSON/time conversions, nullable values, duplicate columns, and typed error causes are covered.
- Encoding fails before I/O. Preparation and execution errors remain distinct from local validation failures where transaction behavior requires it.
- Schema and committed data survive query-connection replacement. Independent in-memory databases stay isolated; close and keeper-loss behavior match the API contract.
- External consumer compilation verifies borrowed bindings, custom optional values, and rejection of unsupported representations.

## 3. Implement explicit transactions and locks

Implement owned transactions with mutable query borrowing, explicit commit and rollback, and cleanup on drop. SQLite uses `BEGIN IMMEDIATE`; PostgreSQL uses READ COMMITTED. Keep connection reuse dependent on completed cleanup. Preserve the distinction between a known abort and an unknown commit outcome. Do not retry application work automatically.

Implement validated row-lock selectors, including composite keys. Use a separate PostgreSQL `SELECT ... FOR UPDATE`, and an existence check within SQLite's write transaction. Keep dependent-row reads after lock acquisition.

Completion checks:

- Commit, rollback, dropped transactions, cancelled acquisition/query work, connection cleanup, and concurrent writers pass on both backends.
- Invalid selectors fail before I/O. Missing and duplicate matches have the specified outcomes. A dependent-row revision test catches stale reads made before PostgreSQL lock acquisition.
- Compile-time tests reject overlapping operations through one explicit transaction. Futures are `Send` where the API permits them.
- Deterministic failure injection covers uncertain commit and cleanup paths that ordinary successful driver tests cannot establish.

## 4. Implement ambient scopes and streaming

First implement buffered ambient operations. Stores hold cloned `ScopedDatabase` values; application arguments and results remain free of transaction handles. Resolve scope membership at execution. Add `write`, `write_locking`, and additional `lock` acquisition within a scope.

Then add pooled and explicitly borrowed streams, followed by ambient streams. Use scope-owned driver cursors with identity-checked public handles. Scope teardown must be able to invalidate and drop a cursor even when its public handle escapes. Never hold an internal guard while invoking application code.

Completion checks:

- Cross-store operations commit or roll back together. Reads join the active transaction and otherwise use the pool. Every unscoped `query` execution method rejects writes and SELECT alike with `NoActiveWriteScope`.
- Nested scopes reject before acquisition. Mismatched database identities and spawned-task use follow the contract. Buffered futures in one task serialize without deadlock.
- Driver preparation/execution errors make the scope rollback-only. Local validation and bind-count rejection do not. Closure errors retain their original cause; panic and cancellation trigger cleanup.
- Streams execute on first poll, decode incrementally, report at most one error, and release or discard connections after exhaustion or early drop.
- A competing ambient query, lock, or stream fails immediately with `ActiveStream` before I/O. That rejection alone does not abort the scope.
- Scope escape, changed poll context, unfinished streams at successful scope exit, and cancellation with a live cursor cannot resume database work or block teardown. Uncertain early-drop cleanup aborts the transaction.
- Explicit streams preserve mutable borrowing. The public stream and async signatures compile from a downstream crate on Rust 1.94.

## 5. Implement migrations and legacy-ledger adoption

Implement migration definitions, namespaces, checksums, compatibility policy, and typed migration errors. Serialize ledger initialization and namespace runs before reading their state. Apply each pending batch and its ledger rows in one transaction. Trust the migration-script contract without SQL policing.

Add explicit generic adoption with application-supplied legacy table identity and compatibility policy. Keep Conveyor table names and schema definitions in Conveyor. Validate the complete legacy prefix before copying metadata. Keep Conveyor's exact checksum byte format, descriptions, versions, and applied timestamps. Reject conflicting canonical rows and preserve target and minimum-upgradeable checks. Adoption and its ledger changes must be atomic; never replay historical DDL.

Completion checks:

- Both backends pass concurrent first-start, namespace separation, failed batch rollback, checksum mismatch, and unsupported-newer-schema tests.
- Valid, corrupt, incomplete, repeated, and conflicting ledger adoption have explicit tested outcomes. An incomplete adoption cannot silently authorize unverified history. Matching repeated adoption is idempotent.
- Concurrent adoption and injected failure leave a consistent ledger.
- Fixtures retain historical SQL bytes and independently pinned checksum evidence. The completed validator goes beyond the earlier transfer probe.

## 6. Fully replace Conveyor's persistence layer

Use the applicable Conveyor process to deliver its repository changes. Read its current instructions and journal, identify the required Work Order scope, and follow its readiness and Fabro launch rules. Do not create a Conveyor Work Order Implementation Plan in this sqly plan. This milestone supplies the integration requirements for that workflow.

Confirm the consumer inventory against current Conveyor source. Replace all store and persistence consumers, runtime connection setup, health checks, transaction paths, migration execution, and affected test infrastructure. Adopt ambient scopes according to Conveyor's operation policies. Replace the card-list SQL assembly with its four static variants. Exercise additional source/feed locks and recheck related-row conditions after acquisition.

Add a new Conveyor-owned migration to normalize historical SQLite timestamp columns before new queries mix encodings. Audit temporal columns and reject unrepresentable values with useful diagnostics. Preserve API serialization, digests, and all historical migration SQL bytes.

Completion checks:

- Fresh databases and upgrades from supported existing database fixtures pass on both backends, without data loss, historical DDL replay, or changed published representations and digests.
- A cutover rehearsal quiesces old binaries before adopting the legacy ledger. Document failure recovery and restart behavior; do not assume an old binary can safely resume after the new ledger advances.
- Conveyor's full applicable database and application behavior suites pass.
- A final source/dependency audit confirms the replaced pool dispatch, execution, codec, and migration-runner implementations are removed. Application schema, domain conversions, and business policies remain local.

## 7. Verify reuse and prepare the first release

Build a small separate consumer crate with a different schema and its own custom value type. Exercise both backends, explicit transactions, optional ambient scopes, streaming, and a distinct migration namespace. Consume the public crate interface only. Use this to detect Conveyor-specific assumptions before calling the library reusable.

Compile documentation examples against the implemented crate. Run the full feature matrix, MSRV checks, routine and extended verification gates, and library packaging checks. Verify the packaged archive contains its required sources and documentation and can build without workspace-only files. Check crate-name availability and complete package metadata before publication.

Completion means a tested, packageable release candidate with complete Conveyor adoption and independent consumer evidence. Publishing the crate is a separate release action; keep automatic publication disabled in this plan.

## Verification and evidence

Use small targeted checks while implementing each behavior. Run the shared backend suite when a milestone changes database behavior, then run the repository gates for the completed change. Do not repeat passing broad checks without a new change or unresolved failure. Nextest runs must be supplemented by explicit documentation and compile-failure checks where required.

Record commands, results, dependency versions, and material limitations under `.ai/reviews/`. Keep a trace from each milestone's completion checks to tests or recorded consumer evidence. Feasibility probe counts are historical evidence, not a substitute for production tests.

The highest-risk work is cursor teardown, keeper lifetime, uncertain commit outcomes, ledger adoption, and timestamp conversion. Verify these before Conveyor cutover. Discovering a new requirement should produce a concrete API change for review rather than silently expanding the finalized contract.

## Remaining work

[CI for `3f4e52d`](https://github.com/lithoscomputer/sqly/actions/runs/33992978994) passed on macOS, Linux x64, and Linux ARM64. Commit `3f4e52d` resolves the missing-Clippy issue by explicitly ensuring the pinned Rust toolchains and components after the Mise cache restore in all three workflows.

Claim the accepted Conveyor WRK-068 through its required workflow and launch Fabro for full replacement. Pin the sqly Git revision and update Conveyor’s own lockfile to select `chacha20 0.10.2` or a later verified fixed version; a Git dependency does not import sqly’s lockfile. Complete timestamp normalization, upgrade/cutover tests, application acceptance, and the removal audit in Conveyor.

The inherited yanked `chacha20 0.10.1` is resolved in both sqly lockfiles by commit `66da8ee`. Local `mise run check:nightly` passed after the update. See the [delivery review](../reviews/remaining-phases.md) for evidence.

## Unresolved questions

No library API or implementation scope questions remain. Before registry publication, select a license and an available crate name (`sqly` is occupied). Private Git consumption can retain the current name. Publication remains a separate action; automatic publication is disabled.
