# Remaining implementation phases

Status: Updated on 2026-09-05. Library implementation and independent-consumer verification are complete. Code is pushed to private `lithoscomputer/sqly` at `66da8ee`, including the fixed chacha20 dependency. Local extended verification passed. Current CI has a toolchain setup failure. Full Conveyor replacement has not started its required Fabro run.

## Delivered

`ScopedDatabase` resolves task-local transactions at execution. Stores take application inputs without transaction handles. Reads join the active scope or use the pool. Writes require a scope. Nested scopes fail before acquisition. Database identities remain distinct, spawned tasks inherit no scope, and buffered operations serialize. Closure errors retain their original value even if rollback fails.

Scopes own both driver cursors and buffered-operation futures. Public handles retain identity rather than sole ownership of database work. Teardown cancels retained work and releases its transaction even when a caller keeps a partially polled future or stream. Application row conversion runs outside internal guards.

`Query::fetch` and `ReadQuery::fetch` return incremental `RowStream` results. Errors end the stream. Explicit streams retain a mutable transaction borrow. Ambient competitors fail with `ActiveStream` before I/O. Exhaustion restores transaction use; early drop conservatively makes it rollback-only. Successful scope exit with a live stream invalidates the cursor, rolls back, and returns `ActiveStream`.

The optional migration module implements namespaced ledgers, target and minimum-upgradeable checks, exact checksums, atomic batches, and validated legacy adoption. Both dialects participate in checksums in single-backend builds. Matching partial copies complete only after validating the full source prefix. Conflicting metadata, missing history, changed checksums, and unsupported versions fail. Adoption preserves applied timestamp bytes and never replays historical SQL.

The separate `consumers/warehouse` crate uses only sqly's public API. It covers a custom SKU type, its own schema and migration namespace, explicit seeding, cross-store ambient reservations, rollback after the second store fails, and a streamed stock report. Routine tests and feature/MSRV checks include this consumer.

The Conveyor audit also found supported PostgreSQL Unix-socket connections and six explicit TLS modes. Sqly now preserves these configuration choices, including percent-decoded socket hosts. The default still verifies server identity. Sqly-owned option inspection and SQLite filename access let the application keep its own directory policy.

## Verification

`CARGO_NET_OFFLINE=true mise run check:nightly` passed:

| Runtime suite | Debug | Release |
| --- | --- | --- |
| Both backends and all features | 43 passed | 43 passed |
| PostgreSQL with integrations | 23 passed | 23 passed |
| SQLite with integrations | 27 passed | 27 passed |
| Neither backend | 4 passed | 4 passed |
| Separate warehouse consumer | 2 passed | 2 passed |

The gate also passed formatting, Clippy, documentation, feature builds, Rust 1.94 builds, GitHub workflow checks, and package verification. Public examples and compile-failure checks cover query representations, transaction/stream borrowing, and the read builder's missing execute method. Documentation-only additions received focused documentation and package checks afterward.

Evidence:

- [Extended gate](evidence/remaining-phases-check.log).
- [Public examples](evidence/remaining-phases-test-doc.log).
- [Rustdoc](evidence/remaining-phases-check-docs.log).
- [Package verification](evidence/remaining-phases-check-package.log).
- [Existing Conveyor database rehearsal](evidence/conveyor-adoption-check.log).

The rehearsal uses Conveyor's actual persistence and schema crates at `5134b2c0`. Its old migration runner creates SQLite and PostgreSQL databases with all seven historical migrations. After closing the old connection, sqly adopts `_conveyor_migrations` into namespace `conveyor`, applies a new probe migration, and runs again. All seven versions, descriptions, checksums, and applied timestamp strings match exactly. A stored feed survives. The repeated run does not replay DDL. This verifies the library compatibility mechanism; it is not evidence that Conveyor's application stores have been replaced.

The original implementation used dependency versions from the validated sqly lock or Conveyor’s `bb8e5aef` lock dated 2026-08-25. The subsequent dependency fix updates only `chacha20` from 0.10.1 to 0.10.2 in the root and warehouse-consumer lockfiles. The crates.io index records publication on 2026-08-27 at 17:51:13 UTC, meeting the 24-hour minimum age. Upstream [fixed an SSE4.1 instruction used in the SSE2 backend](https://github.com/RustCrypto/stream-ciphers/pull/580), which was the reason for yanking 0.10.1. The change preserves the project’s Rust 1.94 minimum.

## Git publication and current CI

Initial implementation commit `628bee8` and dependency fix `66da8ee` are pushed to private [lithoscomputer/sqly](https://github.com/lithoscomputer/sqly). The dependency fix passed `CARGO_NET_OFFLINE=true mise run check:nightly`, including debug/release database tests, the independent consumer, feature/MSRV checks, documentation, lint, and package verification. The yanked-package warning is gone. See [dependency-fix verification](evidence/chacha20-update-check.log).

[CI for `66da8ee`](https://github.com/lithoscomputer/sqly/actions/runs/33992337297) failed on all three runners because `cargo-clippy` was missing from the installed Rust 1.97.1 toolchain. Fix toolchain component setup and obtain a passing CI run before the Conveyor handoff. The earlier initial-commit CI passed on all three runners. See [failed CI evidence](evidence/chacha20-update-ci-failure.log). This CI failure is not a passing verification result for the pushed revision.

## Conveyor workflow and remaining acceptance

The accepted WRK-068 ambient-write-scope order passes the readiness check. Its dependency WRK-067 is complete. The launch preview has no product criterion expansion; the claim changes only status. A separate worktree at `/tmp/sqly-conveyor-integration`, branch `sqly-integration`, preserves the unrelated journal edit in the original checkout. No claim or commit has been made.

Conveyor’s instructions require the status-only WRK-068 claim committed on `origin/main` before Fabro starts. Git publication of sqly is complete; the Conveyor claim and Fabro launch remain pending. The workflow should pin the verified sqly revision and update Conveyor’s own lockfile to select the fixed chacha20 version. Sqly’s lockfile does not control a consuming application’s dependency resolution.

The workflow must replace every application store and persistence consumer, connection setup, health path, migration runner, and related tests. It must remove the old driver dispatch, query execution, codecs, and migration implementation. Domain conversions, schema, serialization, digests, and business policy stay in Conveyor. The card-list query uses static variants. Timestamp normalization is a new Conveyor-owned migration; historical SQL remains unchanged. The [timestamp-column inventory](evidence/conveyor-timestamp-columns.json) identifies 18 schema columns. Reject unrepresentable values with diagnostics; do not silently truncate historical precision or use SQLite's millisecond formatting to normalize microsecond values.

Full Conveyor behavior tests, the timestamp conversion migration, application cutover/restart evidence, and the final source/dependency audit remain outstanding. The rehearsal and warehouse consumer do not replace those checks.

## Release metadata

The registry name `sqly` is already occupied by an unrelated crate. This was verified from the [crates.io index](https://index.crates.io/sq/ly/sqly) and its [published documentation](https://docs.rs/sqly). A pinned Git dependency can retain the local package name. Registry publication would require a separate naming decision.

No license has been selected. Complete package metadata and choose an available name before registry publication. The inherited yanked chacha20 dependency is resolved in both maintained lockfiles. Automatic publication remains disabled. Conveyor integration and release acceptance remain outstanding.
