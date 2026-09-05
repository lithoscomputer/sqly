# Developing

sqly is a single reusable Rust library crate. Its async APIs use Tokio; the caller owns the runtime. SQLx remains an internal implementation detail. Connections, buffered and streaming queries, codecs, explicit transactions, ambient scopes, row locks, migrations, and legacy adoption have real database tests.

## Setup

Install Mise, then install the locked tools and prepare the pinned Rust guide:

```sh
mise trust
mise install --locked --jobs=1
mise run setup
```

The project uses Rust 2024 and Rust 1.94 as its minimum supported version. Mise pins the development compiler and nightly formatter. Dependency versions start from the previously validated SQLx 0.9 set. Before adding or updating a package, verify it was published at least 24 hours ago.

## Common tasks

| Command | Purpose |
| --- | --- |
| `mise run dev` | Check all library targets with all features |
| `mise run fmt` | Format Rust with the pinned nightly |
| `mise run fmt:check` | Check formatting |
| `mise run lint` | Run Clippy with warnings denied |
| `mise run test` | Run tests with Nextest |
| `mise run test:doc` | Run maintained rustdoc examples |
| `mise run check:docs` | Build rustdoc with warnings denied |
| `mise run check:features` | Check supported feature combinations |
| `mise run check:msrv` | Check the same feature builds with Rust 1.94 |
| `mise run check:package` | Package and build the unpublished library |
| `mise run check` | Run the routine gate |
| `mise run check:nightly` | Add MSRV, release-mode tests, and package checks |
| `mise run release` | Verify a release candidate without publishing |

The separate `consumers/warehouse` crate exercises a different schema through sqly’s public API, including custom values, cross-store ambient atomicity, explicit transactions, streaming, and a migration namespace. Its runtime tests run with the normal test tasks, and feature and Rust 1.94 checks compile it independently.

The compile matrix covers defaults, no features, each backend alone, both backends, integrations without backends, each backend with integrations, and all features. The runtime matrix covers both drivers, each alone with value integrations, and neither backend. Disabled-backend tests run in the relevant builds. Nextest does not run doctests; the separate task includes public examples and compile-fail checks for unsupported representations, transaction borrowing, and read-only builder methods.

`mise run test` and `mise run test:release` create disposable PostgreSQL 18.4 containers using a fixed image digest. Docker must be available. The helper removes its container on exit and stores database data in temporary memory. Required PostgreSQL tests fail if their service is unavailable.

For a pre-provisioned disposable PostgreSQL server, set `SQLY_TEST_POSTGRES_URL` with explicit `sslmode=disable`. The test fixture must have TLS disabled and allow ordinary table creation and observation of its own sessions. Ambient and streaming tests cover retained handles and futures, scope confinement, early drop, mid-result failures, and competing operations. Migration tests cover concurrent first startup, namespace separation, failed batches, compatibility checks, exact checksums, and validated legacy adoption. The commit-uncertainty test uses a local TCP proxy to discard a PostgreSQL COMMIT acknowledgement after the server commits. Transaction tests also cover cancellation, deferred constraint failures, competing writers, post-lock reads, and in-memory keeper lifetime. Test runs create and modify tables; never point this variable at an application database. Avoid running two suites against the same fixture at once.

Use `SQLY_TEST_BACKEND=sqlite mise run test` when Docker is unavailable. This runs the SQLite and no-backend suites; it is not PostgreSQL correctness evidence. Hosted macOS CI uses this mode. Linux x86\_64 and arm64 CI provision PostgreSQL and run the full runtime matrix. All platforms compile both drivers.

To run the runtime matrix on the MSRV as a focused check:

```sh
mise exec -- bin/test +1.94.0
```

Run `mise run check` before opening a pull request. Run the extended gate for MSRV, dependency, and packaging changes. `check:package` uses `--allow-dirty` so it can verify local work before a commit; it does not publish anything. The manifest limits the package to library sources, integration tests, Cargo metadata, README, and development instructions.

## Rust policy

Run `mise run setup`, then read `.ai/style-guides/rust-style-guide/SKILL.md` and its required pages before changing Rust, configuration, structure, or tests. Maintain public rustdoc examples as the API is implemented.

## Continuous integration and releases

Routine checks run for pull requests and pushes to `main`. Extended checks run nightly. Both use macOS arm64, Linux x86\_64, and Linux arm64 runners. Pushed `v*` tags and manual release-workflow runs verify the same library release candidate on all three platforms.

Publication stays disabled. Before the first release, complete API behavior checks, Conveyor replacement, independent consumer verification, and package metadata, including a project license and crate-name availability. No license has been selected by the repository yet.
