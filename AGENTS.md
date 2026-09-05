# Repository Instructions

## Project purpose

sqly is an async Rust library for applications using SQLite and PostgreSQL. Connections, buffered and streaming queries, codecs, explicit transactions, ambient scopes, row locks, migrations, and legacy adoption are implemented. `consumers/warehouse` independently tests the public API. See README.md for implementation status and DEVELOPING.md for the development workflow. Applications own the Tokio runtime, schema, and transaction policy.

## Rust style

Before changing Rust code, configuration, project structure, or tests:

1. Run `bin/style-guides prepare`.
2. Read `.ai/style-guides/rust-style-guide/SKILL.md` completely.
3. Read each workflow and policy page that the skill routes for the task.

Project requirements and accepted architecture decisions override general style-guide defaults.

## Repository tasks

- Use `mise run dev` for the normal development path.
- Use `mise run test` for the routine test suite; it provisions disposable PostgreSQL through Docker.
- Use only disposable databases with `SQLY_TEST_POSTGRES_URL`.
- Use `mise run check` for the complete routine verification gate.
- Use `mise run check:nightly` for the extended verification gate.
- Use `mise run fmt` to format Rust with the pinned nightly formatter.
- Use `mise run test:doc` for public documentation examples.
- Use `mise run check:features` for supported feature builds.
- Use `mise run check:package` to verify the unpublished library package.

## Safety

- Never install packages less than 24 hours old.
- Never force push, including with `--force-with-lease`.
- Never amend commits. Create a new commit instead.

## Working documents

Save plans under `.ai/plans/` and reviews under `.ai/reviews/`.
