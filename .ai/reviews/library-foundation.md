# Library foundation verification

Date: 2026-09-05.

The binary template is now a library foundation. This completes the setup work
in milestone 1 of the [implementation plan](../plans/implementation.md).
Database behavior is not implemented. The disabled-backend error check and
PostgreSQL service-backed tests remain dependent on milestone 2; milestone 1's
full behavior acceptance is therefore not yet satisfied.

## Changes

- Replaced the binary entry point with a documented library entry point.
- Set Rust 1.94 as the MSRV and updated the mise tool lock.
- Added independent backend features and the accepted optional integration
  features. All Cargo registry versions and checksums match the validated
  probe lockfile; dependency resolution ran offline.
- Added feature-matrix, doctest, rustdoc, MSRV, and package tasks. Retained
  Nextest, the development compiler, and the pinned nightly formatter.
- Replaced binary archive and draft-release automation with read-only library
  release verification. Publication remains disabled.
- Updated README, development instructions, and repository purpose through
  Quarry and synced them to disk. Their review queues are empty.

## Verification

`CARGO_NET_OFFLINE=true mise run check:nightly` passed on macOS arm64 with
exit status 0. Its [full log](evidence/library-foundation-check.log) records:

- pinned formatting and Clippy;
- the feature matrix on the development compiler and Rust 1.94;
- debug and release Nextest runs;
- doctests and rustdoc with warnings denied;
- workflow audit with no findings;
- package creation and verification from the extracted crate.

Nextest and doctest suites contain zero tests at this foundation stage. Passing
those commands is setup evidence, not database correctness evidence. The
three-platform CI configuration has been updated; hosted Linux and macOS CI
runs have not been performed in this session.

Also verified shell syntax, removal of stale binary task references, package
contents, and registry-version/checksum equality with the validated lockfile.
The package contains Cargo metadata, README, and `src/lib.rs`; it excludes
working plans, probe fixtures, and binary archives.

The first verification attempt exposed an empty-array error in macOS Bash.
The next attempt exposed release-workflow cache audit findings. Both were
fixed before the final passing gate.

## Remaining work

Implement connection options, pools, typed errors, values, rows, and buffered
queries next. Add disabled-backend behavior tests and disposable PostgreSQL
service provisioning with the first database tests. Continue with the remaining
milestones before calling this a usable library or a release candidate.

Cargo package verification warned that the inherited `chacha20 0.10.1`
transitive dependency is yanked. The locked version remains reproducible;
review an appropriate replacement before release. Cargo also reported missing
release metadata. Select the project license and complete that metadata before
publication. This task did not select a license or publish a package.
