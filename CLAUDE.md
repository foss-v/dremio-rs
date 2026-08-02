# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`dremio-rs` is a published crate (crates.io: `dremio-rs`) — a thin async client over Dremio's Flight SQL endpoint. The entire public API lives in `src/lib.rs`: one `Client` struct wrapping `arrow_flight::sql::client::FlightSqlServiceClient<Channel>`, plus a `DremioClientError` enum that is mostly `#[from]` passthrough over the underlying tonic/arrow/flight/parquet/io errors, with two variants of its own for malformed responses.

Edition 2024, MSRV 1.88 (set by `tonic`, which is the highest of the dependency tree).

## Commands

```bash
cargo build
cargo test                           # everything, including the Docker-backed test
cargo test --lib --test flight_sql   # fast path: no Docker needed (--doc cannot be mixed in)
cargo test --doc
cargo test --test flight_sql         # mock Flight SQL server tests
cargo test --test lib                # the Docker-backed integration test
cargo fmt --all
cargo clippy --all-targets --features tls -- -D warnings
```

CI (`.github/workflows/ci.yml`) runs on PRs to `main` and on push to `main`, in two jobs: `check` gates fmt, clippy (`-D warnings`), build and the no-Docker tests; `integration` runs the Dremio container test separately so a formatting slip fails in seconds instead of behind a container boot.

## Tests

`tests/flight_sql.rs` stands up an in-process Flight SQL server (`arrow_flight::sql::server::FlightSqlService`) on an ephemeral port. It covers the result shapes a real Dremio will not produce on demand — an endpoint with no ticket, a result split across several endpoints, an empty result set — which are exactly the shapes that used to panic. Fast, no daemon required; prefer adding here.

`tests/lib.rs` is the Docker-backed one. It uses `testcontainers` to boot `dremio/dremio-oss:latest`, waits on the stdout line `com.dremio.dac.server.DremioServer - Started on http://localhost:9047`, then bootstraps the first user over the REST API (`PUT /apiv2/bootstrap/firstuser` with the magic `Authorization: _dremionull` header) before Flight SQL auth will succeed. It writes `test.parquet` into the repo root as a side effect; that path is gitignored.

Two things commonly bite:

- **`SocketNotFoundError("/var/run/docker.sock")`** — testcontainers speaks to the daemon directly and does not read the Docker CLI context, so any setup that puts the socket elsewhere (colima, Rancher Desktop, Podman, rootless Docker) has to hand it over explicitly. `docker context inspect --format '{{.Endpoints.docker.Host}}'` prints the value to export as `DOCKER_HOST`.
- **The container log stops mid-boot with no exception** — Dremio asks for a 4 GB heap and is being OOM-killed. Where the daemon runs inside a VM (macOS, Windows, colima) that VM needs 8 GB or more; against a native Linux dockerd it does not arise.

## Version lockstep

`arrow`, `arrow-flight`, and `parquet` must be bumped together to the same version — they share types across the API boundary and a mismatch is a compile error, not a runtime surprise. `.github/dependabot.yml` puts them in one `arrow` group for exactly this reason; opened as separate PRs they are individually unmergeable and just accumulate, which is how three of them sat open for months.

Because `arrow` types appear in the public API (`RecordBatch`), an `arrow` major bump is a breaking change for downstream users even when this crate's own code does not change.

## TLS

`arrow-flight` ships no TLS by default, so a plain build cannot connect to `grpc+tls://` or `https://` at all. The `tls*` features in `Cargo.toml` pass straight through to `arrow-flight`; `tls` pairs a provider (`tls-ring`) with a root store (`tls-webpki-roots`). No code in this crate is feature-gated — tonic's `Endpoint` picks TLS up from the URL scheme once a provider is compiled in.

## Releases

Releases are automated by release-plz on push to `main`: it opens a version-bump PR, and merging that PR publishes to crates.io and updates `CHANGELOG.md`. Do not hand-edit `version` in `Cargo.toml` or `CHANGELOG.md`. Commit messages feed the changelog, so Conventional Commits matter here.

The install snippets in `README.md` and the crate docs in `src/lib.rs` are hand-maintained and pin the current minor (`"0.3"`). Under Cargo's 0.x rules `0.2` and `0.3` are incompatible ranges, so a bare major will not track releases and these need updating on every minor bump until 1.0 — grep for the old minor when releasing one.

## Conventions in this code

- Every public item carries rustdoc with an `# Arguments` / `# Returns` / `# Example` structure, and examples are `no_run` doctests. Match that shape when adding public API.
- `Client::inner_mut()` is the escape hatch for Flight SQL operations the wrapper doesn't expose (prepared statements, catalog metadata, `execute_update`) — nearly all of them need `&mut self`. `inner()` exists for the rare shared-reference case. Prefer pointing users at these over widening `Client` for one-off needs.
- Library code does not panic on server responses. A surprising response shape becomes a `DremioClientError` variant; `expect`/indexing on anything Dremio sends is a bug.
- `write_parquet` streams batches to disk as they arrive rather than collecting first, so it stays flat in memory on large exports. Keep it that way.
