# Contributing to notify-rs

Thanks for helping! This guide covers:

- setting up the project
- the checks every change must pass
- the design rules that keep delivery safe
- how to send a pull request

New here? Good first areas are a **delivery provider** (an email or SMS gateway; see
[docs/PROVIDERS.md](docs/PROVIDERS.md)) or an item from the "Not built yet" lists in
[docs/PROVIDERS.md](docs/PROVIDERS.md#8-not-built-yet-good-contributions) and
[ARCHITECTURE.md](ARCHITECTURE.md#6-production-hardening-next-steps).

## Project layout

| Path | What it is |
|---|---|
| `crates/api` | Ingest API: validates, deduplicates and writes notifications to Kafka |
| `crates/worker` | Delivery workers: retries, rate limits, opt-outs, providers (`fcm.rs`, `ses.rs`, `sslwireless.rs`, `inbox.rs`) |
| `crates/gateway` | In-app gateway: WebSocket live feed and inbox REST for apps and browsers |
| `crates/campaign` | Campaign service: contacts, cohorts, campaigns, templates, admin UI (`ui/index.html`), sign-in |
| `crates/common` | Shared message model, Kafka setup, templates, metrics |
| `crates/loadgen` | Synthetic load generator |
| `deploy/` | Prometheus alerts and the Grafana dashboard |
| `tests/` | End-to-end shell suites that run against the Docker stack |
| `docs/` | Guides, such as adding providers |

[ARCHITECTURE.md](ARCHITECTURE.md) explains the design and why it's built this way. Read
sections 3 and 4 before changing the API or worker.

## Setup

You need:

- **Rust** (stable) with `rustfmt` and `clippy`: `rustup component add rustfmt clippy`
- **Docker** with Compose v2
- **Python 3** for the test helpers (standard library only)
- For the browser and in-app tests only: **Node.js** 22+ and (browser test) **Google Chrome**

Start everything:

```bash
docker compose up -d --build
docker compose exec campaign campaign create-admin you@example.com   # prints a temporary password
```

The admin UI is at http://localhost:8090/admin. The README lists every other URL.

To work on one service, run it natively against the Docker infrastructure (see "Run binaries
natively" in the README). It's much faster than rebuilding the image.

## Checks every change must pass

CI runs these on every pull request. Run them before you push:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

If you touch the API, worker or campaign service, also run the end-to-end suites that cover your
change against the running stack:

| Suite | Covers |
|---|---|
| `tests/e2e.sh` | Sync, import, cohorts, campaigns, the bulk lane |
| `tests/history.sh` | People search and delivery history (run after `e2e.sh`) |
| `tests/system.sh` | System tab: Kafka topics and groups, metrics, dead-letter queue |
| `tests/auth.sh` | Sign-in security: hashing, cookies, CSRF, roles, lockout |
| `tests/fcm.sh` | Firebase push against a fake Google server |
| `tests/inapp.sh` | In-app inbox and WebSocket gateway (needs Node.js 22+) |
| `tests/run_ui.sh` | Admin UI in headless Chrome |

The README explains how to start from a clean database when you need exact counts. These suites
need Docker, so CI doesn't run them; say in your pull request which ones you ran.

**Rebuilding after a change:** the `api` service builds the image that every service uses. Run
`docker compose build api && docker compose up -d`, not `up --build <service>`.

## Design rules

These keep messages from being lost, duplicated or sent to people who opted out. A change that
breaks one needs a very good reason, explained in the pull request.

1. **A 202 means durable.** The API answers only after Kafka has the message.
2. **At-least-once, made effectively-once.** Workers store Kafka offsets only after a batch is
   fully handled. The Redis status key is the dedup marker. Don't store offsets early, and don't
   skip the status write.
3. **Fail closed on opt-outs.** If opt-out state can't be read, wait. Never send.
4. **Infrastructure errors block; provider errors route.** Redis or Kafka down: retry in place.
   Provider failures go to the retry tiers or the dead-letter queue and never block a partition.
5. **Batch the round trips.** Use one pipelined Redis call per batch, not one per message. Load
   tests showed per-message calls saturating Redis at about 12k/s.
6. **No SQL built from user input.** Use bound parameters (`push_bind`, `$1`). Field names come
   from allow-lists; see `crates/campaign/src/rules.rs`.
7. **Secrets never in code, git, the database or the admin UI.** Use environment variables or
   `secrets/`; see [docs/PROVIDERS.md](docs/PROVIDERS.md#2-where-configuration-goes).
8. **Escape everything in the admin UI.** Anything inserted into the page goes through `esc()`.
   The page has a strict Content-Security-Policy and loads no external scripts.
9. **New behavior gets metrics.** Counters and histograms follow the names in ARCHITECTURE.md §7.
   Add an alert in `deploy/prometheus/alerts.yml` if a failure would otherwise go unnoticed.

## Code style

- `cargo fmt` decides formatting. `rustfmt.toml` allows 140-character lines.
- Error messages that reach the admin UI (Deliveries, campaign errors) are written for operators:
  say what happened and what to do.
- Comments explain *why*, not *what*.
- Database changes go in a new numbered file in `crates/campaign/migrations/`. Never edit a
  migration that has been released.
- Unit tests go next to the code (`#[cfg(test)] mod tests`). Test the logic that's easy to get
  wrong, such as error classification, parsing and rule compilation.

## Pull requests

1. **Discuss larger changes first.** Open an issue for anything bigger than a bug fix or a new
   provider, so we can agree on the approach before you write it.
2. **One topic per pull request.** Keep formatting-only changes separate from logic changes.
3. **Commit messages:** a short summary line, a blank line, then what changed and why.
4. **Describe in the pull request:** what changed, why, how you tested it (which suites), and any
   new configuration variables.
5. **Update the docs** when behavior or configuration changes: README, ARCHITECTURE.md or `docs/`.

The checklist in the pull-request template covers these.

## Reporting security problems

**Please don't open a public issue for a security problem.** Use GitHub's private reporting
(**Security → Report a vulnerability** on the repository page), so it can be fixed before it's
disclosed.

## License

By contributing, you agree that your contributions are licensed under the same license as the
project (see `LICENSE`).
