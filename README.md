# notify: a Rust notification system for 100M+/day

Design doc: [ARCHITECTURE.md](ARCHITECTURE.md) · Adding email/SMS/push gateways: [docs/PROVIDERS.md](docs/PROVIDERS.md) · Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)

## Run everything locally

```bash
docker compose up -d --build                         # Redpanda, Redis, API x2, workers, Prometheus, Grafana
docker compose --profile load up -d loadgen          # 2,000/s synthetic traffic (≈173M/day)
TARGET_RPS=12000 docker compose --profile load up -d loadgen   # simulate 10x peak
```

- Dashboard: http://localhost:3001 (opens on "Notification System")
- Kafka console (Redpanda Console): http://localhost:8085 for topics, individual messages, consumer groups and lag.
  It has no login and can delete topics, so it only listens on this machine (127.0.0.1).
- Prometheus / alerts: http://localhost:9090/alerts
- API: http://localhost:8080 (`/` lists endpoints, `/metrics` = API metrics)
- Worker metrics aren't published to the host; view them via Prometheus/Grafana.
  `172.x.x.x` addresses on the Prometheus targets page are Docker-internal and
  are not reachable from macOS.

## API

```bash
curl -s localhost:8080/v1/notifications -H 'content-type: application/json' -d '{
  "idempotency_key": "order-123-shipped",
  "user_id": "u42", "channel": "push", "priority": "high",
  "template": "order_shipped", "data": {"order": "123"}, "ttl_seconds": 3600
}'
# => 202 {"id":"0193...","duplicate":false}   (same key again => 200, duplicate:true)

curl -s localhost:8080/v1/notifications/<id>     # queued | retrying | sent | failed | expired | suppressed
# Batch: POST /v1/notifications/batch {"notifications":[...up to 1000]}  => 207 per-item results

# Opt a user out of SMS:
docker compose exec redis redis-cli SADD optout:u42 sms
```

## Run binaries natively (against the compose Kafka/Redis)

```bash
docker compose up -d redpanda topics redis
cargo run --bin api
WORKER_TOPICS= cargo run --bin worker      # empty = all main topics
cargo run --release --bin loadgen
```

## Configuration (env)

| Var | Default | |
|---|---|---|
| `KAFKA_BROKERS` | localhost:19092 | |
| `REDIS_URL` | redis://127.0.0.1:16379 | compose Redis is on host port 16379 |
| `WORKER_TOPICS` / `WORKER_GROUP` | all main / notify-workers | dedicated pools |
| `BATCH_SIZE` / `BATCH_WAIT_MS` / `CONCURRENCY` | 500 / 20 / 1000 | worker tuning (concurrency is per batch) |
| `INFLIGHT_BATCHES` | 4 | batches processed concurrently per worker; offsets still stored in order |
| `STATUS_TTL_SECS` | 86400 | hot status + worker dedup marker in Redis |
| `RATE_LIMIT_{PUSH,EMAIL,SMS,IN_APP}` | 5000 | per pod, per second |
| `PROVIDER_TIMEOUT_MS` | 5000 | |
| `MOCK_TRANSIENT_RATE` / `MOCK_PERMANENT_RATE` | 0.02 / 0.005 | mock provider failure injection |

Push goes through Firebase, SMS through SSL Wireless (`SMS_PROVIDER=sslwireless`) and email through Amazon SES (`EMAIL_PROVIDER=ses`) when configured. In-app still uses a mock provider that only pretends to send. [docs/PROVIDERS.md](docs/PROVIDERS.md) explains where credentials go and how to add a real gateway.

## Campaigns (marketing)

**Admin UI: http://localhost:8090/admin**. Sign in with your email and password.

```bash
# First admin account (prints a one-time temporary password; change it under Account):
docker compose exec campaign campaign create-admin you@example.com
# Forgot a password / locked out (5 failed tries locks the account for 15 min;
# also, 100 failures from one IP in 15 min block that IP: LOGIN_MAX_FAILS_PER_IP):
docker compose exec campaign campaign reset-password you@example.com
```

Admins add other people under **Users** with one of two roles: `admin` (everything, including users) or `marketer` (everything except users).
Passwords are hashed with Argon2id. Sessions use an HttpOnly, SameSite=Strict cookie that lasts 12 h.
Set `COOKIE_SECURE=true` when serving over HTTPS.

The UI has these tabs (admins also see **Users**):
- **People:** search by email, phone (`01712…`), name or user ID, or browse. Opening a person shows their profile, their devices with full FCM/APNs tokens, and every notification sent to them in the last 30 days, with status, address, provider and failure reason.
- **Deliveries:** a live feed of outcomes (sent, retrying, failed, expired, suppressed), filterable by status, channel and campaign. It also shows counts for the window and the top failure reasons. Every campaign has a "deliveries" button that opens this view.

- **System:** the whole pipeline, live:
  - service health
  - traffic per second and p99 latency
  - every Kafka topic (partitions, stored messages, write rate, backlog, reader group)
  - consumer groups
  - Redis and Postgres storage
  - a dead-letter queue viewer with each message's failure reason

- **Templates:** what each message says, per channel (SMS has no subject).
  - **Tokens:** `{{first_name}}`, `{{last_name}}`, `{{email}}`, `{{phone}}`, `{{user_id}}`, `{{attributes.<key>}}`, and any key from the notification's or campaign's `data` (e.g. `{{discount}}`, `{{order}}`).
  - **Defaults:** `{{first_name|Customer}}` prints "Customer" when the value is missing.
  - **Live preview:** shows the exact text for a real person. For SMS it also shows characters and **SMS parts**: Bengali or emoji switch SMS to Unicode, so a part holds 70 characters instead of 160.
  - **Hide in history:** templates marked this way (e.g. `otp`) never store their text in delivery history.
  - **Sending:** workers render at send time. Template edits reach them within 30 s.
  - **Missing templates:** a message whose template doesn't exist fails with "no sms template named '…'" (visible in Deliveries). A campaign can't be created with one at all.
  - **Starter templates:** `otp`, `promo_weekly`, `order_shipped`, `welcome`.
- People and Deliveries show the **exact message that was sent** to each person.

Older tabs:
- **Overview:** contacts, running campaigns, sync health.
- **Campaigns:** create one (with a live audience count and an ETA), pause, resume or cancel.
- **Cohorts:** a point-and-click rule builder with a preview count, plus a JSON editor for nested rules.
- **Audience:** Excel/CSV upload with progress and a per-row error list, identity sync status, and person lookup.

Campaign service at http://localhost:8090. Scripts call the `/v1` API with an **API key**:
`Authorization: Bearer $ADMIN_TOKEN` (compose default: `dev-admin-token-change-me`; set your own, or unset it to disable key access).

```bash
T="Authorization: Bearer dev-admin-token-change-me"; C=localhost:8090   # API key, for scripts

# 1) Audience. Three ways in:
#  a) Live sync from identity Postgres (automatic, every 30s; force a run):
curl -s -H "$T" -X POST $C/v1/sync/run;  curl -s -H "$T" $C/v1/sync
#  b) Excel/CSV upload (optionally saved as a static cohort):
curl -s -H "$T" -F file=@customers.xlsx -F "segment_name=Eid VIP list" $C/v1/imports
curl -s -H "$T" $C/v1/imports/<id>            # progress + per-row errors
#  c) API from other services:
curl -s -H "$T" -H 'content-type: application/json' $C/v1/contacts \
  -d '{"contacts":[{"user_id":"u1","email":"a@x.com","phone":"01712345678","attributes":{"plan":"gold"}}]}'
curl -s -H "$T" -H 'content-type: application/json' $C/v1/devices -d '{"user_id":"u1","platform":"android","token":"<fcm token>"}'

# 2) Cohort: all users, a rule, or an uploaded list. Preview the size first:
curl -s -H "$T" -H 'content-type: application/json' $C/v1/segments/preview -d '{
  "channel": "email",
  "rule": {"all": [
    {"field": "attributes.plan", "op": "in", "value": ["gold", "premium"]},
    {"field": "last_active_at", "op": "within", "value": "30d"}
  ]}}'
curl -s -H "$T" -H 'content-type: application/json' $C/v1/segments -d '{"name":"Gold active","kind":"rule","rule":{...}}'
curl -s -H "$T" -H 'content-type: application/json' $C/v1/segments -d '{"name":"Everyone","kind":"all"}'

# 3) Campaign: start now, or schedule with "scheduled_at": "2026-12-01T10:00:00+06:00"
curl -s -H "$T" -H 'content-type: application/json' $C/v1/campaigns -d '{
  "name": "Winter sale", "segment_id": "<id>", "channel": "email", "template": "winter_sale",
  "data": {"discount": "20%"}, "rate_per_sec": 5000, "frequency_cap": 2, "start": true }'
curl -s -H "$T" $C/v1/campaigns/<id>            # targeted / enqueued / suppressed / progress %
curl -s -H "$T" -X POST $C/v1/campaigns/<id>/pause    # also: /resume, /cancel, /start (for drafts)

# Unsubscribe (also drops messages already queued):
curl -s -H "$T" -H 'content-type: application/json' $C/v1/contacts/u1/unsubscribe -d '{"channel":"sms"}'
```

**Excel/CSV columns** (header row; case and spacing don't matter): `user_id`, `email`, `phone`
(`mobile`/`msisdn` also accepted), `first_name` (`name`), `last_name`, `country`, `language`, `timezone`.
**Any other column becomes an attribute** usable in rules (`Total Orders` → `attributes.total_orders`).
Each row needs one of user_id/email/phone. Rows matching an existing contact by email/phone merge into it.
Phones normalize to E.164: `01712345678`, `1712345678` (Excel number), `8801712345678` → `+8801712345678`.

**Rule operators:** `eq ne in not_in contains exists not_exists` (text and attributes), `gt gte lt lte`
(numbers and dates), `within older_than` (dates, e.g. `"30d"`, `"12h"`, `"2w"`). Combine with `all`, `any`, `not`.
Fields: `country language timezone source email phone first_name last_name user_id last_active_at
created_at updated_at` and `attributes.<key>`. Identity-synced contacts get `attributes.email_verified`,
`attributes.phone_verified`, and `attributes.identity_status`.

**Connecting your real identity DB:** set
`IDENTITY_DATABASE_URL=postgres://<readonly_user>:<pw>@host.docker.internal:5432/identity`.
Use a read-only role with SELECT on `users` only. Add `@@index([updatedAt, id])` to the Prisma `User` model,
or each sync run does a full table scan.

## Tests

CI (`.github/workflows/ci.yml`) runs `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`, and a Docker image build on every push to `main` and every PR. The shell suites below need the running stack, so they aren't in CI.

```bash
cargo test                 # 46 unit tests: rule engine (incl. SQL-injection cases), phone/email/Excel parsing, model, SSL Wireless SMS, SES email
tests/e2e.sh               # 58 end-to-end checks against the running stack (sync, import, cohorts, campaigns, bulk lane)
tests/history.sh           # people search + delivery history (run after e2e.sh)
tests/system.sh            # System view: Kafka topics/groups, metrics, storage, dead-letter queue
tests/fcm.sh               # Firebase push against a fake Google OAuth + FCM server
tests/auth.sh              # sign-in security checks: hashing, cookies, CSRF, roles, lockout, password change
tests/run_ui.sh            # browser checks of the admin UI in headless Chrome (run after e2e.sh)
```

For exact expected counts, start `e2e.sh` from a fresh campaign database:
`docker compose rm -sf campaign postgres && docker volume rm rust_pgdata && docker compose exec redis redis-cli FLUSHALL && docker compose up -d`.
Expected values come from the identity-sim source database or from the fixture generator, never from the database under test.

## Firebase push (FCM)

Push goes through **FCM HTTP v1** when `FCM_CREDENTIALS_FILE` is set; otherwise a mock provider is used.
Step-by-step credential setup: [`secrets/README.md`](secrets/README.md). Short version:
save the service-account JSON as `secrets/firebase.json`, put `FCM_CREDENTIALS_FILE=/run/secrets/firebase.json`
and `FCM_VALIDATE_ONLY=true` in `.env`, then restart the workers. The **System** tab shows which provider is active.

What the adapter does:
- **Sign-in:** signs in with the service account (RS256 → OAuth). It reuses the access token for about an hour, and refreshes once on a 401.
- **All devices:** a push to a `user_id` goes to **every registered device** of that user (tokens mirrored from the `devices` table into Redis).
- **Delivered:** counts as sent if at least one device accepted it.
- **Dead tokens:** `UNREGISTERED`, an invalid token or a sender mismatch means the token is **deleted automatically**.
- **Retries:** `UNAVAILABLE`, `INTERNAL`, `QUOTA_EXCEEDED` and network errors go to the retry tiers.
- **iOS:** `THIRD_PARTY_AUTH_ERROR` means the APNs key isn't uploaded in Firebase.
- **Priority:** `high` priority becomes Android `HIGH` and `apns-priority: 10`. The notification id, template and campaign go in the `data` payload.

### Getting device tokens from the mobile app

The app gets an FCM token from the Firebase SDK and sends it to **your own backend**, which knows the logged-in user.
Your backend then registers it with the API key. **Never put the API key inside the app.**

```bash
# your backend, after login and whenever the SDK reports a new token:
curl -X POST http://notify-campaign:8090/v1/devices -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H 'content-type: application/json' -d '{"user_id":"<your user id>","platform":"android","token":"<fcm token>"}'
# on logout:
curl -X DELETE http://notify-campaign:8090/v1/devices/<fcm token> -H "Authorization: Bearer $ADMIN_TOKEN"
```

```kotlin
// Android: FirebaseMessaging.getInstance().token gives the token; onNewToken() fires when it changes.
FirebaseMessaging.getInstance().token.addOnSuccessListener { token -> myBackend.registerPushToken(token) }
```
```dart
// Flutter (firebase_messaging): ask permission (iOS/Android 13+), then send the token to your backend.
await FirebaseMessaging.instance.requestPermission();
final token = await FirebaseMessaging.instance.getToken();
FirebaseMessaging.instance.onTokenRefresh.listen(myBackend.registerPushToken);
```

Tested with `tests/fcm.sh` against a fake Google server (`tests/fake_fcm.py`). It checks:
- that every sign-in is a correctly signed RS256 token
- that the right text is rendered
- that both devices are reached and the uninstalled one is deleted
- busy → retry, invalid → failed and deleted, no device → clear reason
- that a revoked access token is refreshed
- validate-only mode

## License

[MIT](LICENSE). Contributions are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md).
