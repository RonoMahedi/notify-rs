# Delivery providers: email, SMS and push gateways

A **provider** is the adapter that hands a rendered message to an outside service: Firebase for
push, an SMS gateway, an email service. This guide covers:

- what is connected today
- where credentials go, and why not the admin UI
- how to set up Firebase (no code needed)
- how to add a new email or SMS gateway, step by step, with a working example
- a checklist before you send real messages

## 1. What exists today

| Channel | Provider | Status |
|---|---|---|
| Push | Firebase Cloud Messaging (HTTP v1): `crates/worker/src/fcm.rs` | **Done.** Add credentials and it's live. Covers Android and, through Firebase, iOS. |
| Email | `MockProvider` | **Mock only.** Pretends to send. Needs an adapter (SES, SendGrid, SMTP, …). |
| SMS | SSL Wireless (ISMS Plus, API v3): `crates/worker/src/sslwireless.rs` | **Done.** Set `SMS_PROVIDER=sslwireless` and its settings ([below](#ssl-wireless)). Other gateways need an adapter. |
| In-app | `MockProvider` | **Mock only.** Needs a WebSocket/realtime gateway. |

When a channel has no real provider configured, the worker uses the mock, so the whole pipeline
and the dashboards work locally without any accounts. The admin UI's **System** tab shows which
provider each channel is using.

## 2. Where configuration goes

**Credentials (API keys, passwords, service-account files) go in environment variables or secret
files. Never put them in the admin UI, the database, or git.**

Why:

- Anyone who gets into an admin account could read a key stored in the UI or database, then send
  messages on your bill. Environment variables and secret files never leave the servers.
- A key in the database also ends up in every backup.
- In production, the same variables come from a secret manager (Kubernetes Secrets, AWS Secrets
  Manager, Docker secrets, Vault). The code doesn't change; only where the values come from does.

Locally:

- Put variables in `.env` in the project root. It's git-ignored, and Docker Compose reads it
  automatically.
- Put credential *files* in `secrets/`. It's git-ignored too, and mounted read-only into every
  container at `/run/secrets`.

**Settings that aren't secret** can reasonably be managed elsewhere. Examples: the SMS sender ID,
an email "From" name, or rate limits. Today they're also environment variables; moving them into
the admin UI would be a welcome contribution.

### Naming convention

| Variable | Meaning |
|---|---|
| `<CHANNEL>_PROVIDER` | Which adapter a channel uses, e.g. `SMS_PROVIDER=sslwireless`, `EMAIL_PROVIDER=ses`. Unset means the mock. |
| `<PROVIDER>_*` | That adapter's settings, e.g. `SES_REGION`, `SSLWIRELESS_API_TOKEN`. |
| `RATE_LIMIT_<CHANNEL>` | Max sends per second, **per worker pod** (default 5000). Set it from your provider's quota divided by the number of worker pods. |
| `PROVIDER_TIMEOUT_MS` | A send that takes longer counts as a temporary failure and is retried (default 5000). |

> **Docker Compose only passes variables it lists.** When you add a variable, also add it under
> `x-app → environment` in `docker-compose.yml`, the way the `FCM_*` variables are, or the
> containers will never see it.

## 3. Firebase push (no code needed)

Follow [`secrets/README.md`](../secrets/README.md). In short:

1. Download a service-account JSON from the Firebase console and save it as `secrets/firebase.json`.
2. In `.env`, set `FCM_CREDENTIALS_FILE=/run/secrets/firebase.json` and, for a first dry run,
   `FCM_VALIDATE_ONLY=true` (Firebase checks everything but delivers nothing).
3. Restart the workers and check the **System** tab.
4. For iOS, also upload your APNs auth key (.p8) in Firebase → Project settings → Cloud Messaging.
5. Set `FCM_VALIDATE_ONLY=false` to deliver for real.

Apps register device tokens through your backend with `POST /v1/devices` (see the README).

## 4. How a provider fits into delivery

```
Kafka topic → worker → (dedup, expiry, opt-out, template rendering, rate limit)
            → provider.send(envelope, content) → Ok | Transient error | Permanent error
                 Ok         → status "sent"
                 Transient  → retry tiers: 5s → 1m → 10m, then the dead-letter queue
                 Permanent  → dead-letter queue right away, with your error text as the reason
```

The worker already handles retries, timeouts, rate limiting, deduplication, status and history.
**A provider only has to send one message and say honestly whether a failure is worth retrying.**

The interface (`crates/worker/src/provider.rs`):

```rust
#[async_trait]
pub trait Provider: Send + Sync {
    /// Short label for metrics and delivery history, e.g. "ses", "sslwireless".
    fn name(&self) -> &'static str;
    /// Send one rendered message to one recipient.
    async fn send(&self, env: &Envelope, content: &Content) -> SendResult;
}
```

What a provider receives:

- **`content.title`:** the email subject or push title. It's `None` for SMS.
- **`content.body`:** the final rendered text. Templates are already applied.
- **`env.recipient`:** the address.
  - `email`: a lowercased address.
  - `phone`: E.164 format, e.g. `+8801712345678`. Some gateways want `8801712345678` without the `+`; strip it in your adapter.
  - `push_tokens`: device tokens.
  - Campaigns always fill these in. Transactional callers may not, so handle a missing address (see the example).
- **`env.id`:** a unique, stable id per notification. If your provider supports an idempotency or
  client-reference field, send it there. Delivery is at-least-once, so a rare resend can happen
  after a crash, and this lets the provider drop it.
- **`env.priority`:** `high` (OTPs, security), `normal`, or `bulk` (marketing). It's useful if your
  provider has separate routes for transactional and promotional traffic.

### Transient or Permanent: the most important decision

| Situation | Return | Why |
|---|---|---|
| Timeout, connection error, HTTP 5xx | `Transient` | Likely to work later |
| HTTP 429 / "rate limit" / "quota exceeded" | `Transient` | Back off and retry |
| Invalid number or address, unsubscribed, blacklisted | `Permanent` | Retrying won't help and may cost money |
| Rejected credentials (401/403) | `Permanent` | A config problem. It shows up in Deliveries with your reason. |
| Message rejected (bad content, too long) | `Permanent` | Same message, same result |
| Unknown error | `Permanent` with the provider's own message | Visible to operators instead of silently retried |

Write error text for a human reading the **Deliveries** tab: say what happened and, if possible,
what to do ("sender ID not approved: register it with your gateway").

Rules:

- **Don't retry inside the provider.** The worker's retry tiers do that, without blocking other messages.
- **Don't sleep or rate-limit inside the provider.** `RATE_LIMIT_<CHANNEL>` already does it.
- **Don't log message bodies or full addresses at info level.** OTP templates are marked sensitive for a reason.
- **Fail fast on bad configuration.** `from_env()` should return an error if the provider is
  selected but a required variable is missing. The worker then refuses to start, instead of
  looking healthy while sending nothing.

## 5. Adding a gateway, step by step

The example below is a generic JSON-over-HTTP SMS gateway. It compiles and passes clippy as written.
For a complete real adapter, with error-code mapping, idempotency and a test against a local fake
gateway, see `crates/worker/src/sslwireless.rs`.
Adapt the request body and the error mapping to your gateway's API documentation.

### Step 1: create the adapter

`crates/worker/src/http_sms.rs` (name the file after your gateway, e.g. `sslwireless.rs`):

```rust
use std::time::Duration;

use async_trait::async_trait;
use common::model::Envelope;
use serde_json::{json, Value};

use crate::provider::{Content, Provider, ProviderError, SendResult};

pub struct HttpSms {
    http: reqwest::Client,
    url: String,
    api_key: String,
    sender_id: String,
}

impl HttpSms {
    /// None = not configured (the worker uses the mock). Err = configured but broken,
    /// which stops the worker at startup instead of silently "sending" nothing.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        if std::env::var("SMS_PROVIDER").ok().as_deref() != Some("http") {
            return Ok(None);
        }
        let need = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty()).ok_or_else(|| anyhow::anyhow!("{k} is required"));
        Ok(Some(HttpSms {
            http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
            url: need("SMS_API_URL")?,
            api_key: need("SMS_API_KEY")?,
            sender_id: need("SMS_SENDER_ID")?,
        }))
    }
}

/// Turns the gateway's answer into retry (Transient) or give up (Permanent).
fn classify(status: u16, body: &str) -> ProviderError {
    let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let msg = v["message"].as_str().unwrap_or(body).chars().take(200).collect::<String>();
    match status {
        429 => ProviderError::Transient("SMS gateway rate limit".into()),
        500..=599 => ProviderError::Transient(format!("SMS gateway error {status}")),
        401 | 403 => ProviderError::Permanent(format!("SMS gateway rejected our credentials: {msg}")),
        _ => ProviderError::Permanent(format!("SMS gateway error {status}: {msg}")),
    }
}

#[async_trait]
impl Provider for HttpSms {
    fn name(&self) -> &'static str {
        "http_sms"
    }

    async fn send(&self, env: &Envelope, content: &Content) -> SendResult {
        let Some(phone) = env.recipient.as_ref().and_then(|r| r.phone.as_deref()) else {
            return SendResult::err(ProviderError::Permanent("no phone number for this user".into()));
        };
        let body = json!({
            "to": phone,                    // E.164, e.g. +8801712345678
            "from": self.sender_id,
            "text": content.body,
            "reference": env.id.to_string(), // lets the gateway drop our duplicates, if supported
        });
        let resp = self.http.post(&self.url).bearer_auth(&self.api_key).json(&body).send().await;
        match resp {
            Ok(r) if r.status().is_success() => SendResult::ok(),
            Ok(r) => {
                let status = r.status().as_u16();
                let text = r.text().await.unwrap_or_default();
                SendResult::err(classify(status, &text))
            }
            Err(e) if e.is_timeout() => SendResult::err(ProviderError::Transient("SMS gateway timed out".into())),
            Err(e) => SendResult::err(ProviderError::Transient(format!("SMS gateway unreachable: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_gateway_errors() {
        assert!(matches!(classify(429, ""), ProviderError::Transient(_)));
        assert!(matches!(classify(503, "<html>"), ProviderError::Transient(_)));
        assert!(matches!(classify(401, r#"{"message":"bad key"}"#), ProviderError::Permanent(_)));
        assert!(matches!(classify(400, r#"{"message":"invalid number"}"#), ProviderError::Permanent(_)));
    }
}
```

Many gateways return HTTP 200 even for errors, with the real result in the body (e.g.
`{"status":"FAILED","error_code":1002}`). If yours does, read the body on success too and map its
error codes. A gateway's error-code table is the single most useful thing to copy into `classify`.

### Step 2: register it in the worker

In `crates/worker/src/main.rs`, add `mod http_sms;` next to `mod fcm;`. Then, in `build_ctx`,
choose it when configured, the same way push chooses FCM:

```rust
let sms: Arc<dyn Provider> = match http_sms::HttpSms::from_env().expect("SMS configuration") {
    Some(p) => {
        gauge!("provider_info", "channel" => "sms", "provider" => "http_sms", "mode" => "live").set(1.0);
        Arc::new(p)
    }
    None => {
        gauge!("provider_info", "channel" => "sms", "provider" => "mock (twilio)", "mode" => "mock").set(1.0);
        mock("twilio", 80, 300)
    }
};
```

Use `sms` in the `providers` map instead of the mock. Remove `"sms"` from the loop that publishes
mock `provider_info` for the other channels. The `provider_info` gauge is what the **System** tab
shows.

### Step 3: pass the configuration through

- Add the variables under `x-app → environment` in `docker-compose.yml`, defaulting to empty:
  `SMS_PROVIDER: ${SMS_PROVIDER:-}`.
- Document them in the README's configuration table.

### Step 4: test it

1. **Unit tests**, no network: error classification (above) and request building (see
   `builds_the_v1_message` in `fcm.rs`).
2. **End-to-end against a fake server.** Copy the pattern of `tests/fake_fcm.py` and `tests/fcm.sh`:
   - a small local HTTP server records requests and returns scripted answers (success, 429, 503, invalid number)
   - the script points the workers at it (`SMS_API_URL=http://host.docker.internal:9999/...`)
   - it sends notifications and checks the outcome in Deliveries

   This tests retries and dead-lettering without spending real messages.
3. **A dry run with the real provider:** use its sandbox or test mode, or send to your own number first.

Run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test`
before opening a pull request. CI runs the same checks.

## 6. Notes by channel

### SSL Wireless

Bangladesh SMS through SSL Wireless ISMS Plus (`POST /api/v3/send-sms`).

| Variable | Required | Meaning |
|---|---|---|
| `SMS_PROVIDER` | yes | `sslwireless` |
| `SSLWIRELESS_API_TOKEN` | yes* | API token from the ISMS Plus portal. *Or `SSLWIRELESS_API_TOKEN_FILE` pointing to a file in `secrets/`. |
| `SSLWIRELESS_SID` | yes | Your approved sender ID (masking or non-masking). Used for `high` and `normal` priority. |
| `SSLWIRELESS_SID_BULK` | no | A separate sender ID for `bulk` (campaign) traffic, if SSL gave you a promotional one. Defaults to `SSLWIRELESS_SID`. |
| `SSLWIRELESS_URL` | no | Base URL. Default `https://smsplus.sslwireless.com`. |

Example `.env`:

```bash
SMS_PROVIDER=sslwireless
SSLWIRELESS_API_TOKEN=xxxx-xxxx-xxxx
SSLWIRELESS_SID=MYBRAND
RATE_LIMIT_SMS=50          # your TPS quota divided by the number of SMS worker pods
```

Before going live:

- **Whitelist the server IP.** SSL accepts requests only from registered IPs; otherwise every send
  fails with `4003`. Register the workers' outbound (NAT) IP in the portal.
- **Set `RATE_LIMIT_SMS`** to your TPS quota. Going over it returns `4029`/`4031`, which are retried
  but waste time.
- **Numbers** are sent as `8801XXXXXXXXX`. E.164 (`+880…`) and local (`017…`) formats are converted.

How results are handled:

| Gateway answer | Result |
|---|---|
| `200` with `SUCCESS` | Sent |
| `4023` duplicate `csms_id` | Sent (an earlier copy was already accepted) |
| `4029`, `4031` rate limit, `4033` too many OTPs, `5000`, HTTP 5xx, timeouts | Retried through the retry tiers |
| `4025` invalid number, `4026` blocked number, `4027` too long, `4032` invalid text | Failed, not retried |
| `4001` bad token, `4002` sender ID not permitted, `4003` IP not whitelisted | Failed, with a message saying what to fix |

**Duplicates:** each attempt sends a `csms_id` derived from the notification id and attempt number.
If a worker crashes after sending but before recording it, the redelivered message reuses the same
`csms_id`, and the gateway rejects it as a duplicate instead of sending a second SMS.

### SMS

- **Length:** the Templates tab shows parts per message. GSM text fits 160 characters per part.
  Bengali or emoji switch to Unicode, which fits 70 per part, and gateways bill per part.
- **Sender ID / masking:** many countries require a registered sender ID. In Bangladesh, masked
  (branded) sender IDs need approval through your gateway. Unregistered IDs are rejected or replaced.
- **Number format:** contacts are stored in E.164 (`+8801712345678`). Imports normalize local
  formats using `DEFAULT_COUNTRY_CODE` (default 880).
- **Promotional vs transactional routes:** if your gateway separates them, use `env.priority`.
  Send `bulk` through the promotional route and `high`/`normal` through the transactional one.

### Email

- **Domain authentication:** set up SPF, DKIM and DMARC for your sending domain, or messages land in spam.
- **Amazon SES** starts in a sandbox that can only send to verified addresses. Request production access.
- **HTML:** templates are plain text today. HTML email (and a plain-text alternative) is a good contribution.
- **Bounces and complaints:** providers report these by webhook. Handling them (marking addresses
  as bad, unsubscribing complainers) isn't built yet. See section 8.

### Push

- Firebase covers Android and iOS. A direct APNs adapter is only needed if you don't want Firebase.
- Uninstalled-app tokens are detected and deleted automatically.

## 7. Before sending real messages

- [ ] Credentials are in `.env`, `secrets/` or a secret manager, and **not** committed (`git status` is clean)
- [ ] Provider sandbox or test mode tried first (`FCM_VALIDATE_ONLY=true`, SES sandbox, gateway test mode)
- [ ] **System** tab shows the real provider, not "mock", for that channel
- [ ] `RATE_LIMIT_<CHANNEL>` set from your quota ÷ number of worker pods
- [ ] SMS sender ID approved; email domain has SPF, DKIM and DMARC
- [ ] One test message to yourself, visible in **Deliveries** as `sent` with the right text
- [ ] Alerts in Prometheus/Grafana are being watched (`deploy/prometheus/alerts.yml`)

## 8. Not built yet (good contributions)

- Adapters for SES, SendGrid, SMTP, Twilio and other Bangladeshi SMS gateways (Alpha, BulkSMSBD, …)
- **Delivery receipts:** SMS DLR and email bounce/complaint webhooks feeding back into history and opt-outs
- **Provider failover:** a second SMS gateway when the first is down
- **Circuit breaker:** stop calling a provider that is failing everything, and park traffic in the retry tiers
- HTML email templates
- Managing non-secret provider settings (sender ID, From name, rate limits) from the admin UI
