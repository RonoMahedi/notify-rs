//! SSL Wireless (ISMS Plus) SMS provider, API v3: `POST /api/v3/send-sms`.
//!
//! Selected with `SMS_PROVIDER=sslwireless`. Settings:
//!   SSLWIRELESS_API_TOKEN       API token from the ISMS Plus portal (or SSLWIRELESS_API_TOKEN_FILE)
//!   SSLWIRELESS_SID             registered sender ID, used for high/normal priority
//!   SSLWIRELESS_SID_BULK        optional: a separate (e.g. promotional) sender ID for bulk/campaign traffic
//!   SSLWIRELESS_URL             optional: base URL, default https://smsplus.sslwireless.com
//!
//! The gateway only accepts requests from whitelisted IPs: register the workers' outbound IP.
//!
//! Idempotency: `csms_id` (max 20 chars, unique) is derived from the notification id and the
//! attempt number. A crash-resend of the same attempt reuses it, so the gateway answers
//! "duplicate" (4023) instead of sending twice, and we count that as sent. A retry after a
//! failure is a new attempt with a new id, so a send the gateway dropped is never mistaken for
//! a duplicate.
//!
//! Status codes (from the API documentation):
//!   200 + sms_status SUCCESS          -> sent
//!   4023 duplicate csms_id            -> sent (already accepted earlier)
//!   4029, 4031 rate/TPS limit, 4033 too many OTPs, 5000, HTTP 5xx, network -> transient (retry tiers)
//!   4025 invalid / 4026 blocked number, 4027 too long, 4032 invalid SMS    -> permanent
//!   4001 bad token, 4002 sender ID not permitted, 4003 IP not whitelisted  -> permanent (configuration)

use std::time::Duration;

use async_trait::async_trait;
use common::model::{Envelope, Priority};
use metrics::counter;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::provider::{Content, Provider, ProviderError, SendResult};

const DEFAULT_URL: &str = "https://smsplus.sslwireless.com";

pub struct SslWireless {
    http: reqwest::Client,
    url: String,
    api_token: String,
    sid: String,
    sid_bulk: Option<String>,
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

impl SslWireless {
    /// None = SMS_PROVIDER isn't `sslwireless`. Err = selected but misconfigured, which stops
    /// the worker at startup instead of letting it look healthy while sending nothing.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        if var("SMS_PROVIDER").as_deref() != Some("sslwireless") {
            return Ok(None);
        }
        let api_token = match (var("SSLWIRELESS_API_TOKEN"), var("SSLWIRELESS_API_TOKEN_FILE")) {
            (Some(t), _) => t,
            (None, Some(path)) => std::fs::read_to_string(&path)
                .map_err(|e| anyhow::anyhow!("reading SSLWIRELESS_API_TOKEN_FILE {path}: {e}"))?
                .trim()
                .to_string(),
            (None, None) => anyhow::bail!("SMS_PROVIDER=sslwireless needs SSLWIRELESS_API_TOKEN or SSLWIRELESS_API_TOKEN_FILE"),
        };
        let sid = var("SSLWIRELESS_SID").ok_or_else(|| anyhow::anyhow!("SMS_PROVIDER=sslwireless needs SSLWIRELESS_SID"))?;
        Self::new(var("SSLWIRELESS_URL").unwrap_or_else(|| DEFAULT_URL.into()), api_token, sid, var("SSLWIRELESS_SID_BULK")).map(Some)
    }

    pub fn new(url: String, api_token: String, sid: String, sid_bulk: Option<String>) -> anyhow::Result<Self> {
        if api_token.is_empty() || api_token.len() > 50 {
            anyhow::bail!("SSL Wireless API token must be 1..=50 characters");
        }
        for s in std::iter::once(&sid).chain(sid_bulk.as_ref()) {
            if s.is_empty() || s.len() > 20 {
                anyhow::bail!("SSL Wireless sender ID {s:?} must be 1..=20 characters");
            }
        }
        Ok(SslWireless {
            http: reqwest::Client::builder().timeout(Duration::from_secs(10)).pool_max_idle_per_host(64).build()?,
            url: format!("{}/api/v3/send-sms", url.trim_end_matches('/')),
            api_token,
            sid,
            sid_bulk,
        })
    }

    pub fn has_bulk_sid(&self) -> bool {
        self.sid_bulk.is_some()
    }

    fn sid_for(&self, p: Priority) -> &str {
        match (p, &self.sid_bulk) {
            (Priority::Bulk, Some(b)) => b,
            _ => &self.sid,
        }
    }
}

/// E.164 or local Bangladeshi formats -> the gateway's `8801XXXXXXXXX` (digits only, max 16).
fn msisdn(phone: &str) -> Option<String> {
    let digits: String = phone.chars().filter(|c| !matches!(c, '+' | ' ' | '-' | '(' | ')')).collect();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // 01712345678 (local) -> 8801712345678
    let full = if digits.len() == 11 && digits.starts_with("01") { format!("88{digits}") } else { digits };
    (10..=16).contains(&full.len()).then_some(full)
}

/// Stable 20-hex-char client reference for one attempt of one notification.
fn csms_id(env: &Envelope) -> String {
    let h = Sha256::new().chain_update(env.id.as_bytes()).chain_update(env.attempt.to_be_bytes()).finalize();
    h.iter().take(10).map(|b| format!("{b:02x}")).collect()
}

fn request_body(api_token: &str, sid: &str, msisdn: &str, sms: &str, csms_id: &str) -> Value {
    json!({ "api_token": api_token, "sid": sid, "msisdn": msisdn, "sms": sms, "csms_id": csms_id })
}

/// The gateway's answer -> sent, retry later (Transient) or give up (Permanent).
/// It may report errors with HTTP 200, so the body's `status_code` decides when present.
fn classify(http_status: u16, body: &str) -> Result<(), ProviderError> {
    use ProviderError::{Permanent, Transient};
    let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let Some(code) = v["status_code"].as_u64() else {
        return Err(match http_status {
            429 => Transient("SSL Wireless rate limit (HTTP 429)".into()),
            500..=599 => Transient(format!("SSL Wireless server error (HTTP {http_status})")),
            _ => {
                let snippet: String = body.chars().take(120).collect();
                Permanent(format!("SSL Wireless gave an unexpected answer (HTTP {http_status}): {snippet}"))
            }
        });
    };
    let info = &v["smsinfo"][0];
    let msg = [&v["error_message"], &info["status_message"]]
        .iter()
        .filter_map(|m| m.as_str())
        .find(|m| !m.is_empty())
        .unwrap_or("no details")
        .to_string();
    match code {
        200 => match info["sms_status"].as_str().unwrap_or("SUCCESS") {
            "SUCCESS" | "DUPLICATE" => Ok(()),
            "INVALID" => Err(Permanent(format!("SSL Wireless: invalid number or message: {msg}"))),
            "BLOCKED" => Err(Permanent(format!("SSL Wireless: number blocked: {msg}"))),
            other => Err(Permanent(format!("SSL Wireless: SMS status {other}: {msg}"))),
        },
        4023 => Ok(()),
        4029 | 4031 => Err(Transient(format!("SSL Wireless rate limit ({code}): lower RATE_LIMIT_SMS to your TPS quota"))),
        4033 => Err(Transient("SSL Wireless: too many OTPs to this number recently (4033)".into())),
        5000..=5999 => Err(Transient(format!("SSL Wireless internal error ({code}): {msg}"))),
        4001 => Err(Permanent("SSL Wireless rejected the API token (4001): check SSLWIRELESS_API_TOKEN".into())),
        4002 => Err(Permanent("SSL Wireless: sender ID not permitted (4002): check SSLWIRELESS_SID with SSL".into())),
        4003 => Err(Permanent("SSL Wireless: server IP not whitelisted (4003): register the workers' outbound IP in the portal".into())),
        4024 => Err(Permanent("SSL Wireless: duplicate message to this number (4024)".into())),
        4025 => Err(Permanent("SSL Wireless: invalid phone number (4025)".into())),
        4026 => Err(Permanent("SSL Wireless: phone number is blocked (4026)".into())),
        4027 => Err(Permanent("SSL Wireless: message too long (4027): shorten the template".into())),
        4032 => Err(Permanent("SSL Wireless: invalid SMS text (4032)".into())),
        _ => Err(Permanent(format!("SSL Wireless error {code}: {msg}"))),
    }
}

#[async_trait]
impl Provider for SslWireless {
    fn name(&self) -> &'static str {
        "sslwireless"
    }

    async fn send(&self, env: &Envelope, content: &Content) -> SendResult {
        let Some(phone) = env.recipient.as_ref().and_then(|r| r.phone.as_deref()) else {
            return SendResult::err(ProviderError::Permanent("no phone number for this user".into()));
        };
        let Some(to) = msisdn(phone) else {
            return SendResult::err(ProviderError::Permanent("phone number isn't a valid MSISDN (e.g. +8801712345678)".into()));
        };
        let body = request_body(&self.api_token, self.sid_for(env.priority), &to, &content.body, &csms_id(env));
        let resp = self.http.post(&self.url).header("Accept", "application/json").json(&body).send().await;
        let result = match resp {
            Ok(r) => {
                let status = r.status().as_u16();
                let text = r.text().await.unwrap_or_default();
                classify(status, &text)
            }
            Err(e) if e.is_timeout() => Err(ProviderError::Transient("SSL Wireless timed out".into())),
            Err(e) => Err(ProviderError::Transient(format!("SSL Wireless unreachable: {e}"))),
        };
        let outcome = match &result {
            Ok(()) => "sent",
            Err(ProviderError::Transient(_)) => "transient",
            Err(ProviderError::Permanent(_)) => "permanent",
        };
        counter!("sslwireless_requests_total", "result" => outcome).increment(1);
        SendResult { result, dead_tokens: vec![] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::{Channel, Recipient};

    fn env(phone: Option<&str>, priority: Priority, attempt: u32) -> Envelope {
        Envelope {
            id: uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0001),
            user_id: "u1".into(),
            channel: Channel::Sms,
            priority,
            template: "otp".into(),
            data: json!({}),
            created_at_ms: 0,
            expires_at_ms: None,
            attempt,
            not_before_ms: 0,
            recipient: phone.map(|p| Recipient { email: None, phone: Some(p.into()), push_tokens: vec![] }),
            campaign_id: None,
        }
    }

    fn content() -> Content {
        Content { title: None, body: "Your code is 123456".into() }
    }

    #[test]
    fn normalizes_numbers() {
        assert_eq!(msisdn("+8801712345678").as_deref(), Some("8801712345678"));
        assert_eq!(msisdn("01712345678").as_deref(), Some("8801712345678"));
        assert_eq!(msisdn("+880 1712-345678").as_deref(), Some("8801712345678"));
        assert_eq!(msisdn("abc"), None);
        assert_eq!(msisdn("12345"), None);
        assert_eq!(msisdn("+12345678901234567"), None);
    }

    #[test]
    fn csms_id_is_stable_per_attempt() {
        let a = csms_id(&env(None, Priority::High, 0));
        assert_eq!(a.len(), 20);
        assert!(a.bytes().all(|b| b.is_ascii_alphanumeric()));
        assert_eq!(a, csms_id(&env(None, Priority::High, 0)));
        assert_ne!(a, csms_id(&env(None, Priority::High, 1)));
    }

    #[test]
    fn builds_the_request() {
        let b = request_body("tok", "BRAND", "8801712345678", "hi", "abc");
        assert_eq!(b, json!({"api_token":"tok","sid":"BRAND","msisdn":"8801712345678","sms":"hi","csms_id":"abc"}));
    }

    #[test]
    fn bulk_uses_its_own_sender_id() {
        let p = SslWireless::new("http://x".into(), "t".into(), "TXN".into(), Some("PROMO".into())).unwrap();
        assert_eq!(p.sid_for(Priority::High), "TXN");
        assert_eq!(p.sid_for(Priority::Bulk), "PROMO");
        let p = SslWireless::new("http://x".into(), "t".into(), "TXN".into(), None).unwrap();
        assert_eq!(p.sid_for(Priority::Bulk), "TXN");
        assert!(SslWireless::new("http://x".into(), "t".into(), "A_SENDER_ID_TOO_LONG_X".into(), None).is_err());
    }

    #[test]
    fn classifies_answers() {
        let ok =
            r#"{"status":"SUCCESS","status_code":200,"error_message":"","smsinfo":[{"sms_status":"SUCCESS","status_message":"Success"}]}"#;
        assert!(classify(200, ok).is_ok());
        assert!(classify(200, r#"{"status":"FAILED","status_code":4023,"error_message":"Duplicate CMS ID"}"#).is_ok());
        let perm =
            |c: u64| matches!(classify(200, &format!(r#"{{"status":"FAILED","status_code":{c}}}"#)), Err(ProviderError::Permanent(_)));
        let trans =
            |c: u64| matches!(classify(200, &format!(r#"{{"status":"FAILED","status_code":{c}}}"#)), Err(ProviderError::Transient(_)));
        for c in [4001, 4002, 4003, 4024, 4025, 4026, 4027, 4032, 4099] {
            assert!(perm(c), "{c} should be permanent");
        }
        for c in [4029, 4031, 4033, 5000] {
            assert!(trans(c), "{c} should be transient");
        }
        let invalid = r#"{"status_code":200,"smsinfo":[{"sms_status":"INVALID","status_message":"Invalid MSISDN"}]}"#;
        assert!(matches!(classify(200, invalid), Err(ProviderError::Permanent(m)) if m.contains("Invalid MSISDN")));
        assert!(matches!(classify(502, "<html>bad gateway</html>"), Err(ProviderError::Transient(_))));
        assert!(matches!(classify(429, ""), Err(ProviderError::Transient(_))));
        assert!(matches!(classify(404, "not found"), Err(ProviderError::Permanent(_))));
    }

    /// Full round trip against a local fake gateway.
    #[tokio::test]
    async fn sends_over_http() {
        use axum::{routing::post, Json, Router};
        use std::sync::{Arc, Mutex};

        let seen: Arc<Mutex<Vec<Value>>> = Arc::default();
        let log = seen.clone();
        let app = Router::new().route(
            "/api/v3/send-sms",
            post(move |Json(b): Json<Value>| {
                let log = log.clone();
                async move {
                    log.lock().unwrap().push(b.clone());
                    let code = if b["msisdn"] == "8801900000000" { 4026 } else { 200 };
                    Json(json!({"status": "x", "status_code": code, "error_message": "", "smsinfo": [{"sms_status": "SUCCESS"}]}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });

        let p = SslWireless::new(format!("http://{addr}/"), "tok".into(), "TXN".into(), Some("PROMO".into())).unwrap();
        assert!(p.send(&env(Some("+8801712345678"), Priority::Bulk, 2), &content()).await.result.is_ok());
        assert!(matches!(
            p.send(&env(Some("+8801900000000"), Priority::High, 0), &content()).await.result,
            Err(ProviderError::Permanent(_))
        ));
        assert!(matches!(p.send(&env(None, Priority::High, 0), &content()).await.result, Err(ProviderError::Permanent(_))));

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "no request without a phone number");
        assert_eq!(seen[0]["sid"], "PROMO");
        assert_eq!(seen[0]["msisdn"], "8801712345678");
        assert_eq!(seen[0]["sms"], "Your code is 123456");
        assert_eq!(seen[0]["csms_id"], csms_id(&env(None, Priority::Bulk, 2)));
        assert_eq!(seen[1]["sid"], "TXN");

        // Nothing listening: transient, so the retry tiers take over.
        let down = SslWireless::new("http://127.0.0.1:1".into(), "tok".into(), "TXN".into(), None).unwrap();
        assert!(matches!(
            down.send(&env(Some("01712345678"), Priority::High, 0), &content()).await.result,
            Err(ProviderError::Transient(_))
        ));
    }
}
