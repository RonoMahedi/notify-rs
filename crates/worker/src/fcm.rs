//! Firebase Cloud Messaging (HTTP v1) push provider.
//!
//! Auth: the service account signs a JWT (RS256) that Google's token endpoint trades
//! for a 1-hour OAuth access token; we reuse it until 5 minutes before expiry and
//! refresh once on a 401. Each of the user's device tokens gets its own request.
//!
//! Errors (FCM v1 `errorCode`):
//!   UNREGISTERED, SENDER_ID_MISMATCH, invalid token  -> dead token (deleted), permanent
//!   QUOTA_EXCEEDED, UNAVAILABLE, INTERNAL, network     -> transient (retry tiers)
//!   THIRD_PARTY_AUTH_ERROR                             -> iOS APNs key missing in Firebase
//!   UNAUTHENTICATED (401)                              -> refresh our token, retry once

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use common::model::{now_ms, Envelope, Priority};
use metrics::counter;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::provider::{Content, Provider, ProviderError, SendResult};

const SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

#[derive(Debug, Clone, Deserialize)]
pub struct ServiceAccount {
    pub project_id: String,
    pub client_email: String,
    pub private_key: String,
    #[serde(default = "default_token_uri")]
    pub token_uri: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".into()
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

pub struct Fcm {
    sa: ServiceAccount,
    key: jsonwebtoken::EncodingKey,
    http: reqwest::Client,
    endpoint: String,
    validate_only: bool,
    token: Mutex<Option<(String, Instant)>>,
}

#[derive(Debug, PartialEq)]
enum PerToken {
    Ok,
    Dead(String),
    Transient(String),
    Permanent(String),
    Unauthenticated,
}

impl Fcm {
    /// Configured from FCM_CREDENTIALS_FILE (service-account JSON). None = not configured.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let path = std::env::var("FCM_CREDENTIALS_FILE").unwrap_or_default();
        if path.trim().is_empty() {
            return Ok(None);
        }
        let raw = std::fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("reading FCM_CREDENTIALS_FILE {path}: {e}"))?;
        let mut sa: ServiceAccount = serde_json::from_str(&raw).map_err(|e| anyhow::anyhow!("{path} is not a Firebase service-account JSON: {e}"))?;
        if let Ok(uri) = std::env::var("FCM_TOKEN_URI") {
            if !uri.is_empty() {
                sa.token_uri = uri;
            }
        }
        Ok(Some(Self::new(
            sa,
            std::env::var("FCM_ENDPOINT").ok().filter(|e| !e.is_empty()).unwrap_or_else(|| "https://fcm.googleapis.com".into()),
            std::env::var("FCM_VALIDATE_ONLY").is_ok_and(|v| v == "true" || v == "1"),
        )?))
    }

    pub fn new(sa: ServiceAccount, endpoint: String, validate_only: bool) -> anyhow::Result<Self> {
        let key = jsonwebtoken::EncodingKey::from_rsa_pem(sa.private_key.as_bytes())
            .map_err(|e| anyhow::anyhow!("service-account private_key is not a valid RSA key: {e}"))?;
        Ok(Fcm {
            sa,
            key,
            http: reqwest::Client::builder().timeout(Duration::from_secs(10)).pool_max_idle_per_host(64).build()?,
            endpoint: endpoint.trim_end_matches('/').to_string(),
            validate_only,
            token: Mutex::new(None),
        })
    }

    pub fn project_id(&self) -> &str {
        &self.sa.project_id
    }

    fn assertion(&self) -> anyhow::Result<String> {
        let iat = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let claims = Claims { iss: &self.sa.client_email, scope: SCOPE, aud: &self.sa.token_uri, iat, exp: iat + 3600 };
        Ok(jsonwebtoken::encode(&jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256), &claims, &self.key)?)
    }

    /// Cached OAuth access token; one refresh at a time (the mutex is held while fetching).
    async fn access_token(&self, force: bool) -> Result<String, ProviderError> {
        let mut slot = self.token.lock().await;
        if !force {
            if let Some((t, valid_until)) = &*slot {
                if Instant::now() < *valid_until {
                    return Ok(t.clone());
                }
            }
        }
        let assertion = self.assertion().map_err(|e| ProviderError::Permanent(format!("can't sign FCM credentials: {e}")))?;
        let resp = self
            .http
            .post(&self.sa.token_uri)
            .form(&[("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"), ("assertion", assertion.as_str())])
            .send()
            .await
            .map_err(|e| ProviderError::Transient(format!("Google sign-in unreachable: {e}")))?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            counter!("fcm_oauth_total", "result" => "error").increment(1);
            let why = body["error_description"].as_str().or(body["error"].as_str()).unwrap_or("unknown error").to_string();
            return Err(if status.is_server_error() {
                ProviderError::Transient(format!("Google sign-in failed ({status}): {why}"))
            } else {
                ProviderError::Permanent(format!("Firebase credentials rejected ({status}): {why}"))
            });
        }
        let token = body["access_token"].as_str().ok_or(ProviderError::Transient("Google sign-in: no access_token".into()))?.to_string();
        let ttl = body["expires_in"].as_u64().unwrap_or(3600).saturating_sub(300).max(60);
        *slot = Some((token.clone(), Instant::now() + Duration::from_secs(ttl)));
        counter!("fcm_oauth_total", "result" => "ok").increment(1);
        Ok(token)
    }

    fn message(&self, env: &Envelope, content: &Content, token: &str) -> Value {
        let high = env.priority == Priority::High;
        let mut android = json!({ "priority": if high { "HIGH" } else { "NORMAL" } });
        if let Some(exp) = env.expires_at_ms {
            android["ttl"] = json!(format!("{}s", exp.saturating_sub(now_ms()) / 1000));
        }
        json!({
            "validate_only": self.validate_only,
            "message": {
                "token": token,
                "notification": { "title": content.title.clone().unwrap_or_default(), "body": content.body },
                // FCM data values must be strings.
                "data": {
                    "notification_id": env.id.to_string(),
                    "template": env.template,
                    "campaign_id": env.campaign_id.clone().unwrap_or_default(),
                },
                "android": android,
                "apns": { "headers": { "apns-priority": if high { "10" } else { "5" } } },
            }
        })
    }

    async fn send_one(&self, env: &Envelope, content: &Content, token: &str, bearer: &str) -> PerToken {
        let url = format!("{}/v1/projects/{}/messages:send", self.endpoint, self.sa.project_id);
        match self.http.post(&url).bearer_auth(bearer).json(&self.message(env, content, token)).send().await {
            Err(e) if e.is_timeout() => PerToken::Transient("FCM timed out".into()),
            Err(e) => PerToken::Transient(format!("FCM unreachable: {e}")),
            Ok(r) if r.status().is_success() => PerToken::Ok,
            Ok(r) => {
                let status = r.status().as_u16();
                let body = r.text().await.unwrap_or_default();
                classify(status, &body)
            }
        }
    }
}

/// Maps an FCM v1 error response to what we do about it.
fn classify(status: u16, body: &str) -> PerToken {
    let v: Value = serde_json::from_str(body).unwrap_or(Value::Null);
    let message = v["error"]["message"].as_str().unwrap_or("").to_string();
    let code = v["error"]["details"]
        .as_array()
        .and_then(|d| d.iter().find_map(|x| x["errorCode"].as_str()))
        .or(v["error"]["status"].as_str())
        .unwrap_or("")
        .to_string();
    match code.as_str() {
        "UNREGISTERED" => PerToken::Dead("app uninstalled or token expired (UNREGISTERED)".into()),
        "SENDER_ID_MISMATCH" => PerToken::Dead("token belongs to a different Firebase project".into()),
        "INVALID_ARGUMENT" if message.to_lowercase().contains("registration token") => PerToken::Dead("not a valid FCM token".into()),
        "INVALID_ARGUMENT" => PerToken::Permanent(format!("FCM rejected the message: {message}")),
        "QUOTA_EXCEEDED" => PerToken::Transient("FCM quota exceeded".into()),
        "UNAVAILABLE" => PerToken::Transient("FCM unavailable".into()),
        "INTERNAL" => PerToken::Transient("FCM internal error".into()),
        "THIRD_PARTY_AUTH_ERROR" => PerToken::Permanent("iOS push not set up: upload the APNs key in Firebase".into()),
        "UNAUTHENTICATED" => PerToken::Unauthenticated,
        "PERMISSION_DENIED" => PerToken::Permanent("service account lacks the Firebase Cloud Messaging permission".into()),
        _ if status == 401 => PerToken::Unauthenticated,
        _ if status == 429 || status >= 500 => PerToken::Transient(format!("FCM error {status}")),
        _ => PerToken::Permanent(format!("FCM error {status}: {message}")),
    }
}

#[async_trait]
impl Provider for Fcm {
    fn name(&self) -> &'static str {
        "fcm"
    }

    async fn send(&self, env: &Envelope, content: &Content) -> SendResult {
        let tokens: Vec<String> = env.recipient.as_ref().map(|r| r.push_tokens.clone()).unwrap_or_default();
        if tokens.is_empty() {
            return SendResult::err(ProviderError::Permanent("no push token: the user has no registered device".into()));
        }
        let mut bearer = match self.access_token(false).await {
            Ok(t) => t,
            Err(e) => return SendResult::err(e),
        };
        let mut results: Vec<PerToken> =
            futures::future::join_all(tokens.iter().map(|t| self.send_one(env, content, t, &bearer))).await;
        // Our access token was revoked or expired early: refresh once and retry those.
        if results.contains(&PerToken::Unauthenticated) {
            bearer = match self.access_token(true).await {
                Ok(t) => t,
                Err(e) => return SendResult::err(e),
            };
            for (i, t) in tokens.iter().enumerate() {
                if results[i] == PerToken::Unauthenticated {
                    results[i] = self.send_one(env, content, t, &bearer).await;
                }
            }
        }
        let mut dead = Vec::new();
        let (mut ok, mut transient, mut permanent) = (false, None, None);
        for (t, r) in tokens.iter().zip(results) {
            match r {
                PerToken::Ok => ok = true,
                PerToken::Dead(why) => {
                    dead.push(t.clone());
                    permanent.get_or_insert(why);
                }
                PerToken::Transient(why) => {
                    transient.get_or_insert(why);
                }
                PerToken::Permanent(why) => {
                    permanent.get_or_insert(why);
                }
                PerToken::Unauthenticated => {
                    transient.get_or_insert("FCM rejected our access token".to_string());
                }
            }
        }
        counter!("fcm_dead_tokens_total").increment(dead.len() as u64);
        // Delivered to at least one device = sent; otherwise retry if anything was temporary.
        let result = if ok {
            Ok(())
        } else if let Some(t) = transient {
            Err(ProviderError::Transient(t))
        } else {
            Err(ProviderError::Permanent(permanent.unwrap_or_else(|| "FCM rejected every device".into())))
        };
        SendResult { result, dead_tokens: dead }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY: &str = include_str!("../tests/fixtures/test_only_key.pem");
    const TEST_PUB: &str = include_str!("../tests/fixtures/test_only_pub.pem");

    fn fcm() -> Fcm {
        let sa = ServiceAccount {
            project_id: "demo-project".into(),
            client_email: "push@demo-project.iam.gserviceaccount.com".into(),
            private_key: TEST_KEY.into(),
            token_uri: "https://oauth2.googleapis.com/token".into(),
        };
        Fcm::new(sa, "https://fcm.googleapis.com".into(), false).unwrap()
    }

    #[test]
    fn signs_a_google_oauth_assertion() {
        let jwt = fcm().assertion().unwrap();
        let mut v = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::RS256);
        v.set_audience(&["https://oauth2.googleapis.com/token"]);
        let data = jsonwebtoken::decode::<Value>(&jwt, &jsonwebtoken::DecodingKey::from_rsa_pem(TEST_PUB.as_bytes()).unwrap(), &v).unwrap();
        assert_eq!(data.claims["iss"], "push@demo-project.iam.gserviceaccount.com");
        assert_eq!(data.claims["scope"], SCOPE);
        assert_eq!(data.claims["exp"].as_u64().unwrap() - data.claims["iat"].as_u64().unwrap(), 3600);
    }

    #[test]
    fn rejects_a_bad_private_key() {
        let sa = ServiceAccount { project_id: "p".into(), client_email: "e".into(), private_key: "nope".into(), token_uri: default_token_uri() };
        assert!(Fcm::new(sa, "https://fcm.googleapis.com".into(), false).is_err());
    }

    fn fcm_err(status: u16, code: &str, msg: &str) -> PerToken {
        let body = json!({"error": {"code": status, "message": msg, "status": "X",
            "details": [{"@type": "type.googleapis.com/google.firebase.fcm.v1.FcmError", "errorCode": code}]}});
        classify(status, &body.to_string())
    }

    #[test]
    fn classifies_fcm_errors() {
        assert!(matches!(fcm_err(404, "UNREGISTERED", "Requested entity was not found."), PerToken::Dead(_)));
        assert!(matches!(fcm_err(403, "SENDER_ID_MISMATCH", ""), PerToken::Dead(_)));
        assert!(matches!(fcm_err(400, "INVALID_ARGUMENT", "The registration token is not a valid FCM registration token"), PerToken::Dead(_)));
        assert!(matches!(fcm_err(400, "INVALID_ARGUMENT", "Invalid JSON payload"), PerToken::Permanent(_)));
        assert!(matches!(fcm_err(429, "QUOTA_EXCEEDED", ""), PerToken::Transient(_)));
        assert!(matches!(fcm_err(503, "UNAVAILABLE", ""), PerToken::Transient(_)));
        assert!(matches!(fcm_err(401, "THIRD_PARTY_AUTH_ERROR", ""), PerToken::Permanent(_)));
        assert_eq!(classify(401, r#"{"error":{"code":401,"status":"UNAUTHENTICATED"}}"#), PerToken::Unauthenticated);
        assert!(matches!(classify(502, "<html>bad gateway</html>"), PerToken::Transient(_)));
    }

    #[test]
    fn builds_the_v1_message() {
        let mut env: Envelope = serde_json::from_value(json!({
            "id": "01a1de0a-e621-7579-bcd6-b40519c2d3d1", "user_id": "u1", "channel": "push", "priority": "high",
            "template": "order_shipped", "data": {}, "created_at_ms": 0, "expires_at_ms": null, "attempt": 0, "not_before_ms": 0
        }))
        .unwrap();
        env.campaign_id = Some("c1".into());
        let m = fcm().message(&env, &Content { title: Some("Order shipped".into()), body: "On its way".into() }, "tok-1");
        assert_eq!(m["message"]["token"], "tok-1");
        assert_eq!(m["message"]["notification"]["title"], "Order shipped");
        assert_eq!(m["message"]["android"]["priority"], "HIGH");
        assert_eq!(m["message"]["apns"]["headers"]["apns-priority"], "10");
        assert_eq!(m["message"]["data"]["campaign_id"], "c1");
        assert_eq!(m["validate_only"], false);
    }
}
