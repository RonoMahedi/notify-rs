//! In-app inbox: what the worker stores and the WebSocket gateway serves.
//!
//! Redis layout (keys share a `{user}` hash tag, so they live on one Redis Cluster slot):
//!   inbox:{user}        ZSET  score = created_at_ms, member = InboxItem JSON (newest kept, capped)
//!   inbox:{user}:seen   STRING  read watermark (ms): items newer than this are unread
//!   PUBLISH inapp       {"user_id": ..., "item": InboxItem} for live delivery
//!
//! Re-adding the same item is a no-op (same score, same member), so an at-least-once resend
//! from the worker never duplicates an inbox entry. A live frame can repeat; clients dedupe by `id`.
//!
//! Users authenticate with a short-lived HS256 JWT `{sub: user_id, aud: "notify-inapp", exp}`
//! signed with INAPP_TOKEN_SECRET. Your backend mints it (or calls POST /v1/inapp/token).

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const CHANNEL: &str = "inapp";
pub const AUDIENCE: &str = "notify-inapp";
pub const DEFAULT_MAX_ITEMS: usize = 100;
pub const DEFAULT_TTL_SECS: u64 = 30 * 24 * 3600;
pub const MIN_SECRET_LEN: usize = 32;

pub fn inbox_key(user: &str) -> String {
    format!("inbox:{{{user}}}")
}

pub fn seen_key(user: &str) -> String {
    format!("inbox:{{{user}}}:seen")
}

/// One entry in a user's inbox. Field order is fixed, so the JSON is deterministic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboxItem {
    pub id: Uuid,
    pub template: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub body: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub data: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<String>,
    pub created_at_ms: u64,
}

/// What travels over Redis pub/sub from workers to gateways.
#[derive(Debug, Serialize, Deserialize)]
pub struct LiveMessage {
    pub user_id: String,
    pub item: InboxItem,
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    aud: String,
    exp: u64,
    iat: u64,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TokenError {
    #[error("INAPP_TOKEN_SECRET must be at least {MIN_SECRET_LEN} characters")]
    WeakSecret,
    #[error("invalid or expired token")]
    Invalid,
}

fn check_secret(secret: &str) -> Result<(), TokenError> {
    if secret.len() < MIN_SECRET_LEN {
        return Err(TokenError::WeakSecret);
    }
    Ok(())
}

/// A token for `user_id`, valid for `ttl_secs` from `now_secs`.
pub fn mint(secret: &str, user_id: &str, now_secs: u64, ttl_secs: u64) -> Result<String, TokenError> {
    check_secret(secret)?;
    let claims = Claims { sub: user_id.into(), aud: AUDIENCE.into(), iat: now_secs, exp: now_secs + ttl_secs };
    jsonwebtoken::encode(&Header::new(Algorithm::HS256), &claims, &EncodingKey::from_secret(secret.as_bytes()))
        .map_err(|_| TokenError::Invalid)
}

/// The user id in a valid token. Only HS256 is accepted (no `alg: none`, no key confusion).
pub fn verify(secret: &str, token: &str) -> Result<String, TokenError> {
    check_secret(secret)?;
    let mut v = Validation::new(Algorithm::HS256);
    v.set_audience(&[AUDIENCE]);
    v.set_required_spec_claims(&["exp", "aud", "sub"]);
    v.leeway = 30;
    let data = jsonwebtoken::decode::<Claims>(token, &DecodingKey::from_secret(secret.as_bytes()), &v).map_err(|_| TokenError::Invalid)?;
    if data.claims.sub.is_empty() || data.claims.sub.len() > 128 {
        return Err(TokenError::Invalid);
    }
    Ok(data.claims.sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";

    fn now() -> u64 {
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()
    }

    #[test]
    fn token_round_trip() {
        let t = mint(SECRET, "user-1", now(), 60).unwrap();
        assert_eq!(verify(SECRET, &t).unwrap(), "user-1");
    }

    #[test]
    fn rejects_bad_tokens() {
        let t = mint(SECRET, "user-1", now(), 60).unwrap();
        assert_eq!(verify("another-secret-another-secret-xx", &t), Err(TokenError::Invalid));
        let expired = mint(SECRET, "user-1", now() - 3600, 60).unwrap();
        assert_eq!(verify(SECRET, &expired), Err(TokenError::Invalid));
        assert_eq!(verify(SECRET, "not.a.token"), Err(TokenError::Invalid));
        // Right signature, wrong audience (e.g. a token minted for another service).
        let other = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &Claims { sub: "user-1".into(), aud: "other".into(), iat: now(), exp: now() + 60 },
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        assert_eq!(verify(SECRET, &other), Err(TokenError::Invalid));
        // Unsigned token.
        let none = format!("eyJhbGciOiJub25lIn0.{}.", "eyJzdWIiOiJ1c2VyLTEiLCJhdWQiOiJub3RpZnktaW5hcHAiLCJleHAiOjk5OTk5OTk5OTl9");
        assert_eq!(verify(SECRET, &none), Err(TokenError::Invalid));
    }

    #[test]
    fn refuses_short_secrets() {
        assert_eq!(mint("short", "u", now(), 60), Err(TokenError::WeakSecret));
        assert_eq!(verify("short", "x"), Err(TokenError::WeakSecret));
    }

    #[test]
    fn item_json_is_deterministic() {
        let item = InboxItem {
            id: Uuid::nil(),
            template: "welcome".into(),
            title: Some("Hi".into()),
            body: "Hello".into(),
            data: serde_json::json!({"z": 1, "a": 2}),
            campaign_id: None,
            created_at_ms: 5,
        };
        let a = serde_json::to_string(&item).unwrap();
        assert_eq!(a, serde_json::to_string(&item.clone()).unwrap());
        assert_eq!(
            a,
            r#"{"id":"00000000-0000-0000-0000-000000000000","template":"welcome","title":"Hi","body":"Hello","data":{"a":2,"z":1},"created_at_ms":5}"#
        );
    }

    #[test]
    fn keys_share_a_cluster_slot() {
        assert_eq!(inbox_key("u1"), "inbox:{u1}");
        assert_eq!(seen_key("u1"), "inbox:{u1}:seen");
    }
}
