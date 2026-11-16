use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Push,
    Email,
    Sms,
    InApp,
}

impl Channel {
    pub const ALL: [Channel; 4] = [Channel::Push, Channel::Email, Channel::Sms, Channel::InApp];
    pub fn as_str(&self) -> &'static str {
        match self {
            Channel::Push => "push",
            Channel::Email => "email",
            Channel::Sms => "sms",
            Channel::InApp => "in_app",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    /// OTPs, security alerts, transactional. Separate topic so campaigns never delay them.
    High,
    #[default]
    Normal,
    /// Marketing campaigns. Own topics + worker pool so a 10M-user blast never delays the others.
    Bulk,
}

impl Priority {
    pub const ALL: [Priority; 3] = [Priority::High, Priority::Normal, Priority::Bulk];
    pub fn as_str(&self) -> &'static str {
        match self {
            Priority::High => "high",
            Priority::Normal => "normal",
            Priority::Bulk => "bulk",
        }
    }
}

/// Resolved delivery addresses. Filled by the campaign service (which owns contact
/// data) or by callers that already know them.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Recipient {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phone: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub push_tokens: Vec<String>,
}

/// What clients POST to the ingest API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationRequest {
    #[serde(default)]
    pub idempotency_key: Option<String>,
    pub user_id: String,
    pub channel: Channel,
    #[serde(default)]
    pub priority: Priority,
    pub template: String,
    #[serde(default)]
    pub data: serde_json::Value,
    /// Drop the notification if it can't be delivered within this many seconds.
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient: Option<Recipient>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<String>,
}

pub const MAX_PAYLOAD_BYTES: usize = 4 * 1024;

impl NotificationRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.user_id.is_empty() || self.user_id.len() > 128 {
            return Err("user_id must be 1..=128 chars".into());
        }
        if self.template.is_empty() || self.template.len() > 128 {
            return Err("template must be 1..=128 chars".into());
        }
        if let Some(k) = &self.idempotency_key {
            if k.is_empty() || k.len() > 128 {
                return Err("idempotency_key must be 1..=128 chars".into());
            }
        }
        if let Some(r) = &self.recipient {
            if r.push_tokens.len() > 20 {
                return Err("at most 20 push_tokens".into());
            }
        }
        if self.campaign_id.as_ref().is_some_and(|c| c.len() > 64) {
            return Err("campaign_id must be <= 64 chars".into());
        }
        if self.data.to_string().len() > MAX_PAYLOAD_BYTES {
            return Err(format!("data exceeds {MAX_PAYLOAD_BYTES} bytes"));
        }
        Ok(())
    }
}

/// What travels through Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub id: Uuid,
    pub user_id: String,
    pub channel: Channel,
    pub priority: Priority,
    pub template: String,
    pub data: serde_json::Value,
    pub created_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    /// Number of delivery attempts already made.
    pub attempt: u32,
    /// Do not attempt delivery before this time (used by retry tiers).
    pub not_before_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient: Option<Recipient>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<String>,
}

impl Envelope {
    pub fn from_request(req: NotificationRequest) -> Self {
        let now = now_ms();
        Envelope {
            // v7 = time-ordered: good for DB clustering keys and debugging.
            id: Uuid::now_v7(),
            user_id: req.user_id,
            channel: req.channel,
            priority: req.priority,
            template: req.template,
            data: req.data,
            created_at_ms: now,
            expires_at_ms: req.ttl_seconds.map(|t| now + t * 1000),
            attempt: 0,
            not_before_ms: 0,
            recipient: req.recipient,
            campaign_id: req.campaign_id,
        }
    }

    pub fn is_expired(&self, now: u64) -> bool {
        self.expires_at_ms.is_some_and(|e| now > e)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Queued,
    Retrying,
    Sent,
    Failed,
    Expired,
    Suppressed,
}

impl Status {
    /// Terminal states: a redelivered message in one of these is skipped.
    pub fn is_terminal_str(s: &str) -> bool {
        matches!(s, "sent" | "failed" | "expired" | "suppressed")
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Status::Queued => "queued",
            Status::Retrying => "retrying",
            Status::Sent => "sent",
            Status::Failed => "failed",
            Status::Expired => "expired",
            Status::Suppressed => "suppressed",
        }
    }
}

pub fn main_topic(c: Channel, p: Priority) -> String {
    format!("notify.{}.{}", c.as_str(), p.as_str())
}

pub fn all_main_topics() -> Vec<String> {
    Channel::ALL.iter().flat_map(|c| Priority::ALL.iter().map(move |p| main_topic(*c, *p))).collect()
}

/// Tiered delay queues: attempt N goes to RETRY_TIERS[N].
pub const RETRY_TIERS: [(&str, u64); 3] = [("notify.retry.5s", 5_000), ("notify.retry.1m", 60_000), ("notify.retry.10m", 600_000)];
pub const DLQ_TOPIC: &str = "notify.dlq";

/// Longest a batch from these topics can hold its consumer: the biggest retry delay
/// among them plus its max jitter (delay / 5). 0 for main topics.
pub fn max_retry_delay_ms(topics: &[String]) -> u64 {
    RETRY_TIERS.iter().filter(|(t, _)| topics.iter().any(|x| x == t)).map(|(_, d)| d + d / 5).max().unwrap_or(0)
}
/// Delivery history: one event per processing outcome (sent, retrying, failed, expired, suppressed).
pub const EVENTS_TOPIC: &str = "notify.events";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryEvent {
    pub ts_ms: u64,
    pub notification_id: Uuid,
    pub user_id: String,
    pub channel: Channel,
    pub priority: Priority,
    pub template: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub campaign_id: Option<String>,
    pub status: Status,
    /// 0 = first try.
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// The address actually used (email, phone, push token).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Time from API acceptance to this event.
    pub latency_ms: u64,
    /// The rendered message (None for hidden templates or when not rendered).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// Push tokens the provider reported as gone; the history writer deletes them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dead_tokens: Vec<String>,
}

impl Envelope {
    /// The address this channel delivers to, when known.
    pub fn address(&self) -> Option<String> {
        let r = self.recipient.as_ref()?;
        match self.channel {
            Channel::Email => r.email.clone(),
            Channel::Sms => r.phone.clone(),
            Channel::Push => r.push_tokens.first().cloned(),
            Channel::InApp => None,
        }
    }
}

/// Idempotency-key retention at the API.
pub const DEDUP_TTL_SECS: u64 = 24 * 3600;

/// Status keys double as the worker's dedup marker, so this must exceed the
/// longest redelivery window (retry tiers sum to ~11 min). Hot status only:
/// long-term history belongs in ScyllaDB (see ARCHITECTURE.md).
pub fn status_ttl_secs() -> u64 {
    crate::config::env_parse("STATUS_TTL_SECS", 24 * 3600)
}

pub fn status_key(id: &Uuid) -> String {
    format!("st:{id}")
}
pub fn idem_key(k: &str) -> String {
    format!("idem:{k}")
}
/// Redis SET of a user's push tokens (mirror of the devices table, read by workers).
pub fn devices_key(user_id: &str) -> String {
    format!("devices:{user_id}")
}

pub fn optout_key(user_id: &str) -> String {
    format!("optout:{user_id}")
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> NotificationRequest {
        NotificationRequest {
            idempotency_key: None,
            user_id: "u1".into(),
            channel: Channel::Push,
            priority: Priority::Normal,
            template: "welcome".into(),
            data: serde_json::json!({"name": "A"}),
            ttl_seconds: Some(60),
            recipient: None,
            campaign_id: None,
        }
    }

    #[test]
    fn topics() {
        assert_eq!(main_topic(Channel::InApp, Priority::High), "notify.in_app.high");
        assert_eq!(main_topic(Channel::Email, Priority::Bulk), "notify.email.bulk");
        assert_eq!(all_main_topics().len(), 12);
    }

    #[test]
    fn validation() {
        assert!(req().validate().is_ok());
        let mut r = req();
        r.user_id.clear();
        assert!(r.validate().is_err());
        let mut r = req();
        r.data = serde_json::json!({"x": "y".repeat(MAX_PAYLOAD_BYTES)});
        assert!(r.validate().is_err());
    }

    #[test]
    fn expiry() {
        let e = Envelope::from_request(req());
        assert!(!e.is_expired(e.created_at_ms));
        assert!(e.is_expired(e.created_at_ms + 61_000));
    }

    #[test]
    fn terminal_states() {
        assert!(Status::is_terminal_str(Status::Sent.as_str()));
        assert!(Status::is_terminal_str(Status::Failed.as_str()));
        assert!(!Status::is_terminal_str(Status::Queued.as_str()));
        assert!(!Status::is_terminal_str(Status::Retrying.as_str()));
    }

    #[test]
    fn retry_delay_bound() {
        let t = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(max_retry_delay_ms(&t(&["notify.push.high"])), 0);
        assert_eq!(max_retry_delay_ms(&t(&["notify.retry.5s"])), 6_000);
        assert_eq!(max_retry_delay_ms(&t(&["notify.retry.5s", "notify.retry.10m"])), 720_000);
    }

    #[test]
    fn priority_defaults_to_normal() {
        let r: NotificationRequest = serde_json::from_str(r#"{"user_id":"u","channel":"sms","template":"otp"}"#).unwrap();
        assert_eq!(r.priority, Priority::Normal);
    }
}
