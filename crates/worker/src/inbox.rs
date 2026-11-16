//! In-app provider: stores the notification in the user's Redis inbox and publishes it for
//! live delivery by the WebSocket gateway (`crates/gateway`). Selected with
//! `IN_APP_PROVIDER=inbox`. Layout and guarantees: `common::inapp`.
//!
//! "Sent" means stored in the inbox, whether or not the user is online right now: an offline
//! user sees it on the next connect. Redis errors are transient (retry tiers).

use async_trait::async_trait;
use common::inapp::{self, InboxItem, LiveMessage};
use common::model::Envelope;
use metrics::counter;
use redis::aio::ConnectionManager;

use crate::provider::{Content, Provider, ProviderError, SendResult};

pub struct Inbox {
    redis: ConnectionManager,
    max_items: usize,
    ttl_secs: u64,
}

impl Inbox {
    pub fn new(redis: ConnectionManager, max_items: usize, ttl_secs: u64) -> Self {
        Inbox { redis, max_items: max_items.max(1), ttl_secs: ttl_secs.max(60) }
    }
}

pub fn item(env: &Envelope, content: &Content) -> InboxItem {
    InboxItem {
        id: env.id,
        template: env.template.clone(),
        title: content.title.clone(),
        body: content.body.clone(),
        data: env.data.clone(),
        campaign_id: env.campaign_id.clone(),
        created_at_ms: env.created_at_ms,
    }
}

#[async_trait]
impl Provider for Inbox {
    fn name(&self) -> &'static str {
        "inbox"
    }

    async fn send(&self, env: &Envelope, content: &Content) -> SendResult {
        let item = item(env, content);
        let (Ok(member), Ok(live)) =
            (serde_json::to_string(&item), serde_json::to_string(&LiveMessage { user_id: env.user_id.clone(), item: item.clone() }))
        else {
            return SendResult::err(ProviderError::Permanent("couldn't encode the in-app item".into()));
        };
        let key = inapp::inbox_key(&env.user_id);
        let ttl = self.ttl_secs as i64;
        let mut p = redis::pipe();
        p.zadd(&key, member, item.created_at_ms)
            .ignore()
            // Keep only the newest max_items.
            .zremrangebyrank(&key, 0, -(self.max_items as isize) - 1)
            .ignore()
            .expire(&key, ttl)
            .ignore()
            .expire(inapp::seen_key(&env.user_id), ttl)
            .ignore()
            .publish(inapp::CHANNEL, live)
            .ignore();
        let mut conn = self.redis.clone();
        let result =
            p.query_async::<()>(&mut conn).await.map_err(|e| ProviderError::Transient(format!("in-app inbox (Redis) unavailable: {e}")));
        counter!("inbox_writes_total", "result" => if result.is_ok() { "ok" } else { "error" }).increment(1);
        SendResult { result, dead_tokens: vec![] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::model::{Channel, Priority};

    #[test]
    fn item_from_envelope() {
        let env = Envelope {
            id: uuid::Uuid::nil(),
            user_id: "u1".into(),
            channel: Channel::InApp,
            priority: Priority::Normal,
            template: "promo".into(),
            data: serde_json::json!({"url": "/sale"}),
            created_at_ms: 42,
            expires_at_ms: None,
            attempt: 3,
            not_before_ms: 9,
            recipient: None,
            campaign_id: Some("c1".into()),
        };
        let i = item(&env, &Content { title: Some("Sale".into()), body: "50% off".into() });
        assert_eq!(
            serde_json::to_value(&i).unwrap(),
            serde_json::json!({"id": uuid::Uuid::nil(), "template": "promo", "title": "Sale", "body": "50% off",
                               "data": {"url": "/sale"}, "campaign_id": "c1", "created_at_ms": 42})
        );
    }
}
