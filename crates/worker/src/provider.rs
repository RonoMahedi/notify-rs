//! Channel providers. `MockProvider` simulates latency and failures so the
//! whole pipeline and the dashboard can be exercised locally. Replace with real
//! adapters (FCM HTTP v1 / APNs HTTP/2, SES/SendGrid, Twilio, WebSocket gateway).

use std::time::Duration;

use async_trait::async_trait;
use common::model::Envelope;
use rand::Rng;

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// Timeouts, 5xx, 429: worth retrying.
    #[error("transient: {0}")]
    Transient(String),
    /// Invalid token, unsubscribed, bad address: never retry.
    #[error("permanent: {0}")]
    Permanent(String),
}

/// The rendered message for one recipient.
#[derive(Debug, Clone)]
pub struct Content {
    /// Email subject / push title.
    pub title: Option<String>,
    pub body: String,
}

/// What a provider reports back: the outcome, plus device tokens that no longer exist.
pub struct SendResult {
    pub result: Result<(), ProviderError>,
    pub dead_tokens: Vec<String>,
}

impl SendResult {
    pub fn ok() -> Self {
        SendResult { result: Ok(()), dead_tokens: vec![] }
    }
    pub fn err(e: ProviderError) -> Self {
        SendResult { result: Err(e), dead_tokens: vec![] }
    }
}

#[async_trait]
pub trait Provider: Send + Sync {
    fn name(&self) -> &'static str;
    async fn send(&self, env: &Envelope, content: &Content) -> SendResult;
}

pub struct MockProvider {
    pub name: &'static str,
    pub base_latency_ms: u64,
    pub jitter_ms: u64,
    pub transient_rate: f64,
    pub permanent_rate: f64,
}

#[async_trait]
impl Provider for MockProvider {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn send(&self, _env: &Envelope, _content: &Content) -> SendResult {
        let (delay, roll) = {
            let mut rng = rand::rng();
            (
                self.base_latency_ms + rng.random_range(0..=self.jitter_ms),
                rng.random::<f64>(),
            )
        };
        tokio::time::sleep(Duration::from_millis(delay)).await;
        if roll < self.permanent_rate {
            SendResult::err(ProviderError::Permanent("invalid recipient".into()))
        } else if roll < self.permanent_rate + self.transient_rate {
            SendResult::err(ProviderError::Transient("upstream 503".into()))
        } else {
            SendResult::ok()
        }
    }
}
