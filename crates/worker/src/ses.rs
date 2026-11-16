//! Amazon SES (API v2) email provider: `SendEmail` through the official AWS SDK.
//!
//! Selected with `EMAIL_PROVIDER=ses`. Settings:
//!   SES_FROM                 sender, e.g. `Brand <no-reply@example.com>` (a verified SES identity)
//!   SES_REGION               e.g. ap-south-1 (falls back to AWS_REGION)
//!   SES_CONFIGURATION_SET    optional: for bounce/complaint/delivery events
//!   SES_REPLY_TO             optional reply-to address
//!   SES_ENDPOINT             optional: custom endpoint (VPC endpoint, tests)
//!
//! Credentials come from the standard AWS chain: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY,
//! a profile (AWS_PROFILE + mounted credentials file), or an IAM role (ECS task role, EC2
//! instance profile, EKS IRSA). Prefer a role in production: no long-lived key to leak.
//!
//! The SDK's own retries are turned off: the worker's retry tiers handle retries without
//! blocking a partition (see docs/PROVIDERS.md).
//!
//! Errors:
//!   TooManyRequests, LimitExceeded, Throttling, HTTP 5xx, timeouts, network -> transient
//!   MessageRejected, BadRequest (e.g. bad address)                          -> permanent
//!   MailFromDomainNotVerified, NotFound (configuration set), AccountSuspended,
//!   SendingPaused, rejected credentials                                     -> permanent, says what to fix

use std::time::Duration;

use async_trait::async_trait;
use aws_config::{retry::RetryConfig, timeout::TimeoutConfig, BehaviorVersion, Region};
use aws_sdk_sesv2::{
    error::{ProvideErrorMetadata, SdkError},
    operation::send_email::SendEmailError,
    types::{Body, Content as SesContent, Destination, EmailContent, Message, MessageTag},
    Client,
};
use common::model::Envelope;
use metrics::counter;

use crate::provider::{Content, Provider, ProviderError, SendResult};

pub struct Ses {
    client: Client,
    from: String,
    reply_to: Option<String>,
    configuration_set: Option<String>,
}

fn var(k: &str) -> Option<String> {
    std::env::var(k).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

impl Ses {
    /// None = EMAIL_PROVIDER isn't `ses`. Err = selected but misconfigured (including no AWS
    /// credentials found), which stops the worker at startup instead of sending nothing.
    pub async fn from_env() -> anyhow::Result<Option<Self>> {
        if var("EMAIL_PROVIDER").as_deref() != Some("ses") {
            return Ok(None);
        }
        let from =
            var("SES_FROM").ok_or_else(|| anyhow::anyhow!("EMAIL_PROVIDER=ses needs SES_FROM, e.g. 'Brand <no-reply@example.com>'"))?;
        let region = var("SES_REGION")
            .or_else(|| var("AWS_REGION"))
            .ok_or_else(|| anyhow::anyhow!("EMAIL_PROVIDER=ses needs SES_REGION (the region where your SES identity is verified)"))?;
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(Region::new(region));
        if let Some(ep) = var("SES_ENDPOINT") {
            loader = loader.endpoint_url(ep);
        }
        let config = loader.load().await;
        let creds = config.credentials_provider().ok_or_else(|| anyhow::anyhow!("SES: no AWS credentials provider"))?;
        aws_sdk_sesv2::config::ProvideCredentials::provide_credentials(&creds).await.map_err(|e| {
            anyhow::anyhow!(
                "SES: no AWS credentials found ({e}). Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or give the worker an IAM role"
            )
        })?;
        Ok(Some(Self::new(&config, from, var("SES_REPLY_TO"), var("SES_CONFIGURATION_SET"))))
    }

    pub fn new(config: &aws_config::SdkConfig, from: String, reply_to: Option<String>, configuration_set: Option<String>) -> Self {
        let conf = aws_sdk_sesv2::config::Builder::from(config)
            .retry_config(RetryConfig::disabled())
            .timeout_config(TimeoutConfig::builder().operation_timeout(Duration::from_secs(10)).build())
            .build();
        Ses { client: Client::from_conf(conf), from, reply_to, configuration_set }
    }

    pub fn sender(&self) -> &str {
        &self.from
    }
}

/// SES tag values allow only letters, digits, `_ - . @`.
fn tag_value(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || "_-.@".contains(c) { c } else { '_' }).take(256).collect()
}

fn tags(env: &Envelope) -> Vec<MessageTag> {
    let mut t =
        vec![("notification_id", env.id.to_string()), ("template", env.template.clone()), ("priority", env.priority.as_str().into())];
    if let Some(c) = &env.campaign_id {
        t.push(("campaign_id", c.clone()));
    }
    t.into_iter().filter_map(|(k, v)| MessageTag::builder().name(k).value(tag_value(&v)).build().ok()).collect()
}

fn text(s: &str) -> Option<SesContent> {
    SesContent::builder().data(s).charset("UTF-8").build().ok()
}

/// Maps an SES / SDK error to retry (Transient) or give up (Permanent).
fn classify(err: &SdkError<SendEmailError>) -> ProviderError {
    use ProviderError::{Permanent, Transient};
    let svc = match err {
        SdkError::ServiceError(e) => e,
        SdkError::TimeoutError(_) => return Transient("SES timed out".into()),
        SdkError::DispatchFailure(e) => return Transient(format!("SES unreachable: {e:?}")),
        SdkError::ResponseError(_) => return Transient("SES sent an unreadable response".into()),
        SdkError::ConstructionFailure(e) => return Permanent(format!("couldn't build the SES request: {e:?}")),
        other => return Transient(format!("SES: {other}")),
    };
    let e = svc.err();
    let msg = e.message().unwrap_or("no details").to_string();
    match e {
        SendEmailError::TooManyRequestsException(_) | SendEmailError::LimitExceededException(_) => {
            Transient(format!("SES rate limit: {msg}. Lower RATE_LIMIT_EMAIL to your SES sending rate"))
        }
        SendEmailError::MessageRejected(_) => Permanent(format!("SES rejected the message: {msg}")),
        SendEmailError::BadRequestException(_) => Permanent(format!("SES: bad request: {msg}")),
        SendEmailError::MailFromDomainNotVerifiedException(_) => {
            Permanent(format!("SES: the MAIL FROM domain isn't verified: {msg}. Check the domain in the SES console"))
        }
        SendEmailError::NotFoundException(_) => Permanent(format!("SES: not found (check SES_CONFIGURATION_SET): {msg}")),
        SendEmailError::AccountSuspendedException(_) => Permanent(format!("SES account suspended: {msg}")),
        SendEmailError::SendingPausedException(_) => {
            Permanent(format!("SES sending is paused for this account or configuration set: {msg}"))
        }
        _ => {
            let code = e.code().unwrap_or("Unknown");
            let status = svc.raw().status().as_u16();
            if status >= 500 || code.contains("Throttl") {
                Transient(format!("SES {code} (HTTP {status}): {msg}"))
            } else if matches!(
                code,
                "UnrecognizedClientException"
                    | "InvalidClientTokenId"
                    | "SignatureDoesNotMatch"
                    | "AccessDeniedException"
                    | "ExpiredTokenException"
            ) {
                Permanent(format!(
                    "SES rejected our AWS credentials ({code}): {msg}. Check the key or the IAM role's ses:SendEmail permission"
                ))
            } else {
                Permanent(format!("SES {code} (HTTP {status}): {msg}"))
            }
        }
    }
}

#[async_trait]
impl Provider for Ses {
    fn name(&self) -> &'static str {
        "ses"
    }

    async fn send(&self, env: &Envelope, content: &Content) -> SendResult {
        let Some(to) = env.recipient.as_ref().and_then(|r| r.email.as_deref()).filter(|e| e.contains('@')) else {
            return SendResult::err(ProviderError::Permanent("no email address for this user".into()));
        };
        let Some(subject) = content.title.as_deref().filter(|s| !s.trim().is_empty()) else {
            return SendResult::err(ProviderError::Permanent(format!("email template '{}' has no subject", env.template)));
        };
        let (Some(subject), Some(body)) = (text(subject), text(&content.body)) else {
            return SendResult::err(ProviderError::Permanent("couldn't build the email content".into()));
        };
        let message = Message::builder().subject(subject).body(Body::builder().text(body).build()).build();
        let mut req = self
            .client
            .send_email()
            .from_email_address(&self.from)
            .destination(Destination::builder().to_addresses(to).build())
            .content(EmailContent::builder().simple(message).build())
            .set_email_tags(Some(tags(env)))
            .set_configuration_set_name(self.configuration_set.clone());
        if let Some(r) = &self.reply_to {
            req = req.reply_to_addresses(r);
        }
        let result = req.send().await.map(|_| ()).map_err(|e| classify(&e));
        let outcome = match &result {
            Ok(()) => "sent",
            Err(ProviderError::Transient(_)) => "transient",
            Err(ProviderError::Permanent(_)) => "permanent",
        };
        counter!("ses_requests_total", "result" => outcome).increment(1);
        SendResult { result, dead_tokens: vec![] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        http::{HeaderMap, StatusCode},
        routing::post,
        Router,
    };
    use common::model::{Channel, Priority, Recipient};
    use serde_json::{json, Value};
    use std::sync::{Arc, Mutex};

    fn env(email: Option<&str>) -> Envelope {
        Envelope {
            id: uuid::Uuid::from_u128(0x0190_0000_0000_7000_8000_0000_0000_0001),
            user_id: "u1".into(),
            channel: Channel::Email,
            priority: Priority::Bulk,
            template: "welcome mail!".into(),
            data: json!({}),
            created_at_ms: 0,
            expires_at_ms: None,
            attempt: 0,
            not_before_ms: 0,
            recipient: email.map(|e| Recipient { email: Some(e.into()), phone: None, push_tokens: vec![] }),
            campaign_id: Some("c-42".into()),
        }
    }

    fn content(subject: Option<&str>) -> Content {
        Content { title: subject.map(Into::into), body: "হ্যালো, welcome!".into() }
    }

    #[test]
    fn sanitizes_tag_values() {
        assert_eq!(tag_value("welcome mail!"), "welcome_mail_");
        assert_eq!(tag_value("a.b-c_d@e"), "a.b-c_d@e");
    }

    /// Fake SES v2 endpoint: answers by recipient, like the real API's error shapes.
    async fn fake_ses() -> (String, Arc<Mutex<Vec<(HeaderMap, Value)>>>) {
        let seen: Arc<Mutex<Vec<(HeaderMap, Value)>>> = Arc::default();
        let log = seen.clone();
        let app = Router::new().route(
            "/v2/email/outbound-emails",
            post(move |headers: HeaderMap, body: String| {
                let log = log.clone();
                async move {
                    let b: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                    let to = b["Destination"]["ToAddresses"][0].as_str().unwrap_or("").to_string();
                    log.lock().unwrap().push((headers, b));
                    let err = |status: u16, code: &str, msg: &str| {
                        (
                            StatusCode::from_u16(status).unwrap(),
                            [("x-amzn-errortype", code.to_string())],
                            json!({ "message": msg }).to_string(),
                        )
                    };
                    match to.split('@').next().unwrap_or("") {
                        "rejected" => err(400, "MessageRejected", "Email address is not verified."),
                        "busy" => err(429, "TooManyRequestsException", "Maximum sending rate exceeded."),
                        "down" => err(503, "ServiceUnavailable", "try later"),
                        "denied" => err(403, "AccessDeniedException", "not authorized to perform ses:SendEmail"),
                        _ => (StatusCode::OK, [("x-amzn-requestid", "r1".to_string())], json!({ "MessageId": "m-1" }).to_string()),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await });
        (format!("http://{addr}"), seen)
    }

    async fn provider(endpoint: &str) -> Ses {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new("ap-south-1"))
            .endpoint_url(endpoint)
            .credentials_provider(aws_sdk_sesv2::config::Credentials::new("AKIDTEST", "secret", None, None, "test"))
            .load()
            .await;
        Ses::new(&config, "Brand <no-reply@example.com>".into(), Some("help@example.com".into()), Some("events".into()))
    }

    #[tokio::test]
    async fn sends_and_classifies() {
        let (url, seen) = fake_ses().await;
        let p = provider(&url).await;
        let send = |to: Option<&'static str>, subject: Option<&'static str>| {
            let p = &p;
            async move { p.send(&env(to), &content(subject)).await.result }
        };

        assert!(send(Some("ok@example.com"), Some("Welcome")).await.is_ok());
        assert!(
            matches!(send(Some("rejected@example.com"), Some("Hi")).await, Err(ProviderError::Permanent(m)) if m.contains("not verified"))
        );
        assert!(matches!(send(Some("busy@example.com"), Some("Hi")).await, Err(ProviderError::Transient(_))));
        assert!(matches!(send(Some("down@example.com"), Some("Hi")).await, Err(ProviderError::Transient(_))));
        assert!(
            matches!(send(Some("denied@example.com"), Some("Hi")).await, Err(ProviderError::Permanent(m)) if m.contains("credentials"))
        );
        assert!(matches!(send(None, Some("Hi")).await, Err(ProviderError::Permanent(_))));
        assert!(matches!(send(Some("ok@example.com"), None).await, Err(ProviderError::Permanent(m)) if m.contains("no subject")));

        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 5, "no request without an address or subject, and no SDK-level retries");
        let (headers, b) = &seen[0];
        assert!(headers["authorization"].to_str().unwrap().starts_with("AWS4-HMAC-SHA256 Credential=AKIDTEST/"), "SigV4 signed");
        assert_eq!(b["FromEmailAddress"], "Brand <no-reply@example.com>");
        assert_eq!(b["ReplyToAddresses"], json!(["help@example.com"]));
        assert_eq!(b["ConfigurationSetName"], "events");
        assert_eq!(b["Content"]["Simple"]["Subject"]["Data"], "Welcome");
        assert_eq!(b["Content"]["Simple"]["Body"]["Text"]["Data"], "হ্যালো, welcome!");
        assert_eq!(b["Content"]["Simple"]["Body"]["Text"]["Charset"], "UTF-8");
        let tags: Vec<(String, String)> = b["EmailTags"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| (t["Name"].as_str().unwrap().into(), t["Value"].as_str().unwrap().into()))
            .collect();
        assert!(tags.contains(&("template".into(), "welcome_mail_".into())));
        assert!(tags.contains(&("campaign_id".into(), "c-42".into())));
        assert!(tags.contains(&("priority".into(), "bulk".into())));
    }

    #[tokio::test]
    async fn unreachable_is_transient() {
        let p = provider("http://127.0.0.1:1").await;
        assert!(matches!(p.send(&env(Some("a@example.com")), &content(Some("Hi"))).await.result, Err(ProviderError::Transient(_))));
    }
}
