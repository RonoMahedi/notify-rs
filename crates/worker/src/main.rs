//! Delivery worker. Consumes notification topics in micro-batches, delivers
//! concurrently, then commits offsets for the batch => at-least-once delivery,
//! made effectively-once by a `done:{id}` dedup marker in Redis.

mod fcm;
mod provider;
mod ses;
mod sslwireless;

use std::{
    collections::HashMap,
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{routing::get, Router};
use common::template::{self, Template};
use common::{
    config::{env_or, env_parse},
    kafka,
    model::*,
};
use futures::StreamExt;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use metrics::{counter, gauge, histogram};
use provider::{Content, MockProvider, Provider, ProviderError};
use rand::Rng;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::{Message, OwnedMessage},
    producer::{FutureProducer, FutureRecord},
    Offset, TopicPartitionList,
};
use redis::aio::ConnectionManager;

struct Ctx {
    producer: FutureProducer,
    redis: ConnectionManager,
    providers: HashMap<Channel, Arc<dyn Provider>>,
    limiters: HashMap<Channel, DefaultDirectRateLimiter>,
    provider_timeout: Duration,
    /// (channel, template key) -> template, cached briefly from Redis.
    templates: std::sync::Mutex<HashMap<(Channel, String), (Instant, Option<Arc<Template>>)>>,
}

const TEMPLATE_TTL: Duration = Duration::from_secs(30);
const MISSING_TEMPLATE_TTL: Duration = Duration::from_secs(5);

/// Templates for this batch: from the in-process cache, else one pipelined Redis read.
async fn load_templates(ctx: &Ctx, envs: &[Envelope]) -> HashMap<(Channel, String), Option<Arc<Template>>> {
    let mut out = HashMap::new();
    let mut need: Vec<(Channel, String)> = Vec::new();
    {
        let cache = ctx.templates.lock().unwrap();
        for e in envs {
            let k = (e.channel, e.template.clone());
            if out.contains_key(&k) || need.contains(&k) {
                continue;
            }
            match cache.get(&k) {
                Some((at, t)) if at.elapsed() < if t.is_some() { TEMPLATE_TTL } else { MISSING_TEMPLATE_TTL } => {
                    out.insert(k, t.clone());
                }
                _ => need.push(k),
            }
        }
    }
    if need.is_empty() {
        return out;
    }
    let mut pipe = redis::pipe();
    for (c, k) in &need {
        pipe.get(template::redis_key(c.as_str(), k));
    }
    let mut r = ctx.redis.clone();
    let mut n = 0;
    let vals: Vec<Option<String>> = loop {
        match pipe.query_async(&mut r).await {
            Ok(v) => break v,
            Err(e) => backoff(&mut n, "redis_templates", &e).await,
        }
    };
    let mut cache = ctx.templates.lock().unwrap();
    for (k, v) in need.into_iter().zip(vals) {
        let t = v.and_then(|s| serde_json::from_str::<Template>(&s).ok()).map(Arc::new);
        cache.insert(k.clone(), (Instant::now(), t.clone()));
        out.insert(k, t);
    }
    out
}

/// Token values: the message's data plus what the envelope knows about the person.
fn vars_for(env: &Envelope) -> serde_json::Value {
    let mut v = match &env.data {
        serde_json::Value::Object(m) => serde_json::Value::Object(m.clone()),
        _ => serde_json::json!({}),
    };
    let o = v.as_object_mut().expect("object");
    o.entry("user_id").or_insert_with(|| serde_json::json!(env.user_id));
    if let Some(r) = &env.recipient {
        if let Some(e) = &r.email {
            o.entry("email").or_insert_with(|| serde_json::json!(e));
        }
        if let Some(p) = &r.phone {
            o.entry("phone").or_insert_with(|| serde_json::json!(p));
        }
    }
    v
}

#[derive(Clone, Copy)]
enum Outcome {
    Sent,
    Retry,
    Dlq,
    Expired,
    Suppressed,
    Duplicate,
}

impl Outcome {
    fn as_str(&self) -> &'static str {
        match self {
            Outcome::Sent => "sent",
            Outcome::Retry => "retry",
            Outcome::Dlq => "dlq",
            Outcome::Expired => "expired",
            Outcome::Suppressed => "suppressed",
            Outcome::Duplicate => "duplicate",
        }
    }
}

async fn backoff(attempt: &mut u32, what: &str, err: &(dyn std::fmt::Display + Sync)) {
    *attempt += 1;
    let ms = (100u64 << (*attempt).min(6)).min(5_000);
    tracing::warn!(%err, what, retry_in_ms = ms, "infra error, retrying");
    counter!("worker_infra_errors_total", "op" => what.to_string()).increment(1);
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

/// Publishes to Kafka, retrying until it succeeds. Blocking the batch here is
/// intentional: it is backpressure, and we must not drop a retry/DLQ record.
async fn publish(ctx: &Ctx, topic: &str, env: &Envelope) {
    let payload = serde_json::to_vec(env).expect("serialize");
    let mut n = 0;
    loop {
        let rec = FutureRecord::to(topic).key(&env.user_id).payload(&payload);
        match ctx.producer.send(rec, Duration::from_secs(5)).await {
            Ok(_) => return,
            Err((e, _)) => backoff(&mut n, "publish", &e).await,
        }
    }
}

impl Outcome {
    fn status(&self) -> Option<Status> {
        match self {
            Outcome::Sent => Some(Status::Sent),
            Outcome::Retry => Some(Status::Retrying),
            Outcome::Dlq => Some(Status::Failed),
            Outcome::Expired => Some(Status::Expired),
            Outcome::Suppressed => Some(Status::Suppressed),
            Outcome::Duplicate => None,
        }
    }
}

/// Per-message facts fetched for the whole batch in ONE pipelined round trip.
struct Precheck {
    already_terminal: bool,
    opted_out: bool,
    /// The user's registered push tokens (only fetched for push without tokens).
    tokens: Vec<String>,
}

fn needs_tokens(e: &Envelope) -> bool {
    e.channel == Channel::Push && e.recipient.as_ref().is_none_or(|r| r.push_tokens.is_empty())
}

async fn precheck(ctx: &Ctx, envs: &[Envelope]) -> Vec<Precheck> {
    if envs.is_empty() {
        return vec![];
    }
    let mut pipe = redis::pipe();
    for e in envs {
        pipe.get(status_key(&e.id)).sismember(optout_key(&e.user_id), e.channel.as_str());
        if needs_tokens(e) {
            pipe.smembers(devices_key(&e.user_id));
        }
    }
    let mut r = ctx.redis.clone();
    let mut n = 0;
    // Fail closed: never send without knowing dedup/opt-out state.
    let vals: Vec<redis::Value> = loop {
        match pipe.query_async(&mut r).await {
            Ok(v) => break v,
            Err(e) => backoff(&mut n, "redis_precheck", &e).await,
        }
    };
    let mut it = vals.iter();
    envs.iter()
        .map(|e| {
            let st = it.next().expect("status");
            let opt = it.next().expect("optout");
            let tokens = if needs_tokens(e) {
                it.next().and_then(|v| redis::from_redis_value::<Vec<String>>(v).ok()).unwrap_or_default()
            } else {
                vec![]
            };
            Precheck {
                already_terminal: redis::from_redis_value::<Option<String>>(st).ok().flatten().is_some_and(|s| Status::is_terminal_str(&s)),
                opted_out: redis::from_redis_value::<bool>(opt).unwrap_or(false),
                tokens,
            }
        })
        .collect()
}

/// Writes every status from a batch in ONE pipelined round trip.
async fn write_statuses(ctx: &Ctx, results: &[(uuid::Uuid, Status)]) {
    if results.is_empty() {
        return;
    }
    let ttl = status_ttl_secs();
    let mut pipe = redis::pipe();
    for (id, st) in results {
        pipe.set_ex(status_key(id), st.as_str(), ttl).ignore();
    }
    let mut r = ctx.redis.clone();
    for attempt in 1..=3 {
        match pipe.query_async::<()>(&mut r).await {
            Ok(()) => return,
            Err(e) if attempt < 3 => {
                tracing::warn!(%e, attempt, "status write failed, retrying");
                tokio::time::sleep(Duration::from_millis(100 * attempt)).await;
            }
            Err(e) => {
                tracing::error!(%e, n = results.len(), "dropping status writes");
                counter!("worker_infra_errors_total", "op" => "status_write").increment(1);
            }
        }
    }
}

/// Outcome + what the history needs: why it failed, which provider, what was sent.
struct Delivered {
    outcome: Outcome,
    error: Option<String>,
    provider: Option<&'static str>,
    content: Option<Content>,
    hide_content: bool,
    dead_tokens: Vec<String>,
}

impl From<Outcome> for Delivered {
    fn from(outcome: Outcome) -> Self {
        let error = match outcome {
            Outcome::Expired => Some("expired before delivery (ttl)".to_string()),
            Outcome::Suppressed => Some("user unsubscribed from this channel".to_string()),
            _ => None,
        };
        Delivered { outcome, error, provider: None, content: None, hide_content: false, dead_tokens: vec![] }
    }
}

/// A message that can't be built (no template, bad template) goes to the DLQ with the reason.
async fn dead_letter(ctx: &Ctx, env: &Envelope, why: String) -> Delivered {
    tracing::info!(id = %env.id, reason = %why, "sending to DLQ");
    publish(ctx, DLQ_TOPIC, env).await;
    Delivered { outcome: Outcome::Dlq, error: Some(why), provider: None, content: None, hide_content: false, dead_tokens: vec![] }
}

async fn deliver(ctx: &Ctx, env: &Envelope, pre: Precheck, tpl: Option<Arc<Template>>) -> Delivered {
    if pre.already_terminal {
        return Outcome::Duplicate.into();
    }
    let now = now_ms();
    if env.is_expired(now) {
        return Outcome::Expired.into();
    }
    // Unsubscribes stop marketing (campaigns use the bulk lane). Transactional messages
    // (OTPs, order updates) are still delivered, or an opted-out user couldn't log in.
    if pre.opted_out && env.priority == Priority::Bulk {
        return Outcome::Suppressed.into();
    }

    let Some(tpl) = tpl else {
        return dead_letter(ctx, env, format!("no {} template named '{}'", env.channel.as_str(), env.template)).await;
    };
    let vars = vars_for(env);
    let content = match (tpl.title.as_deref().map(|t| template::render(t, &vars)).transpose(), template::render(&tpl.body, &vars)) {
        (Ok(title), Ok(body)) => Content { title: title.map(|t| t.text), body: body.text },
        (Err(e), _) | (_, Err(e)) => return dead_letter(ctx, env, format!("template '{}' is invalid: {e}", env.template)).await,
    };
    if env.not_before_ms > now {
        tokio::time::sleep(Duration::from_millis(env.not_before_ms - now)).await;
        // A retry can wait up to ~12 min: an OTP may have expired meanwhile.
        if env.is_expired(now_ms()) {
            return Outcome::Expired.into();
        }
    }

    let limiter = &ctx.limiters[&env.channel];
    if limiter.check().is_err() {
        counter!("worker_rate_limited_total", "channel" => env.channel.as_str()).increment(1);
        limiter.until_ready().await;
    }

    let provider = &ctx.providers[&env.channel];
    let started = Instant::now();
    let (result, dead_tokens) = match tokio::time::timeout(ctx.provider_timeout, provider.send(env, &content)).await {
        Ok(r) => (r.result, r.dead_tokens),
        Err(_) => (Err(ProviderError::Transient("timeout".into())), vec![]),
    };
    let label = match &result {
        Ok(_) => "ok",
        Err(ProviderError::Transient(_)) => "transient_error",
        Err(ProviderError::Permanent(_)) => "permanent_error",
    };
    histogram!("provider_request_duration_seconds", "provider" => provider.name(), "result" => label)
        .record(started.elapsed().as_secs_f64());

    let pname = Some(provider.name());
    match result {
        Ok(()) => {
            let e2e = now_ms().saturating_sub(env.created_at_ms) as f64 / 1000.0;
            histogram!("notification_e2e_latency_seconds",
                "channel" => env.channel.as_str(), "priority" => env.priority.as_str())
            .record(e2e);
            Delivered {
                outcome: Outcome::Sent,
                error: None,
                provider: pname,
                content: Some(content),
                hide_content: tpl.hide_content,
                dead_tokens,
            }
        }
        Err(ProviderError::Transient(reason)) if (env.attempt as usize) < RETRY_TIERS.len() => {
            let (topic, delay) = RETRY_TIERS[env.attempt as usize];
            let jitter = rand::rng().random_range(0..=delay / 5);
            let mut next = env.clone();
            next.attempt += 1;
            next.not_before_ms = now_ms() + delay + jitter;
            tracing::debug!(id = %env.id, %reason, topic, "scheduling retry");
            publish(ctx, topic, &next).await;
            counter!("notifications_retried_total", "channel" => env.channel.as_str(), "tier" => topic).increment(1);
            Delivered {
                outcome: Outcome::Retry,
                error: Some(format!("{reason} (will retry)")),
                provider: pname,
                content: Some(content),
                hide_content: tpl.hide_content,
                dead_tokens,
            }
        }
        Err(e) => {
            tracing::info!(id = %env.id, err = %e, attempt = env.attempt, "sending to DLQ");
            publish(ctx, DLQ_TOPIC, env).await;
            let why = match e {
                ProviderError::Transient(r) => format!("{r} (retries exhausted)"),
                ProviderError::Permanent(r) => r,
            };
            Delivered {
                outcome: Outcome::Dlq,
                error: Some(why),
                provider: pname,
                content: Some(content),
                hide_content: tpl.hide_content,
                dead_tokens,
            }
        }
    }
}

async fn build_ctx(redis: ConnectionManager) -> Ctx {
    let mock = |name, base, jitter| -> Arc<dyn Provider> {
        Arc::new(MockProvider {
            name,
            base_latency_ms: base,
            jitter_ms: jitter,
            transient_rate: env_parse("MOCK_TRANSIENT_RATE", 0.02),
            permanent_rate: env_parse("MOCK_PERMANENT_RATE", 0.005),
        })
    };
    // Real providers when configured, mocks otherwise. A broken credentials file is fatal:
    // silently falling back to a mock would "send" nothing while looking healthy.
    let push: Arc<dyn Provider> = match fcm::Fcm::from_env().expect("FCM configuration") {
        Some(f) => {
            let mode = if std::env::var("FCM_VALIDATE_ONLY").is_ok_and(|v| v == "true" || v == "1") { "validate_only" } else { "live" };
            tracing::info!(project = f.project_id(), mode, "push: Firebase Cloud Messaging");
            gauge!("provider_info", "channel" => "push", "provider" => "fcm", "mode" => mode).set(1.0);
            Arc::new(f)
        }
        None => {
            gauge!("provider_info", "channel" => "push", "provider" => "mock", "mode" => "mock").set(1.0);
            mock("fcm_apns", 20, 80)
        }
    };
    let sms: Arc<dyn Provider> = match std::env::var("SMS_PROVIDER").unwrap_or_default().trim() {
        "" | "mock" => {
            gauge!("provider_info", "channel" => "sms", "provider" => "mock (twilio)", "mode" => "mock").set(1.0);
            mock("twilio", 80, 300)
        }
        "sslwireless" => {
            let p = sslwireless::SslWireless::from_env().expect("SSL Wireless configuration").expect("SMS_PROVIDER=sslwireless");
            tracing::info!(bulk_sender_id = p.has_bulk_sid(), "sms: SSL Wireless");
            gauge!("provider_info", "channel" => "sms", "provider" => "sslwireless", "mode" => "live").set(1.0);
            Arc::new(p)
        }
        other => panic!("unknown SMS_PROVIDER {other:?}: use sslwireless, or leave it empty for the mock"),
    };
    let email: Arc<dyn Provider> = match std::env::var("EMAIL_PROVIDER").unwrap_or_default().trim() {
        "" | "mock" => {
            gauge!("provider_info", "channel" => "email", "provider" => "mock (ses)", "mode" => "mock").set(1.0);
            mock("ses", 40, 160)
        }
        "ses" => {
            let p = ses::Ses::from_env().await.expect("SES configuration").expect("EMAIL_PROVIDER=ses");
            tracing::info!(from = p.sender(), "email: Amazon SES");
            gauge!("provider_info", "channel" => "email", "provider" => "ses", "mode" => "live").set(1.0);
            Arc::new(p)
        }
        other => panic!("unknown EMAIL_PROVIDER {other:?}: use ses, or leave it empty for the mock"),
    };
    gauge!("provider_info", "channel" => "in_app", "provider" => "mock (ws_gateway)", "mode" => "mock").set(1.0);
    let providers: HashMap<Channel, Arc<dyn Provider>> =
        HashMap::from([(Channel::Push, push), (Channel::Email, email), (Channel::Sms, sms), (Channel::InApp, mock("ws_gateway", 2, 8))]);
    // Per-instance limits; the global limit is this x replica count (see ARCHITECTURE.md).
    let limiters = Channel::ALL
        .iter()
        .map(|c| {
            let key = format!("RATE_LIMIT_{}", c.as_str().to_uppercase());
            let rps: u32 = env_parse(&key, 5_000);
            (*c, RateLimiter::direct(Quota::per_second(NonZeroU32::new(rps.max(1)).unwrap())))
        })
        .collect();
    Ctx {
        producer: kafka::producer(),
        redis,
        providers,
        limiters,
        provider_timeout: Duration::from_millis(env_parse("PROVIDER_TIMEOUT_MS", 5_000)),
        templates: std::sync::Mutex::new(HashMap::new()),
    }
}

async fn run(ctx: Arc<Ctx>, consumer: StreamConsumer<kafka::LagContext>) {
    let max_batch: usize = env_parse("BATCH_SIZE", 500);
    let concurrency: usize = env_parse("CONCURRENCY", 1_000);
    let batch_wait = Duration::from_millis(env_parse("BATCH_WAIT_MS", 20));
    let max_inflight: usize = env_parse::<usize>("INFLIGHT_BATCHES", 4).max(1);
    let mut inflight: std::collections::VecDeque<tokio::task::JoinHandle<TopicPartitionList>> = std::collections::VecDeque::new();

    loop {
        let mut batch: Vec<OwnedMessage> = Vec::with_capacity(max_batch);
        // While waiting for traffic, keep storing offsets of batches that finish;
        // otherwise an idle consumer would hold completed work uncommitted (fake lag,
        // and reprocessing after a restart).
        enum Ev<M> {
            Msg(M),
            Done(Result<TopicPartitionList, tokio::task::JoinError>),
        }
        let ev = match inflight.front_mut() {
            None => Ev::Msg(consumer.recv().await),
            Some(front) => tokio::select! {
                m = consumer.recv() => Ev::Msg(m), // recv is cancellation-safe
                d = front => Ev::Done(d),
            },
        };
        match ev {
            Ev::Done(d) => {
                inflight.pop_front();
                store_batch_offsets(&consumer, d);
                gauge!("worker_inflight_batches").set(inflight.len() as f64);
                continue;
            }
            Ev::Msg(Ok(m)) => batch.push(m.detach()),
            Ev::Msg(Err(e)) => {
                tracing::warn!(%e, "kafka recv");
                continue;
            }
        }
        let deadline = tokio::time::Instant::now() + batch_wait;
        while batch.len() < max_batch {
            match tokio::time::timeout_at(deadline, consumer.recv()).await {
                Ok(Ok(m)) => batch.push(m.detach()),
                Ok(Err(e)) => tracing::warn!(%e, "kafka recv"),
                Err(_) => break,
            }
        }

        // Highest offset seen per partition; stored only once the whole batch is done.
        let mut offsets: HashMap<(String, i32), i64> = HashMap::new();
        let mut envs = Vec::with_capacity(batch.len());
        for m in &batch {
            let k = (m.topic().to_string(), m.partition());
            let e = offsets.entry(k).or_insert(m.offset());
            *e = (*e).max(m.offset());
            match m.payload().map(serde_json::from_slice::<Envelope>) {
                Some(Ok(env)) => envs.push(env),
                _ => counter!("worker_poison_messages_total", "topic" => m.topic().to_string()).increment(1),
            }
        }

        let mut tpl = TopicPartitionList::new();
        for ((topic, part), off) in offsets {
            let _ = tpl.add_partition_offset(&topic, part, Offset::Offset(off + 1));
        }

        // Several batches in flight so one slow message (e.g. a 0.5s SMS) doesn't stall
        // the consumer. Offsets are still stored strictly in batch order => at-least-once.
        let ctx2 = ctx.clone();
        inflight.push_back(tokio::spawn(async move {
            process_batch(ctx2, envs, concurrency).await;
            tpl
        }));
        gauge!("worker_inflight_batches").set(inflight.len() as f64);
        while inflight.len() >= max_inflight || inflight.front().is_some_and(|h| h.is_finished()) {
            let done = inflight.pop_front().unwrap();
            store_batch_offsets(&consumer, done.await);
        }
        gauge!("worker_inflight_batches").set(inflight.len() as f64);
    }
}

fn store_batch_offsets(consumer: &StreamConsumer<kafka::LagContext>, done: Result<TopicPartitionList, tokio::task::JoinError>) {
    match done {
        Ok(tpl) => {
            if let Err(e) = consumer.store_offsets(&tpl) {
                // Expected after a rebalance revokes the partition; the new owner reprocesses.
                tracing::warn!(%e, "store offsets");
            }
        }
        Err(e) => tracing::error!(%e, "batch task panicked; offsets not stored"),
    }
}

/// Delivery history events. Best effort: losing one never blocks or redoes a delivery.
async fn publish_events(ctx: &Ctx, done: &[(Envelope, Delivered)]) {
    let now = now_ms();
    let sends =
        done.iter().filter_map(|(env, d)| {
            let status = d.outcome.status()?; // duplicates aren't new history
            let ev = DeliveryEvent {
                ts_ms: now,
                notification_id: env.id,
                user_id: env.user_id.clone(),
                channel: env.channel,
                priority: env.priority,
                template: env.template.clone(),
                campaign_id: env.campaign_id.clone(),
                status,
                attempt: env.attempt,
                provider: d.provider.map(str::to_string),
                recipient: env.address(),
                error: d.error.clone(),
                latency_ms: now.saturating_sub(env.created_at_ms),
                title: if d.hide_content { None } else { d.content.as_ref().and_then(|c| c.title.clone()) },
                body: d.content.as_ref().map(|c| {
                    if d.hide_content {
                        "(hidden: this template is marked sensitive)".to_string()
                    } else {
                        c.body.chars().take(1000).collect()
                    }
                }),
                dead_tokens: d.dead_tokens.clone(),
            };
            let payload = serde_json::to_vec(&ev).ok()?;
            let key = ev.user_id;
            Some(async move {
                ctx.producer.send(FutureRecord::to(EVENTS_TOPIC).key(&key).payload(&payload), Duration::from_secs(5)).await.is_ok()
            })
        });
    let results = futures::future::join_all(sends).await;
    let failed = results.iter().filter(|ok| !**ok).count();
    counter!("worker_history_events_total", "result" => "ok").increment((results.len() - failed) as u64);
    if failed > 0 {
        counter!("worker_history_events_total", "result" => "dropped").increment(failed as u64);
        tracing::warn!(failed, "history events not published");
    }
}

async fn process_batch(ctx: Arc<Ctx>, envs: Vec<Envelope>, concurrency: usize) {
    let started = Instant::now();
    gauge!("worker_batch_size").set(envs.len() as f64);
    let pre = precheck(&ctx, &envs).await;
    let tpls = Arc::new(load_templates(&ctx, &envs).await);
    let done: Vec<(Envelope, Delivered)> = futures::stream::iter(envs.into_iter().zip(pre))
        .map(|(env, pre)| {
            let ctx = ctx.clone();
            let tpl = tpls.get(&(env.channel, env.template.clone())).cloned().flatten();
            let mut env = env;
            if !pre.tokens.is_empty() {
                env.recipient.get_or_insert_with(Default::default).push_tokens = pre.tokens.clone();
            }
            async move {
                let d = deliver(&ctx, &env, pre, tpl).await;
                let o = d.outcome;
                counter!("notifications_processed_total",
                    "channel" => env.channel.as_str(), "outcome" => o.as_str())
                .increment(1);
                if let Some(c) = &env.campaign_id {
                    counter!("campaign_notifications_total", "campaign" => c.clone(), "outcome" => o.as_str()).increment(1);
                }
                (env, d)
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    let statuses: Vec<(uuid::Uuid, Status)> = done.iter().filter_map(|(e, d)| d.outcome.status().map(|st| (e.id, st))).collect();
    write_statuses(&ctx, &statuses).await;
    publish_events(&ctx, &done).await;
    histogram!("worker_batch_duration_seconds").record(started.elapsed().as_secs_f64());
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common::telemetry::init_tracing();
    let handle = common::telemetry::init_metrics();

    let redis_client = redis::Client::open(env_or("REDIS_URL", "redis://127.0.0.1:16379"))?;
    let ctx = Arc::new(build_ctx(ConnectionManager::new(redis_client).await?).await);

    // WORKER_TOPICS lets you run dedicated pools (e.g. high priority, retry tiers).
    let topics: Vec<String> = match std::env::var("WORKER_TOPICS") {
        Ok(t) if !t.trim().is_empty() => t.split(',').map(|s| s.trim().to_string()).collect(),
        _ => all_main_topics(),
    };
    let group = env_or("WORKER_GROUP", "notify-workers");
    // Retry tiers sleep inside the batch, so the consumer can go that long without
    // calling recv(). Give it the delay plus the usual 5 minutes, or the broker evicts it.
    let retry_wait = max_retry_delay_ms(&topics);
    if RETRY_TIERS.iter().filter(|(t, _)| topics.iter().any(|x| x == t)).count() > 1 {
        tracing::warn!("several retry tiers in one worker: short retries wait behind long ones; run one pool per tier");
    }
    let poll_ms = env_parse("KAFKA_MAX_POLL_INTERVAL_MS", 300_000 + retry_wait);
    let consumer = kafka::consumer_with_poll_interval(&group, poll_ms);
    let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    consumer.subscribe(&refs)?;
    tracing::info!(?topics, %group, max_poll_interval_ms = poll_ms, "worker subscribed");

    let addr = env_or("METRICS_ADDR", "0.0.0.0:9100");
    let app = Router::new()
        .route(
            "/metrics",
            get(move || {
                let h = handle.clone();
                async move { h.render() }
            }),
        )
        .route("/healthz", get(|| async { "ok" }));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tokio::spawn(async move { axum::serve(listener, app).await });

    tokio::select! {
        _ = run(ctx, consumer) => {},
        // Dropping the consumer closes it, which commits offsets of finished batches.
        // Unfinished batches are redelivered to the next owner; the dedup marker skips
        // anything that was already sent.
        _ = common::shutdown::signal() => {},
    }
    Ok(())
}
