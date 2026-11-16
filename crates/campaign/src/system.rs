//! System view for the admin UI: Kafka topics and consumer groups, live traffic
//! from Prometheus, service health, Redis and Postgres, and a dead-letter viewer.
//! Read-only: nothing here changes Kafka or the data.

use std::{collections::HashMap, sync::Arc, time::Duration};

use axum::{
    extract::{Query, State},
    Json,
};
use common::{kafka, model::Envelope};
use rdkafka::{
    consumer::{BaseConsumer, Consumer},
    message::Message,
    ClientConfig, Offset, TopicPartitionList,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{error::*, App};

const TIMEOUT: Duration = Duration::from_secs(5);

/// Shared, cached (2 s) so many open admin tabs don't hammer Kafka.
#[derive(Clone)]
pub struct Inspector {
    consumer: Arc<BaseConsumer>,
    cache: Arc<Mutex<Option<(std::time::Instant, Value)>>>,
}

impl Inspector {
    pub fn new() -> Self {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", kafka::brokers())
            .set("group.id", "notify-admin-inspect") // never subscribes or commits
            .set("enable.auto.commit", "false")
            .create()
            .expect("kafka inspector");
        Inspector { consumer: Arc::new(consumer), cache: Arc::new(Mutex::new(None)) }
    }
}

/// Which consumer group reads a topic (naming convention of this deployment).
fn reader_of(topic: &str) -> Option<&'static str> {
    if topic == common::model::EVENTS_TOPIC {
        Some("notify-history")
    } else if topic == "notify.retry.5s" {
        Some("notify-workers-retry")
    } else if topic == "notify.retry.1m" {
        Some("notify-workers-retry-1m")
    } else if topic == "notify.retry.10m" {
        Some("notify-workers-retry-10m")
    } else if topic.ends_with(".high") {
        Some("notify-workers-high")
    } else if topic.ends_with(".bulk") {
        Some("notify-workers-bulk")
    } else if topic.ends_with(".normal") {
        Some("notify-workers")
    } else {
        None // notify.dlq is kept for inspection, not consumed
    }
}

fn kafka_snapshot(c: &BaseConsumer) -> anyhow::Result<Value> {
    let md = c.fetch_metadata(None, TIMEOUT)?;
    let mut topics = Vec::new();
    for t in md.topics() {
        let name = t.name();
        if name.starts_with('_') {
            continue; // internal topics
        }
        let (mut stored, mut produced) = (0i64, 0i64);
        for p in t.partitions() {
            let (lo, hi) = c.fetch_watermarks(name, p.id(), TIMEOUT)?;
            stored += (hi - lo).max(0);
            produced += hi.max(0);
        }
        topics.push(json!({
            "name": name, "partitions": t.partitions().len(),
            "stored": stored, "produced": produced, "reader": reader_of(name),
        }));
    }
    topics.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    let groups: Vec<Value> = c
        .fetch_group_list(None, TIMEOUT)?
        .groups()
        .iter()
        .filter(|g| g.name() != "notify-admin-inspect" && !g.name().starts_with("notify-dlq-peek"))
        .map(|g| json!({ "name": g.name(), "state": g.state(), "members": g.members().len() }))
        .collect();
    Ok(json!({ "brokers": md.brokers().len(), "topics": topics, "groups": groups }))
}

#[derive(Deserialize)]
struct PromResp {
    data: PromData,
}
#[derive(Deserialize)]
struct PromData {
    result: Vec<PromSample>,
}
#[derive(Deserialize)]
struct PromSample {
    metric: HashMap<String, String>,
    value: (f64, String),
}

async fn prom(app: &App, q: &str) -> anyhow::Result<Vec<(HashMap<String, String>, f64)>> {
    let r: PromResp = app
        .http
        .get(format!("{}/api/v1/query", app.prometheus_url))
        .query(&[("query", q)])
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(r.data.result.into_iter().map(|s| (s.metric, s.value.1.parse().unwrap_or(f64::NAN))).collect())
}

async fn one(app: &App, q: &str) -> Value {
    match prom(app, q).await {
        Ok(v) => {
            let finite: Vec<f64> = v.iter().map(|(_, x)| *x).filter(|x| x.is_finite()).collect();
            if v.is_empty() && !q.contains("histogram_quantile") {
                json!(0.0) // a rate with no series = nothing happening
            } else if finite.is_empty() {
                Value::Null // a quantile with no samples is "unknown", not 0
            } else {
                json!((finite.iter().sum::<f64>() * 100.0).round() / 100.0)
            }
        }
        Err(_) => Value::Null,
    }
}

async fn metrics(app: &App) -> Value {
    let lag = prom(app, "sum by (topic) (kafka_consumer_lag)").await.unwrap_or_default();
    let providers = prom(app, "max by (channel, provider, mode) (provider_info)").await.unwrap_or_default();
    let services = prom(app, "up").await.unwrap_or_default();
    json!({
        "ingest_per_s": one(app, "sum(rate(notifications_accepted_total[1m]))").await,
        "sent_per_s": one(app, r#"sum(rate(notifications_processed_total{outcome="sent"}[1m]))"#).await,
        "failed_per_s": one(app, r#"sum(rate(notifications_processed_total{outcome="dlq"}[1m]))"#).await,
        "retry_per_s": one(app, r#"sum(rate(notifications_processed_total{outcome="retry"}[1m]))"#).await,
        "p99_high_s": one(app, r#"histogram_quantile(0.99, sum by (le) (rate(notification_e2e_latency_seconds_bucket{priority="high"}[5m])))"#).await,
        "p99_all_s": one(app, "histogram_quantile(0.99, sum by (le) (rate(notification_e2e_latency_seconds_bucket[5m])))").await,
        "lag_by_topic": lag.into_iter().map(|(m, v)| (m.get("topic").cloned().unwrap_or_default(), v)).collect::<HashMap<_, _>>(),
        "providers": providers.into_iter().map(|(m, _)| json!({
            "channel": m.get("channel"), "provider": m.get("provider"), "mode": m.get("mode")
        })).collect::<Vec<_>>(),
        "services": services.into_iter().map(|(m, v)| json!({
            "job": m.get("job"), "instance": m.get("instance"), "up": v >= 1.0
        })).collect::<Vec<_>>(),
    })
}

fn info_field(info: &str, key: &str) -> Option<String> {
    info.lines().find_map(|l| l.strip_prefix(&format!("{key}:")).map(|v| v.trim().to_string()))
}

async fn redis_info(app: &App) -> anyhow::Result<Value> {
    let mut r = app.redis.clone();
    let mem: String = redis::cmd("INFO").arg("memory").query_async(&mut r).await?;
    let stats: String = redis::cmd("INFO").arg("stats").query_async(&mut r).await?;
    let keys: i64 = redis::cmd("DBSIZE").query_async(&mut r).await?;
    Ok(json!({
        "used_memory": info_field(&mem, "used_memory_human"),
        "max_memory": info_field(&mem, "maxmemory_human"),
        "ops_per_s": info_field(&stats, "instantaneous_ops_per_sec").and_then(|v| v.parse::<i64>().ok()),
        "evicted_keys": info_field(&stats, "evicted_keys").and_then(|v| v.parse::<i64>().ok()),
        "keys": keys,
    }))
}

async fn pg_info(app: &App) -> anyhow::Result<Value> {
    let (db, hist, hist_rows, contacts): (i64, i64, f64, f32) = sqlx::query_as(
        "SELECT pg_database_size(current_database()),
                coalesce((SELECT sum(pg_total_relation_size(inhrelid)) FROM pg_inherits WHERE inhparent = 'delivery_events'::regclass), 0)::bigint,
                coalesce((SELECT sum(greatest(c.reltuples, 0)) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
                          WHERE i.inhparent = 'delivery_events'::regclass), 0)::float8,
                coalesce((SELECT reltuples FROM pg_class WHERE relname = 'contacts'), 0)",
    )
    .fetch_one(&app.db)
    .await?;
    Ok(json!({ "database_bytes": db, "history_bytes": hist, "history_rows_estimate": hist_rows as i64,
               "contacts_estimate": contacts.max(0.0) as i64 }))
}

fn part<T: serde::Serialize>(r: anyhow::Result<T>) -> Value {
    match r {
        Ok(v) => json!(v),
        Err(e) => json!({ "error": e.to_string() }),
    }
}

/// GET /v1/system
pub async fn overview(State(app): State<App>) -> ApiResult<Json<Value>> {
    let k = {
        let mut cache = app.inspector.cache.lock().await;
        match &*cache {
            Some((at, v)) if at.elapsed() < Duration::from_secs(2) => v.clone(),
            _ => {
                let c = app.inspector.consumer.clone();
                let v = part(tokio::task::spawn_blocking(move || kafka_snapshot(&c)).await.map_err(anyhow::Error::from).and_then(|r| r));
                *cache = Some((std::time::Instant::now(), v.clone()));
                v
            }
        }
    };
    let (m, r, p) = tokio::join!(metrics(&app), redis_info(&app), pg_info(&app));
    Ok(Json(json!({
        "at": chrono::Utc::now(),
        "kafka": k, "metrics": m, "redis": part(r), "postgres": part(p),
    })))
}

#[derive(Deserialize)]
pub struct DlqQ {
    limit: Option<i64>,
}

/// GET /v1/system/dlq — newest dead-lettered messages with their failure reason.
pub async fn dlq(State(app): State<App>, Query(q): Query<DlqQ>) -> ApiResult<Json<Value>> {
    let n = q.limit.unwrap_or(50).clamp(1, 200);
    let read = tokio::task::spawn_blocking(move || -> anyhow::Result<(i64, Vec<Envelope>)> {
        // A throwaway assign-only reader: no group membership, no commits.
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", kafka::brokers())
            .set("group.id", format!("notify-dlq-peek-{}", Uuid::new_v4()))
            .set("enable.auto.commit", "false")
            .create()?;
        let md = c.fetch_metadata(Some(common::model::DLQ_TOPIC), TIMEOUT)?;
        let mut tpl = TopicPartitionList::new();
        let (mut total, mut want) = (0i64, 0i64);
        for t in md.topics() {
            for p in t.partitions() {
                let (lo, hi) = c.fetch_watermarks(t.name(), p.id(), TIMEOUT)?;
                total += hi - lo;
                let start = lo.max(hi - n);
                want += hi - start;
                if hi > start {
                    tpl.add_partition_offset(t.name(), p.id(), Offset::Offset(start))?;
                }
            }
        }
        let mut out = Vec::new();
        if want > 0 {
            c.assign(&tpl)?;
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while (out.len() as i64) < want && std::time::Instant::now() < deadline {
                if let Some(Ok(m)) = c.poll(Duration::from_millis(200)) {
                    if let Some(Ok(e)) = m.payload().map(serde_json::from_slice::<Envelope>) {
                        out.push(e);
                    }
                }
            }
        }
        out.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms));
        out.truncate(n as usize);
        Ok((total, out))
    })
    .await
    .map_err(anyhow::Error::from)??;
    let (total, envs) = read;

    let ids: Vec<Uuid> = envs.iter().map(|e| e.id).collect();
    let reasons: Vec<(Uuid, Option<String>)> = sqlx::query_as(
        "SELECT DISTINCT ON (notification_id) notification_id, error FROM delivery_events
         WHERE notification_id = ANY($1) AND status = 'failed' ORDER BY notification_id, ts DESC",
    )
    .bind(&ids)
    .fetch_all(&app.db)
    .await?;
    let reasons: HashMap<Uuid, Option<String>> = reasons.into_iter().collect();
    let camp_ids: Vec<Uuid> = envs.iter().filter_map(|e| e.campaign_id.as_deref()?.parse().ok()).collect();
    let names: HashMap<Uuid, String> = sqlx::query_as::<_, (Uuid, String)>("SELECT id, name FROM campaigns WHERE id = ANY($1)")
        .bind(&camp_ids)
        .fetch_all(&app.db)
        .await?
        .into_iter()
        .collect();
    let items: Vec<Value> = envs
        .iter()
        .map(|e| {
            let cid: Option<Uuid> = e.campaign_id.as_deref().and_then(|c| c.parse().ok());
            json!({
                "notification_id": e.id, "user_id": e.user_id, "channel": e.channel, "priority": e.priority,
                "template": e.template, "attempts": e.attempt + 1, "address": e.address(),
                "campaign_name": cid.and_then(|c| names.get(&c).cloned()),
                "created_at": chrono::DateTime::from_timestamp_millis(e.created_at_ms as i64),
                "reason": reasons.get(&e.id).cloned().flatten(),
            })
        })
        .collect();
    Ok(Json(json!({ "total": total, "items": items })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_readers() {
        assert_eq!(reader_of("notify.push.high"), Some("notify-workers-high"));
        assert_eq!(reader_of("notify.email.normal"), Some("notify-workers"));
        assert_eq!(reader_of("notify.sms.bulk"), Some("notify-workers-bulk"));
        assert_eq!(reader_of("notify.retry.5s"), Some("notify-workers-retry"));
        assert_eq!(reader_of("notify.retry.1m"), Some("notify-workers-retry-1m"));
        assert_eq!(reader_of("notify.retry.10m"), Some("notify-workers-retry-10m"));
        assert_eq!(reader_of("notify.events"), Some("notify-history"));
        assert_eq!(reader_of("notify.dlq"), None);
    }

    #[test]
    fn redis_info_parse() {
        let info = "# Memory\r\nused_memory:123\r\nused_memory_human:1.5M\r\n";
        assert_eq!(info_field(info, "used_memory_human").as_deref(), Some("1.5M"));
        assert_eq!(info_field(info, "nope"), None);
    }
}
