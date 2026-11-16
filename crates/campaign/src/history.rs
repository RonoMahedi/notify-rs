//! Delivery history: who got what, when, through which provider, and why it failed.
//!
//! Workers publish a `DeliveryEvent` per outcome to `notify.events`. The writer here
//! batches them into the daily-partitioned `delivery_events` table and stores Kafka
//! offsets only after the insert commits (at-least-once; the views read the latest
//! event per notification, so a rare duplicate row is harmless).

use std::{collections::BTreeSet, time::Duration};

use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use common::{
    config::env_parse,
    kafka,
    model::{DeliveryEvent, EVENTS_TOPIC},
};
use metrics::{counter, gauge, histogram};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    message::Message,
    Offset, TopicPartitionList,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{error::*, App};

// ------------------------------------------------------------------ partitions

fn part_name(d: NaiveDate) -> String {
    format!("delivery_events_{}", d.format("%Y%m%d"))
}

async fn ensure_partition(db: &PgPool, d: NaiveDate) -> sqlx::Result<()> {
    let next = d.succ_opt().expect("date");
    // Names and bounds come from a NaiveDate, never from user input.
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} PARTITION OF delivery_events FOR VALUES FROM ('{} 00:00:00+00') TO ('{} 00:00:00+00')",
        part_name(d),
        d.format("%Y-%m-%d"),
        next.format("%Y-%m-%d")
    );
    sqlx::query(&sql).execute(db).await?;
    Ok(())
}

/// Keeps yesterday..+2 days ready and drops partitions past retention.
pub async fn maintain_partitions(db: &PgPool) -> anyhow::Result<()> {
    let today = Utc::now().date_naive();
    for offset in -1..=2 {
        ensure_partition(db, today + chrono::Duration::days(offset)).await?;
    }
    let keep_days: i64 = env_parse("HISTORY_RETENTION_DAYS", 7);
    let cutoff = today - chrono::Duration::days(keep_days);
    let parts: Vec<String> = sqlx::query_scalar(
        "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
         JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'delivery_events'",
    )
    .fetch_all(db)
    .await?;
    for name in parts {
        let Some(day) = name.strip_prefix("delivery_events_").and_then(|d| NaiveDate::parse_from_str(d, "%Y%m%d").ok()) else {
            continue;
        };
        if day < cutoff {
            sqlx::query(&format!("DROP TABLE IF EXISTS {}", part_name(day))).execute(db).await?;
            tracing::info!(partition = %name, "dropped expired history partition");
        }
    }
    Ok(())
}

// ------------------------------------------------------------------ writer

pub async fn run_writer(app: App) {
    if let Err(e) = maintain_partitions(&app.db).await {
        tracing::error!(err = %e, "history partition setup failed");
    }
    let db = app.db.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            if let Err(e) = maintain_partitions(&db).await {
                tracing::warn!(err = %e, "history partition maintenance failed");
            }
        }
    });

    let consumer: StreamConsumer<kafka::LagContext> = kafka::consumer("notify-history");
    if let Err(e) = consumer.subscribe(&[EVENTS_TOPIC]) {
        tracing::error!(err = %e, "history writer can't subscribe");
        return;
    }
    let max: usize = env_parse("HISTORY_BATCH", 5_000);
    tracing::info!("history writer running");
    loop {
        let mut events: Vec<DeliveryEvent> = Vec::with_capacity(max);
        let mut tpl = TopicPartitionList::new();
        let mut offsets: std::collections::HashMap<i32, i64> = Default::default();
        match consumer.recv().await {
            Ok(m) => {
                offsets.insert(m.partition(), m.offset());
                if let Some(Ok(e)) = m.payload().map(serde_json::from_slice::<DeliveryEvent>) {
                    events.push(e);
                }
            }
            Err(e) => {
                tracing::warn!(err = %e, "history recv");
                continue;
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while events.len() < max {
            match tokio::time::timeout_at(deadline, consumer.recv()).await {
                Ok(Ok(m)) => {
                    let o = offsets.entry(m.partition()).or_insert(m.offset());
                    *o = (*o).max(m.offset());
                    match m.payload().map(serde_json::from_slice::<DeliveryEvent>) {
                        Some(Ok(e)) => events.push(e),
                        _ => counter!("history_poison_events_total").increment(1),
                    }
                }
                Ok(Err(e)) => tracing::warn!(err = %e, "history recv"),
                Err(_) => break,
            }
        }
        let started = std::time::Instant::now();
        let mut attempt = 0u32;
        loop {
            match insert(&app.db, &events).await {
                Ok(()) => break,
                Err(e) => {
                    // 23514: no partition for these rows (e.g. writer was down past midnight).
                    if let sqlx::Error::Database(d) = &e {
                        if d.code().as_deref() == Some("23514") {
                            let days: BTreeSet<NaiveDate> = events.iter().map(|e| ts(e.ts_ms).date_naive()).collect();
                            for d in days {
                                let _ = ensure_partition(&app.db, d).await;
                            }
                            continue;
                        }
                    }
                    attempt += 1;
                    counter!("history_insert_errors_total").increment(1);
                    tracing::warn!(err = %e, attempt, "history insert failed, retrying");
                    tokio::time::sleep(Duration::from_millis((200u64 << attempt.min(5)).min(10_000))).await;
                }
            }
        }
        let dead: Vec<String> = events.iter().flat_map(|e| e.dead_tokens.iter().cloned()).collect();
        if !dead.is_empty() {
            match crate::contacts::remove_dead_tokens(&app, &dead).await {
                Ok(n) => {
                    counter!("push_tokens_removed_total").increment(n);
                    tracing::info!(removed = n, "removed dead push tokens");
                }
                Err(e) => tracing::warn!(err = %e, "dead token cleanup failed"),
            }
        }
        histogram!("history_insert_duration_seconds").record(started.elapsed().as_secs_f64());
        counter!("history_events_written_total").increment(events.len() as u64);
        gauge!("history_last_write_timestamp_seconds").set(Utc::now().timestamp() as f64);
        for (p, o) in offsets {
            let _ = tpl.add_partition_offset(EVENTS_TOPIC, p, Offset::Offset(o + 1));
        }
        if let Err(e) = consumer.store_offsets(&tpl) {
            tracing::warn!(err = %e, "history store offsets");
        }
    }
}

fn ts(ms: u64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(ms as i64).single().unwrap_or_else(Utc::now)
}

async fn insert(db: &PgPool, ev: &[DeliveryEvent]) -> sqlx::Result<()> {
    if ev.is_empty() {
        return Ok(());
    }
    let campaign: Vec<Option<Uuid>> = ev.iter().map(|e| e.campaign_id.as_deref().and_then(|c| c.parse().ok())).collect();
    sqlx::query(
        "INSERT INTO delivery_events (ts, notification_id, user_id, channel, priority, template, campaign_id,
                                      status, attempt, provider, recipient, error, latency_ms, title, body)
         SELECT * FROM UNNEST($1::timestamptz[], $2::uuid[], $3::text[], $4::text[], $5::text[], $6::text[], $7::uuid[],
                              $8::text[], $9::int[], $10::text[], $11::text[], $12::text[], $13::bigint[], $14::text[], $15::text[])",
    )
    .bind(ev.iter().map(|e| ts(e.ts_ms)).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.notification_id).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.user_id.clone()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.channel.as_str().to_string()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.priority.as_str().to_string()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.template.clone()).collect::<Vec<_>>())
    .bind(campaign)
    .bind(ev.iter().map(|e| e.status.as_str().to_string()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.attempt as i32).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.provider.clone()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.recipient.clone()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.error.clone()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.latency_ms as i64).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.title.clone()).collect::<Vec<_>>())
    .bind(ev.iter().map(|e| e.body.clone()).collect::<Vec<_>>())
    .execute(db)
    .await?;
    Ok(())
}

// ------------------------------------------------------------------ read API

#[derive(Serialize, sqlx::FromRow)]
pub struct Row {
    ts: DateTime<Utc>,
    notification_id: Uuid,
    user_id: String,
    channel: String,
    template: String,
    campaign_id: Option<Uuid>,
    campaign_name: Option<String>,
    status: String,
    attempt: i32,
    provider: Option<String>,
    recipient: Option<String>,
    error: Option<String>,
    latency_ms: i64,
    title: Option<String>,
    body: Option<String>,
}

#[derive(Deserialize)]
pub struct PersonQ {
    limit: Option<i64>,
}

/// GET /v1/contacts/{user_id}/notifications — latest state of each notification (30 days).
pub async fn for_person(State(app): State<App>, Path(user_id): Path<String>, Query(q): Query<PersonQ>) -> ApiResult<Json<Vec<Row>>> {
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT t.*, cp.name AS campaign_name FROM (
            SELECT DISTINCT ON (e.notification_id) e.ts, e.notification_id, e.user_id, e.channel, e.template, e.campaign_id,
                   e.status, e.attempt, e.provider, e.recipient, e.error, e.latency_ms, e.title, e.body
            FROM delivery_events e
            WHERE e.user_id = $1 AND e.ts > now() - interval '30 days'
            ORDER BY e.notification_id, e.ts DESC, e.attempt DESC
         ) t LEFT JOIN campaigns cp ON cp.id = t.campaign_id
         ORDER BY t.ts DESC LIMIT $2",
    )
    .bind(&user_id)
    .bind(q.limit.unwrap_or(100).clamp(1, 500))
    .fetch_all(&app.db)
    .await?;
    Ok(Json(rows))
}

/// GET /v1/notifications/{id}/events — every attempt of one notification.
pub async fn events(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<Json<Vec<Row>>> {
    let rows: Vec<Row> = sqlx::query_as(
        "SELECT e.ts, e.notification_id, e.user_id, e.channel, e.template, e.campaign_id, cp.name AS campaign_name,
                e.status, e.attempt, e.provider, e.recipient, e.error, e.latency_ms, e.title, e.body
         FROM delivery_events e LEFT JOIN campaigns cp ON cp.id = e.campaign_id
         WHERE e.notification_id = $1 ORDER BY e.ts, e.attempt",
    )
    .bind(id)
    .fetch_all(&app.db)
    .await?;
    Ok(Json(rows))
}

const STATUSES: [&str; 5] = ["sent", "retrying", "failed", "expired", "suppressed"];
const CHANNELS: [&str; 4] = ["push", "email", "sms", "in_app"];

#[derive(Deserialize)]
pub struct FeedQ {
    status: Option<String>,
    channel: Option<String>,
    campaign_id: Option<Uuid>,
    before: Option<DateTime<Utc>>,
    limit: Option<i64>,
}

fn filters(qb: &mut QueryBuilder<'_, Postgres>, status: &Option<String>, channel: &Option<String>, campaign: Option<Uuid>) -> ApiResult<()> {
    if let Some(s) = status.as_deref().filter(|s| !s.is_empty()) {
        if !STATUSES.contains(&s) {
            return Err(AppError::bad("status must be sent, retrying, failed, expired or suppressed"));
        }
        qb.push(" AND e.status = ").push_bind(s.to_string());
    }
    if let Some(c) = channel.as_deref().filter(|c| !c.is_empty()) {
        if !CHANNELS.contains(&c) {
            return Err(AppError::bad("channel must be push, email, sms or in_app"));
        }
        qb.push(" AND e.channel = ").push_bind(c.to_string());
    }
    if let Some(c) = campaign {
        qb.push(" AND e.campaign_id = ").push_bind(c);
    }
    Ok(())
}

/// GET /v1/deliveries — newest events first, filterable; page with `before` = last row's ts.
pub async fn feed(State(app): State<App>, Query(q): Query<FeedQ>) -> ApiResult<Json<Value>> {
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT e.ts, e.notification_id, e.user_id, e.channel, e.template, e.campaign_id, cp.name AS campaign_name,
                e.status, e.attempt, e.provider, e.recipient, e.error, e.latency_ms, e.title, e.body
         FROM delivery_events e LEFT JOIN campaigns cp ON cp.id = e.campaign_id
         WHERE e.ts > now() - interval '7 days'",
    );
    filters(&mut qb, &q.status, &q.channel, q.campaign_id)?;
    if let Some(b) = q.before {
        qb.push(" AND e.ts < ").push_bind(b);
    }
    qb.push(" ORDER BY e.ts DESC LIMIT ").push_bind(limit);
    let rows: Vec<Row> = qb.build_query_as().fetch_all(&app.db).await?;
    let next = (rows.len() as i64 == limit).then(|| rows.last().map(|r| r.ts)).flatten();
    Ok(Json(json!({ "items": rows, "next_before": next })))
}

#[derive(Deserialize)]
pub struct SummaryQ {
    minutes: Option<i32>,
    campaign_id: Option<Uuid>,
    channel: Option<String>,
}

/// GET /v1/deliveries/summary — counts by status and top failure reasons over a window.
pub async fn summary(State(app): State<App>, Query(q): Query<SummaryQ>) -> ApiResult<Json<Value>> {
    let minutes = q.minutes.unwrap_or(60).clamp(1, 1440);
    let mut tx = app.db.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '10s'").execute(&mut *tx).await?;

    let mut qb = QueryBuilder::<Postgres>::new("SELECT e.status, count(*) FROM delivery_events e WHERE e.ts > now() - make_interval(mins => ");
    qb.push_bind(minutes).push(")");
    filters(&mut qb, &None, &q.channel, q.campaign_id)?;
    qb.push(" GROUP BY e.status");
    let by_status: Vec<(String, i64)> = qb.build_query_as().fetch_all(&mut *tx).await?;

    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT e.error, e.status, count(*) FROM delivery_events e WHERE e.error IS NOT NULL AND e.status IN ('failed', 'retrying', 'expired')
         AND e.ts > now() - make_interval(mins => ",
    );
    qb.push_bind(minutes).push(")");
    filters(&mut qb, &None, &q.channel, q.campaign_id)?;
    qb.push(" GROUP BY e.error, e.status ORDER BY 3 DESC LIMIT 10");
    let reasons: Vec<(String, String, i64)> = qb.build_query_as().fetch_all(&mut *tx).await?;
    tx.commit().await?;

    Ok(Json(json!({
        "minutes": minutes,
        "by_status": by_status.into_iter().collect::<std::collections::HashMap<_, _>>(),
        "top_reasons": reasons.into_iter().map(|(error, status, count)| json!({"error": error, "status": status, "count": count})).collect::<Vec<_>>(),
    })))
}
