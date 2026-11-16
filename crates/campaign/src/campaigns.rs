//! Campaigns: cohort + channel + template + schedule + speed.
//!
//! Runner guarantees:
//! - Resumable: the cursor (last user_id) is saved after every chunk, so pause /
//!   resume / crash all continue where they stopped.
//! - No double sends: idempotency key `camp:{campaign}:{user}` at the ingest API
//!   makes a replayed chunk harmless.
//! - Multi-instance safe: a lease (`locked_until`) is claimed with SKIP LOCKED and
//!   renewed per chunk; a crashed runner's campaign is picked up after the lease expires.
//! - Throttled: token bucket at `rate_per_sec`, on the `bulk` priority lane.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use common::model::{Channel, NotificationRequest, Priority, Recipient};
use governor::{Quota, RateLimiter};
use metrics::{counter, gauge, histogram};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{
    error::*,
    rules::{push_audience, reach_sql},
    segments, App,
};

const LEASE_SECS: i64 = 60;
const DEFAULT_TTL_SECS: i64 = 24 * 3600;

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct Campaign {
    pub id: Uuid,
    pub name: String,
    pub segment_id: Uuid,
    pub channel: String,
    pub template: String,
    pub data: Value,
    pub status: String,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub rate_per_sec: i32,
    pub frequency_cap: Option<i32>,
    pub ttl_seconds: Option<i64>,
    pub cursor: Option<String>,
    pub targeted: Option<i64>,
    pub enqueued: i64,
    pub suppressed: i64,
    pub last_error: Option<String>,
    pub locked_until: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Campaign {
    fn channel(&self) -> Channel {
        serde_json::from_value(json!(self.channel)).expect("db constraint")
    }
    fn view(&self) -> Value {
        let mut v = serde_json::to_value(self).unwrap();
        let processed = self.enqueued + self.suppressed;
        v["progress"] = match self.targeted {
            Some(t) if t > 0 => json!(((processed as f64 / t as f64) * 1000.0).round() / 10.0),
            Some(_) => json!(100.0),
            None => Value::Null,
        };
        v.as_object_mut().unwrap().remove("locked_until");
        v
    }
}

#[derive(Deserialize)]
pub struct CampaignIn {
    name: String,
    segment_id: Uuid,
    channel: Channel,
    template: String,
    #[serde(default)]
    data: Value,
    rate_per_sec: Option<i32>,
    frequency_cap: Option<i32>,
    ttl_seconds: Option<i64>,
    /// Start at this time. Omit (and leave `start` false) to create a draft.
    scheduled_at: Option<DateTime<Utc>>,
    #[serde(default)]
    start: bool,
}

pub async fn create(State(app): State<App>, Json(c): Json<CampaignIn>) -> ApiResult<(StatusCode, Json<Value>)> {
    if c.name.trim().is_empty() || c.name.len() > 200 {
        return Err(AppError::bad("name must be 1..=200 chars"));
    }
    if c.template.is_empty() || c.template.len() > 128 {
        return Err(AppError::bad("template must be 1..=128 chars"));
    }
    let rate = c.rate_per_sec.unwrap_or(2_000);
    if !(1..=50_000).contains(&rate) {
        return Err(AppError::bad("rate_per_sec must be 1..=50000"));
    }
    if c.frequency_cap.is_some_and(|f| f < 1) {
        return Err(AppError::bad("frequency_cap must be >= 1"));
    }
    if !(c.data.is_null() || c.data.is_object()) || c.data.to_string().len() > 3_000 {
        return Err(AppError::bad("data must be a JSON object under 3KB"));
    }
    segments::load(&app, c.segment_id).await?; // 404 if missing
    let has_template: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM templates WHERE channel = $1 AND key = $2)")
        .bind(c.channel.as_str())
        .bind(&c.template)
        .fetch_one(&app.db)
        .await?;
    if !has_template {
        return Err(AppError::bad(format!(
            "no {} template named '{}': create it under Templates first",
            c.channel.as_str(),
            c.template
        )));
    }
    let (status, at) = match (c.scheduled_at, c.start) {
        (Some(at), _) => ("scheduled", Some(at)),
        (None, true) => ("scheduled", Some(Utc::now())),
        (None, false) => ("draft", None),
    };
    let data = if c.data.is_null() { json!({}) } else { c.data };
    let row: Campaign = sqlx::query_as(
        "INSERT INTO campaigns (id, name, segment_id, channel, template, data, status, scheduled_at,
                                rate_per_sec, frequency_cap, ttl_seconds)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING *",
    )
    .bind(Uuid::now_v7())
    .bind(c.name.trim())
    .bind(c.segment_id)
    .bind(c.channel.as_str())
    .bind(&c.template)
    .bind(data)
    .bind(status)
    .bind(at)
    .bind(rate)
    .bind(c.frequency_cap)
    .bind(c.ttl_seconds.unwrap_or(DEFAULT_TTL_SECS))
    .fetch_one(&app.db)
    .await?;
    Ok((StatusCode::CREATED, Json(row.view())))
}

pub async fn list(State(app): State<App>) -> ApiResult<Json<Vec<Value>>> {
    let rows: Vec<Campaign> = sqlx::query_as("SELECT * FROM campaigns ORDER BY created_at DESC LIMIT 200")
        .fetch_all(&app.db)
        .await?;
    Ok(Json(rows.iter().map(Campaign::view).collect()))
}

pub async fn get(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<Json<Value>> {
    let c: Campaign = sqlx::query_as("SELECT * FROM campaigns WHERE id = $1").bind(id).fetch_one(&app.db).await?;
    Ok(Json(c.view()))
}

/// 404 if the campaign doesn't exist, else 409 naming its current state.
async fn invalid_transition(app: &App, id: Uuid, verb: &str) -> ApiResult<Json<Value>> {
    let st: Option<String> = sqlx::query_scalar("SELECT status FROM campaigns WHERE id = $1")
        .bind(id)
        .fetch_optional(&app.db)
        .await?;
    match st {
        None => Err(AppError::not_found("campaign not found")),
        Some(s) => Err(AppError::conflict(format!("cannot {verb} a campaign that is {s}"))),
    }
}

async fn transition(app: &App, id: Uuid, sql: &str, verb: &str) -> ApiResult<Json<Value>> {
    let row: Option<Campaign> = sqlx::query_as(sql).bind(id).fetch_optional(&app.db).await?;
    match row {
        Some(c) => Ok(Json(c.view())),
        None => invalid_transition(app, id, verb).await,
    }
}

#[derive(Deserialize, Default)]
pub struct StartIn {
    scheduled_at: Option<DateTime<Utc>>,
}

pub async fn start(
    State(app): State<App>,
    Path(id): Path<Uuid>,
    body: Option<Json<StartIn>>,
) -> ApiResult<Json<Value>> {
    let at = body.and_then(|b| b.0.scheduled_at).unwrap_or_else(Utc::now);
    let row: Option<Campaign> = sqlx::query_as(
        "UPDATE campaigns SET status = 'scheduled', scheduled_at = $2, updated_at = now()
         WHERE id = $1 AND status = 'draft' RETURNING *",
    )
    .bind(id)
    .bind(at)
    .fetch_optional(&app.db)
    .await?;
    match row {
        Some(c) => Ok(Json(c.view())),
        None => invalid_transition(&app, id, "start").await,
    }
}

pub async fn pause(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<Json<Value>> {
    transition(
        &app,
        id,
        "UPDATE campaigns SET status = 'paused', locked_until = NULL, updated_at = now()
         WHERE id = $1 AND status IN ('scheduled', 'running') RETURNING *",
        "pause",
    )
    .await
}

pub async fn resume(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<Json<Value>> {
    transition(
        &app,
        id,
        "UPDATE campaigns SET status = 'scheduled', scheduled_at = now(), last_error = NULL, updated_at = now()
         WHERE id = $1 AND status = 'paused' RETURNING *",
        "resume",
    )
    .await
}

pub async fn cancel(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<Json<Value>> {
    let res = transition(
        &app,
        id,
        "UPDATE campaigns SET status = 'cancelled', finished_at = now(), locked_until = NULL, updated_at = now()
         WHERE id = $1 AND status IN ('draft', 'scheduled', 'running', 'paused') RETURNING *",
        "cancel",
    )
    .await?;
    let app2 = app.clone();
    tokio::spawn(async move { drop_snapshot(&app2, id).await });
    Ok(res)
}

// ---------------------------------------------------------------- runner

/// Campaign ids this process is running. `claim` skips them, so a slow chunk can never
/// lead this process to start a second runner for the same campaign.
type Running = Arc<std::sync::Mutex<std::collections::HashSet<Uuid>>>;

/// Removes the id from `Running` and stops the lease heartbeat, however the run ends.
struct RunGuard {
    id: Uuid,
    running: Running,
    heartbeat: tokio::task::JoinHandle<()>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.heartbeat.abort();
        self.running.lock().unwrap().remove(&self.id);
    }
}

/// Renews the lease every LEASE_SECS / 3 while the campaign is running, independently of
/// chunk progress. A chunk can take far longer than the lease when the ingest API is
/// struggling (up to 12 attempts with backoff); without this another instance would
/// claim the campaign and run it concurrently.
fn spawn_heartbeat(app: App, id: Uuid) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(LEASE_SECS as u64 / 3));
        tick.tick().await; // the claim just set the lease
        loop {
            tick.tick().await;
            // GREATEST: never shorten a longer lease (the snapshot takes 30 minutes).
            let r = sqlx::query(
                "UPDATE campaigns SET locked_until = GREATEST(locked_until, now() + make_interval(secs => $2))
                 WHERE id = $1 AND status = 'running'",
            )
            .bind(id)
            .bind(LEASE_SECS as f64)
            .execute(&app.db)
            .await;
            match r {
                Ok(r) if r.rows_affected() == 0 => return, // paused, cancelled or finished
                Ok(_) => {}
                Err(e) => tracing::warn!(campaign = %id, err = %e, "lease renewal failed"),
            }
        }
    })
}

pub async fn run_loop(app: App) {
    let max: usize = common::config::env_parse("MAX_CONCURRENT_CAMPAIGNS", 3);
    let sem = Arc::new(Semaphore::new(max));
    let running: Running = Default::default();
    loop {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let mine: Vec<Uuid> = running.lock().unwrap().iter().copied().collect();
        match claim(&app, &mine).await {
            Ok(Some(c)) => {
                let app = app.clone();
                running.lock().unwrap().insert(c.id);
                let guard = RunGuard { id: c.id, running: running.clone(), heartbeat: spawn_heartbeat(app.clone(), c.id) };
                tokio::spawn(async move {
                    let _permit = permit;
                    let _guard = guard;
                    gauge!("campaigns_running").increment(1.0);
                    let id = c.id;
                    if let Err(e) = run(&app, c).await {
                        tracing::error!(campaign = %id, err = %e.1, "campaign stopped with error");
                        let _ = sqlx::query(
                            "UPDATE campaigns SET status = 'paused', last_error = $2, locked_until = NULL, updated_at = now()
                             WHERE id = $1 AND status = 'running'",
                        )
                        .bind(id)
                        .bind(&e.1)
                        .execute(&app.db)
                        .await;
                    }
                    gauge!("campaigns_running").decrement(1.0);
                });
            }
            Ok(None) => {
                drop(permit);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => {
                drop(permit);
                tracing::warn!(err = %e, "claim failed");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// Due scheduled campaigns, or running ones whose runner died (lease expired).
/// `skip`: campaigns this process is already running.
async fn claim(app: &App, skip: &[Uuid]) -> sqlx::Result<Option<Campaign>> {
    sqlx::query_as(
        "UPDATE campaigns SET status = 'running', started_at = COALESCE(started_at, now()),
                locked_until = now() + make_interval(secs => $1), updated_at = now()
         WHERE id = (
             SELECT id FROM campaigns
             WHERE ((status = 'scheduled' AND scheduled_at <= now())
                    OR (status = 'running' AND (locked_until IS NULL OR locked_until < now())))
               -- also covers pause + quick resume while our runner finishes its last chunk
               AND NOT (id = ANY($2))
             ORDER BY scheduled_at NULLS FIRST
             LIMIT 1 FOR UPDATE SKIP LOCKED)
         RETURNING *",
    )
    .bind(LEASE_SECS as f64)
    .bind(skip)
    .fetch_optional(&app.db)
    .await
}

#[derive(sqlx::FromRow)]
struct Target {
    user_id: String,
    email: Option<String>,
    phone: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    tokens: Vec<String>,
    /// Still active + reachable + subscribed now (may have changed since the snapshot).
    reachable: bool,
    attributes: Option<Value>,
}

/// Freezes the audience at launch: exact targeted count / progress, and chunk reads
/// become an index range scan on (campaign_id, user_id) instead of filter + sort.
async fn snapshot(app: &App, c: &Campaign, seg: &crate::rules::Segment, ch: Channel) -> ApiResult<i64> {
    let mut tx = app.db.begin().await?;
    // Big cohorts take a while to snapshot: hold the lease long enough.
    sqlx::query("UPDATE campaigns SET locked_until = now() + interval '30 minutes' WHERE id = $1")
        .bind(c.id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM campaign_audience WHERE campaign_id = $1").bind(c.id).execute(&mut *tx).await?;
    let mut qb = QueryBuilder::<Postgres>::new("INSERT INTO campaign_audience (campaign_id, user_id) SELECT ");
    qb.push_bind(c.id).push(", c.user_id FROM contacts c");
    push_audience(&mut qb, seg, Some(ch)).map_err(AppError::bad)?;
    let n = qb.build().execute(&mut *tx).await?.rows_affected() as i64;
    sqlx::query("UPDATE campaigns SET targeted = $2, locked_until = now() + make_interval(secs => $3) WHERE id = $1")
        .bind(c.id)
        .bind(n)
        .bind(LEASE_SECS as f64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    // Fresh table, no statistics yet: without this the planner can pick a full scan of
    // contacts for each chunk (measured: 1-4.6s chunks instead of ~0.2s).
    sqlx::query("ANALYZE campaign_audience").execute(&app.db).await?;
    Ok(n)
}

async fn fetch_chunk(app: &App, campaign: Uuid, ch: Channel, after: &str, n: i64) -> ApiResult<Vec<Target>> {
    let mut qb = QueryBuilder::<Postgres>::new("SELECT a.user_id, c.email, c.phone, c.first_name, c.last_name, ");
    qb.push(if ch == Channel::Push {
        "ARRAY(SELECT d.token FROM devices d WHERE d.user_id = c.user_id ORDER BY d.updated_at DESC LIMIT 20) AS tokens"
    } else {
        "ARRAY[]::text[] AS tokens"
    });
    qb.push(", COALESCE(c.status = 'active' AND ").push(reach_sql(ch)).push(", FALSE) AS reachable, c.attributes");
    qb.push(" FROM campaign_audience a LEFT JOIN contacts c ON c.user_id = a.user_id WHERE a.campaign_id = ");
    qb.push_bind(campaign);
    qb.push(" AND a.user_id > ").push_bind(after.to_string());
    qb.push(" ORDER BY a.user_id LIMIT ").push_bind(n);
    Ok(qb.build_query_as().fetch_all(&app.db).await?)
}

async fn drop_snapshot(app: &App, id: Uuid) {
    if let Err(e) = sqlx::query("DELETE FROM campaign_audience WHERE campaign_id = $1").bind(id).execute(&app.db).await {
        tracing::warn!(campaign = %id, err = %e, "audience snapshot cleanup failed");
    }
}

/// Frequency cap via a per-user-per-day Redis SET of campaign ids. SADD is idempotent,
/// so replaying a chunk never consumes an extra slot. Returns the over-cap user ids.
async fn over_cap(app: &App, campaign: &str, users: &[String], cap: i32) -> ApiResult<std::collections::HashSet<String>> {
    let day = Utc::now().format("%Y%m%d");
    let mut pipe = redis::pipe();
    for u in users {
        let k = format!("fcap:{u}:{day}");
        pipe.sadd(&k, campaign).ignore().expire(&k, 2 * 86_400).ignore().scard(&k);
    }
    let mut r = app.redis.clone();
    let counts: Vec<i64> = pipe.query_async(&mut r).await?;
    let over: std::collections::HashSet<String> =
        users.iter().zip(counts).filter(|(_, n)| *n > cap as i64).map(|(u, _)| u.clone()).collect();
    if !over.is_empty() {
        // Release the slot we just took for users we won't message.
        let mut pipe = redis::pipe();
        for u in &over {
            pipe.srem(format!("fcap:{u}:{day}"), campaign).ignore();
        }
        let _: () = pipe.query_async(&mut r).await?;
    }
    Ok(over)
}

#[derive(Deserialize)]
struct BatchItem {
    id: Option<String>,
    error: Option<String>,
}

/// Sends a chunk as parallel sub-batches: cuts the per-chunk round trip so the
/// configured rate_per_sec is actually reachable. On failure the cursor isn't
/// advanced and a resume replays the chunk; idempotency keys make that harmless.
async fn enqueue(app: &App, reqs: Vec<NotificationRequest>) -> Result<(i64, i64), String> {
    const SUB_BATCH: usize = 250;
    let mut parts = Vec::new();
    let mut it = reqs.into_iter().peekable();
    while it.peek().is_some() {
        parts.push(it.by_ref().take(SUB_BATCH).collect::<Vec<_>>());
    }
    let results = futures::future::join_all(parts.into_iter().map(|p| enqueue_batch(app, p))).await;
    results.into_iter().try_fold((0, 0), |(a, b), r| r.map(|(x, y)| (a + x, b + y)))
}

/// Sends through the ingest API (validation, idempotency, metrics in one place).
/// Retries only failed items, with backoff. Returns (accepted, rejected_invalid).
async fn enqueue_batch(app: &App, reqs: Vec<NotificationRequest>) -> Result<(i64, i64), String> {
    let url = format!("{}/v1/notifications/batch", app.api_url);
    let mut pending = reqs;
    let (mut ok, mut invalid) = (0i64, 0i64);
    for attempt in 0..12u32 {
        let resp = app.http.post(&url).json(&json!({ "notifications": pending })).send().await;
        let items: Vec<BatchItem> = match resp {
            Ok(r) if r.status() == StatusCode::MULTI_STATUS => r.json().await.map_err(|e| e.to_string())?,
            Ok(r) => {
                tracing::warn!(status = %r.status(), attempt, "ingest API rejected batch");
                vec![]
            }
            Err(e) => {
                tracing::warn!(err = %e, attempt, "ingest API unreachable");
                vec![]
            }
        };
        if items.len() == pending.len() {
            let mut retry = Vec::new();
            for (req, item) in pending.into_iter().zip(items) {
                match (item.id, item.error) {
                    (Some(_), _) => ok += 1,
                    (None, Some(e)) if e == "overloaded" || e.starts_with("kafka") || e.starts_with("redis") => retry.push(req),
                    (None, _) => invalid += 1,
                }
            }
            pending = retry;
            if pending.is_empty() {
                return Ok((ok, invalid));
            }
        }
        let backoff = Duration::from_millis((250u64 << attempt.min(5)).min(10_000));
        tokio::time::sleep(backoff).await;
    }
    Err(format!("ingest API kept failing for {} notifications; campaign paused, resume to retry", pending.len()))
}

async fn run(app: &App, c: Campaign) -> ApiResult<()> {
    let id = c.id.to_string();
    let ch = c.channel();
    let seg = segments::load(app, c.segment_id).await?;
    // Ship only the attributes the template uses ({{attributes.plan}} -> "plan").
    let tpl: Option<(Option<String>, String)> = sqlx::query_as("SELECT title, body FROM templates WHERE channel = $1 AND key = $2")
        .bind(&c.channel)
        .bind(&c.template)
        .fetch_optional(&app.db)
        .await?;
    let attr_keys = tpl.as_ref().map(|(t, b)| crate::templates::attribute_keys(t.as_deref(), b)).unwrap_or_default();
    gauge!("campaign_info", "campaign" => id.clone(), "name" => c.name.clone(), "channel" => c.channel.clone()).set(1.0);

    let targeted = match c.targeted {
        Some(t) => t, // resumed: snapshot already taken
        None => snapshot(app, &c, &seg, ch).await?,
    };
    tracing::info!(campaign = %id, name = %c.name, targeted, resume_from = ?c.cursor, "campaign running");

    let rate = NonZeroU32::new(c.rate_per_sec.max(1) as u32).unwrap();
    let limiter = RateLimiter::direct(Quota::per_second(rate));
    let chunk = rate.get().min(1_000);
    let mut cursor = c.cursor.clone().unwrap_or_default();
    let (mut enqueued, mut suppressed) = (c.enqueued, c.suppressed);

    loop {
        let started = std::time::Instant::now();
        let targets = fetch_chunk(app, c.id, ch, &cursor, chunk as i64).await?;
        let Some(last) = targets.last().map(|t| t.user_id.clone()) else {
            sqlx::query(
                "UPDATE campaigns SET status = 'completed', finished_at = now(), locked_until = NULL, updated_at = now()
                 WHERE id = $1 AND status = 'running'",
            )
            .bind(c.id)
            .execute(&app.db)
            .await?;
            gauge!("campaign_progress_ratio", "campaign" => id.clone()).set(1.0);
            drop_snapshot(app, c.id).await;
            tracing::info!(campaign = %id, enqueued, suppressed, "campaign completed");
            return Ok(());
        };

        // Unsubscribed / deactivated since launch: skip, counted as suppressed.
        let (targets, gone): (Vec<Target>, Vec<Target>) = targets.into_iter().partition(|t| t.reachable);
        let n_gone = gone.len() as i64;
        let capped = match c.frequency_cap {
            Some(cap) if !targets.is_empty() => {
                let users: Vec<String> = targets.iter().map(|t| t.user_id.clone()).collect();
                over_cap(app, &id, &users, cap).await?
            }
            _ => Default::default(),
        };
        let n_capped = capped.len() as i64;
        let reqs: Vec<NotificationRequest> = targets
            .into_iter()
            .filter(|t| !capped.contains(&t.user_id))
            .map(|t| {
                let mut data = c.data.clone();
                if let Some(obj) = data.as_object_mut() {
                    obj.insert("first_name".into(), json!(t.first_name));
                    obj.insert("last_name".into(), json!(t.last_name));
                    if !attr_keys.is_empty() {
                        let picked: serde_json::Map<String, Value> = attr_keys
                            .iter()
                            .filter_map(|k| t.attributes.as_ref()?.get(k).map(|v| (k.clone(), v.clone())))
                            .collect();
                        obj.insert("attributes".into(), Value::Object(picked));
                    }
                }
                NotificationRequest {
                    idempotency_key: Some(format!("camp:{id}:{}", t.user_id)),
                    user_id: t.user_id,
                    channel: ch,
                    priority: Priority::Bulk,
                    template: c.template.clone(),
                    data,
                    ttl_seconds: c.ttl_seconds.map(|s| s.max(1) as u64),
                    recipient: Some(Recipient { email: t.email, phone: t.phone, push_tokens: t.tokens }),
                    campaign_id: Some(id.clone()),
                }
            })
            .collect();

        let (ok, invalid) = if reqs.is_empty() {
            (0, 0)
        } else {
            let n = NonZeroU32::new(reqs.len() as u32).unwrap();
            limiter.until_n_ready(n).await.expect("chunk <= burst");
            enqueue(app, reqs).await.map_err(|e| AppError(StatusCode::BAD_GATEWAY, e))?
        };
        enqueued += ok;
        suppressed += n_capped + invalid + n_gone;
        counter!("campaign_enqueued_total", "campaign" => id.clone()).increment(ok as u64);
        counter!("campaign_suppressed_total", "campaign" => id.clone(), "reason" => "frequency_cap").increment(n_capped as u64);
        counter!("campaign_suppressed_total", "campaign" => id.clone(), "reason" => "invalid").increment(invalid as u64);
        counter!("campaign_suppressed_total", "campaign" => id.clone(), "reason" => "unreachable").increment(n_gone as u64);
        if targeted > 0 {
            gauge!("campaign_progress_ratio", "campaign" => id.clone())
                .set(((enqueued + suppressed) as f64 / targeted as f64).min(1.0));
        }

        // Always record what this chunk did (it was already sent, even if the campaign
        // was paused/cancelled meanwhile), then stop unless still running.
        let still: Option<String> = sqlx::query_scalar(
            "UPDATE campaigns SET cursor = $2, enqueued = enqueued + $3, suppressed = suppressed + $4,
                    locked_until = CASE WHEN status = 'running' THEN now() + make_interval(secs => $5) ELSE locked_until END,
                    updated_at = now()
             WHERE id = $1 RETURNING status",
        )
        .bind(c.id)
        .bind(&last)
        .bind(ok)
        .bind(n_capped + invalid + n_gone)
        .bind(LEASE_SECS as f64)
        .fetch_optional(&app.db)
        .await?;
        histogram!("campaign_chunk_duration_seconds").record(started.elapsed().as_secs_f64());
        if still.as_deref() != Some("running") {
            tracing::info!(campaign = %id, status = ?still, "campaign stopped (paused or cancelled)");
            return Ok(());
        }
        cursor = last;
    }
}
