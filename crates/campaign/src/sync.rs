//! Incremental sync from the identity service's Postgres `users` table (Prisma schema).
//! Pull-based: every SYNC_INTERVAL_SECS, read rows with (updated_at, id) past the
//! watermark in pages. Idempotent upserts + a small overlap window make it safe to
//! re-read and resilient to late-committing transactions. Upgrade path: Debezium CDC.

use std::time::{Duration, Instant};

use axum::{extract::State, Json};
use chrono::{DateTime, NaiveDateTime, Utc};
use metrics::{counter, gauge, histogram};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::PgPool;

use crate::{error::*, imports::normalize_phone, App};

const SOURCE: &str = "identity";
const ZERO_UUID: &str = "00000000-0000-0000-0000-000000000000";

#[derive(sqlx::FromRow)]
struct IdentityUser {
    id: String,
    email: String,
    phone: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    status: String,
    email_verified_at: Option<NaiveDateTime>,
    phone_verified_at: Option<NaiveDateTime>,
    last_login_at: Option<NaiveDateTime>,
    updated_at: NaiveDateTime,
}

pub async fn run_loop(app: App, src: PgPool) {
    let every = Duration::from_secs(common::config::env_parse("SYNC_INTERVAL_SECS", 60));
    loop {
        let started = Instant::now();
        match sync_once(&app, &src).await {
            Ok(n) => {
                histogram!("identity_sync_duration_seconds").record(started.elapsed().as_secs_f64());
                gauge!("identity_sync_last_success_timestamp_seconds").set(Utc::now().timestamp() as f64);
                if n > 0 {
                    tracing::info!(rows = n, "identity sync");
                }
            }
            Err(e) => {
                counter!("identity_sync_errors_total").increment(1);
                tracing::warn!(err = %e, "identity sync failed");
                let _ = sqlx::query(
                    "INSERT INTO sync_state (source, watermark_ts, watermark_id, last_error, last_run_at)
                     VALUES ($1, 'epoch', $2, $3, now())
                     ON CONFLICT (source) DO UPDATE SET last_error = $3, last_run_at = now()",
                )
                .bind(SOURCE)
                .bind(ZERO_UUID)
                .bind(e.to_string())
                .execute(&app.db)
                .await;
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = app.sync_now.notified() => {}
        }
    }
}

async fn sync_once(app: &App, src: &PgPool) -> anyhow::Result<u64> {
    let batch: i64 = common::config::env_parse("SYNC_BATCH", 5_000);
    let wm: Option<(NaiveDateTime, String)> =
        sqlx::query_as("SELECT watermark_ts, watermark_id FROM sync_state WHERE source = $1")
            .bind(SOURCE)
            .fetch_optional(&app.db)
            .await?;
    // Re-read a 2-minute overlap: catches rows whose transaction committed after we passed them.
    let (mut ts, mut id) = match wm {
        Some((ts, _)) if ts > DateTime::<Utc>::UNIX_EPOCH.naive_utc() => (ts - chrono::Duration::minutes(2), ZERO_UUID.to_string()),
        _ => (DateTime::<Utc>::UNIX_EPOCH.naive_utc(), ZERO_UUID.to_string()),
    };
    let mut total = 0u64;
    loop {
        // Only the columns we need. Never touches credentials/sessions.
        let users: Vec<IdentityUser> = sqlx::query_as(
            "SELECT id::text AS id, email, phone, first_name, last_name, status::text AS status,
                    email_verified_at, phone_verified_at, last_login_at, updated_at
             FROM users
             WHERE (updated_at, id) > ($1, $2::uuid)
             ORDER BY updated_at, id
             LIMIT $3",
        )
        .bind(ts)
        .bind(&id)
        .bind(batch)
        .fetch_all(src)
        .await?;
        if users.is_empty() {
            break;
        }
        let n = users.len();
        upsert(app, &users).await?;
        let last = users.last().unwrap();
        ts = last.updated_at;
        id = last.id.clone();
        // Advance only forward (the overlap re-read must not move it back).
        sqlx::query(
            "INSERT INTO sync_state (source, watermark_ts, watermark_id, rows_synced, last_run_at, last_error)
             VALUES ($1, $2, $3, $4, now(), NULL)
             ON CONFLICT (source) DO UPDATE SET
               watermark_ts = CASE WHEN (EXCLUDED.watermark_ts, EXCLUDED.watermark_id) > (sync_state.watermark_ts, sync_state.watermark_id)
                                   THEN EXCLUDED.watermark_ts ELSE sync_state.watermark_ts END,
               watermark_id = CASE WHEN (EXCLUDED.watermark_ts, EXCLUDED.watermark_id) > (sync_state.watermark_ts, sync_state.watermark_id)
                                   THEN EXCLUDED.watermark_id ELSE sync_state.watermark_id END,
               rows_synced = sync_state.rows_synced + EXCLUDED.rows_synced,
               last_run_at = now(), last_error = NULL",
        )
        .bind(SOURCE)
        .bind(ts)
        .bind(&id)
        .bind(n as i64)
        .execute(&app.db)
        .await?;
        counter!("identity_sync_rows_total").increment(n as u64);
        total += n as u64;
        if (n as i64) < batch {
            break;
        }
    }
    // Cheap estimate; count(*) on 10M rows every minute would be wasteful.
    let est: Option<f32> = sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE relname = 'contacts'")
        .fetch_optional(&app.db)
        .await?;
    gauge!("contacts_total_estimate").set(est.unwrap_or(0.0).max(0.0) as f64);
    Ok(total)
}

async fn upsert(app: &App, users: &[IdentityUser]) -> anyhow::Result<()> {
    let mut ids = Vec::with_capacity(users.len());
    let mut emails = Vec::with_capacity(users.len());
    let mut phones = Vec::with_capacity(users.len());
    let mut first = Vec::with_capacity(users.len());
    let mut last = Vec::with_capacity(users.len());
    let mut status = Vec::with_capacity(users.len());
    let mut attrs = Vec::with_capacity(users.len());
    let mut active = Vec::with_capacity(users.len());
    for u in users {
        ids.push(u.id.clone());
        emails.push(crate::imports::normalize_email(&u.email));
        let p = u.phone.as_deref().and_then(|p| normalize_phone(p, &app.phone));
        if u.phone.is_some() && p.is_none() {
            counter!("identity_sync_invalid_phone_total").increment(1);
        }
        phones.push(p);
        first.push(u.first_name.clone());
        last.push(u.last_name.clone());
        status.push(if u.status == "ACTIVE" { "active" } else { "inactive" }.to_string());
        attrs.push(json!({
            "email_verified": u.email_verified_at.is_some(),
            "phone_verified": u.phone_verified_at.is_some(),
            "identity_status": u.status,
        }));
        active.push(u.last_login_at.map(|t| t.and_utc()));
    }
    // Identity is the source of truth for these fields: overwrite (but keep imported attributes).
    sqlx::query(
        "INSERT INTO contacts (user_id, email, phone, first_name, last_name, status, attributes, last_active_at, source)
         SELECT u, e, p, f, l, s, a, la, 'identity'
         FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::jsonb[], $8::timestamptz[])
              AS t(u, e, p, f, l, s, a, la)
         ON CONFLICT (user_id) DO UPDATE SET
           email = EXCLUDED.email, phone = EXCLUDED.phone,
           first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name,
           status = EXCLUDED.status, last_active_at = EXCLUDED.last_active_at,
           attributes = contacts.attributes || EXCLUDED.attributes,
           updated_at = now()",
    )
    .bind(&ids)
    .bind(&emails)
    .bind(&phones)
    .bind(&first)
    .bind(&last)
    .bind(&status)
    .bind(&attrs)
    .bind(&active)
    .execute(&app.db)
    .await?;
    Ok(())
}

#[derive(Serialize, sqlx::FromRow)]
pub struct SyncState {
    source: String,
    watermark_ts: NaiveDateTime,
    rows_synced: i64,
    last_run_at: Option<chrono::DateTime<Utc>>,
    last_error: Option<String>,
}

pub async fn status(State(app): State<App>) -> ApiResult<Json<Value>> {
    let rows: Vec<SyncState> =
        sqlx::query_as("SELECT source, watermark_ts, rows_synced, last_run_at, last_error FROM sync_state")
            .fetch_all(&app.db)
            .await?;
    Ok(Json(json!({ "enabled": app.identity_enabled, "sources": rows })))
}

pub async fn trigger(State(app): State<App>) -> ApiResult<Json<Value>> {
    if !app.identity_enabled {
        return Err(AppError::bad("identity sync disabled: set IDENTITY_DATABASE_URL"));
    }
    app.sync_now.notify_one();
    Ok(Json(json!({ "triggered": true })))
}
