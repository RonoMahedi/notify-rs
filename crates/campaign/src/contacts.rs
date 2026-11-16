use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use common::model::{devices_key, optout_key, Channel};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    error::*,
    imports::{dedupe, normalize_email, normalize_phone, upsert_contacts, ContactRow},
    App,
};

#[derive(Deserialize)]
pub struct ContactIn {
    user_id: String,
    email: Option<String>,
    phone: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    country: Option<String>,
    language: Option<String>,
    timezone: Option<String>,
    #[serde(default)]
    attributes: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
pub struct ContactsIn {
    contacts: Vec<ContactIn>,
}

/// POST /v1/contacts — upsert up to 1000 contacts from another service.
pub async fn upsert(State(app): State<App>, Json(b): Json<ContactsIn>) -> ApiResult<Json<Value>> {
    if b.contacts.is_empty() || b.contacts.len() > 1000 {
        return Err(AppError::bad("contacts must have 1..=1000 items"));
    }
    let mut rows = Vec::with_capacity(b.contacts.len());
    let mut errors = Vec::new();
    for (i, c) in b.contacts.into_iter().enumerate() {
        let email = c.email.as_deref().map(normalize_email);
        let phone = c.phone.as_deref().map(|p| normalize_phone(p, &app.phone));
        if c.user_id.is_empty() || c.user_id.len() > 128 {
            errors.push(json!({"index": i, "error": "user_id must be 1..=128 chars"}));
        } else if matches!(email, Some(None)) {
            errors.push(json!({"index": i, "error": "invalid email"}));
        } else if matches!(phone, Some(None)) {
            errors.push(json!({"index": i, "error": "invalid phone"}));
        } else if c.attributes.keys().any(|k| k.len() > 64 || !k.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')) {
            errors.push(json!({"index": i, "error": "attribute keys must match [A-Za-z0-9_]{1,64}"}));
        } else {
            rows.push(ContactRow {
                user_id: Some(c.user_id),
                email: email.flatten(),
                phone: phone.flatten(),
                first_name: c.first_name,
                last_name: c.last_name,
                country: c.country.map(|s| s.to_uppercase()),
                language: c.language.map(|s| s.to_lowercase()),
                timezone: c.timezone,
                attributes: c.attributes,
            });
        }
    }
    let rows = dedupe(rows);
    let n = if rows.is_empty() { 0 } else { upsert_contacts(&app, &rows, "api").await? };
    Ok(Json(json!({ "upserted": n, "errors": errors })))
}

#[derive(Serialize, sqlx::FromRow)]
pub struct Contact {
    user_id: String,
    email: Option<String>,
    phone: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    country: Option<String>,
    language: Option<String>,
    timezone: Option<String>,
    status: String,
    unsubscribed: Vec<String>,
    attributes: Value,
    source: String,
    last_active_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

pub async fn get(State(app): State<App>, Path(user_id): Path<String>) -> ApiResult<Json<Value>> {
    let c: Contact = sqlx::query_as(
        "SELECT user_id, email, phone, first_name, last_name, country, language, timezone, status,
                unsubscribed, attributes, source, last_active_at, updated_at
         FROM contacts WHERE user_id = $1",
    )
    .bind(&user_id)
    .fetch_one(&app.db)
    .await?;
    let devices: Vec<(String, String)> = sqlx::query_as("SELECT platform, token FROM devices WHERE user_id = $1 ORDER BY updated_at DESC")
        .bind(&user_id)
        .fetch_all(&app.db)
        .await?;
    let devices: Vec<Value> = devices.into_iter().map(|(p, t)| json!({"platform": p, "token": t})).collect();
    let mut v = serde_json::to_value(c).map_err(anyhow::Error::from)?;
    v["devices"] = json!(devices);
    Ok(Json(v))
}

#[derive(Deserialize)]
pub struct ChannelIn {
    channel: Channel,
}

/// Unsubscribe is written to Postgres (cohort exclusion) AND Redis (workers drop
/// already-queued messages immediately).
pub async fn unsubscribe(State(app): State<App>, Path(user_id): Path<String>, Json(b): Json<ChannelIn>) -> ApiResult<Json<Value>> {
    let ch = b.channel.as_str();
    let r = sqlx::query(
        "UPDATE contacts SET unsubscribed = array_append(unsubscribed, $2), updated_at = now()
         WHERE user_id = $1 AND NOT ($2 = ANY(unsubscribed))",
    )
    .bind(&user_id)
    .bind(ch)
    .execute(&app.db)
    .await?;
    let mut redis = app.redis.clone();
    let _: () = redis.sadd(optout_key(&user_id), ch).await?;
    Ok(Json(json!({ "user_id": user_id, "unsubscribed": ch, "changed": r.rows_affected() > 0 })))
}

pub async fn resubscribe(State(app): State<App>, Path(user_id): Path<String>, Json(b): Json<ChannelIn>) -> ApiResult<Json<Value>> {
    let ch = b.channel.as_str();
    sqlx::query("UPDATE contacts SET unsubscribed = array_remove(unsubscribed, $2), updated_at = now() WHERE user_id = $1")
        .bind(&user_id)
        .bind(ch)
        .execute(&app.db)
        .await?;
    let mut redis = app.redis.clone();
    let _: () = redis.srem(optout_key(&user_id), ch).await?;
    Ok(Json(json!({ "user_id": user_id, "resubscribed": ch })))
}

#[derive(Deserialize)]
pub struct DeviceIn {
    user_id: String,
    platform: String,
    token: String,
}

/// POST /v1/devices — called by the mobile app on login / token refresh.
pub async fn register_device(State(app): State<App>, Json(d): Json<DeviceIn>) -> ApiResult<StatusCode> {
    if !matches!(d.platform.as_str(), "ios" | "android" | "web") {
        return Err(AppError::bad("platform must be ios, android or web"));
    }
    if d.token.is_empty() || d.token.len() > 4096 || d.user_id.is_empty() || d.user_id.len() > 128 {
        return Err(AppError::bad("invalid user_id or token"));
    }
    // A token belongs to exactly one user: re-registering moves it (shared/resold phones).
    let previous: Option<String> =
        sqlx::query_scalar("SELECT user_id FROM devices WHERE token = $1").bind(&d.token).fetch_optional(&app.db).await?;
    sqlx::query(
        "INSERT INTO devices (token, user_id, platform) VALUES ($1, $2, $3)
         ON CONFLICT (token) DO UPDATE SET user_id = EXCLUDED.user_id, platform = EXCLUDED.platform, updated_at = now()",
    )
    .bind(&d.token)
    .bind(&d.user_id)
    .bind(&d.platform)
    .execute(&app.db)
    .await?;
    // Mirror for the workers (push to a user_id reaches all their devices).
    let mut pipe = redis::pipe();
    if let Some(prev) = previous.filter(|p| *p != d.user_id) {
        pipe.srem(devices_key(&prev), &d.token).ignore();
    }
    pipe.sadd(devices_key(&d.user_id), &d.token).ignore();
    let mut r = app.redis.clone();
    let _: () = pipe.query_async(&mut r).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_device(State(app): State<App>, Path(token): Path<String>) -> ApiResult<StatusCode> {
    let owner: Option<String> =
        sqlx::query_scalar("DELETE FROM devices WHERE token = $1 RETURNING user_id").bind(&token).fetch_optional(&app.db).await?;
    if let Some(u) = owner {
        let mut r = app.redis.clone();
        let _: () = r.srem(devices_key(&u), &token).await?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Removes tokens a provider reported as gone (uninstalled app, invalid token).
pub async fn remove_dead_tokens(app: &App, tokens: &[String]) -> anyhow::Result<u64> {
    if tokens.is_empty() {
        return Ok(0);
    }
    let gone: Vec<(String, String)> =
        sqlx::query_as("DELETE FROM devices WHERE token = ANY($1) RETURNING user_id, token").bind(tokens).fetch_all(&app.db).await?;
    let mut pipe = redis::pipe();
    for (u, t) in &gone {
        pipe.srem(devices_key(u), t).ignore();
    }
    let mut r = app.redis.clone();
    let _: () = pipe.query_async(&mut r).await?;
    Ok(gone.len() as u64)
}

/// Rebuilds the Redis device mirror when it's missing (first start, Redis flushed/restarted).
/// Redis runs without persistence, so after a restart or FLUSHALL the worker-side copies
/// of Postgres data are gone. These loops rebuild them whenever their marker key is missing.
/// The marker is written only after a complete pass, so a failed pass is retried next tick.
pub async fn devices_mirror_loop(app: App) {
    mirror_loop(&app, "devices:synced", "push token mirror", rebuild_devices).await
}

/// Opt-outs: the worker suppresses bulk messages for users in `optout:{user_id}`. Without
/// this rebuild, a Redis restart would silently drop every unsubscribe.
pub async fn optouts_mirror_loop(app: App) {
    mirror_loop(&app, "optouts:synced", "opt-out mirror", rebuild_optouts).await
}

async fn mirror_loop<F, Fut>(app: &App, marker: &str, what: &str, rebuild: F)
where
    F: Fn(App) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<usize>>,
{
    loop {
        let mut r = app.redis.clone();
        let synced: Option<String> = r.get(marker).await.unwrap_or(None);
        if synced.is_none() {
            match rebuild(app.clone()).await {
                Ok(n) => {
                    let _: Result<(), _> = r.set(marker, 1).await;
                    tracing::info!(rows = n, "{what} rebuilt");
                }
                Err(e) => tracing::warn!(err = %e, "{what} rebuild failed; retrying in 60s"),
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

async fn rebuild_devices(app: App) -> anyhow::Result<usize> {
    let mut r = app.redis.clone();
    let (mut after, mut total) = (String::new(), 0usize);
    loop {
        let rows: Vec<(String, String)> = sqlx::query_as("SELECT token, user_id FROM devices WHERE token > $1 ORDER BY token LIMIT 10000")
            .bind(&after)
            .fetch_all(&app.db)
            .await?;
        let Some(last) = rows.last().map(|(t, _)| t.clone()) else { return Ok(total) };
        let mut pipe = redis::pipe();
        for (t, u) in &rows {
            pipe.sadd(devices_key(u), t).ignore();
        }
        pipe.query_async::<()>(&mut r).await?;
        total += rows.len();
        after = last;
    }
}

/// Only adds opt-outs. If someone resubscribes during a rebuild they may stay suppressed
/// until they resubscribe again; that errs on the side of not messaging them.
async fn rebuild_optouts(app: App) -> anyhow::Result<usize> {
    let mut r = app.redis.clone();
    let (mut after, mut total) = (String::new(), 0usize);
    loop {
        let rows: Vec<(String, Vec<String>)> = sqlx::query_as(
            "SELECT user_id, unsubscribed FROM contacts
             WHERE user_id > $1 AND cardinality(unsubscribed) > 0 ORDER BY user_id LIMIT 10000",
        )
        .bind(&after)
        .fetch_all(&app.db)
        .await?;
        let Some(last) = rows.last().map(|(u, _)| u.clone()) else { return Ok(total) };
        let mut pipe = redis::pipe();
        for (u, chans) in &rows {
            pipe.sadd(optout_key(u), chans).ignore();
        }
        pipe.query_async::<()>(&mut r).await?;
        total += rows.len();
        after = last;
    }
}

// ------------------------------------------------------------------ people list / search

#[derive(Serialize, sqlx::FromRow)]
pub struct PersonRow {
    user_id: String,
    first_name: Option<String>,
    last_name: Option<String>,
    email: Option<String>,
    phone: Option<String>,
    status: String,
    source: String,
    unsubscribed: Vec<String>,
    devices: i64,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
pub struct ListQ {
    q: Option<String>,
    after: Option<String>,
    limit: Option<i64>,
}

fn like_prefix(s: &str) -> String {
    format!("{}%", s.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))
}

/// Phone-ish input -> E.164 prefix: "01712" -> "+8801712", "8801712" -> "+8801712", "+1415" kept.
fn phone_prefix(raw: &str, cc: &str) -> Option<String> {
    let t = raw.trim();
    if !t.chars().all(|c| c.is_ascii_digit() || " +-()".contains(c)) {
        return None;
    }
    let digits: String = t.chars().filter(char::is_ascii_digit).collect();
    if digits.len() < 4 {
        return None;
    }
    Some(if t.starts_with('+') {
        format!("+{digits}")
    } else if let Some(rest) = digits.strip_prefix("00") {
        format!("+{rest}")
    } else if digits.starts_with(cc) {
        format!("+{digits}")
    } else if let Some(rest) = digits.strip_prefix('0') {
        format!("+{cc}{rest}")
    } else {
        format!("+{cc}{digits}")
    })
}

/// GET /v1/contacts?q=&after=&limit= — browse (no q) or search by email prefix,
/// phone prefix, or name / exact user_id. `next` = cursor for the following page.
pub async fn list(State(app): State<App>, axum::extract::Query(q): axum::extract::Query<ListQ>) -> ApiResult<Json<Value>> {
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let term = q.q.unwrap_or_default().trim().to_string();
    let after = q.after.unwrap_or_default();
    let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "SELECT c.user_id, c.first_name, c.last_name, c.email, c.phone, c.status, c.source, c.unsubscribed,
                (SELECT count(*) FROM devices d WHERE d.user_id = c.user_id) AS devices, c.updated_at
         FROM contacts c WHERE ",
    );
    let (mode, key): (&str, fn(&PersonRow) -> String) = if term.is_empty() {
        qb.push("c.user_id > ").push_bind(after).push(" ORDER BY c.user_id");
        ("browse", |r| r.user_id.clone())
    } else if term.contains('@') {
        qb.push("c.email LIKE ").push_bind(like_prefix(&term.to_lowercase())).push(" ESCAPE '\\'");
        qb.push(" AND c.email > ").push_bind(after).push(" ORDER BY c.email");
        ("email", |r| r.email.clone().unwrap_or_default())
    } else if let Some(p) = phone_prefix(&term, &app.phone.cc) {
        qb.push("c.phone LIKE ").push_bind(like_prefix(&p)).push(" ESCAPE '\\'");
        qb.push(" AND c.phone > ").push_bind(after).push(" ORDER BY c.phone");
        ("phone", |r| r.phone.clone().unwrap_or_default())
    } else {
        if term.chars().count() > 100 {
            return Err(AppError::bad("search text too long"));
        }
        // The name index needs 3+ letters/digits; fewer would scan every contact.
        if term.chars().filter(|c| c.is_alphanumeric()).count() < 3 {
            return Err(AppError::bad("type at least 3 letters or digits to search by name"));
        }
        let pat = format!("%{}%", term.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"));
        qb.push("(c.user_id = ").push_bind(term.clone());
        qb.push(" OR (coalesce(c.first_name, '') || ' ' || coalesce(c.last_name, '')) ILIKE ").push_bind(pat).push(" ESCAPE '\\')");
        qb.push(" AND c.user_id > ").push_bind(after).push(" ORDER BY c.user_id");
        ("name", |r| r.user_id.clone())
    };
    qb.push(" LIMIT ").push_bind(limit + 1);
    let mut tx = app.db.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '10s'").execute(&mut *tx).await?;
    let mut rows: Vec<PersonRow> = qb.build_query_as().fetch_all(&mut *tx).await?;
    tx.commit().await?;
    let more = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let next = if more { rows.last().map(key) } else { None };
    Ok(Json(json!({ "mode": mode, "items": rows, "next": next })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phone_search_prefixes() {
        assert_eq!(phone_prefix("01712", "880").as_deref(), Some("+8801712"));
        assert_eq!(phone_prefix("8801712", "880").as_deref(), Some("+8801712"));
        assert_eq!(phone_prefix("+1 415", "880").as_deref(), Some("+1415"));
        assert_eq!(phone_prefix("017-12", "880").as_deref(), Some("+8801712"));
        assert_eq!(phone_prefix("123", "880"), None);
        assert_eq!(phone_prefix("rahim", "880"), None);
    }

    #[test]
    fn like_prefix_escapes() {
        assert_eq!(like_prefix("a_b%"), "a\\_b\\%%");
    }
}
