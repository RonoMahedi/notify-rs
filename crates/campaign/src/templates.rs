//! Message templates: CRUD, live preview, and the Redis mirror the workers read.

use std::time::Duration;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Extension, Json,
};
use chrono::{DateTime, Utc};
use common::{
    model::Channel,
    template::{self, Template},
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{auth::Principal, error::*, App};

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct TemplateRow {
    pub channel: String,
    pub key: String,
    pub title: Option<String>,
    pub body: String,
    pub hide_content: bool,
    pub description: Option<String>,
    pub updated_by: Option<String>,
    pub updated_at: DateTime<Utc>,
}

fn channel(c: &str) -> ApiResult<Channel> {
    serde_json::from_value(json!(c)).map_err(|_| AppError::bad("channel must be push, email, sms or in_app"))
}

fn valid_key(k: &str) -> ApiResult<()> {
    if !k.is_empty() && k.len() <= 64 && k.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        Ok(())
    } else {
        Err(AppError::bad("template name must be 1-64 characters: a-z, 0-9 and _ (e.g. winter_sale)"))
    }
}

/// Mirror one template into Redis (what workers render from).
async fn mirror(app: &App, t: &TemplateRow) -> anyhow::Result<()> {
    let v = serde_json::to_string(&Template { title: t.title.clone(), body: t.body.clone(), hide_content: t.hide_content })?;
    let mut r = app.redis.clone();
    let _: () = r.set(template::redis_key(&t.channel, &t.key), v).await?;
    Ok(())
}

/// Full re-sync every minute: survives a Redis restart or flush.
pub async fn sync_loop(app: App) {
    loop {
        match sqlx::query_as::<_, TemplateRow>("SELECT * FROM templates").fetch_all(&app.db).await {
            Ok(rows) => {
                for t in &rows {
                    if let Err(e) = mirror(&app, t).await {
                        tracing::warn!(err = %e, "template mirror failed");
                        break;
                    }
                }
            }
            Err(e) => tracing::warn!(err = %e, "template sync read failed"),
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

fn describe(t: &TemplateRow) -> Value {
    let mut v = serde_json::to_value(t).unwrap();
    let tokens: Vec<String> =
        template::validate(t.title.as_deref(), &t.body).map(|ts| ts.into_iter().map(|t| t.path).collect()).unwrap_or_default();
    v["tokens"] = json!(tokens);
    if t.channel == "sms" {
        v["sms"] = json!(template::sms_info(&t.body));
    }
    v
}

#[derive(Deserialize)]
pub struct ListQ {
    channel: Option<String>,
}

pub async fn list(State(app): State<App>, Query(q): Query<ListQ>) -> ApiResult<Json<Vec<Value>>> {
    let rows: Vec<TemplateRow> = match q.channel.filter(|c| !c.is_empty()) {
        Some(c) => {
            channel(&c)?;
            sqlx::query_as("SELECT * FROM templates WHERE channel = $1 ORDER BY key").bind(c).fetch_all(&app.db).await?
        }
        None => sqlx::query_as("SELECT * FROM templates ORDER BY key, channel").fetch_all(&app.db).await?,
    };
    Ok(Json(rows.iter().map(describe).collect()))
}

#[derive(Deserialize)]
pub struct TemplateIn {
    title: Option<String>,
    body: String,
    #[serde(default)]
    hide_content: bool,
    description: Option<String>,
}

/// PUT /v1/templates/{channel}/{key} — create or update.
pub async fn upsert(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Path((ch, key)): Path<(String, String)>,
    Json(b): Json<TemplateIn>,
) -> ApiResult<Json<Value>> {
    channel(&ch)?;
    valid_key(&key)?;
    // SMS has no subject line.
    let title = if ch == "sms" { None } else { b.title.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()) };
    template::validate(title.as_deref(), &b.body).map_err(AppError::bad)?;
    let row: TemplateRow = sqlx::query_as(
        "INSERT INTO templates (channel, key, title, body, hide_content, description, updated_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (channel, key) DO UPDATE SET title = EXCLUDED.title, body = EXCLUDED.body,
           hide_content = EXCLUDED.hide_content, description = EXCLUDED.description,
           updated_by = EXCLUDED.updated_by, updated_at = now()
         RETURNING *",
    )
    .bind(&ch)
    .bind(&key)
    .bind(&title)
    .bind(&b.body)
    .bind(b.hide_content)
    .bind(b.description.map(|d| d.trim().chars().take(200).collect::<String>()).filter(|d| !d.is_empty()))
    .bind(&p.email)
    .fetch_one(&app.db)
    .await?;
    mirror(&app, &row).await?;
    tracing::info!(channel = %ch, key = %key, by = %p.email, "template saved");
    Ok(Json(describe(&row)))
}

pub async fn delete(State(app): State<App>, Path((ch, key)): Path<(String, String)>) -> ApiResult<StatusCode> {
    let in_use: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM campaigns WHERE channel = $1 AND template = $2 AND status IN ('draft', 'scheduled', 'running', 'paused')",
    )
    .bind(&ch)
    .bind(&key)
    .fetch_all(&app.db)
    .await?;
    if !in_use.is_empty() {
        return Err(AppError::conflict(format!("used by campaign(s): {}", in_use.join(", "))));
    }
    let r = sqlx::query("DELETE FROM templates WHERE channel = $1 AND key = $2").bind(&ch).bind(&key).execute(&app.db).await?;
    if r.rows_affected() == 0 {
        return Err(AppError::not_found("template not found"));
    }
    let mut redis = app.redis.clone();
    let _: () = redis.del(template::redis_key(&ch, &key)).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// The token values a contact provides (same shape the campaign runner sends).
pub async fn contact_vars(app: &App, user_id: &str) -> ApiResult<Value> {
    let row: Option<(String, Option<String>, Option<String>, Option<String>, Option<String>, Value)> =
        sqlx::query_as("SELECT user_id, first_name, last_name, email, phone, attributes FROM contacts WHERE user_id = $1")
            .bind(user_id)
            .fetch_optional(&app.db)
            .await?;
    let (uid, f, l, e, p, a) = row.ok_or_else(|| AppError::not_found("no such person"))?;
    Ok(json!({ "user_id": uid, "first_name": f, "last_name": l, "email": e, "phone": p, "attributes": a }))
}

#[derive(Deserialize)]
pub struct PreviewIn {
    channel: String,
    title: Option<String>,
    body: String,
    user_id: Option<String>,
    #[serde(default)]
    data: Value,
}

/// POST /v1/templates/preview — the exact text a person would receive.
pub async fn preview(State(app): State<App>, Json(b): Json<PreviewIn>) -> ApiResult<Json<Value>> {
    channel(&b.channel)?;
    let title = if b.channel == "sms" { None } else { b.title.filter(|t| !t.trim().is_empty()) };
    template::validate(title.as_deref(), &b.body).map_err(AppError::bad)?;
    // Campaign data first, then the person's fields win (as when a campaign sends).
    let mut vars = if b.data.is_object() { b.data.clone() } else { json!({}) };
    if let Some(uid) = b.user_id.filter(|u| !u.is_empty()) {
        if let Value::Object(person) = contact_vars(&app, &uid).await? {
            vars.as_object_mut().unwrap().extend(person);
        }
    }
    let body = template::render(&b.body, &vars).map_err(AppError::bad)?;
    let title = title.map(|t| template::render(&t, &vars)).transpose().map_err(AppError::bad)?;
    let mut missing = body.missing.clone();
    if let Some(t) = &title {
        missing.extend(t.missing.iter().filter(|m| !body.missing.contains(m)).cloned());
    }
    Ok(Json(json!({
        "title": title.map(|t| t.text),
        "body": body.text,
        "missing": missing,
        "sms": (b.channel == "sms").then(|| template::sms_info(&body.text)),
    })))
}

/// Attribute keys a template uses (`{{attributes.plan}}` -> "plan"), so a campaign only ships those.
pub fn attribute_keys(title: Option<&str>, body: &str) -> Vec<String> {
    template::validate(title, body)
        .map(|ts| {
            ts.into_iter()
                .filter_map(|t| t.path.strip_prefix("attributes.").map(|k| k.split('.').next().unwrap_or(k).to_string()))
                .collect()
        })
        .unwrap_or_default()
}
