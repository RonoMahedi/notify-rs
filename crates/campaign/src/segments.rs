use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use common::model::Channel;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    error::*,
    rules::{self, push_audience, Segment},
    App,
};

#[derive(Deserialize)]
pub struct SegmentIn {
    name: String,
    kind: String,
    rule: Option<Value>,
}

fn check_segment(kind: &str, rule: &Option<Value>) -> ApiResult<()> {
    match (kind, rule) {
        ("all", _) => Ok(()),
        ("rule", Some(r)) => rules::validate(r).map(|_| ()).map_err(AppError::bad),
        ("rule", None) => Err(AppError::bad("kind 'rule' requires a 'rule'")),
        ("static", _) => Err(AppError::bad("static cohorts are created by uploading a file to /v1/imports with segment_name")),
        _ => Err(AppError::bad("kind must be 'all' or 'rule'")),
    }
}

pub async fn create(State(app): State<App>, Json(s): Json<SegmentIn>) -> ApiResult<(StatusCode, Json<Segment>)> {
    let name = s.name.trim();
    if name.is_empty() || name.len() > 200 {
        return Err(AppError::bad("name must be 1..=200 chars"));
    }
    check_segment(&s.kind, &s.rule)?;
    let seg: Segment = sqlx::query_as("INSERT INTO segments (id, name, kind, rule) VALUES ($1, $2, $3, $4) RETURNING *")
        .bind(Uuid::now_v7())
        .bind(name)
        .bind(&s.kind)
        .bind(if s.kind == "rule" { s.rule } else { None })
        .fetch_one(&app.db)
        .await?;
    Ok((StatusCode::CREATED, Json(seg)))
}

pub async fn list(State(app): State<App>) -> ApiResult<Json<Vec<Value>>> {
    let rows: Vec<(Uuid, String, String, Option<Value>, chrono::DateTime<chrono::Utc>, i64)> = sqlx::query_as(
        "SELECT s.id, s.name, s.kind, s.rule, s.created_at,
                CASE WHEN s.kind = 'static' THEN (SELECT count(*) FROM segment_members m WHERE m.segment_id = s.id) ELSE -1 END
         FROM segments s ORDER BY s.created_at DESC LIMIT 500",
    )
    .fetch_all(&app.db)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|(id, name, kind, rule, created_at, members)| {
                json!({ "id": id, "name": name, "kind": kind, "rule": rule, "created_at": created_at,
                        "members": (members >= 0).then_some(members) })
            })
            .collect(),
    ))
}

pub async fn load(app: &App, id: Uuid) -> ApiResult<Segment> {
    Ok(sqlx::query_as("SELECT * FROM segments WHERE id = $1").bind(id).fetch_one(&app.db).await?)
}

pub async fn get(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<Json<Segment>> {
    Ok(Json(load(&app, id).await?))
}

pub async fn delete(State(app): State<App>, Path(id): Path<Uuid>) -> ApiResult<StatusCode> {
    let r = sqlx::query("DELETE FROM segments WHERE id = $1").bind(id).execute(&app.db).await?;
    if r.rows_affected() == 0 {
        return Err(AppError::not_found("segment not found"));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Audience size for a segment on a channel (only active, reachable, subscribed contacts).
pub async fn count(app: &App, seg: &Segment, channel: Option<Channel>) -> ApiResult<i64> {
    let mut tx = app.db.begin().await?;
    sqlx::query("SET LOCAL statement_timeout = '15s'").execute(&mut *tx).await?;
    let mut qb = QueryBuilder::<Postgres>::new("SELECT count(*) FROM contacts c");
    push_audience(&mut qb, seg, channel).map_err(AppError::bad)?;
    let n: i64 = qb.build_query_scalar().fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(n)
}

async fn preview_inner(app: &App, seg: &Segment, channel: Option<Channel>) -> ApiResult<Value> {
    let total = count(app, seg, channel).await?;
    let mut qb = QueryBuilder::<Postgres>::new("SELECT c.user_id, c.email, c.phone, c.first_name, c.country, c.attributes FROM contacts c");
    push_audience(&mut qb, seg, channel).map_err(AppError::bad)?;
    qb.push(" LIMIT 10");
    let sample: Vec<(String, Option<String>, Option<String>, Option<String>, Option<String>, Value)> =
        qb.build_query_as().fetch_all(&app.db).await?;
    let sample: Vec<Value> = sample
        .into_iter()
        .map(|(u, e, p, f, c, a)| json!({"user_id": u, "email": e, "phone": p, "first_name": f, "country": c, "attributes": a}))
        .collect();
    Ok(json!({ "count": total, "channel": channel, "sample": sample }))
}

#[derive(Deserialize)]
pub struct PreviewIn {
    kind: Option<String>,
    rule: Option<Value>,
    channel: Option<Channel>,
}

/// POST /v1/segments/preview — try a rule before saving it.
pub async fn preview_adhoc(State(app): State<App>, Json(p): Json<PreviewIn>) -> ApiResult<Json<Value>> {
    let kind = p.kind.unwrap_or_else(|| if p.rule.is_some() { "rule".into() } else { "all".into() });
    check_segment(&kind, &p.rule)?;
    let seg = Segment { id: Uuid::nil(), name: "preview".into(), kind, rule: p.rule, created_at: chrono::Utc::now() };
    Ok(Json(preview_inner(&app, &seg, p.channel).await?))
}

#[derive(Deserialize)]
pub struct ChannelQ {
    channel: Option<Channel>,
}

pub async fn preview_saved(State(app): State<App>, Path(id): Path<Uuid>, Query(q): Query<ChannelQ>) -> ApiResult<Json<Value>> {
    let seg = load(&app, id).await?;
    Ok(Json(preview_inner(&app, &seg, q.channel).await?))
}
