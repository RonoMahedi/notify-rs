//! Ingest API: validates, de-duplicates (idempotency key), and durably enqueues
//! notifications to Kafka. Returns 202 only after Kafka acks (acks=all).

use std::time::{Duration, Instant};

use axum::{
    extract::{MatchedPath, Path, Request, State},
    http::{HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use common::{config::env_or, kafka, model::*, telemetry};
use metrics::{counter, histogram};
use metrics_exporter_prometheus::PrometheusHandle;
use rdkafka::{
    error::{KafkaError, RDKafkaErrorCode},
    producer::{FutureProducer, FutureRecord},
};
use redis::{aio::ConnectionManager, AsyncCommands};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const MAX_BATCH: usize = 1000;

#[derive(Clone)]
struct AppState {
    producer: FutureProducer,
    redis: ConnectionManager,
    metrics: PrometheusHandle,
    /// INAPP_TOKEN_SECRET, shared with the in-app gateway. None = token endpoint disabled.
    inapp_secret: Option<std::sync::Arc<str>>,
}

enum ApiError {
    BadRequest(String),
    Overloaded,
    Unavailable(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (code, msg) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Overloaded => (StatusCode::SERVICE_UNAVAILABLE, "overloaded, retry later".into()),
            ApiError::Unavailable(m) => (StatusCode::NOT_IMPLEMENTED, m),
            ApiError::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        };
        let mut resp = (code, Json(serde_json::json!({ "error": msg }))).into_response();
        if code == StatusCode::SERVICE_UNAVAILABLE {
            resp.headers_mut().insert("retry-after", HeaderValue::from_static("1"));
        }
        resp
    }
}

#[derive(Serialize)]
struct Accepted {
    id: Uuid,
    duplicate: bool,
}

#[derive(Deserialize)]
struct BatchRequest {
    notifications: Vec<NotificationRequest>,
}

#[derive(Serialize)]
struct BatchItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Uuid>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    duplicate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Enqueue a batch with O(1) Redis round trips instead of O(n):
/// 1) pipelined idempotency SET NX, 2) concurrent Kafka produce, 3) pipelined status write.
async fn enqueue_many(s: &AppState, reqs: Vec<NotificationRequest>) -> Vec<Result<Accepted, ApiError>> {
    let mut out: Vec<Option<Result<Accepted, ApiError>>> = Vec::with_capacity(reqs.len());
    let mut pending: Vec<(usize, Option<String>, Envelope)> = Vec::new();
    for (i, req) in reqs.into_iter().enumerate() {
        match req.validate() {
            Ok(()) => {
                let idem = req.idempotency_key.clone();
                pending.push((i, idem, Envelope::from_request(req)));
                out.push(None);
            }
            Err(e) => {
                counter!("notifications_rejected_total", "reason" => "invalid").increment(1);
                out.push(Some(Err(ApiError::BadRequest(e))));
            }
        }
    }
    let mut redis = s.redis.clone();

    // 1) Idempotency: first writer wins. One round trip for the whole batch.
    let keyed: Vec<usize> = (0..pending.len()).filter(|&j| pending[j].1.is_some()).collect();
    let mut dup_of: Vec<Option<Uuid>> = vec![None; pending.len()];
    if !keyed.is_empty() {
        let mut pipe = redis::pipe();
        for &j in &keyed {
            let (_, k, env) = &pending[j];
            pipe.cmd("SET").arg(idem_key(k.as_ref().unwrap())).arg(env.id.to_string()).arg("NX").arg("EX").arg(DEDUP_TTL_SECS);
        }
        match pipe.query_async::<Vec<Option<String>>>(&mut redis).await {
            Ok(set) => {
                let losers: Vec<usize> = keyed.iter().zip(set).filter(|(_, r)| r.is_none()).map(|(&j, _)| j).collect();
                if !losers.is_empty() {
                    let keys: Vec<String> = losers.iter().map(|&j| idem_key(pending[j].1.as_ref().unwrap())).collect();
                    let existing: Vec<Option<String>> = redis.mget(&keys).await.unwrap_or_else(|_| vec![None; keys.len()]);
                    for (j, v) in losers.into_iter().zip(existing) {
                        // Key vanished between SET and MGET (expiry): treat as new.
                        dup_of[j] = v.and_then(|v| v.parse().ok());
                    }
                }
            }
            Err(e) => {
                for (i, _, _) in &pending {
                    out[*i] = Some(Err(ApiError::Internal(format!("redis: {e}"))));
                }
                return out.into_iter().map(Option::unwrap).collect();
            }
        }
    }

    // 2) Produce non-duplicates concurrently. Timeout 0 => fail fast when the producer queue is full.
    let sends = pending.iter().zip(&dup_of).filter(|(_, d)| d.is_none()).map(|((i, idem, env), _)| async move {
        let topic = main_topic(env.channel, env.priority);
        let payload = serde_json::to_vec(env).expect("serialize");
        let started = Instant::now();
        let res = s.producer.send(FutureRecord::to(&topic).key(&env.user_id).payload(&payload), Duration::from_secs(0)).await;
        histogram!("kafka_produce_duration_seconds", "topic" => topic).record(started.elapsed().as_secs_f64());
        (*i, idem, env, res.map(|_| ()).map_err(|(e, _)| e))
    });
    let produced = futures::future::join_all(sends).await;

    // 3) One round trip: mark queued (NX: never clobber a worker's newer status),
    //    and release idempotency keys of failed sends so client retries can succeed.
    let ttl = status_ttl_secs();
    let mut pipe = redis::pipe();
    for (i, idem, env, res) in produced {
        match res {
            Ok(()) => {
                pipe.cmd("SET").arg(status_key(&env.id)).arg(Status::Queued.as_str()).arg("NX").arg("EX").arg(ttl).ignore();
                counter!("notifications_accepted_total",
                    "channel" => env.channel.as_str(), "priority" => env.priority.as_str())
                .increment(1);
                out[i] = Some(Ok(Accepted { id: env.id, duplicate: false }));
            }
            Err(err) => {
                if let Some(k) = idem {
                    pipe.del(idem_key(k)).ignore();
                }
                out[i] = Some(Err(match err {
                    KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) => {
                        counter!("notifications_rejected_total", "reason" => "backpressure").increment(1);
                        ApiError::Overloaded
                    }
                    e => {
                        counter!("notifications_rejected_total", "reason" => "kafka_error").increment(1);
                        ApiError::Internal(format!("kafka: {e}"))
                    }
                }));
            }
        }
    }
    if let Err(e) = pipe.query_async::<()>(&mut redis).await {
        tracing::warn!(%e, "post-produce redis pipeline failed");
    }

    for ((i, _, env), d) in pending.iter().zip(dup_of) {
        if let Some(id) = d {
            counter!("notifications_deduplicated_total", "channel" => env.channel.as_str()).increment(1);
            out[*i] = Some(Ok(Accepted { id, duplicate: true }));
        }
    }
    out.into_iter().map(Option::unwrap).collect()
}

async fn create(State(s): State<AppState>, Json(req): Json<NotificationRequest>) -> Result<(StatusCode, Json<Accepted>), ApiError> {
    let a = enqueue_many(&s, vec![req]).await.pop().unwrap()?;
    let code = if a.duplicate { StatusCode::OK } else { StatusCode::ACCEPTED };
    Ok((code, Json(a)))
}

async fn create_batch(State(s): State<AppState>, Json(b): Json<BatchRequest>) -> Result<(StatusCode, Json<Vec<BatchItem>>), ApiError> {
    if b.notifications.is_empty() || b.notifications.len() > MAX_BATCH {
        return Err(ApiError::BadRequest(format!("batch size must be 1..={MAX_BATCH}")));
    }
    let items = enqueue_many(&s, b.notifications)
        .await
        .into_iter()
        .map(|r| match r {
            Ok(a) => BatchItem { id: Some(a.id), duplicate: a.duplicate, error: None },
            Err(ApiError::BadRequest(m)) => BatchItem { id: None, duplicate: false, error: Some(m) },
            Err(ApiError::Overloaded) => BatchItem { id: None, duplicate: false, error: Some("overloaded".into()) },
            Err(ApiError::Internal(m) | ApiError::Unavailable(m)) => BatchItem { id: None, duplicate: false, error: Some(m) },
        })
        .collect();
    Ok((StatusCode::MULTI_STATUS, Json(items)))
}

async fn get_status(State(s): State<AppState>, Path(id): Path<Uuid>) -> Response {
    let mut r = s.redis.clone();
    match r.get::<_, Option<String>>(status_key(&id)).await {
        Ok(Some(st)) => Json(serde_json::json!({ "id": id, "status": st })).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => ApiError::Internal(format!("redis: {e}")).into_response(),
    }
}

#[derive(Deserialize)]
struct InAppTokenRequest {
    user_id: String,
    ttl_seconds: Option<u64>,
}

/// Mints a short-lived token for one user to connect to the in-app gateway. Call it from your
/// backend (after your own login) and hand the token to the app; never ship the secret.
async fn inapp_token(State(s): State<AppState>, Json(r): Json<InAppTokenRequest>) -> Result<Json<serde_json::Value>, ApiError> {
    let Some(secret) = s.inapp_secret.as_deref() else {
        return Err(ApiError::Unavailable("in-app tokens are disabled: set INAPP_TOKEN_SECRET".into()));
    };
    if r.user_id.is_empty() || r.user_id.len() > 128 {
        return Err(ApiError::BadRequest("user_id must be 1..=128 chars".into()));
    }
    let ttl = r.ttl_seconds.unwrap_or(900).clamp(30, 86_400);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs();
    let token = common::inapp::mint(secret, &r.user_id, now, ttl).map_err(|e| ApiError::Internal(e.to_string()))?;
    Ok(Json(serde_json::json!({ "token": token, "expires_at": now + ttl })))
}

async fn index() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "service": "notify-api",
        "endpoints": {
            "POST /v1/notifications": "enqueue one notification (202, or 200 if idempotent duplicate)",
            "POST /v1/notifications/batch": "enqueue up to 1000: {\"notifications\": [...]} (207)",
            "GET /v1/notifications/{id}": "status: queued | retrying | sent | failed | expired | suppressed",
            "POST /v1/inapp/token": "token for the in-app WebSocket gateway: {\"user_id\": \"u42\", \"ttl_seconds\": 900}",
            "GET /healthz": "liveness",
            "GET /metrics": "Prometheus metrics"
        },
        "example": {
            "idempotency_key": "order-123-shipped", "user_id": "u42", "channel": "push",
            "priority": "high", "template": "order_shipped", "data": {"order": "123"}, "ttl_seconds": 3600
        },
        "dashboard": "http://localhost:3001"
    }))
}

async fn track_http(req: Request, next: Next) -> Response {
    let route = req.extensions().get::<MatchedPath>().map(|p| p.as_str().to_owned()).unwrap_or_else(|| "unmatched".into());
    let method = req.method().to_string();
    let start = Instant::now();
    let resp = next.run(req).await;
    let status = resp.status().as_u16().to_string();
    counter!("http_requests_total", "route" => route.clone(), "method" => method.clone(), "status" => status).increment(1);
    histogram!("http_request_duration_seconds", "route" => route, "method" => method).record(start.elapsed().as_secs_f64());
    resp
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing();
    let metrics = telemetry::init_metrics();
    let redis_client = redis::Client::open(env_or("REDIS_URL", "redis://127.0.0.1:16379"))?;
    let inapp_secret = std::env::var("INAPP_TOKEN_SECRET").ok().filter(|v| !v.is_empty());
    if let Some(sec) = &inapp_secret {
        anyhow::ensure!(
            sec.len() >= common::inapp::MIN_SECRET_LEN,
            "INAPP_TOKEN_SECRET must be at least {} characters",
            common::inapp::MIN_SECRET_LEN
        );
    }
    let state = AppState {
        producer: kafka::producer(),
        redis: ConnectionManager::new(redis_client).await?,
        metrics,
        inapp_secret: inapp_secret.map(Into::into),
    };

    let app = Router::new()
        .route("/v1/notifications", post(create))
        .route("/v1/notifications/batch", post(create_batch))
        .route("/v1/notifications/{id}", get(get_status))
        .route("/v1/inapp/token", post(inapp_token))
        .route_layer(middleware::from_fn(track_http))
        .route("/", get(index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(|State(s): State<AppState>| async move { s.metrics.render() }))
        .with_state(state);

    let addr = env_or("BIND_ADDR", "0.0.0.0:8080");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "api listening");
    axum::serve(listener, app).with_graceful_shutdown(common::shutdown::signal()).await?;
    Ok(())
}
