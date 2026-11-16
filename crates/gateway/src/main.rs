//! In-app gateway: live notifications over WebSocket plus a small inbox REST API.
//!
//! Workers (IN_APP_PROVIDER=inbox) store each in-app notification in the user's Redis inbox and
//! PUBLISH it. Every gateway instance subscribes and forwards to the users connected to it, so
//! instances scale horizontally behind any load balancer without sticky sessions.
//!
//!   GET  /v1/inbox/ws?token=...      WebSocket (token may also be an Authorization: Bearer header)
//!   GET  /v1/inbox?limit=&before=    newest items first, plus the unread count
//!   POST /v1/inbox/read              {"up_to_ms": optional} marks items read (default: all)
//!
//! Frames, server -> client (JSON text):
//!   {"type":"hello","unread":3,"read_up_to_ms":...}   on connect
//!   {"type":"notification","item":{...}}               a new item (dedupe by item.id)
//!   {"type":"unread","unread":0,"read_up_to_ms":...}   after a read
//!   {"type":"resync"}                                  live feed was interrupted: re-fetch /v1/inbox
//!   {"type":"pong"}
//! Client -> server: {"type":"read","up_to_ms":optional} and {"type":"ping"}.

mod hub;

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{
        ws::{CloseFrame, Message, Utf8Bytes, WebSocket, WebSocketUpgrade},
        Query, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use common::{
    config::{env_or, env_parse},
    inapp::{self, InboxItem, LiveMessage},
    telemetry,
};
use futures::{SinkExt, StreamExt};
use hub::Hub;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusHandle;
use redis::{aio::ConnectionManager, Script};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::watch;
use tower_http::cors::{AllowOrigin, CorsLayer};

const PING_EVERY: Duration = Duration::from_secs(25);
const IDLE_TIMEOUT: Duration = Duration::from_secs(70);

#[derive(Clone)]
struct AppState {
    redis: ConnectionManager,
    secret: Arc<str>,
    hub: Arc<Hub>,
    shutdown: watch::Receiver<bool>,
    max_per_user: usize,
    ttl_secs: u64,
    allowed_origins: Arc<Vec<String>>,
    metrics: PrometheusHandle,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn error(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({ "error": msg }))).into_response()
}

// ---------- auth ----------

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers.get(header::AUTHORIZATION)?.to_str().ok()?.strip_prefix("Bearer ").map(str::trim)
}

fn authenticate(state: &AppState, token: Option<&str>) -> Result<String, (StatusCode, &'static str)> {
    let Some(token) = token.filter(|t| !t.is_empty()) else {
        counter!("inapp_auth_failures_total", "reason" => "missing").increment(1);
        return Err((StatusCode::UNAUTHORIZED, "missing token"));
    };
    inapp::verify(&state.secret, token).map_err(|_| {
        counter!("inapp_auth_failures_total", "reason" => "invalid").increment(1);
        (StatusCode::UNAUTHORIZED, "invalid or expired token")
    })
}

/// Browsers send Origin on WebSocket upgrades. With an allow-list configured, refuse other
/// sites. Native apps send no Origin and rely on the token alone.
fn origin_allowed(state: &AppState, headers: &HeaderMap) -> bool {
    if state.allowed_origins.is_empty() {
        return true;
    }
    match headers.get(header::ORIGIN).and_then(|o| o.to_str().ok()) {
        None => true,
        Some(o) => state.allowed_origins.iter().any(|a| a == o),
    }
}

// ---------- inbox storage ----------

/// Returns (read_up_to_ms, unread).
async fn unread(state: &AppState, user: &str) -> redis::RedisResult<(u64, u64)> {
    let script = Script::new(
        r"local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
          return {cur, redis.call('ZCOUNT', KEYS[2], '(' .. cur, '+inf')}",
    );
    let mut conn = state.redis.clone();
    script.key(inapp::seen_key(user)).key(inapp::inbox_key(user)).invoke_async(&mut conn).await
}

/// Moves the read watermark forward (never back). Returns (read_up_to_ms, unread).
async fn mark_read(state: &AppState, user: &str, up_to_ms: Option<u64>) -> redis::RedisResult<(u64, u64)> {
    let script = Script::new(
        r"local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
          local v = tonumber(ARGV[1])
          if v > cur then redis.call('SET', KEYS[1], ARGV[1]); cur = v end
          redis.call('EXPIRE', KEYS[1], ARGV[2])
          return {cur, redis.call('ZCOUNT', KEYS[2], '(' .. cur, '+inf')}",
    );
    let up_to = up_to_ms.unwrap_or(u64::MAX).min(now_ms());
    let mut conn = state.redis.clone();
    script.key(inapp::seen_key(user)).key(inapp::inbox_key(user)).arg(up_to).arg(state.ttl_secs).invoke_async(&mut conn).await
}

fn unavailable(e: redis::RedisError) -> Response {
    tracing::warn!(error = %e, "inbox store unavailable");
    error(StatusCode::SERVICE_UNAVAILABLE, "inbox temporarily unavailable")
}

// ---------- REST ----------

#[derive(Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    /// Only items created before this time (ms): pass the last item's created_at_ms to page.
    before: Option<u64>,
}

async fn list(State(state): State<AppState>, headers: HeaderMap, Query(q): Query<ListQuery>) -> Response {
    let user = match authenticate(&state, bearer(&headers)) {
        Ok(u) => u,
        Err((st, m)) => return error(st, m),
    };
    let limit = q.limit.unwrap_or(20).clamp(1, 100);
    let max = q.before.map(|b| format!("({b}")).unwrap_or_else(|| "+inf".into());
    let mut conn = state.redis.clone();
    let raw: Vec<String> = match redis::cmd("ZRANGE")
        .arg(inapp::inbox_key(&user))
        .arg(&max)
        .arg("-inf")
        .arg("BYSCORE")
        .arg("REV")
        .arg("LIMIT")
        .arg(0)
        .arg(limit)
        .query_async(&mut conn)
        .await
    {
        Ok(v) => v,
        Err(e) => return unavailable(e),
    };
    let (read_up_to_ms, unread) = match unread(&state, &user).await {
        Ok(v) => v,
        Err(e) => return unavailable(e),
    };
    let items: Vec<InboxItem> = raw.iter().filter_map(|s| serde_json::from_str(s).ok()).collect();
    let items: Vec<Value> = items
        .into_iter()
        .map(|i| {
            let read = i.created_at_ms <= read_up_to_ms;
            let mut v = serde_json::to_value(i).unwrap_or_default();
            v["read"] = json!(read);
            v
        })
        .collect();
    Json(json!({ "items": items, "unread": unread, "read_up_to_ms": read_up_to_ms })).into_response()
}

#[derive(Deserialize, Default)]
struct ReadBody {
    up_to_ms: Option<u64>,
}

async fn read(State(state): State<AppState>, headers: HeaderMap, body: Option<Json<ReadBody>>) -> Response {
    let user = match authenticate(&state, bearer(&headers)) {
        Ok(u) => u,
        Err((st, m)) => return error(st, m),
    };
    match mark_read(&state, &user, body.unwrap_or_default().0.up_to_ms).await {
        Ok((read_up_to_ms, unread)) => Json(json!({ "unread": unread, "read_up_to_ms": read_up_to_ms })).into_response(),
        Err(e) => unavailable(e),
    }
}

// ---------- WebSocket ----------

#[derive(Deserialize)]
struct WsQuery {
    token: Option<String>,
}

async fn ws(State(state): State<AppState>, headers: HeaderMap, Query(q): Query<WsQuery>, upgrade: WebSocketUpgrade) -> Response {
    if !origin_allowed(&state, &headers) {
        counter!("inapp_auth_failures_total", "reason" => "origin").increment(1);
        return error(StatusCode::FORBIDDEN, "origin not allowed");
    }
    let user = match authenticate(&state, q.token.as_deref().or(bearer(&headers))) {
        Ok(u) => u,
        Err((st, m)) => return error(st, m),
    };
    let Some((conn_id, rx)) = state.hub.register(&user, state.max_per_user) else {
        counter!("inapp_rejected_total", "reason" => "too_many_connections").increment(1);
        return error(StatusCode::TOO_MANY_REQUESTS, "too many open connections for this user");
    };
    upgrade.max_message_size(4096).on_upgrade(move |socket| async move {
        session(socket, &state, &user, rx).await;
        state.hub.unregister(&user, conn_id);
    })
}

fn frame(v: Value) -> Message {
    Message::Text(v.to_string().into())
}

fn close(code: u16, reason: &'static str) -> Message {
    Message::Close(Some(CloseFrame { code, reason: reason.into() }))
}

async fn session(socket: WebSocket, state: &AppState, user: &str, mut rx: tokio::sync::mpsc::Receiver<Utf8Bytes>) {
    let (mut sink, mut stream) = socket.split();
    let mut shutdown = state.shutdown.clone();
    let hello = match unread(state, user).await {
        Ok((read_up_to_ms, unread)) => json!({ "type": "hello", "unread": unread, "read_up_to_ms": read_up_to_ms }),
        Err(_) => json!({ "type": "hello", "unread": null }),
    };
    if sink.send(frame(hello)).await.is_err() {
        return;
    }
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    let mut last_heard = Instant::now();
    loop {
        tokio::select! {
            out = rx.recv() => match out {
                Some(text) => {
                    if sink.send(Message::Text(text)).await.is_err() { break; }
                    counter!("inapp_frames_total", "result" => "sent").increment(1);
                }
                // Dropped from the hub for falling behind.
                None => { let _ = sink.send(close(1013, "too slow, reconnect")).await; break; }
            },
            incoming = stream.next() => {
                let Some(Ok(msg)) = incoming else { break };
                last_heard = Instant::now();
                match msg {
                    Message::Text(t) => {
                        let v: Value = serde_json::from_str(t.as_str()).unwrap_or(Value::Null);
                        let reply = match v["type"].as_str() {
                            Some("read") => match mark_read(state, user, v["up_to_ms"].as_u64()).await {
                                Ok((read_up_to_ms, unread)) => Some(json!({ "type": "unread", "unread": unread, "read_up_to_ms": read_up_to_ms })),
                                Err(_) => Some(json!({ "type": "error", "error": "inbox temporarily unavailable" })),
                            },
                            Some("ping") => Some(json!({ "type": "pong" })),
                            _ => None,
                        };
                        if let Some(r) = reply {
                            if sink.send(frame(r)).await.is_err() { break; }
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            },
            _ = ping.tick() => {
                if last_heard.elapsed() > IDLE_TIMEOUT { break; }
                if sink.send(Message::Ping(Default::default())).await.is_err() { break; }
            },
            _ = shutdown.changed() => {
                // 1012 = service restart: clients reconnect, landing on another instance.
                let _ = sink.send(close(1012, "server restarting")).await;
                break;
            }
        }
    }
}

// ---------- Redis subscription ----------

/// Forwards published items to local connections. Reconnects forever; after a gap, tells every
/// client to re-fetch the inbox, since anything published meanwhile was missed live.
async fn subscribe_loop(client: redis::Client, hub: Arc<Hub>) {
    let resync: Utf8Bytes = json!({ "type": "resync" }).to_string().into();
    let mut first = true;
    let mut backoff = Duration::from_millis(500);
    loop {
        match client.get_async_pubsub().await {
            Ok(mut ps) => match ps.subscribe(inapp::CHANNEL).await {
                Ok(()) => {
                    tracing::info!("subscribed to in-app feed");
                    gauge!("inapp_subscribed").set(1.0);
                    backoff = Duration::from_millis(500);
                    if !first {
                        hub.broadcast(&resync);
                    }
                    first = false;
                    let mut msgs = ps.on_message();
                    while let Some(m) = msgs.next().await {
                        let Ok(payload) = m.get_payload::<String>() else { continue };
                        let Ok(live) = serde_json::from_str::<LiveMessage>(&payload) else {
                            counter!("inapp_bad_messages_total").increment(1);
                            continue;
                        };
                        let text: Utf8Bytes = json!({ "type": "notification", "item": live.item }).to_string().into();
                        let f = hub.send_to(&live.user_id, &text);
                        if f.dropped_slow > 0 {
                            counter!("inapp_frames_total", "result" => "dropped_slow").increment(f.dropped_slow as u64);
                        }
                    }
                    tracing::warn!("in-app feed subscription lost");
                }
                Err(e) => tracing::warn!(error = %e, "subscribing to in-app feed failed"),
            },
            Err(e) => tracing::warn!(error = %e, "connecting to Redis for the in-app feed failed"),
        }
        gauge!("inapp_subscribed").set(0.0);
        counter!("inapp_pubsub_reconnects_total").increment(1);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

// ---------- main ----------

fn cors(origins: &[String]) -> Option<CorsLayer> {
    if origins.is_empty() {
        return None;
    }
    let list: Vec<HeaderValue> = origins.iter().filter_map(|o| HeaderValue::from_str(o).ok()).collect();
    Some(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(list))
            .allow_methods([Method::GET, Method::POST])
            .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
            .max_age(Duration::from_secs(600)),
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_tracing();
    let metrics = telemetry::init_metrics();

    let secret = std::env::var("INAPP_TOKEN_SECRET").unwrap_or_default();
    if secret.len() < inapp::MIN_SECRET_LEN {
        anyhow::bail!("INAPP_TOKEN_SECRET must be set to at least {} random characters", inapp::MIN_SECRET_LEN);
    }
    if secret.contains("change-me") {
        tracing::warn!("INAPP_TOKEN_SECRET is the development default: set your own before going live");
    }
    let redis_url = std::env::var("INBOX_REDIS_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .unwrap_or_else(|| env_or("REDIS_URL", "redis://127.0.0.1:16379"));
    let client = redis::Client::open(redis_url)?;
    let allowed_origins: Vec<String> = env_or("INAPP_ALLOWED_ORIGINS", "")
        .split(',')
        .map(|s| s.trim().trim_end_matches('/').to_string())
        .filter(|s| !s.is_empty())
        .collect();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let hub = Arc::new(Hub::default());
    let state = AppState {
        redis: ConnectionManager::new(client.clone()).await?,
        secret: secret.into(),
        hub: hub.clone(),
        shutdown: shutdown_rx,
        max_per_user: env_parse("INAPP_MAX_CONNECTIONS_PER_USER", 10),
        ttl_secs: env_parse("INBOX_TTL_SECS", inapp::DEFAULT_TTL_SECS),
        allowed_origins: Arc::new(allowed_origins.clone()),
        metrics,
    };
    tokio::spawn(subscribe_loop(client, hub));

    let mut api = Router::new().route("/v1/inbox", get(list)).route("/v1/inbox/read", post(read));
    if let Some(c) = cors(&allowed_origins) {
        api = api.layer(c);
    }
    let app = Router::new()
        .route("/v1/inbox/ws", get(ws))
        .merge(api)
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/metrics",
            get(|State(s): State<AppState>| async move {
                gauge!("inapp_connections").set(s.hub.connections() as f64);
                s.metrics.render()
            }),
        )
        .with_state(state);

    let addr = env_or("BIND_ADDR", "0.0.0.0:8070");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, origins = ?allowed_origins, "in-app gateway listening");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async move {
            common::shutdown::signal().await;
            // Tell open sockets to close (1012) so graceful shutdown doesn't wait on them.
            let _ = shutdown_tx.send(true);
        })
        .await?;
    Ok(())
}
