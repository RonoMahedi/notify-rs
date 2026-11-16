//! Campaign service: audience (contacts, devices, Excel/CSV import, identity sync),
//! cohorts (all / rule / static) and campaigns that fan out through the ingest API
//! on the `bulk` priority lane.

mod auth;
mod campaigns;
mod contacts;
mod error;
mod history;
mod imports;
mod rules;
mod segments;
mod sync;
mod system;
mod templates;

use std::{sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, State},
    middleware,
    routing::{delete, get, patch, post},
    Router,
};
use common::config::{env_or, env_parse};
use redis::aio::ConnectionManager;
use sqlx::{postgres::PgPoolOptions, PgPool};

#[derive(Clone)]
pub struct App {
    pub db: PgPool,
    pub redis: ConnectionManager,
    pub http: reqwest::Client,
    pub api_url: String,
    pub phone: imports::PhoneCfg,
    pub identity_enabled: bool,
    pub sync_now: Arc<tokio::sync::Notify>,
    /// Optional key for scripts/automation (Bearer). The UI signs in with email + password.
    pub api_key: Option<String>,
    /// Add `Secure` to the session cookie (set true behind HTTPS).
    pub cookie_secure: bool,
    pub prometheus_url: String,
    pub inspector: system::Inspector,
}

const ADMIN_HTML: &str = include_str!("../ui/index.html");

/// GET /v1/stats — overview numbers for the admin UI (cheap queries only).
async fn stats(State(app): State<App>) -> error::ApiResult<axum::Json<serde_json::Value>> {
    let contacts: Option<f32> =
        sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE relname = 'contacts'").fetch_optional(&app.db).await?;
    let devices: i64 = sqlx::query_scalar("SELECT count(*) FROM devices").fetch_one(&app.db).await?;
    let segments: i64 = sqlx::query_scalar("SELECT count(*) FROM segments").fetch_one(&app.db).await?;
    let by: Vec<(String, i64)> = sqlx::query_as("SELECT status, count(*) FROM campaigns GROUP BY status").fetch_all(&app.db).await?;
    Ok(axum::Json(serde_json::json!({
        // reltuples is -1 before the first ANALYZE on PG14+
        "contacts_estimate": contacts.unwrap_or(0.0).max(0.0) as i64,
        "devices": devices,
        "segments": segments,
        "campaigns_by_status": by.into_iter().collect::<std::collections::HashMap<_, _>>(),
    })))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    common::telemetry::init_tracing();
    let metrics = common::telemetry::init_metrics();

    let api_key = std::env::var("ADMIN_TOKEN").ok().filter(|t| !t.is_empty());
    anyhow::ensure!(api_key.as_ref().is_none_or(|k| k.len() >= 16), "ADMIN_TOKEN (API key) must be >= 16 chars when set");

    let db = PgPoolOptions::new()
        .max_connections(env_parse("DB_MAX_CONNECTIONS", 20))
        .acquire_timeout(Duration::from_secs(10))
        .connect(&env_or("DATABASE_URL", "postgres://notify:notify@localhost:15432/notify"))
        .await?;
    sqlx::migrate!("./migrations").run(&db).await?;

    // Account management CLI: `campaign create-admin <email> [role]`, `campaign reset-password <email>`.
    let args: Vec<String> = std::env::args().collect();
    if matches!(args.get(1).map(String::as_str), Some("create-admin" | "reset-password")) {
        return auth::cli(&args, &db, &env_or("REDIS_URL", "redis://127.0.0.1:16379")).await;
    }
    let admins: i64 = sqlx::query_scalar("SELECT count(*) FROM admin_users WHERE role = 'admin' AND NOT disabled").fetch_one(&db).await?;
    if admins == 0 {
        tracing::warn!("no admin account yet: run `docker compose exec campaign campaign create-admin you@example.com`");
    }

    let identity = match std::env::var("IDENTITY_DATABASE_URL") {
        Ok(url) if !url.is_empty() => Some(
            PgPoolOptions::new()
                .max_connections(2) // be gentle with the identity service's database
                .acquire_timeout(Duration::from_secs(10))
                .connect_lazy(&url)?,
        ),
        _ => None,
    };

    let redis = ConnectionManager::new(redis::Client::open(env_or("REDIS_URL", "redis://127.0.0.1:16379"))?).await?;
    let app = App {
        db,
        redis,
        http: reqwest::Client::builder().timeout(Duration::from_secs(30)).build()?,
        api_url: env_or("NOTIFY_API_URL", "http://localhost:8080"),
        phone: imports::PhoneCfg { cc: env_or("DEFAULT_COUNTRY_CODE", "880"), national_len: env_parse("NATIONAL_NUMBER_LEN", 10) },
        identity_enabled: identity.is_some(),
        sync_now: Arc::new(tokio::sync::Notify::new()),
        api_key,
        cookie_secure: env_parse("COOKIE_SECURE", false),
        prometheus_url: env_or("PROMETHEUS_URL", "http://localhost:9090"),
        inspector: system::Inspector::new(),
    };

    if let Some(src) = identity {
        tokio::spawn(sync::run_loop(app.clone(), src));
    } else {
        tracing::warn!("IDENTITY_DATABASE_URL not set: identity sync disabled");
    }
    tokio::spawn(campaigns::run_loop(app.clone()));
    tokio::spawn(history::run_writer(app.clone()));
    tokio::spawn(templates::sync_loop(app.clone()));
    tokio::spawn(contacts::devices_mirror_loop(app.clone()));
    tokio::spawn(contacts::optouts_mirror_loop(app.clone()));

    let max_upload: usize = env_parse("MAX_UPLOAD_MB", 100) * 1024 * 1024;
    let v1 = Router::new()
        .route("/contacts", post(contacts::upsert).get(contacts::list))
        .route("/contacts/{user_id}/notifications", get(history::for_person))
        .route("/notifications/{id}/events", get(history::events))
        .route("/deliveries", get(history::feed))
        .route("/deliveries/summary", get(history::summary))
        .route("/templates", get(templates::list))
        .route("/templates/preview", post(templates::preview))
        .route("/templates/{channel}/{key}", axum::routing::put(templates::upsert).delete(templates::delete))
        .route("/system", get(system::overview))
        .route("/system/dlq", get(system::dlq))
        .route("/contacts/{user_id}", get(contacts::get))
        .route("/contacts/{user_id}/unsubscribe", post(contacts::unsubscribe))
        .route("/contacts/{user_id}/resubscribe", post(contacts::resubscribe))
        .route("/devices", post(contacts::register_device))
        .route("/devices/{token}", delete(contacts::delete_device))
        .route("/imports", post(imports::create).get(imports::list).layer(DefaultBodyLimit::max(max_upload)))
        .route("/imports/{id}", get(imports::get))
        .route("/segments", post(segments::create).get(segments::list))
        .route("/segments/preview", post(segments::preview_adhoc))
        .route("/segments/{id}", get(segments::get).delete(segments::delete))
        .route("/segments/{id}/preview", get(segments::preview_saved))
        .route("/campaigns", post(campaigns::create).get(campaigns::list))
        .route("/campaigns/{id}", get(campaigns::get))
        .route("/campaigns/{id}/start", post(campaigns::start))
        .route("/campaigns/{id}/pause", post(campaigns::pause))
        .route("/campaigns/{id}/resume", post(campaigns::resume))
        .route("/campaigns/{id}/cancel", post(campaigns::cancel))
        .route("/stats", get(stats))
        .route("/sync", get(sync::status))
        .route("/sync/run", post(sync::trigger))
        .route("/auth/me", get(auth::me))
        .route("/auth/password", post(auth::change_password))
        .route("/users", get(auth::list_users).post(auth::create_user))
        .route("/users/{id}", patch(auth::update_user))
        .route_layer(middleware::from_fn_with_state(app.clone(), auth::require_auth));
    let public = Router::new().route("/auth/login", post(auth::login)).route("/auth/logout", post(auth::logout));
    let v1 = v1.merge(public).layer(middleware::from_fn(auth::origin_guard));

    let router = Router::new()
        .nest("/v1", v1)
        // The page itself is public; its API calls need a signed-in session.
        .route("/admin", get(|| async {
            ([("content-security-policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"),
              ("x-content-type-options", "nosniff")],
             axum::response::Html(ADMIN_HTML))
        }))
        .route("/", get(|| async { axum::response::Redirect::temporary("/admin") }))
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(move || { let m = metrics.clone(); async move { m.render() } }))
        .with_state(app);

    let addr = env_or("BIND_ADDR", "0.0.0.0:8090");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "campaign service listening");
    // ConnectInfo: sign-in rate limits are per client IP.
    axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>())
        .with_graceful_shutdown(common::shutdown::signal())
        .await?;
    Ok(())
}
