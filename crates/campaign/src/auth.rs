//! Email + password sign-in for the admin UI.
//!
//! - Passwords: Argon2id (PHC string), verified on a blocking thread.
//! - Sessions: random 256-bit token in an HttpOnly, SameSite=Strict cookie; only its
//!   SHA-256 is stored, so a leaked sessions table can't be replayed. 12 h absolute expiry.
//! - Brute force: 5 failures per email -> 15 min lockout (Redis), counted before the
//!   password check so parallel guesses can't race past it; plus a per-IP failure budget
//!   (LOGIN_MAX_FAILS_PER_IP, default 100 / 15 min) and a cap on concurrent Argon2 work.
//!   One message for unknown email and wrong password, and a dummy hash keeps timing the same.
//! - CSRF: SameSite=Strict + a same-origin check on every state-changing request.
//! - Roles: `admin` (everything + user management) and `marketer` (no user management).
//! - Automation: an optional API key (ADMIN_TOKEN) as a Bearer token acts as admin.

use std::sync::OnceLock;

use argon2::{
    password_hash::{rand_core::OsRng, rand_core::RngCore, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use axum::{
    extract::{ConnectInfo, Path, Request, State},
    http::{header, HeaderMap, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension, Json,
};
use metrics::counter;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{error::*, App};

pub const COOKIE: &str = "notify_session";
const SESSION_SECS: i64 = 12 * 3600;
const MAX_FAILS: i64 = 5;
const LOCK_SECS: i64 = 15 * 60;
const MIN_PASSWORD: usize = 10;
const MAX_PASSWORD: usize = 256; // bounds Argon2 work per request

#[derive(Clone, Debug, Serialize)]
pub struct Principal {
    /// None for the API key.
    pub user_id: Option<Uuid>,
    pub email: String,
    pub name: Option<String>,
    pub role: String,
}

impl Principal {
    pub fn require_admin(&self) -> ApiResult<()> {
        if self.role == "admin" {
            Ok(())
        } else {
            Err(AppError(StatusCode::FORBIDDEN, "admins only".into()))
        }
    }
}

// ------------------------------------------------------------------ primitives

pub fn hash_password(pw: &str) -> anyhow::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default().hash_password(pw.as_bytes(), &salt).map(|h| h.to_string()).map_err(|e| anyhow::anyhow!("hash: {e}"))
}

pub fn verify_password(pw: &str, hash: &str) -> bool {
    PasswordHash::new(hash).map(|h| Argon2::default().verify_password(pw.as_bytes(), &h).is_ok()).unwrap_or(false)
}

/// Same cost as a real verify, so "no such user" isn't faster than "wrong password".
fn dummy_hash() -> &'static str {
    static H: OnceLock<String> = OnceLock::new();
    H.get_or_init(|| hash_password("timing-equalizer-not-a-real-password").expect("hash"))
}

pub fn check_policy(pw: &str) -> Result<(), String> {
    if pw.chars().count() < MIN_PASSWORD {
        Err(format!("password must be at least {MIN_PASSWORD} characters"))
    } else if pw.len() > MAX_PASSWORD {
        Err(format!("password must be at most {MAX_PASSWORD} bytes"))
    } else {
        Ok(())
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn new_token() -> String {
    let mut b = [0u8; 32];
    OsRng.fill_bytes(&mut b);
    hex(&b)
}

fn token_hash(t: &str) -> Vec<u8> {
    Sha256::digest(t.as_bytes()).to_vec()
}

/// 20 chars, no look-alikes (0/O, 1/l/I).
pub fn generate_password() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";
    let mut out = String::with_capacity(20);
    while out.len() < 20 {
        let mut b = [0u8; 1];
        OsRng.fill_bytes(&mut b);
        // rejection sampling: no modulo bias
        if (b[0] as usize) < 256 - (256 % ALPHABET.len()) {
            out.push(ALPHABET[b[0] as usize % ALPHABET.len()] as char);
        }
    }
    out
}

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn cookie_value(headers: &HeaderMap) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|kv| kv.trim().split_once('='))
        .find(|(k, _)| *k == COOKIE)
        .map(|(_, v)| v.to_string())
        .filter(|v| v.len() == 64 && v.chars().all(|c| c.is_ascii_hexdigit()))
}

fn session_cookie(app: &App, token: &str, max_age: i64) -> String {
    format!("{COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age={max_age}{}", if app.cookie_secure { "; Secure" } else { "" })
}

/// `Origin: http://localhost:8090` must match `Host: localhost:8090`.
pub fn origin_matches(origin: &str, host: &str) -> bool {
    let o = origin.strip_prefix("https://").or_else(|| origin.strip_prefix("http://")).unwrap_or(origin);
    !host.is_empty() && o.eq_ignore_ascii_case(host)
}

// ------------------------------------------------------------------ middleware

/// Blocks cross-site state-changing requests (defense in depth beside SameSite=Strict).
pub async fn origin_guard(req: Request, next: Next) -> Response {
    if !matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS) {
        let h = req.headers();
        if let Some(origin) = h.get(header::ORIGIN).and_then(|v| v.to_str().ok()) {
            let host = h.get(header::HOST).and_then(|v| v.to_str().ok()).unwrap_or("");
            if !origin_matches(origin, host) {
                counter!("admin_cross_origin_blocked_total").increment(1);
                return AppError(StatusCode::FORBIDDEN, "cross-origin request blocked".into()).into_response();
            }
        }
    }
    next.run(req).await
}

async fn load_session(app: &App, token: &str) -> Option<Principal> {
    let row: Option<(Uuid, String, Option<String>, String)> = sqlx::query_as(
        "SELECT u.id, u.email, u.name, u.role FROM admin_sessions s JOIN admin_users u ON u.id = s.user_id
         WHERE s.token_hash = $1 AND s.expires_at > now() AND NOT u.disabled",
    )
    .bind(token_hash(token))
    .fetch_optional(&app.db)
    .await
    .ok()?;
    row.map(|(id, email, name, role)| Principal { user_id: Some(id), email, name, role })
}

pub async fn require_auth(State(app): State<App>, mut req: Request, next: Next) -> Response {
    let bearer =
        req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).and_then(|v| v.strip_prefix("Bearer ")).map(str::to_string);
    let principal = match bearer {
        Some(t) => app.api_key.as_deref().filter(|k| ct_eq(t.as_bytes(), k.as_bytes())).map(|_| Principal {
            user_id: None,
            email: "api-key".into(),
            name: Some("API key".into()),
            role: "admin".into(),
        }),
        None => match cookie_value(req.headers()) {
            Some(t) => load_session(&app, &t).await,
            None => None,
        },
    };
    match principal {
        Some(p) => {
            req.extensions_mut().insert(p);
            next.run(req).await
        }
        None => AppError(StatusCode::UNAUTHORIZED, "not signed in".into()).into_response(),
    }
}

// ------------------------------------------------------------------ handlers

#[derive(Deserialize)]
pub struct LoginIn {
    email: String,
    password: String,
}

/// Failed sign-ins allowed per client IP per LOCK_SECS window, across all emails.
/// Behind a reverse proxy (or Docker Desktop's port forwarding) every client shares one
/// IP, so this acts as a global failure budget; raise LOGIN_MAX_FAILS_PER_IP if needed.
fn max_fails_per_ip() -> i64 {
    static N: OnceLock<i64> = OnceLock::new();
    *N.get_or_init(|| common::config::env_parse("LOGIN_MAX_FAILS_PER_IP", 100))
}

/// Bounds concurrent Argon2 verifications so a flood of sign-ins queues instead of
/// pinning every CPU (each verify costs ~19 MiB and tens of ms).
fn argon2_slots() -> &'static tokio::sync::Semaphore {
    static S: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    S.get_or_init(|| {
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(2);
        tokio::sync::Semaphore::new(n.max(2))
    })
}

fn too_many(ttl: i64) -> AppError {
    AppError(StatusCode::TOO_MANY_REQUESTS, format!("Too many failed attempts. Try again in {} minutes.", (ttl.max(60) + 59) / 60))
}

pub async fn login(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    Json(b): Json<LoginIn>,
) -> ApiResult<Response> {
    let email = b.email.trim().to_lowercase();
    if email.is_empty() || email.len() > 254 || b.password.is_empty() || b.password.len() > MAX_PASSWORD {
        return Err(AppError(StatusCode::UNAUTHORIZED, "Wrong email or password".into()));
    }
    // Count the attempt BEFORE checking the password. Reading the counter first and
    // incrementing after a failure let a burst of parallel guesses all see "0 failures".
    // A successful sign-in gives its slot back below.
    let key = format!("login_fail:{email}");
    let ip_key = format!("login_fail_ip:{}", peer.ip());
    let mut r = app.redis.clone();
    let (fails, ip_fails): (i64, i64) = redis::pipe().incr(&key, 1).incr(&ip_key, 1).query_async(&mut r).await?;
    if fails > MAX_FAILS || ip_fails > max_fails_per_ip() {
        // Already locked: don't extend the lock, so it ends LOCK_SECS after the last real try.
        let k = if fails > MAX_FAILS { &key } else { &ip_key };
        let ttl: i64 = r.ttl(k).await.unwrap_or(LOCK_SECS);
        let reason = if fails > MAX_FAILS { "locked" } else { "ip_limited" };
        counter!("admin_login_total", "result" => reason).increment(1);
        return Err(too_many(ttl));
    }
    // Email: the lock runs LOCK_SECS from the latest failure. IP: fixed window from the first.
    let _: () = redis::pipe()
        .expire(&key, LOCK_SECS)
        .ignore()
        .cmd("EXPIRE")
        .arg(&ip_key)
        .arg(LOCK_SECS)
        .arg("NX")
        .ignore()
        .query_async(&mut r)
        .await?;

    let row: Option<(Uuid, String, Option<String>, String, String, bool)> =
        sqlx::query_as("SELECT id, email, name, role, password_hash, disabled FROM admin_users WHERE email = $1")
            .bind(&email)
            .fetch_optional(&app.db)
            .await?;
    let hash = row.as_ref().map(|r| r.4.clone()).unwrap_or_else(|| dummy_hash().to_string());
    let pw = b.password;
    let ok = {
        let _slot = argon2_slots().acquire().await.map_err(anyhow::Error::from)?;
        tokio::task::spawn_blocking(move || verify_password(&pw, &hash)).await.unwrap_or(false)
    };

    let user = match row {
        Some(u) if ok => u,
        _ => {
            // The failure was already counted above.
            counter!("admin_login_total", "result" => "failed").increment(1);
            return Err(AppError(StatusCode::UNAUTHORIZED, "Wrong email or password".into()));
        }
    };
    if user.5 {
        counter!("admin_login_total", "result" => "disabled").increment(1);
        return Err(AppError(StatusCode::FORBIDDEN, "This account is disabled. Ask an admin.".into()));
    }
    // Right password: clear this email's failures and return the IP slot we took.
    // EXPIRE NX: if the window lapsed in between, DECR recreated the key without a TTL.
    let _: () = redis::pipe()
        .del(&key)
        .ignore()
        .decr(&ip_key, 1)
        .ignore()
        .cmd("EXPIRE")
        .arg(&ip_key)
        .arg(LOCK_SECS)
        .arg("NX")
        .ignore()
        .query_async(&mut r)
        .await?;

    let token = new_token();
    sqlx::query("INSERT INTO admin_sessions (token_hash, user_id, expires_at) VALUES ($1, $2, now() + make_interval(secs => $3))")
        .bind(token_hash(&token))
        .bind(user.0)
        .bind(SESSION_SECS as f64)
        .execute(&app.db)
        .await?;
    sqlx::query("UPDATE admin_users SET last_login_at = now() WHERE id = $1").bind(user.0).execute(&app.db).await?;
    // Opportunistic cleanup of expired sessions.
    let _ = sqlx::query("DELETE FROM admin_sessions WHERE expires_at < now()").execute(&app.db).await;
    counter!("admin_login_total", "result" => "ok").increment(1);
    tracing::info!(email = %user.1, "admin signed in");

    let body = json!({ "email": user.1, "name": user.2, "role": user.3 });
    Ok(([(header::SET_COOKIE, session_cookie(&app, &token, SESSION_SECS))], Json(body)).into_response())
}

pub async fn logout(State(app): State<App>, headers: HeaderMap) -> ApiResult<Response> {
    if let Some(t) = cookie_value(&headers) {
        sqlx::query("DELETE FROM admin_sessions WHERE token_hash = $1").bind(token_hash(&t)).execute(&app.db).await?;
    }
    Ok(([(header::SET_COOKIE, session_cookie(&app, "", 0))], StatusCode::NO_CONTENT).into_response())
}

pub async fn me(Extension(p): Extension<Principal>) -> Json<Principal> {
    Json(p)
}

#[derive(Deserialize)]
pub struct ChangePasswordIn {
    current_password: String,
    new_password: String,
}

/// Changing your password signs out every other session of yours.
pub async fn change_password(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    headers: HeaderMap,
    Json(b): Json<ChangePasswordIn>,
) -> ApiResult<Json<Value>> {
    let uid = p.user_id.ok_or_else(|| AppError::bad("the API key has no password"))?;
    check_policy(&b.new_password).map_err(AppError::bad)?;
    if b.new_password == b.current_password {
        return Err(AppError::bad("new password must differ from the current one"));
    }
    let hash: String = sqlx::query_scalar("SELECT password_hash FROM admin_users WHERE id = $1").bind(uid).fetch_one(&app.db).await?;
    let cur = b.current_password;
    if !tokio::task::spawn_blocking(move || verify_password(&cur, &hash)).await.unwrap_or(false) {
        return Err(AppError::bad("current password is wrong"));
    }
    let new = b.new_password;
    let new_hash = tokio::task::spawn_blocking(move || hash_password(&new)).await.map_err(anyhow::Error::from)??;
    let keep = cookie_value(&headers).map(|t| token_hash(&t)).unwrap_or_default();
    let mut tx = app.db.begin().await?;
    sqlx::query("UPDATE admin_users SET password_hash = $2 WHERE id = $1").bind(uid).bind(new_hash).execute(&mut *tx).await?;
    let r =
        sqlx::query("DELETE FROM admin_sessions WHERE user_id = $1 AND token_hash <> $2").bind(uid).bind(keep).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Json(json!({ "changed": true, "other_sessions_signed_out": r.rows_affected() })))
}

#[derive(Serialize, sqlx::FromRow)]
pub struct AdminUser {
    id: Uuid,
    email: String,
    name: Option<String>,
    role: String,
    disabled: bool,
    created_at: chrono::DateTime<chrono::Utc>,
    last_login_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn list_users(State(app): State<App>, Extension(p): Extension<Principal>) -> ApiResult<Json<Vec<AdminUser>>> {
    p.require_admin()?;
    let rows = sqlx::query_as("SELECT id, email, name, role, disabled, created_at, last_login_at FROM admin_users ORDER BY created_at")
        .fetch_all(&app.db)
        .await?;
    Ok(Json(rows))
}

#[derive(Deserialize)]
pub struct NewUserIn {
    email: String,
    name: Option<String>,
    role: String,
    password: String,
}

fn valid_role(r: &str) -> ApiResult<()> {
    if matches!(r, "admin" | "marketer") {
        Ok(())
    } else {
        Err(AppError::bad("role must be admin or marketer"))
    }
}

pub async fn create_user(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Json(b): Json<NewUserIn>,
) -> ApiResult<(StatusCode, Json<AdminUser>)> {
    p.require_admin()?;
    let email = crate::imports::normalize_email(&b.email).ok_or_else(|| AppError::bad("invalid email"))?;
    valid_role(&b.role)?;
    check_policy(&b.password).map_err(AppError::bad)?;
    let pw = b.password;
    let hash = tokio::task::spawn_blocking(move || hash_password(&pw)).await.map_err(anyhow::Error::from)??;
    let u: AdminUser = sqlx::query_as(
        "INSERT INTO admin_users (id, email, name, role, password_hash) VALUES ($1, $2, $3, $4, $5)
         RETURNING id, email, name, role, disabled, created_at, last_login_at",
    )
    .bind(Uuid::now_v7())
    .bind(&email)
    .bind(b.name.map(|n| n.trim().to_string()).filter(|n| !n.is_empty()))
    .bind(&b.role)
    .bind(hash)
    .fetch_one(&app.db)
    .await
    .map_err(|e| match &e {
        sqlx::Error::Database(d) if d.code().as_deref() == Some("23505") => AppError::conflict("a user with this email already exists"),
        _ => e.into(),
    })?;
    tracing::info!(by = %p.email, email = %u.email, role = %u.role, "admin user created");
    Ok((StatusCode::CREATED, Json(u)))
}

#[derive(Deserialize)]
pub struct UpdateUserIn {
    role: Option<String>,
    disabled: Option<bool>,
    password: Option<String>,
    name: Option<String>,
}

/// Disabling or resetting a password signs that user out everywhere.
pub async fn update_user(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Path(id): Path<Uuid>,
    Json(b): Json<UpdateUserIn>,
) -> ApiResult<Json<AdminUser>> {
    p.require_admin()?;
    if Some(id) == p.user_id && (b.disabled == Some(true) || b.role.as_deref().is_some_and(|r| r != "admin")) {
        return Err(AppError::bad("you can't disable or demote yourself"));
    }
    if let Some(r) = &b.role {
        valid_role(r)?;
    }
    let hash = match b.password {
        Some(pw) => {
            check_policy(&pw).map_err(AppError::bad)?;
            Some(tokio::task::spawn_blocking(move || hash_password(&pw)).await.map_err(anyhow::Error::from)??)
        }
        None => None,
    };
    let revoke = b.disabled == Some(true) || hash.is_some();
    let mut tx = app.db.begin().await?;
    let u: AdminUser = sqlx::query_as(
        "UPDATE admin_users SET role = COALESCE($2, role), disabled = COALESCE($3, disabled),
                password_hash = COALESCE($4, password_hash), name = COALESCE($5, name)
         WHERE id = $1 RETURNING id, email, name, role, disabled, created_at, last_login_at",
    )
    .bind(id)
    .bind(&b.role)
    .bind(b.disabled)
    .bind(hash)
    .bind(b.name)
    .fetch_one(&mut *tx)
    .await?;
    // Never leave the system with no enabled admin.
    let admins: i64 =
        sqlx::query_scalar("SELECT count(*) FROM admin_users WHERE role = 'admin' AND NOT disabled").fetch_one(&mut *tx).await?;
    if admins == 0 {
        return Err(AppError::conflict("at least one enabled admin must remain"));
    }
    if revoke {
        sqlx::query("DELETE FROM admin_sessions WHERE user_id = $1").bind(id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    tracing::info!(by = %p.email, email = %u.email, "admin user updated");
    Ok(Json(u))
}

// ------------------------------------------------------------------ CLI

/// `campaign create-admin <email> [admin|marketer]` and `campaign reset-password <email>`.
/// Password from NEW_PASSWORD, else a generated one printed once to stdout (never logged).
pub async fn cli(args: &[String], db: &sqlx::PgPool, redis_url: &str) -> anyhow::Result<()> {
    let usage = "usage: campaign create-admin <email> [admin|marketer]  |  campaign reset-password <email>";
    let cmd = args.get(1).map(String::as_str).unwrap_or_default();
    let email = args.get(2).and_then(|e| crate::imports::normalize_email(e)).ok_or_else(|| anyhow::anyhow!("{usage}"))?;
    let (password, generated) = match std::env::var("NEW_PASSWORD") {
        Ok(p) if !p.is_empty() => (p, false),
        _ => (generate_password(), true),
    };
    check_policy(&password).map_err(anyhow::Error::msg)?;
    let hash = hash_password(&password)?;
    match cmd {
        "create-admin" => {
            let role = args.get(3).map(String::as_str).unwrap_or("admin");
            anyhow::ensure!(matches!(role, "admin" | "marketer"), "{usage}");
            let res = sqlx::query(
                "INSERT INTO admin_users (id, email, role, password_hash) VALUES ($1, $2, $3, $4) ON CONFLICT (email) DO NOTHING",
            )
            .bind(Uuid::now_v7())
            .bind(&email)
            .bind(role)
            .bind(&hash)
            .execute(db)
            .await?;
            anyhow::ensure!(res.rows_affected() == 1, "{email} already exists; use reset-password");
            println!("Created {role} {email}");
        }
        "reset-password" => {
            let mut tx = db.begin().await?;
            let id: Option<Uuid> =
                sqlx::query_scalar("UPDATE admin_users SET password_hash = $2, disabled = false WHERE email = $1 RETURNING id")
                    .bind(&email)
                    .bind(&hash)
                    .fetch_optional(&mut *tx)
                    .await?;
            let id = id.ok_or_else(|| anyhow::anyhow!("no user {email}"))?;
            sqlx::query("DELETE FROM admin_sessions WHERE user_id = $1").bind(id).execute(&mut *tx).await?;
            tx.commit().await?;
            if let Ok(c) = redis::Client::open(redis_url) {
                if let Ok(mut r) = c.get_multiplexed_async_connection().await {
                    let _: Result<(), _> = r.del(format!("login_fail:{email}")).await;
                }
            }
            println!("Password reset for {email} (account enabled, sessions signed out, lockout cleared)");
        }
        _ => anyhow::bail!("{usage}"),
    }
    if generated {
        println!("Temporary password: {password}");
        println!("Sign in at /admin and change it under Account.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_argon2id_salted_and_verify() {
        let a = hash_password("correct horse battery").unwrap();
        let b = hash_password("correct horse battery").unwrap();
        assert!(a.starts_with("$argon2id$"));
        assert_ne!(a, b, "salted: same password, different hashes");
        assert!(verify_password("correct horse battery", &a));
        assert!(!verify_password("correct horse batterY", &a));
        assert!(!verify_password("x", "not-a-hash"));
    }

    #[test]
    fn policy() {
        assert!(check_policy("short").is_err());
        assert!(check_policy("ten chars!").is_ok());
        assert!(check_policy(&"x".repeat(300)).is_err());
    }

    #[test]
    fn generated_passwords() {
        let p = generate_password();
        assert_eq!(p.len(), 20);
        assert!(check_policy(&p).is_ok());
        assert!(!p.contains(['0', 'O', '1', 'l', 'I']));
        assert_ne!(p, generate_password());
    }

    #[test]
    fn tokens_and_cookies() {
        let t = new_token();
        assert_eq!(t.len(), 64);
        assert_eq!(token_hash(&t).len(), 32);
        let mut h = HeaderMap::new();
        h.insert(header::COOKIE, format!("a=b; {COOKIE}={t}; c=d").parse().unwrap());
        assert_eq!(cookie_value(&h).as_deref(), Some(t.as_str()));
        h.insert(header::COOKIE, format!("{COOKIE}=../../etc").parse().unwrap());
        assert_eq!(cookie_value(&h), None);
    }

    #[test]
    fn origin_check() {
        assert!(origin_matches("http://localhost:8090", "localhost:8090"));
        assert!(origin_matches("https://admin.example.com", "admin.example.com"));
        assert!(!origin_matches("http://evil.example", "localhost:8090"));
        assert!(!origin_matches("http://localhost:8090.evil.example", "localhost:8090"));
        assert!(!origin_matches("http://localhost:8090", ""));
    }
}
