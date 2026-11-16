use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

pub struct AppError(pub StatusCode, pub String);
pub type ApiResult<T> = Result<T, AppError>;

impl AppError {
    pub fn bad(m: impl Into<String>) -> Self {
        AppError(StatusCode::BAD_REQUEST, m.into())
    }
    pub fn not_found(m: impl Into<String>) -> Self {
        AppError(StatusCode::NOT_FOUND, m.into())
    }
    pub fn conflict(m: impl Into<String>) -> Self {
        AppError(StatusCode::CONFLICT, m.into())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        match &e {
            sqlx::Error::RowNotFound => AppError::not_found("not found"),
            sqlx::Error::Database(d) if d.code().as_deref() == Some("23505") => {
                AppError::conflict("already exists (name must be unique)")
            }
            sqlx::Error::Database(d) if d.code().as_deref() == Some("23503") => {
                AppError::conflict("referenced record missing or still in use")
            }
            sqlx::Error::Database(d) if d.code().as_deref() == Some("57014") => {
                AppError(StatusCode::GATEWAY_TIMEOUT, "this took too long; narrow it down and try again".into())
            }
            _ => {
                tracing::error!(err = %e, "database error");
                AppError(StatusCode::INTERNAL_SERVER_ERROR, "database error".into())
            }
        }
    }
}

impl From<redis::RedisError> for AppError {
    fn from(e: redis::RedisError) -> Self {
        tracing::error!(err = %e, "redis error");
        AppError(StatusCode::INTERNAL_SERVER_ERROR, "redis error".into())
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!(err = %e, "internal error");
        AppError(StatusCode::INTERNAL_SERVER_ERROR, "internal error".into())
    }
}
