/// Resolves on Ctrl-C (SIGINT) or SIGTERM. Docker and Kubernetes stop containers with
/// SIGTERM, and a process running as PID 1 ignores it unless it installs a handler.
pub async fn signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!(%e, "can't listen for SIGTERM");
                std::future::pending::<()>().await
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => tracing::info!("SIGINT received, shutting down"),
        _ = term => tracing::info!("SIGTERM received, shutting down"),
    }
}
