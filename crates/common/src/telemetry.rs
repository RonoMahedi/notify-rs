use metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle};
use tracing_subscriber::EnvFilter;

pub fn init_tracing() {
    tracing_subscriber::fmt().with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).json().init();
}

pub fn init_metrics() -> PrometheusHandle {
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full("notification_e2e_latency_seconds".into()),
            &[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 900.0],
        )
        .unwrap()
        .set_buckets_for_metric(
            Matcher::Suffix("duration_seconds".into()),
            &[0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        )
        .unwrap()
        .install_recorder()
        .expect("prometheus recorder")
}
