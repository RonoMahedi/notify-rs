//! Load generator: drives the API at TARGET_RPS using the batch endpoint.
//! 100M/day ~= 1,157/s average; try TARGET_RPS=12000 to simulate a 10x peak.

use std::{sync::Arc, time::Duration};

use common::config::{env_or, env_parse};
use rand::Rng;
use serde_json::json;
use tokio::sync::Semaphore;

fn pick_channel(r: f64) -> &'static str {
    match r {
        x if x < 0.70 => "push",
        x if x < 0.90 => "email",
        x if x < 0.95 => "sms",
        _ => "in_app",
    }
}

#[tokio::main]
async fn main() {
    let url = format!("{}/v1/notifications/batch", env_or("API_URL", "http://localhost:8080"));
    let rps: u64 = env_parse("TARGET_RPS", 2_000);
    let batch: u64 = env_parse("BATCH", 100);
    let ticks_per_sec = (rps / batch).max(1);
    let client = reqwest::Client::builder().pool_max_idle_per_host(256).build().unwrap();
    let inflight = Arc::new(Semaphore::new(512));
    let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / ticks_per_sec));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    println!("loadgen: {rps} notifications/s ({batch}/request) -> {url}");

    loop {
        tick.tick().await;
        let Ok(permit) = inflight.clone().try_acquire_owned() else {
            eprintln!("loadgen saturated: API too slow, skipping tick");
            continue;
        };
        let body = {
            let mut rng = rand::rng();
            let items: Vec<_> = (0..batch)
                .map(|_| {
                    let high = rng.random::<f64>() < 0.1;
                    json!({
                        "idempotency_key": uuid::Uuid::new_v4().to_string(),
                        "user_id": format!("user-{}", rng.random_range(0..10_000_000u32)),
                        "channel": pick_channel(rng.random()),
                        "priority": if high { "high" } else { "normal" },
                        "template": if high { "otp" } else { "promo_weekly" },
                        "data": { "code": rng.random_range(100_000..999_999u32) },
                        "ttl_seconds": if high { 300 } else { 86_400 }
                    })
                })
                .collect();
            json!({ "notifications": items })
        };
        let (client, url) = (client.clone(), url.clone());
        tokio::spawn(async move {
            let _permit = permit;
            match client.post(&url).json(&body).send().await {
                Ok(r) if !r.status().is_success() => eprintln!("HTTP {}", r.status()),
                Err(e) => eprintln!("request error: {e}"),
                _ => {}
            }
        });
    }
}
