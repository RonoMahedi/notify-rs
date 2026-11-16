use metrics::gauge;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{ConsumerContext, StreamConsumer};
use rdkafka::producer::FutureProducer;
use rdkafka::statistics::Statistics;

use crate::config::env_or;

pub fn brokers() -> String {
    env_or("KAFKA_BROKERS", "localhost:19092")
}

/// Producer tuned for throughput + durability:
/// idempotent, acks=all, lz4, small linger to batch.
pub fn producer() -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("compression.type", "lz4")
        .set("linger.ms", env_or("KAFKA_LINGER_MS", "5"))
        .set("batch.num.messages", "10000")
        .set("queue.buffering.max.messages", env_or("KAFKA_QUEUE_MAX", "200000"))
        .set("message.timeout.ms", "30000")
        .create()
        .expect("kafka producer")
}

/// Exposes per-partition consumer lag as a Prometheus gauge from librdkafka stats.
pub struct LagContext;

impl ClientContext for LagContext {
    fn stats(&self, stats: Statistics) {
        for (topic, t) in &stats.topics {
            for (pid, p) in &t.partitions {
                if *pid < 0 || p.consumer_lag < 0 {
                    continue;
                }
                gauge!("kafka_consumer_lag", "topic" => topic.clone(), "partition" => pid.to_string()).set(p.consumer_lag as f64);
            }
        }
    }
}
impl ConsumerContext for LagContext {}

pub fn consumer(group: &str) -> StreamConsumer<LagContext> {
    consumer_with_poll_interval(group, 300_000)
}

/// `max_poll_interval_ms`: the longest the app may go without calling `recv()` before
/// the broker evicts it from the group. Must exceed the longest time a batch can hold
/// the consumer (retry tiers sleep until `not_before_ms` inside the batch).
pub fn consumer_with_poll_interval(group: &str, max_poll_interval_ms: u64) -> StreamConsumer<LagContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .set("group.id", group)
        .set("max.poll.interval.ms", max_poll_interval_ms.to_string())
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "1000")
        // We store offsets manually only after a batch is fully handled => at-least-once.
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .set("statistics.interval.ms", "5000")
        .set("fetch.min.bytes", "1")
        .create_with_context(LagContext)
        .expect("kafka consumer")
}
