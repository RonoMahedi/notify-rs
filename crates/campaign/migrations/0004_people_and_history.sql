-- People search: prefix search on email/phone, substring search on name.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX contacts_email_prefix_idx ON contacts (email text_pattern_ops);
CREATE INDEX contacts_phone_prefix_idx ON contacts (phone text_pattern_ops);
CREATE INDEX contacts_name_trgm_idx ON contacts
    USING GIN ((coalesce(first_name, '') || ' ' || coalesce(last_name, '')) gin_trgm_ops);

-- Delivery history: one row per processing outcome, written from the notify.events topic.
-- Daily partitions: retention = dropping old partitions (instant, no vacuum debt).
-- At full 100M/day scale this belongs in ClickHouse or ScyllaDB; see the technical guide.
CREATE TABLE delivery_events (
    ts               TIMESTAMPTZ NOT NULL,
    notification_id  UUID NOT NULL,
    user_id          TEXT NOT NULL,
    channel          TEXT NOT NULL,
    priority         TEXT NOT NULL,
    template         TEXT NOT NULL,
    campaign_id      UUID,
    status           TEXT NOT NULL,        -- sent | retrying | failed | expired | suppressed
    attempt          INT NOT NULL,
    provider         TEXT,
    recipient        TEXT,
    error            TEXT,
    latency_ms       BIGINT NOT NULL
) PARTITION BY RANGE (ts);

CREATE INDEX delivery_events_user_idx ON delivery_events (user_id, ts DESC);
CREATE INDEX delivery_events_notification_idx ON delivery_events (notification_id);
CREATE INDEX delivery_events_campaign_idx ON delivery_events (campaign_id, ts DESC) WHERE campaign_id IS NOT NULL;
CREATE INDEX delivery_events_status_idx ON delivery_events (status, ts DESC);
CREATE INDEX delivery_events_ts_idx ON delivery_events (ts DESC);
