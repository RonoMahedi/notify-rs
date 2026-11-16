-- Audience: one row per reachable person. Fed by identity sync, Excel/CSV imports and the API.
CREATE TABLE contacts (
    user_id         TEXT PRIMARY KEY,
    email           TEXT,
    phone           TEXT,                 -- E.164, e.g. +8801712345678
    first_name      TEXT,
    last_name       TEXT,
    country         TEXT,
    language        TEXT,
    timezone        TEXT,
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
    unsubscribed    TEXT[] NOT NULL DEFAULT '{}',   -- channels the user opted out of
    attributes      JSONB NOT NULL DEFAULT '{}',    -- any marketing field: plan, city, orders...
    source          TEXT NOT NULL,                  -- identity | import | api
    last_active_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX contacts_email_idx ON contacts (email);
CREATE INDEX contacts_phone_idx ON contacts (phone);
CREATE INDEX contacts_country_idx ON contacts (country);
CREATE INDEX contacts_last_active_idx ON contacts (last_active_at);
CREATE INDEX contacts_attributes_idx ON contacts USING GIN (attributes jsonb_path_ops);

CREATE TABLE devices (
    token       TEXT PRIMARY KEY,
    user_id     TEXT NOT NULL,
    platform    TEXT NOT NULL CHECK (platform IN ('ios', 'android', 'web')),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX devices_user_idx ON devices (user_id);

CREATE TABLE segments (
    id          UUID PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    kind        TEXT NOT NULL CHECK (kind IN ('all', 'rule', 'static')),
    rule        JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Members of static segments (e.g. an uploaded Excel list).
CREATE TABLE segment_members (
    segment_id  UUID NOT NULL REFERENCES segments (id) ON DELETE CASCADE,
    user_id     TEXT NOT NULL,
    PRIMARY KEY (segment_id, user_id)
);

CREATE TABLE imports (
    id             UUID PRIMARY KEY,
    filename       TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    total_rows     BIGINT NOT NULL DEFAULT 0,
    imported_rows  BIGINT NOT NULL DEFAULT 0,
    failed_rows    BIGINT NOT NULL DEFAULT 0,
    errors         JSONB NOT NULL DEFAULT '[]',     -- first 1000 row errors
    segment_id     UUID REFERENCES segments (id) ON DELETE SET NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ
);

CREATE TABLE campaigns (
    id             UUID PRIMARY KEY,
    name           TEXT NOT NULL,
    segment_id     UUID NOT NULL REFERENCES segments (id),
    channel        TEXT NOT NULL CHECK (channel IN ('push', 'email', 'sms', 'in_app')),
    template       TEXT NOT NULL,
    data           JSONB NOT NULL DEFAULT '{}',
    status         TEXT NOT NULL CHECK (status IN ('draft', 'scheduled', 'running', 'paused', 'completed', 'cancelled')),
    scheduled_at   TIMESTAMPTZ,
    rate_per_sec   INT NOT NULL DEFAULT 2000 CHECK (rate_per_sec BETWEEN 1 AND 50000),
    frequency_cap  INT CHECK (frequency_cap > 0),   -- max marketing messages per user per day
    ttl_seconds    BIGINT,
    cursor         TEXT,                            -- last user_id processed (resume point)
    targeted       BIGINT,
    enqueued       BIGINT NOT NULL DEFAULT 0,
    suppressed     BIGINT NOT NULL DEFAULT 0,
    last_error     TEXT,
    locked_until   TIMESTAMPTZ,                     -- runner lease (multi-instance safe)
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX campaigns_due_idx ON campaigns (status, scheduled_at);

-- Incremental sync watermark per source. Identity uses Prisma DateTime = timestamp(3) without tz (UTC).
CREATE TABLE sync_state (
    source        TEXT PRIMARY KEY,
    watermark_ts  TIMESTAMP NOT NULL,
    watermark_id  TEXT NOT NULL,
    rows_synced   BIGINT NOT NULL DEFAULT 0,
    last_run_at   TIMESTAMPTZ,
    last_error    TEXT
);
