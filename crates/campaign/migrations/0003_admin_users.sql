-- Email + password sign-in for the admin UI.
CREATE TABLE admin_users (
    id             UUID PRIMARY KEY,
    email          TEXT NOT NULL UNIQUE,            -- stored lowercase
    name           TEXT,
    password_hash  TEXT NOT NULL,                   -- Argon2id PHC string, never the password
    role           TEXT NOT NULL DEFAULT 'marketer' CHECK (role IN ('admin', 'marketer')),
    disabled       BOOLEAN NOT NULL DEFAULT false,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login_at  TIMESTAMPTZ
);

-- Only a SHA-256 of the session token is stored: a leaked table can't be replayed.
CREATE TABLE admin_sessions (
    token_hash  BYTEA PRIMARY KEY,
    user_id     UUID NOT NULL REFERENCES admin_users (id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX admin_sessions_user_idx ON admin_sessions (user_id);
CREATE INDEX admin_sessions_expiry_idx ON admin_sessions (expires_at);
