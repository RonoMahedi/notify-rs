-- Mirrors identity-platform/apps/prisma/schema.prisma (model User -> "users").
-- Test double only: the real identity-postgres is never written to.
CREATE TYPE "UserStatus" AS ENUM ('ACTIVE', 'INACTIVE', 'SUSPENDED', 'LOCKED');

CREATE TABLE users (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email              VARCHAR(255) NOT NULL UNIQUE,
    phone              VARCHAR(30) UNIQUE,
    first_name         VARCHAR(100),
    last_name          VARCHAR(100),
    status             "UserStatus" NOT NULL DEFAULT 'ACTIVE',
    email_verified_at  TIMESTAMP(3),
    phone_verified_at  TIMESTAMP(3),
    last_login_at      TIMESTAMP(3),
    created_at         TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP(3) NOT NULL
);
-- Recommended for the real identity DB too (Prisma: @@index([updatedAt, id])):
-- makes each incremental sync an index range scan instead of a full table scan.
CREATE INDEX users_updated_at_id_idx ON users (updated_at, id);
