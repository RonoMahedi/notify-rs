``#!/bin/bash
# Seeds SEED_USERS fake users (default 1,000,000). Emails use the reserved example.com domain.
set -e
N="${SEED_USERS:-1000000}"
echo "seeding $N users"
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
INSERT INTO users (email, phone, first_name, last_name, status, email_verified_at, phone_verified_at, last_login_at, created_at, updated_at)
SELECT
  'user' || g || '@example.com',
  CASE WHEN g % 5 <> 0 THEN '01' || lpad(((g::bigint * 7919) % 1000000000)::text, 9, '0') END,
  (ARRAY['Rahim','Karim','Ayesha','Fatima','Nusrat','Tanvir','Sadia','Imran','Farhana','Jamal'])[1 + g % 10],
  (ARRAY['Ahmed','Hossain','Khan','Rahman','Islam','Chowdhury','Akter','Sarkar'])[1 + g % 8],
  (CASE WHEN g % 50 = 0 THEN 'SUSPENDED' WHEN g % 97 = 0 THEN 'INACTIVE' ELSE 'ACTIVE' END)::"UserStatus",
  CASE WHEN g % 10 <> 0 THEN now() - (g % 900) * interval '1 day' END,
  CASE WHEN g % 5 <> 0 AND g % 3 <> 0 THEN now() - (g % 700) * interval '1 day' END,
  now() - (g % 180) * interval '1 day' - (g % 1440) * interval '1 minute',
  now() - (g % 1000) * interval '1 day',
  now() - (g % 1000) * interval '1 minute'
FROM generate_series(1, $N) g;
ANALYZE users;
SQL
