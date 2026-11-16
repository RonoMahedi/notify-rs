#!/bin/bash
# Browser test of the admin UI in headless Chrome (throwaway profile). Run after tests/e2e.sh and tests/history.sh.
HERE="$(cd "$(dirname "$0")" && pwd)"; OUT="${OUT:-$HERE/ui-shots}"; PROF="$(mktemp -d)"; mkdir -p "$OUT"
# Dedicated test admin (created, or its password reset, via the CLI).
export UI_EMAIL="ui-admin@example.com" UI_PW="Ui-Test-Pass-123"
cd "$HERE/.." && { docker compose exec -T -e NEW_PASSWORD="$UI_PW" campaign campaign create-admin "$UI_EMAIL" admin >/dev/null 2>&1 ||
  docker compose exec -T -e NEW_PASSWORD="$UI_PW" campaign campaign reset-password "$UI_EMAIL" >/dev/null; }
CH="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
"$CH" --headless=new --remote-debugging-port=9333 --user-data-dir="$PROF" --no-first-run --disable-extensions about:blank >/dev/null 2>&1 &
CPID=$!
# Remove the test admin (its password is in this file), accounts it created, and the XSS test contact.
cleanup(){ kill $CPID 2>/dev/null; rm -rf "$PROF"
  docker compose exec -T postgres psql -U notify -d notify -qc "DELETE FROM admin_users WHERE email = '$UI_EMAIL' OR email ~ '^ui-marketer-[0-9]+@example\.com$'; DELETE FROM contacts WHERE user_id = 'xss-test-1'" >/dev/null 2>&1; }
trap cleanup EXIT
for i in $(seq 1 20); do curl -s 127.0.0.1:9333/json/version >/dev/null && break; sleep 0.5; done
node "$HERE/ui_test.mjs" "$OUT"
