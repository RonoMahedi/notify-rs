#!/bin/bash
# Firebase push end to end against a fake Google OAuth + FCM server (tests/fake_fcm.py).
# Temporarily switches the workers to FCM, then restores the mock provider.
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/.."
C=http://localhost:8090; I=http://localhost:8080; H='content-type: application/json'; KEY="${ADMIN_TOKEN:-dev-admin-token-change-me}"
F=http://127.0.0.1:9999; T=$(date +%s); W=$(mktemp -d)
WORKERS="worker worker-high worker-retry worker-retry-1m worker-retry-10m worker-bulk"
PASS=0; FAIL=0
check(){ if [ $# -ne 3 ]; then echo "  FAIL $1: harness got $# args"; FAIL=$((FAIL+1)); return; fi
         if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; PASS=$((PASS+1)); else echo "  FAIL $1: got '$2' want '$3'"; FAIL=$((FAIL+1)); fi; }
jq_(){ python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1" 2>/dev/null || echo "<non-json>"; }
api(){ curl -s -H "Authorization: Bearer $KEY" -H "$H" "$@"; }
DB(){ docker compose exec -T postgres psql -U notify -d notify -tAc "$1" | tr -d '[:space:]'; }
sent_log(){ python3 -c "
import json,sys
for l in open(sys.argv[1]):
    r=json.loads(l)
    if r['token'].startswith(sys.argv[2]): print(json.dumps(r))" "$W/log.jsonl" "$1" 2>/dev/null; }

echo "== setup: test key, service account, fake Google"
openssl genrsa -out "$W/key.pem" 2048 2>/dev/null; openssl rsa -in "$W/key.pem" -pubout -out "$W/pub.pem" 2>/dev/null
python3 -c "import json,sys; print(json.dumps({'type':'service_account','project_id':'fcm-test','client_email':'push@fcm-test.iam.gserviceaccount.com','private_key':open(sys.argv[1]).read(),'token_uri':'http://host.docker.internal:9999/token'}))" "$W/key.pem" > secrets/fcm-test.json
chmod 644 secrets/fcm-test.json
python3 -I tests/fake_fcm.py 9999 "$W/pub.pem" "$W/log.jsonl" fcm-test push@fcm-test.iam.gserviceaccount.com & FPID=$!
cleanup(){ kill $FPID 2>/dev/null; rm -rf "$W" secrets/fcm-test.json
  docker compose up -d --force-recreate $WORKERS >/dev/null 2>&1; echo "  (workers restored to the mock provider)"; }
trap cleanup EXIT
for i in $(seq 1 20); do curl -s $F/stats >/dev/null && break; sleep 0.3; done

start_fcm(){ FCM_CREDENTIALS_FILE=/run/secrets/fcm-test.json FCM_ENDPOINT=http://host.docker.internal:9999 FCM_VALIDATE_ONLY=$1 \
  docker compose up -d --force-recreate $WORKERS >/dev/null 2>&1
  for i in $(seq 1 40); do M=$(api $C/v1/system | jq_ "[p['mode'] for p in d['metrics']['providers'] if p['channel']=='push']"); [ "$M" = "['$2']" ] && break; sleep 3; done; }
start_fcm false live
check "System tab: push via FCM, live" "$(api $C/v1/system | jq_ "[(p['provider'], p['mode']) for p in d['metrics']['providers'] if p['channel']=='push']")" "[('fcm', 'live')]"

reg(){ local b="{\"user_id\":\"$1\",\"platform\":\"$2\",\"token\":\"$3\"}"; api -o /dev/null -w '%{http_code}' -X POST $C/v1/devices -d "$b"; }
U1="fcm-u1-$T"; U2="fcm-u2-$T"; U3="fcm-u3-$T"; U4="fcm-u4-$T"; U5="fcm-u5-$T"
check "register 2 devices for one user" "$(reg $U1 android tok-ok-$T)$(reg $U1 ios dead-$T)" 204204
reg $U2 android busy-$T >/dev/null; reg $U3 android badtok-$T >/dev/null; reg $U5 android revoke-$T >/dev/null
check "tokens mirrored to Redis for the workers" "$(docker compose exec -T redis redis-cli SCARD devices:$U1 | tr -d '[:space:]')" 2
sleep 2   # let the new worker consumers settle

send(){ local b="{\"user_id\":\"$1\",\"channel\":\"push\",\"priority\":\"high\",\"template\":\"order_shipped\",\"data\":{\"order\":\"A-77\"}}"; curl -s -H "$H" $I/v1/notifications -d "$b" | jq_ "d['id']"; }
N1=$(send $U1); N2=$(send $U2); N3=$(send $U3); N4=$(send $U4); N5=$(send $U5)
st(){ api "$C/v1/notifications/$1/events" | jq_ "[(e['status'], e['provider'], e['error']) for e in d][:1]"; }
for i in $(seq 1 40); do [ "$(st $N1)" != "[]" ] && [ "$(st $N2)" != "[]" ] && [ "$(st $N3)" != "[]" ] && [ "$(st $N4)" != "[]" ] && [ "$(st $N5)" != "[]" ] && break; sleep 2; done

echo "== a user with two devices: one live, one uninstalled"
codes(){ sent_log "$1" | python3 -c "import json,sys; print([json.loads(l)['code'] for l in sys.stdin])"; }
echo "  info: responses per device: tok-ok $(codes tok-ok-$T), dead $(codes dead-$T) (401 = shared access token revoked by the revoke-* user in the same batch, then retried)"
check "each device delivered once (ignoring auth retries)" "$(codes tok-ok-$T | python3 -c "import sys; print(eval(sys.stdin.read()).count(200))")/$(codes dead-$T | python3 -c "import sys; print(eval(sys.stdin.read()).count(404))")" "1/1"
MSG=$(sent_log tok-ok-$T | tail -1)
check "title + body rendered from the template" "$(echo "$MSG" | jq_ "(d['body']['message']['notification']['title'], d['body']['message']['notification']['body'])")" "('Order shipped', 'Your order A-77 is on its way.')"
check "high priority on Android and iOS" "$(echo "$MSG" | jq_ "(d['body']['message']['android']['priority'], d['body']['message']['apns']['headers']['apns-priority'])")" "('HIGH', '10')"
check "notification id in the data payload" "$(echo "$MSG" | jq_ "d['body']['message']['data']['notification_id']")" "$N1"
check "counted as sent via fcm" "$(st $N1)" "[('sent', 'fcm', None)]"
for i in $(seq 1 15); do [ "$(DB "select count(*) from devices where token='dead-$T'")" = 0 ] && break; sleep 1; done
check "uninstalled device's token deleted from the database" "$(DB "select count(*) from devices where token='dead-$T'")" 0
check "  ...and from the workers' Redis copy" "$(docker compose exec -T redis redis-cli SISMEMBER devices:$U1 dead-$T | tr -d '[:space:]')" 0
check "live device kept" "$(DB "select count(*) from devices where token='tok-ok-$T'")" 1

echo "== error handling"
check "FCM busy -> retried" "$(st $N2)" "[('retrying', 'fcm', 'FCM unavailable (will retry)')]"
check "invalid token -> failed, permanent" "$(st $N3)" "[('failed', 'fcm', 'not a valid FCM token')]"
check "  ...and the invalid token is deleted" "$(DB "select count(*) from devices where token='badtok-$T'")" 0
check "no device -> failed with a clear reason" "$(st $N4)" "[('failed', 'fcm', 'no push token: the user has no registered device')]"
check "revoked access token -> refreshed and delivered" "$(st $N5)" "[('sent', 'fcm', None)]"

echo "== Google sign-in"
S=$(curl -s $F/stats)
check "every sign-in JWT had a valid RS256 signature + claims" "$(echo "$S" | jq_ "d['bad_jwt']")" 0
check "access token reused (sign-ins far fewer than sends)" "$(echo "$S" | jq_ "1 <= d['token_requests'] <= 8 and d['sends'] >= 6")" True

echo "== validate-only mode (safe first test with real credentials)"
start_fcm true validate_only
check "System tab: push via FCM, validate only" "$(api $C/v1/system | jq_ "[(p['provider'], p['mode']) for p in d['metrics']['providers'] if p['channel']=='push']")" "[('fcm', 'validate_only')]"
U6="fcm-u6-$T"; reg $U6 android tok-val-$T >/dev/null; sleep 2
N6=$(send $U6)
for i in $(seq 1 30); do [ "$(sent_log tok-val-$T | wc -l | tr -d ' ')" -ge 1 ] && break; sleep 2; done
check "request carries validate_only=true" "$(sent_log tok-val-$T | head -1 | jq_ "d['body']['validate_only']")" True

echo; echo "FCM RESULT: $PASS passed, $FAIL failed"
