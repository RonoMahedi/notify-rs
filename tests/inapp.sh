#!/bin/bash
# In-app end to end: Redis inbox + WebSocket gateway (crates/gateway).
# Temporarily switches the workers to IN_APP_PROVIDER=inbox, then restores the mock provider.
# Needs Node.js 22+ (built-in WebSocket).
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/.."
C=http://localhost:8090; I=http://localhost:8080; G=http://localhost:8070; H='content-type: application/json'
KEY="${ADMIN_TOKEN:-dev-admin-token-change-me}"; T=$(date +%s); W=$(mktemp -d)
WORKERS="worker worker-high worker-retry worker-retry-1m worker-retry-10m worker-bulk"
PASS=0; FAIL=0
check(){ if [ $# -ne 3 ]; then echo "  FAIL $1: harness got $# args"; FAIL=$((FAIL+1)); return; fi
         if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; PASS=$((PASS+1)); else echo "  FAIL $1: got '$2' want '$3'"; FAIL=$((FAIL+1)); fi; }
jq_(){ python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1" 2>/dev/null || echo "<non-json>"; }
api(){ curl -s -H "Authorization: Bearer $KEY" -H "$H" "$@"; }
code(){ curl -s -o /dev/null -w '%{http_code}' "$@"; }
frames(){ python3 -c "
import json,sys
print(json.dumps([json.loads(l) for l in open(sys.argv[1])]))" "$1" 2>/dev/null; }
upgrade(){ code -H "Connection: Upgrade" -H "Upgrade: websocket" -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" "$@"; }
cleanup(){ kill $(jobs -p) 2>/dev/null; rm -rf "$W"
  docker compose up -d --force-recreate $WORKERS >/dev/null 2>&1; echo "  (workers restored to the mock provider)"; }
trap cleanup EXIT

echo "== setup: workers on the Redis inbox, gateway up"
IN_APP_PROVIDER=inbox docker compose up -d --force-recreate $WORKERS gateway >/dev/null 2>&1
for i in $(seq 1 40); do M=$(api $C/v1/system | jq_ "[(p['provider'], p['mode']) for p in d['metrics']['providers'] if p['channel']=='in_app']"); [ "$M" = "[('inbox', 'live')]" ] && break; sleep 3; done
check "System tab: in-app via inbox, live" "$M" "[('inbox', 'live')]"
for i in $(seq 1 20); do [ "$(code $G/healthz)" = 200 ] && break; sleep 1; done
check "gateway healthy" "$(code $G/healthz)" 200
sleep 2   # let the new worker consumers settle

U1="inapp-u1-$T"; U2="inapp-u2-$T"
tok(){ curl -s -H "$H" $I/v1/inapp/token -d "{\"user_id\":\"$1\"}" | jq_ "d['token']"; }
TOK1=$(tok $U1); TOK2=$(tok $U2)

echo "== auth"
check "token endpoint validates user_id" "$(code -H "$H" $I/v1/inapp/token -d '{"user_id":""}')" 400
check "WebSocket without a token -> 401" "$(upgrade "$G/v1/inbox/ws")" 401
check "WebSocket with a forged token -> 401" "$(upgrade "$G/v1/inbox/ws?token=${TOK1%?}x")" 401
check "inbox REST without a token -> 401" "$(code $G/v1/inbox)" 401
OTHER=$(python3 -c "
import base64,hmac,hashlib,json,time
b=lambda x: base64.urlsafe_b64encode(x).rstrip(b'=').decode()
h=b(json.dumps({'alg':'HS256','typ':'JWT'}).encode()); p=b(json.dumps({'sub':'$U1','aud':'notify-inapp','exp':int(time.time())+60}).encode())
print(h+'.'+p+'.'+b(hmac.new(b'some-other-secret-some-other-secret',(h+'.'+p).encode(),hashlib.sha256).digest()))")
check "token signed with another secret -> 401" "$(code -H "Authorization: Bearer $OTHER" $G/v1/inbox)" 401

echo "== live delivery over WebSocket"
node tests/inapp_client.mjs "ws://localhost:8070/v1/inbox/ws?token=$TOK1" "$W/u1.jsonl" 2 & 
for i in $(seq 1 20); do grep -q hello "$W/u1.jsonl" 2>/dev/null && break; sleep 0.3; done
check "hello on connect, nothing unread yet" "$(frames $W/u1.jsonl | jq_ "[f['unread'] for f in d if f['type']=='hello']")" "[0]"
send(){ curl -s -H "$H" $I/v1/notifications -d "{\"user_id\":\"$1\",\"channel\":\"in_app\",\"priority\":\"$2\",\"template\":\"promo_weekly\",\"data\":{\"discount\":\"$3\"}}" | jq_ "d['id']"; }
N1=$(send $U1 high 50%); N2=$(send $U1 normal 20%)
for i in $(seq 1 30); do [ "$(frames $W/u1.jsonl | jq_ "len([f for f in d if f['type']=='unread'])")" = 1 ] && break; sleep 1; done
check "both notifications pushed live" "$(frames $W/u1.jsonl | jq_ "sorted(f['item']['id'] for f in d if f['type']=='notification') == sorted(['$N1','$N2'])")" True
check "title + body rendered from the template" "$(frames $W/u1.jsonl | jq_ "[(f['item']['title'], f['item']['body']) for f in d if f['type']=='notification' and f['item']['id']=='$N1']")" "[(\"This week's deals\", '50% off selected items, this week only')]"
check "client's read frame -> unread 0" "$(frames $W/u1.jsonl | jq_ "[f['unread'] for f in d if f['type']=='unread']")" "[0]"
check "status: sent via inbox" "$(curl -s $I/v1/notifications/$N1 | jq_ "d['status']")" sent

echo "== inbox REST (offline user)"
N3=$(send $U2 normal 30%); N4=$(send $U2 normal 40%)
for i in $(seq 1 30); do [ "$(curl -s -H "Authorization: Bearer $TOK2" $G/v1/inbox | jq_ "d['unread']")" = 2 ] && break; sleep 1; done
L=$(curl -s -H "Authorization: Bearer $TOK2" $G/v1/inbox)
check "offline user's items kept, newest first" "$(echo "$L" | jq_ "[i['id'] for i in d['items']] == sorted(['$N3','$N4'], reverse=True)")" True
check "both unread" "$(echo "$L" | jq_ "(d['unread'], [i['read'] for i in d['items']])")" "(2, [False, False])"
OLDEST=$(echo "$L" | jq_ "d['items'][1]['created_at_ms']")
check "paging: before= returns older items only" "$(curl -s -H "Authorization: Bearer $TOK2" "$G/v1/inbox?before=$OLDEST" | jq_ "len(d['items'])")" 0
check "mark one read (up_to_ms) -> unread 1" "$(curl -s -H "Authorization: Bearer $TOK2" -H "$H" $G/v1/inbox/read -d "{\"up_to_ms\":$OLDEST}" | jq_ "d['unread']")" 1
check "mark all read -> unread 0" "$(curl -s -H "Authorization: Bearer $TOK2" -X POST $G/v1/inbox/read | jq_ "d['unread']")" 0
check "watermark never moves back" "$(curl -s -H "Authorization: Bearer $TOK2" -H "$H" $G/v1/inbox/read -d '{"up_to_ms":1}' | jq_ "d['unread']")" 0
check "users can't see each other's inbox" "$(curl -s -H "Authorization: Bearer $TOK1" $G/v1/inbox | jq_ "any(i['id'] in ('$N3','$N4') for i in d['items'])")" False

echo "== the same item stored twice stays one entry (at-least-once resend)"
K="inbox:{$U2}"; M=$(docker compose exec -T redis redis-cli ZRANGE "$K" 0 0 | tr -d '\r')
docker compose exec -T redis redis-cli ZADD "$K" "$(echo "$M" | jq_ "d['created_at_ms']")" "$M" >/dev/null
check "still 2 items" "$(docker compose exec -T redis redis-cli ZCARD "$K" | tr -d '[:space:]')" 2

echo "== Redis feed interrupted -> clients told to resync"
docker compose exec -T redis redis-cli CLIENT KILL TYPE pubsub >/dev/null
for i in $(seq 1 30); do grep -q resync "$W/u1.jsonl" && break; sleep 0.5; done
check "resync frame after the gateway resubscribes" "$(frames $W/u1.jsonl | jq_ "len([f for f in d if f['type']=='resync'])")" 1
N5=$(send $U1 high 70%)
for i in $(seq 1 30); do grep -q "$N5" "$W/u1.jsonl" && break; sleep 1; done
check "live delivery works again after resubscribing" "$(grep -c "$N5" "$W/u1.jsonl")" 1

echo "== gateway restart closes sockets with 1012 so clients reconnect"
docker compose restart gateway >/dev/null 2>&1
for i in $(seq 1 30); do grep -q _close "$W/u1.jsonl" && break; sleep 0.5; done
check "close code 1012 (service restart)" "$(frames $W/u1.jsonl | jq_ "[f['code'] for f in d if f['type']=='_close']")" "[1012]"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ $FAIL -eq 0 ]
