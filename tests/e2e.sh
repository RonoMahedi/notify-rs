#!/bin/bash
# End-to-end test of the campaign system. Expected values come from the identity-sim
# source DB or the fixture generator's logic, never from the DB under test.
# NOTE: macOS bash 3.2 mangles quoted JSON inside "$(...)": bodies always go in variables.
# Usage: tests/e2e.sh   (stack running: docker compose up -d; use a fresh campaign DB + FLUSHALL for exact counts)
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/.."
S="$HERE"; mkdir -p "$S/fixtures"
[ -f "$S/fixtures/customers.xlsx" ] || python3 "$S/make_xlsx.py" "$S/fixtures/customers.xlsx" >/dev/null
C=http://localhost:8090; AUTH="Authorization: Bearer dev-admin-token-change-me"; H='content-type: application/json'
PASS=0; FAIL=0
check(){ if [ $# -ne 3 ]; then echo "  FAIL $1: harness got $# args"; FAIL=$((FAIL+1)); return; fi
         if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; PASS=$((PASS+1)); else echo "  FAIL $1: got '$2' want '$3'"; FAIL=$((FAIL+1)); fi; }
jq_(){ python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1" 2>/dev/null || echo "<non-json>"; }
src(){ docker compose exec -T identity-sim psql -U identity -d identity -tAc "$1" | tr -d '[:space:]'; }
DB(){ docker compose exec -T postgres psql -U notify -d notify -tAc "$1" | tr -d '[:space:]'; }
api(){ curl -s -H "$AUTH" -H "$H" "$@"; }
code(){ curl -s -o /dev/null -w '%{http_code}' -H "$AUTH" -H "$H" "$@"; }
# Sum of log-end offsets for bulk topics in the bulk consumer group, and its lag.
bulk(){ docker compose exec -T redpanda rpk group describe notify-workers-bulk 2>/dev/null |
        awk -v want="$1" '$1 ~ /^notify\..*\.bulk$/ { end += $5; lag += $6 } END { print (want == "lag" ? lag : end) + 0 }'; }

echo "== 1. auth"
check "no token -> 401" "$(curl -s -o /dev/null -w '%{http_code}' $C/v1/segments)" 401
check "bad token -> 401" "$(curl -s -o /dev/null -w '%{http_code}' -H 'Authorization: Bearer wrong' $C/v1/segments)" 401

check "admin UI served at /admin" "$(curl -s localhost:8090/admin | grep -c '<title>Campaign Admin</title>')" 1
check "admin UI has CSP header" "$(curl -sI localhost:8090/admin | grep -ci '^content-security-policy')" 1
check "/v1/stats needs token" "$(curl -s -o /dev/null -w '%{http_code}' localhost:8090/v1/stats)" 401

echo "== 2. identity sync"
SRC_TOTAL=$(src "select count(*) from users")
for i in $(seq 1 120); do N=$(api $C/v1/sync | jq_ "d['sources'][0]['rows_synced'] if d['sources'] else 0"); [ "$N" -ge "$SRC_TOTAL" ] 2>/dev/null && break; sleep 5; done
check "all source users synced" "$( [ "$N" -ge "$SRC_TOTAL" ] && echo yes || echo "no ($N/$SRC_TOTAL)")" yes
check "contacts from identity" "$(DB "select count(*) from contacts where source='identity'")" "$SRC_TOTAL"
check "inactive mapped (SUSPENDED+INACTIVE)" "$(DB "select count(*) from contacts where status='inactive'")" "$(src "select count(*) from users where status<>'ACTIVE'")"
check "phones normalized to E.164" "$(DB "select count(*) from contacts where phone is not null and phone !~ '^\+8801[0-9]{9}$'")" 0
UID1=$(src "select id from users where email='user7@example.com'")
docker compose exec -T identity-sim psql -U identity -d identity -qc "update users set first_name='Changed', updated_at=now() where email='user7@example.com'"
api -X POST $C/v1/sync/run >/dev/null; sleep 4
check "incremental sync picked up a change" "$(DB "select first_name from contacts where user_id='$UID1'")" Changed

echo "== 3. Excel import (20,000 rows: 10k existing, 9,990 new, 10 broken)"
COLL=$(src "select count(*) from users where phone between '01700010001' and '01700019990'")
echo "  info: $COLL 'new' Excel rows share a phone with an identity user (merge by phone)"
IMP=$(curl -s -H "$AUTH" -F "file=@$S/fixtures/customers.xlsx" -F "segment_name=Excel VIP list" $C/v1/imports)
IID=$(echo "$IMP" | jq_ "d['id']"); SEG_X=$(echo "$IMP" | jq_ "d['segment_id']")
for i in $(seq 1 60); do ST=$(api $C/v1/imports/$IID | jq_ "d['status']"); [ "$ST" != processing ] && break; sleep 2; done
R=$(api $C/v1/imports/$IID)
check "import status" "$ST" completed
check "total rows" "$(echo "$R" | jq_ "d['total_rows']")" 20000
check "imported rows" "$(echo "$R" | jq_ "d['imported_rows']")" 19990
check "failed rows" "$(echo "$R" | jq_ "d['failed_rows']")" 10
check "errors name rows 19992-20001" "$(echo "$R" | jq_ "sorted(e['row'] for e in d['errors'])==list(range(19992,20002))")" True
check "error text explains reason" "$(echo "$R" | jq_ "'invalid phone' in d['errors'][0]['error'] or 'invalid email' in d['errors'][0]['error']")" True
check "existing user merged by email (no duplicate)" "$(DB "select count(*) from contacts where email='user5@example.com'")" 1
check "  ...kept identity uuid" "$(DB "select user_id from contacts where email='user5@example.com'")" "$(src "select id from users where email='user5@example.com'")"
check "  ...got Excel attributes (plan(5)=basic, city[5%4]=Chattogram)" "$(DB "select attributes->>'plan' || '/' || (attributes->>'city') || '/' || (attributes->>'identity_status') from contacts where email='user5@example.com'")" "basic/Chattogram/ACTIVE"
check "identity emails never overwritten by import" "$(DB "select count(*) from contacts where source='identity' and email like '%@shop.example.com'")" 0
NC=$(src "select min(i) from generate_series(10001,19990) i where not exists (select 1 from users where phone = '0' || (1700000000+i)::text)")
check "Excel numeric phone $((1700000000+NC)) -> +880$((1700000000+NC))" "$(DB "select phone from contacts where email='new$NC@shop.example.com'")" "+880$((1700000000+NC))"
check "numeric attribute stays a number" "$(DB "select jsonb_typeof(attributes->'total_orders') from contacts where email='new$NC@shop.example.com'")" number
check "static cohort members" "$(DB "select count(*) from segment_members where segment_id='$SEG_X'")" 19990
NEW=$((9990 - COLL))
check "new contacts = 9,990 - phone merges" "$(DB "select count(*) from contacts where source='import'")" "$NEW"
IMP2=$(curl -s -H "$AUTH" -F "file=@$S/fixtures/customers.xlsx" $C/v1/imports | jq_ "d['id']")
for i in $(seq 1 60); do [ "$(api $C/v1/imports/$IMP2 | jq_ "d['status']")" != processing ] && break; sleep 2; done
check "re-import is idempotent" "$(DB "select count(*) from contacts")" "$((SRC_TOTAL + NEW))"

echo "== 4. cohorts + preview counts"
EXP_ALL_EMAIL=$(( $(src "select count(*) from users where status='ACTIVE'") + NEW ))
B='{"name":"All users","kind":"all"}'; SEG_ALL=$(api -X POST $C/v1/segments -d "$B" | jq_ "d['id']")
check "all users / email" "$(api "$C/v1/segments/$SEG_ALL/preview?channel=email" | jq_ "d['count']")" "$EXP_ALL_EMAIL"
check "all users / sms" "$(api "$C/v1/segments/$SEG_ALL/preview?channel=sms" | jq_ "d['count']")" "$(( $(src "select count(*) from users where status='ACTIVE' and phone is not null") + NEW ))"
RECENT="status='ACTIVE' and email_verified_at is not null and last_login_at >= now() - interval '30 days'"
B='{"name":"Active 30d, verified","kind":"rule","rule":{"all":[{"field":"last_active_at","op":"within","value":"30d"},{"field":"attributes.email_verified","op":"eq","value":true}]}}'
SEG_R=$(api -X POST $C/v1/segments -d "$B" | jq_ "d['id']")
check "rule: active 30d + verified (vs source SQL)" "$(api "$C/v1/segments/$SEG_R/preview?channel=email" | jq_ "d['count']")" "$(src "select count(*) from users where $RECENT")"
B='{"channel":"email","rule":{"all":[{"field":"attributes.plan","op":"in","value":["gold","premium"]},{"field":"attributes.city","op":"eq","value":"Dhaka"},{"field":"attributes.total_orders","op":"gte","value":10}]}}'
EXP_VIP=$(python3 -c "
bad=lambda g: g%50==0 or g%97==0
plan=lambda i: 'gold' if i%10==0 else 'premium' if i%10 in (1,2) else 'basic'
print(sum(1 for i in range(1,10001) if plan(i)!='basic' and i%4==0 and i%25>=10 and not bad(i)))")
# rows 10001+ are new contacts or merged by phone into identity users (whose status then applies): count via DB-independent source query
EXP_VIP_NEW=$(python3 -c "
plan=lambda i: 'gold' if i%10==0 else 'premium' if i%10 in (1,2) else 'basic'
print(' '.join('0'+str(1700000000+i) for i in range(10001,19991) if plan(i)!='basic' and i%4==0 and i%25>=10))")
VIP_NEW_TOTAL=$(echo $EXP_VIP_NEW | wc -w | tr -d ' ')
VIP_NEW_MERGED_INACTIVE=$(src "select count(*) from users where status<>'ACTIVE' and phone in ('$(echo $EXP_VIP_NEW | sed "s/ /','/g")')")
check "rule: VIP Dhaka 10+ orders (vs generator)" "$(api -X POST $C/v1/segments/preview -d "$B" | jq_ "d['count']")" "$((EXP_VIP + VIP_NEW_TOTAL - VIP_NEW_MERGED_INACTIVE))"
XSET="(email ~ '^user([1-9][0-9]{0,3}|10000)@example.com$' or phone between '01700010001' and '01700019990')"
check "static Excel cohort / email" "$(api "$C/v1/segments/$SEG_X/preview?channel=email" | jq_ "d['count']")" "$(( $(src "select count(*) from users where status='ACTIVE' and $XSET") + NEW ))"
B='{"rule":{"field":"country; DROP TABLE contacts--","op":"eq","value":"x"}}'
check "SQL injection in field -> 400" "$(code -X POST $C/v1/segments/preview -d "$B")" 400
check "  ...contacts table intact" "$(DB "select count(*)>0 from contacts")" t
B="{\"rule\":{\"field\":\"country\",\"op\":\"eq\",\"value\":\"x' OR '1'='1\"}}"
check "injection in value is just data -> 0 matches" "$(api -X POST $C/v1/segments/preview -d "$B" | jq_ "d['count']")" 0

echo "== 5. unsubscribe + devices"
B='{"channel":"email"}'; api -X POST $C/v1/contacts/$UID1/unsubscribe -d "$B" >/dev/null
check "unsubscribed user excluded" "$(api "$C/v1/segments/$SEG_ALL/preview?channel=email" | jq_ "d['count']")" "$((EXP_ALL_EMAIL - 1))"
check "  ...and in Redis opt-out (workers drop queued msgs)" "$(docker compose exec -T redis redis-cli SISMEMBER optout:$UID1 email | tr -d '[:space:]')" 1
check "push cohort empty before devices" "$(api "$C/v1/segments/$SEG_ALL/preview?channel=push" | jq_ "d['count']")" 0
B="{\"user_id\":\"$UID1\",\"platform\":\"android\",\"token\":\"fcm-test-token-1\"}"
check "register device -> 204" "$(code -X POST $C/v1/devices -d "$B")" 204
B='{"user_id":"x","platform":"nokia","token":"t"}'
check "bad platform -> 400" "$(code -X POST $C/v1/devices -d "$B")" 400
check "push cohort = 1" "$(api "$C/v1/segments/$SEG_ALL/preview?channel=push" | jq_ "d['count']")" 1

echo "== templates for the test campaigns"
for spec in "email winter_sale" "email vip" "in_app blast"; do
  CH=${spec% *}; KEY=${spec#* }
  B='{"title":"Hi {{first_name|there}}","body":"{{first_name|Friend}}, {{discount|10%}} off for {{attributes.plan|standard}} members."}'
  check "template $KEY ($CH) saved" "$(code -X PUT $C/v1/templates/$CH/$KEY -d "$B")" 200
done
B="{\"name\":\"x\",\"segment_id\":\"$SEG_R\",\"channel\":\"sms\",\"template\":\"no_such_template\",\"start\":true}"
check "campaign with a missing template -> 400" "$(code -X POST $C/v1/campaigns -d "$B")" 400

BULK0=$(bulk end)
echo "== 6. campaign A: 'Active 30d' email, 5k/s, frequency cap 1/day"
EXP_A=$(src "select count(*) from users where $RECENT and id <> '$UID1'")
B="{\"name\":\"Winter sale\",\"segment_id\":\"$SEG_R\",\"channel\":\"email\",\"template\":\"winter_sale\",\"data\":{\"discount\":\"20%\"},\"rate_per_sec\":5000,\"frequency_cap\":1,\"start\":true}"
CA=$(api -X POST $C/v1/campaigns -d "$B" | jq_ "d['id']"); T1=$(date +%s)
for i in $(seq 1 180); do ST=$(api $C/v1/campaigns/$CA | jq_ "d['status']"); [ "$ST" == completed ] && break; sleep 1; done
SECS=$(( $(date +%s) - T1 )); A=$(api $C/v1/campaigns/$CA)
check "campaign A completed" "$ST" completed
check "targeted = source count minus unsubscribed user7" "$(echo "$A" | jq_ "d['targeted']")" "$EXP_A"
check "enqueued = targeted" "$(echo "$A" | jq_ "d['enqueued']")" "$EXP_A"
check "progress 100%" "$(echo "$A" | jq_ "d['progress']")" 100.0
check "audience snapshot cleaned up" "$(DB "select count(*) from campaign_audience where campaign_id='$CA'")" 0
echo "  info: $EXP_A messages in ${SECS}s at rate limit 5000/s (~$(( EXP_A / (SECS>0?SECS:1) ))/s)"

echo "== 7. campaign B: Excel list, same day, cap 1 -> overlap with A suppressed"
EXP_OVERLAP=$(src "select count(*) from users where $RECENT and $XSET and id <> '$UID1'")
B="{\"name\":\"VIP follow-up\",\"segment_id\":\"$SEG_X\",\"channel\":\"email\",\"template\":\"vip\",\"rate_per_sec\":5000,\"frequency_cap\":1,\"start\":true}"
CB=$(api -X POST $C/v1/campaigns -d "$B" | jq_ "d['id']")
for i in $(seq 1 60); do ST=$(api $C/v1/campaigns/$CB | jq_ "d['status']"); [ "$ST" == completed ] && break; sleep 1; done
BB=$(api $C/v1/campaigns/$CB)
check "campaign B completed" "$ST" completed
check "suppressed by frequency cap = exact overlap with A" "$(echo "$BB" | jq_ "d['suppressed']")" "$EXP_OVERLAP"
check "enqueued + suppressed = targeted" "$(echo "$BB" | jq_ "d['enqueued']+d['suppressed']==d['targeted']")" True

echo "== 8. campaign C: pause / resume / cancel + mid-campaign unsubscribe"
B="{\"name\":\"Big blast\",\"segment_id\":\"$SEG_ALL\",\"channel\":\"in_app\",\"template\":\"blast\",\"rate_per_sec\":2000,\"start\":true}"
CC=$(api -X POST $C/v1/campaigns -d "$B" | jq_ "d['id']")
for i in $(seq 1 60); do [ "$(api $C/v1/campaigns/$CC | jq_ "d['enqueued']")" -gt 0 ] 2>/dev/null && break; sleep 1; done
check "snapshot = live count at launch" "$(api $C/v1/campaigns/$CC | jq_ "d['targeted']")" "$(api "$C/v1/segments/$SEG_ALL/preview?channel=in_app" | jq_ "d['count']")"
check "pause -> paused" "$(api -X POST $C/v1/campaigns/$CC/pause | jq_ "d['status']")" paused
sleep 3; E1=$(api $C/v1/campaigns/$CC | jq_ "d['enqueued']"); sleep 5; E2=$(api $C/v1/campaigns/$CC | jq_ "d['enqueued']")
check "no progress while paused" "$E1" "$E2"
check "pause again -> 409" "$(code -X POST $C/v1/campaigns/$CC/pause)" 409
LAST=$(DB "select user_id from campaign_audience where campaign_id='$CC' order by user_id desc limit 1")
B='{"channel":"in_app"}'; api -X POST "$C/v1/contacts/$LAST/unsubscribe" -d "$B" >/dev/null
api -X POST $C/v1/campaigns/$CC/resume >/dev/null; sleep 8
E3=$(api $C/v1/campaigns/$CC | jq_ "d['enqueued']")
check "progress after resume" "$( [ "$E3" -gt "$E2" ] && echo yes || echo "no ($E2 -> $E3)")" yes
check "cancel -> cancelled" "$(api -X POST $C/v1/campaigns/$CC/cancel | jq_ "d['status']")" cancelled
sleep 2
check "cancel cleans snapshot" "$(DB "select count(*) from campaign_audience where campaign_id='$CC'")" 0

echo "== 9. bulk lane: every campaign message consumed by bulk workers"
TOTAL_ENQ=$(DB "select sum(enqueued) from campaigns")
for i in $(seq 1 90); do [ "$(bulk lag)" -eq 0 ] && break; sleep 2; done
check "messages on bulk topics = enqueued by campaigns" "$(( $(bulk end) - BULK0 ))" "$TOTAL_ENQ"
check "bulk consumer lag drained" "$(bulk lag)" 0
q(){ curl -s --data-urlencode "query=$1" localhost:9090/api/v1/query | python3 -c "import json,sys;r=json.load(sys.stdin)['data']['result'];print(round(sum(float(x['value'][1]) for x in r),3))"; }
RATIO=$(q "sum(campaign_notifications_total{campaign=\"$CA\",outcome=\"sent\"}) / $EXP_A")
check "campaign A delivered >= 96% (mock injects 2% transient, 0.5% permanent)" "$(python3 -c "print($RATIO >= 0.96)")" True
echo "  info: campaign A outcomes so far: $(curl -s --data-urlencode "query=sum by (outcome) (campaign_notifications_total{campaign=\"$CA\"})" localhost:9090/api/v1/query | python3 -c "import json,sys;print({x['metric']['outcome']:int(float(x['value'][1])) for x in json.load(sys.stdin)['data']['result']})")"

check "/v1/stats reports campaigns" "$(api $C/v1/stats | jq_ "d['campaigns_by_status'].get('completed',0)>=2")" True
echo; echo "RESULT: $PASS passed, $FAIL failed"
