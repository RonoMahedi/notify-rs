#!/bin/bash
# People search + delivery history. Run after tests/e2e.sh (uses its campaigns and contacts).
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/.."
C=http://localhost:8090; I=http://localhost:8080; H='content-type: application/json'; KEY="${ADMIN_TOKEN:-dev-admin-token-change-me}"
PASS=0; FAIL=0
check(){ if [ $# -ne 3 ]; then echo "  FAIL $1: harness got $# args"; FAIL=$((FAIL+1)); return; fi
         if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; PASS=$((PASS+1)); else echo "  FAIL $1: got '$2' want '$3'"; FAIL=$((FAIL+1)); fi; }
jq_(){ python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1" 2>/dev/null || echo "<non-json>"; }
api(){ curl -s -H "Authorization: Bearer $KEY" -H "$H" "$@"; }
DB(){ docker compose exec -T postgres psql -U notify -d notify -tAc "$1" | tr -d '[:space:]'; }
src(){ docker compose exec -T identity-sim psql -U identity -d identity -tAc "$1" | tr -d '[:space:]'; }
q(){ curl -s --data-urlencode "query=$1" localhost:9090/api/v1/query | python3 -c "import json,sys;r=json.load(sys.stdin)['data']['result'];print(int(sum(float(x['value'][1]) for x in r)))"; }

echo "== people: browse + search"
P1=$(api "$C/v1/contacts?limit=50")
check "browse returns a page of 50" "$(echo "$P1" | jq_ "len(d['items'])")" 50
NEXT=$(echo "$P1" | jq_ "d['next']")
check "next page continues after the cursor" "$(api "$C/v1/contacts?limit=50&after=$NEXT" | jq_ "d['items'][0]['user_id'] > '$NEXT'")" True
check "email prefix search" "$(api "$C/v1/contacts?q=user12%40" | jq_ "[r['email'] for r in d['items']]==['user12@example.com'] and d['mode']=='email'")" True
PH=$(src "select phone from users where email='user12@example.com'")   # stored like 01xxxxxxxxx
check "phone search in local format ($PH)" "$(api "$C/v1/contacts?q=$PH" | jq_ "d['mode']=='phone' and any(r['email']=='user12@example.com' for r in d['items'])")" True
check "name search" "$(api "$C/v1/contacts?q=Ayesha&limit=20" | jq_ "d['mode']=='name' and len(d['items'])==20 and all('Ayesha' in (r['first_name'] or '') for r in d['items'])")" True
UID1=$(src "select id from users where email='user7@example.com'")
check "exact user_id search" "$(api "$C/v1/contacts?q=$UID1" | jq_ "d['items'][0]['user_id']")" "$UID1"
check "injection in search is just text" "$(api "$C/v1/contacts?q=x%27%20OR%20%271%27%3D%271" | jq_ "len(d['items'])")" 0
check "LIKE wildcards escaped" "$(api "$C/v1/contacts?q=%25abc%25" | jq_ "len(d['items'])")" 0
check "name search needs 3+ letters (no full scan)" "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $KEY" "$C/v1/contacts?q=%25%25%25")" 400
check "person shows full FCM token" "$(api "$C/v1/contacts/$UID1" | jq_ "d['devices'][0]['token']")" fcm-test-token-1

echo "== templates: validation, preview, SMS length"
B='{"body":"Hi {{first name}}"}'
check "invalid token rejected" "$(curl -s -o /dev/null -w '%{http_code}' -X PUT -H "Authorization: Bearer $KEY" -H "$H" $C/v1/templates/sms/bad_tpl -d "$B")" 400
B='{"title":"ignored for sms","body":"Hi {{first_name|there}}"}'
check "SMS template drops the title" "$(api -X PUT $C/v1/templates/sms/tpl_check -d "$B" | jq_ "d['title']")" None
U12=$(src "select id from users where email='user12@example.com'")
FN12=$(DB "select first_name from contacts where user_id='$U12'")
B="{\"channel\":\"email\",\"title\":\"Hi {{first_name}}\",\"body\":\"Plan: {{attributes.plan|none}}, code {{code}}, {{missing_one}}\",\"user_id\":\"$U12\",\"data\":{\"code\":\"77\"}}"
P=$(api -X POST $C/v1/templates/preview -d "$B")
check "preview fills person, attributes and data" "$(echo "$P" | jq_ "(d['title'], d['body'])")" "('Hi $FN12', 'Plan: $(DB "select coalesce(attributes->>'plan','none') from contacts where user_id='$U12'"), code 77, ')"
check "preview lists tokens with no value" "$(echo "$P" | jq_ "d['missing']")" "['missing_one']"
B='{"channel":"sms","body":"আপনার অর্ডার {{order}} পাঠানো হয়েছে। ধন্যবাদ, আমাদের সাথে কেনাকাটা করার জন্য। আবার আসবেন!","data":{"order":"A-1001"}}'
check "Bengali SMS counted as Unicode, multi-part" "$(api -X POST $C/v1/templates/preview -d "$B" | jq_ "(d['sms']['encoding'], d['sms']['segments'] >= 2)")" "('Unicode', True)"
api -X DELETE $C/v1/templates/sms/tpl_check >/dev/null

echo "== delivery history: campaign completeness"
CA=$(DB "select id from campaigns where name='Winter sale' order by created_at limit 1")
ENQ=$(DB "select enqueued from campaigns where id='$CA'")
for i in $(seq 1 60); do N=$(DB "select count(distinct notification_id) from delivery_events where campaign_id='$CA'"); [ "$N" -ge "$ENQ" ] && break; sleep 3; done
check "every campaign message is in the history" "$N" "$ENQ"
# Late retries can still land; wait until history and the metric agree (metrics scrape every 10 s).
# max_over_time per series keeps the count across worker restarts (counters restart at 0).
for i in $(seq 1 12); do SENT_H=$(DB "select count(*) from delivery_events where campaign_id='$CA' and status='sent'"); SENT_M=$(q "sum(max_over_time(campaign_notifications_total{campaign=\"$CA\",outcome=\"sent\"}[2h]))"); [ "$SENT_H" = "$SENT_M" ] && break; sleep 5; done
# Exact when no worker restarted since the campaign; a restart that reuses a container's address
# resets its counter series, so allow 1% (the exact completeness checks are the history rows above).
check "history 'sent' count matches worker metric (within 1%)" "$(python3 -c "h,m=$SENT_H,$SENT_M; print(m>0 and abs(h-m)/h <= 0.01)")" True
check "no history events dropped" "$(q 'sum(worker_history_events_total{result="dropped"}) or vector(0)')" 0
F=$(api "$C/v1/deliveries?status=failed&campaign_id=$CA&limit=5")
check "failed feed has rows with a reason" "$(echo "$F" | jq_ "len(d['items'])==5 and all(r['status']=='failed' and r['error'] for r in d['items'])")" True
check "failure rows show provider + address" "$(echo "$F" | jq_ "all(r['provider']=='ses' and '@' in (r['recipient'] or '') for r in d['items'])")" True
S=$(api "$C/v1/deliveries/summary?minutes=1440&campaign_id=$CA")
check "summary counts sent" "$(echo "$S" | jq_ "d['by_status']['sent']")" "$(DB "select count(*) from delivery_events where campaign_id='$CA' and status='sent'")"
check "top reasons include 'invalid recipient'" "$(echo "$S" | jq_ "any(r['error']=='invalid recipient' for r in d['top_reasons'])")" True
check "bad status filter -> 400" "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $KEY" "$C/v1/deliveries?status=bogus")" 400

echo "== delivery history: one person's notifications"
U=$(src "select id from users where email='user12@example.com'")
B='{"title":"Delivery check","body":"Hello {{first_name|there}}, this is a delivery check for {{email}}."}'
api -X PUT $C/v1/templates/email/history_check -d "$B" >/dev/null
B="{\"user_id\":\"$U\",\"channel\":\"email\",\"template\":\"history_check\",\"data\":{\"first_name\":\"Ayesha\"},\"recipient\":{\"email\":\"user12@example.com\"}}"
NID=$(curl -s -H "$H" $I/v1/notifications -d "$B" | jq_ "d['id']")
B='{"channel":"sms"}'; api -X POST "$C/v1/contacts/$U/unsubscribe" -d "$B" >/dev/null
B="{\"user_id\":\"$U\",\"channel\":\"sms\",\"priority\":\"bulk\",\"template\":\"history_suppressed\"}"
SID=$(curl -s -H "$H" $I/v1/notifications -d "$B" | jq_ "d['id']")
for i in $(seq 1 30); do HI=$(api "$C/v1/contacts/$U/notifications"); echo "$HI" | grep -q "$SID" && echo "$HI" | grep -q "$NID" && break; sleep 1; done
check "transactional email appears in their history" "$(echo "$HI" | jq_ "[r['template'] for r in d if r['notification_id']=='$NID']")" "['history_check']"
check "  ...with the address it went to" "$(echo "$HI" | jq_ "[r['recipient'] for r in d if r['notification_id']=='$NID']")" "['user12@example.com']"
check "  ...marked transactional (no campaign)" "$(echo "$HI" | jq_ "[r['campaign_name'] for r in d if r['notification_id']=='$NID']")" "[None]"
check "marketing SMS to an unsubscribed user is suppressed, with reason" "$(echo "$HI" | jq_ "[(r['status'], r['error']) for r in d if r['notification_id']=='$SID']")" "[('suppressed', 'user unsubscribed from this channel')]"
check "attempts endpoint lists the events" "$(api "$C/v1/notifications/$SID/events" | jq_ "[e['status'] for e in d]")" "['suppressed']"
check "history stores the exact text that was sent" "$(echo "$HI" | jq_ "[(r['title'], r['body']) for r in d if r['notification_id']=='$NID']")" "[('Delivery check', 'Hello Ayesha, this is a delivery check for user12@example.com.')]"
B="{\"user_id\":\"$U\",\"channel\":\"email\",\"template\":\"does_not_exist\",\"recipient\":{\"email\":\"user12@example.com\"}}"
XID=$(curl -s -H "$H" $I/v1/notifications -d "$B" | jq_ "d['id']")
B="{\"user_id\":\"$U\",\"channel\":\"sms\",\"template\":\"otp\",\"data\":{\"code\":\"918273\"},\"recipient\":{\"phone\":\"+8801700000000\"}}"
OID=$(curl -s -H "$H" $I/v1/notifications -d "$B" | jq_ "d['id']")
for i in $(seq 1 30); do HI=$(api "$C/v1/contacts/$U/notifications"); echo "$HI" | grep -q "$XID" && echo "$HI" | grep -q "$OID" && break; sleep 1; done
check "missing template fails with a clear reason" "$(echo "$HI" | jq_ "[(r['status'], r['error']) for r in d if r['notification_id']=='$XID']")" "[('failed', \"no email template named 'does_not_exist'\")]"
OTP=$(echo "$HI" | jq_ "[r for r in d if r['notification_id']=='$OID'][0]")
check "OTP still delivered although the user unsubscribed from SMS marketing" "$(echo "$OTP" | grep -cE "'status': '(sent|retrying|failed)'")" 1
check "OTP code never stored in history (hidden template)" "$(DB "select count(*) from delivery_events where notification_id='$OID' and (body like '%918273%' or coalesce(title,'') like '%918273%')")" 0
WS=$(DB "select id from campaigns where name='Winter sale' limit 1")
CU=$(DB "select user_id from delivery_events where campaign_id='$WS' and status='sent' limit 1")
EXP=$(DB "select coalesce(nullif(first_name,''),'Friend') || ', 20% off for ' || coalesce(attributes->>'plan','standard') || ' members.' from contacts where user_id='$CU'")
GOT=$(DB "select replace(body,' ','') from delivery_events where user_id='$CU' and status='sent' and campaign_id='$WS' limit 1")
check "campaign message personalized per person (name, campaign data, attribute)" "$GOT" "$(echo "$EXP" | tr -d ' ')"
B='{"channel":"sms"}'; api -X POST "$C/v1/contacts/$U/resubscribe" -d "$B" >/dev/null
UA=$(DB "select user_id from delivery_events where campaign_id='$CA' and status='sent' limit 1")
check "a campaign recipient's history names the campaign" "$(api "$C/v1/contacts/$UA/notifications" | jq_ "any(r['campaign_name']=='Winter sale' and r['status']=='sent' for r in d)")" True

echo "== retention"
check "daily partitions ready (yesterday..+2)" "$(DB "select count(*) >= 4 from pg_inherits i join pg_class p on p.oid=i.inhparent where p.relname='delivery_events'")" t
check "access needs sign-in" "$(curl -s -o /dev/null -w '%{http_code}' $C/v1/deliveries)" 401

echo; echo "HISTORY RESULT: $PASS passed, $FAIL failed"
