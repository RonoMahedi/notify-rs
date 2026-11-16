#!/bin/bash
# System view: Kafka topics/groups, live metrics, storage, dead-letter queue. Run after tests/e2e.sh.
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE/.."
C=http://localhost:8090; I=http://localhost:8080; H='content-type: application/json'; KEY="${ADMIN_TOKEN:-dev-admin-token-change-me}"
PASS=0; FAIL=0
check(){ if [ $# -ne 3 ]; then echo "  FAIL $1: harness got $# args"; FAIL=$((FAIL+1)); return; fi
         if [ "$2" == "$3" ]; then echo "  PASS $1 ($2)"; PASS=$((PASS+1)); else echo "  FAIL $1: got '$2' want '$3'"; FAIL=$((FAIL+1)); fi; }
jq_(){ python3 -c "import json,sys; d=json.load(sys.stdin); print(eval(sys.argv[1]))" "$1" 2>/dev/null || echo "<non-json>"; }
api(){ curl -s -H "Authorization: Bearer $KEY" "$@"; }

check "needs sign-in" "$(curl -s -o /dev/null -w '%{http_code}' $C/v1/system)" 401
S=$(api $C/v1/system)
echo "== kafka"
TOPICS=$(docker compose exec -T redpanda rpk topic list 2>/dev/null | awk 'NR>1 && /^notify\./{print $1}' | sort | tr '\n' ',')
check "every Kafka topic listed" "$(echo "$S" | jq_ "','.join(sorted(t['name'] for t in d['kafka']['topics'] if t['name'].startswith('notify.'))) + ','")" "$TOPICS"
check "partition counts match Kafka" "$(echo "$S" | jq_ "[t['partitions'] for t in d['kafka']['topics'] if t['name'] in ('notify.email.bulk','notify.retry.5s','notify.dlq')]")" "[3, 12, 6]"
check "each topic names its reader" "$(echo "$S" | jq_ "{t['name']: t['reader'] for t in d['kafka']['topics']}['notify.email.bulk']")" notify-workers-bulk
# Right after a service restart a group rebalances for a few seconds: wait for Stable.
for i in $(seq 1 20); do G=$(api $C/v1/system | jq_ "sorted((g['name'], g['state']) for g in d['kafka']['groups'])"); echo "$G" | grep -q -v "Rebalanc" && break; sleep 3; done
check "7 consumer groups, all Stable" "$G" "[('notify-history', 'Stable'), ('notify-workers', 'Stable'), ('notify-workers-bulk', 'Stable'), ('notify-workers-high', 'Stable'), ('notify-workers-retry', 'Stable'), ('notify-workers-retry-10m', 'Stable'), ('notify-workers-retry-1m', 'Stable')]"
BEFORE=$(echo "$S" | jq_ "{t['name']: t['produced'] for t in d['kafka']['topics']}['notify.sms.high']")
U="sys-check-$(date +%s)"; B="{\"user_id\":\"$U\",\"channel\":\"sms\",\"priority\":\"high\",\"template\":\"otp\"}"
curl -s -H "$H" $I/v1/notifications -d "$B" >/dev/null; sleep 3
AFTER=$(api $C/v1/system | jq_ "{t['name']: t['produced'] for t in d['kafka']['topics']}['notify.sms.high']")
check "one OTP -> notify.sms.high grows by exactly 1" "$((AFTER - BEFORE))" 1
echo "== metrics, services, storage"
check "services: 2 API instances up" "$(echo "$S" | jq_ "sum(1 for x in d['metrics']['services'] if x['job']=='api' and x['up'])")" 2
check "services: every scraped target up" "$(echo "$S" | jq_ "all(x['up'] for x in d['metrics']['services'])")" True
check "traffic numbers present" "$(echo "$S" | jq_ "all(isinstance(d['metrics'][k], (int, float)) for k in ('ingest_per_s','sent_per_s','failed_per_s','retry_per_s'))")" True
check "redis stats present" "$(echo "$S" | jq_ "d['redis']['keys'] > 0 and d['redis']['used_memory'] is not None")" True
check "postgres sizes present" "$(echo "$S" | jq_ "d['postgres']['database_bytes'] > 0 and d['postgres']['history_bytes'] > 0")" True
echo "== dead-letter queue"
D=$(api "$C/v1/system/dlq?limit=20")
check "DLQ total = Kafka's stored count" "$(echo "$D" | jq_ "d['total']")" "$(echo "$S" | jq_ "{t['name']: t['stored'] for t in d['kafka']['topics']}['notify.dlq']")"
check "returns the 20 newest" "$(echo "$D" | jq_ "len(d['items'])==20 and [i['created_at'] for i in d['items']]==sorted([i['created_at'] for i in d['items']], reverse=True)")" True
check "each has user, failure reason, and address (in-app has none)" "$(echo "$D" | jq_ "all(i['user_id'] and i['reason'] and (i['address'] or i['channel']=='in_app') for i in d['items'])")" True
check "reading the DLQ leaves no consumer group behind" "$(docker compose exec -T redpanda rpk group list 2>/dev/null | grep -c 'notify-dlq-peek')" 0

echo; echo "SYSTEM RESULT: $PASS passed, $FAIL failed"
