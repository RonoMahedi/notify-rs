"""Generates deploy/grafana/dashboards/notifications.json. Run: python3 deploy/grafana/gen_dashboard.py"""
import json, os, sys
DS = {"type": "prometheus", "uid": "prom"}
panels, pid, y = [], 1, 0

def nid():
    global pid; pid += 1; return pid

def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}}); y += 1

def stat(title, expr, x, w=4, unit="short", thresholds=None, desc=""):
    steps = thresholds or [{"color": "green", "value": None}]
    panels.append({"type": "stat", "title": title, "id": nid(), "datasource": DS, "description": desc,
        "gridPos": {"x": x, "y": y, "w": w, "h": 4},
        "targets": [{"refId": "A", "expr": expr, "datasource": DS}],
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area"},
        "fieldConfig": {"defaults": {"unit": unit, "decimals": 2 if unit in ("percentunit", "s") else 0,
            "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []}})

def ts(title, targets, x, w=12, h=8, unit="short", stack=False, desc=""):
    panels.append({"type": "timeseries", "title": title, "id": nid(), "datasource": DS, "description": desc,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS} for i, (e, l) in enumerate(targets)],
        "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["mean", "max", "lastNotNull"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"fillOpacity": 15 if stack else 5, "lineWidth": 1,
            "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []}})

def adv(h):
    global y; y += h

sent = 'sum(rate(notifications_processed_total{outcome="sent"}[$__rate_interval]))'
final = 'sum(rate(notifications_processed_total{outcome=~"sent|dlq"}[$__rate_interval]))'
G, Y, R = "green", "#EAB839", "red"

row("Overview / SLOs")
stat("Ingest rate", 'sum(rate(notifications_accepted_total[$__rate_interval]))', 0, unit="reqps")
stat("Delivered rate", sent, 4, unit="reqps")
stat("Projected / day", 'sum(rate(notifications_processed_total{outcome="sent"}[1h])) * 86400', 8,
     desc="Last hour's delivery rate x 86400. Target: 100M.",
     thresholds=[{"color": R, "value": None}, {"color": Y, "value": 5e7}, {"color": G, "value": 1e8}])
stat("Success rate", f'{sent} / clamp_min({final}, 1e-9)', 12, unit="percentunit",
     thresholds=[{"color": R, "value": None}, {"color": Y, "value": 0.98}, {"color": G, "value": 0.99}])
stat("p99 E2E (high)", 'histogram_quantile(0.99, sum by (le) (rate(notification_e2e_latency_seconds_bucket{priority="high"}[$__rate_interval])))', 16, unit="s",
     thresholds=[{"color": G, "value": None}, {"color": Y, "value": 2}, {"color": R, "value": 5}])
stat("Total backlog", 'sum(kafka_consumer_lag{topic=~"notify\\\\..+\\\\.(high|normal|bulk)"})', 20,
     thresholds=[{"color": G, "value": None}, {"color": Y, "value": 100000}, {"color": R, "value": 500000}])
adv(4)

row("Throughput")
ts("Ingest by channel", [('sum by (channel) (rate(notifications_accepted_total[$__rate_interval]))', "{{channel}}")], 0, unit="reqps", stack=True)
ts("Outcomes", [('sum by (outcome) (rate(notifications_processed_total[$__rate_interval]))', "{{outcome}}")], 12, unit="reqps", stack=True,
   desc="sent / retry / dlq / expired / suppressed / duplicate")
adv(8)
ts("Delivered by channel", [('sum by (channel) (rate(notifications_processed_total{outcome="sent"}[$__rate_interval]))', "{{channel}}")], 0, unit="reqps")
ts("Rejected at API", [('sum by (reason) (rate(notifications_rejected_total[$__rate_interval]))', "{{reason}}"),
                       ('sum(rate(notifications_deduplicated_total[$__rate_interval]))', "idempotent duplicate")], 12, unit="reqps")
adv(8)

row("Latency")
ts("End-to-end latency (created -> provider ack)", [
    (f'histogram_quantile({q}, sum by (le, priority) (rate(notification_e2e_latency_seconds_bucket[$__rate_interval])))', f"p{int(q*100)} {{{{priority}}}}")
    for q in (0.5, 0.95, 0.99)], 0, unit="s")
ts("Provider p99 latency", [('histogram_quantile(0.99, sum by (le, provider) (rate(provider_request_duration_seconds_bucket[$__rate_interval])))', "{{provider}}")], 12, unit="s")
adv(8)

row("Queues / Kafka")
ts("Consumer lag by topic", [('sum by (topic) (kafka_consumer_lag)', "{{topic}}")], 0, desc="Backlog. Rising main-topic lag => scale workers (KEDA on this metric).")
ts("Retries by tier & DLQ", [('sum by (tier) (rate(notifications_retried_total[$__rate_interval]))', "{{tier}}"),
                             ('sum(rate(notifications_processed_total{outcome="dlq"}[$__rate_interval]))', "dlq")], 12, unit="reqps")
adv(8)
ts("Kafka produce p99 (API)", [('histogram_quantile(0.99, sum by (le) (rate(kafka_produce_duration_seconds_bucket[$__rate_interval])))', "p99")], 0, w=8, unit="s")
ts("Rate limited (waits)", [('sum by (channel) (rate(worker_rate_limited_total[$__rate_interval]))', "{{channel}}")], 8, w=8, unit="reqps")
ts("Worker batch", [('histogram_quantile(0.99, sum by (le) (rate(worker_batch_duration_seconds_bucket[$__rate_interval])))', "batch p99 duration"),
                    ('avg(worker_batch_size)', "avg batch size")], 16, w=8)
adv(8)

row("Campaigns & Audience")
stat("Running campaigns", 'sum(campaigns_running)', 0)
stat("Campaign enqueue/s", 'sum(rate(campaign_enqueued_total[$__rate_interval]))', 4, unit="reqps")
stat("Bulk lane backlog", 'sum(kafka_consumer_lag{topic=~"notify\\\\..+\\\\.bulk"})', 8,
     thresholds=[{"color": G, "value": None}, {"color": Y, "value": 200000}, {"color": R, "value": 1000000}])
stat("Contacts", 'max(contacts_total_estimate)', 12, desc="Estimate from pg_class.reltuples")
stat("Since last identity sync", 'time() - max(identity_sync_last_success_timestamp_seconds)', 16, unit="s",
     thresholds=[{"color": G, "value": None}, {"color": Y, "value": 120}, {"color": R, "value": 600}])
stat("Import rows/s", 'sum(rate(import_rows_total{result="imported"}[$__rate_interval]))', 20, unit="reqps")
adv(4)
NAME = ' * on(campaign) group_left(name) max by (campaign, name) (campaign_info)'
ts("Campaign progress", [('max by (campaign) (campaign_progress_ratio)' + NAME, "{{name}}")], 0, unit="percentunit",
   desc="(enqueued + suppressed) / targeted")
ts("Campaign enqueue rate", [('sum by (campaign) (rate(campaign_enqueued_total[$__rate_interval]))' + NAME, "{{name}}")], 12, unit="reqps")
adv(8)
ts("Campaign delivery outcomes", [('sum by (campaign, outcome) (rate(campaign_notifications_total[$__rate_interval]))'
                                   ' * on(campaign) group_left(name) max by (campaign, name) (campaign_info)', "{{name}} {{outcome}}")], 0, unit="reqps")
ts("Campaign suppressed (cap / invalid)", [('sum by (campaign, reason) (rate(campaign_suppressed_total[$__rate_interval]))'
                                            ' * on(campaign) group_left(name) max by (campaign, name) (campaign_info)', "{{name}} {{reason}}")], 12, unit="reqps")
adv(8)
ts("Identity sync rows/s", [('sum(rate(identity_sync_rows_total[$__rate_interval]))', "rows"),
                            ('sum(rate(identity_sync_errors_total[$__rate_interval]))', "errors")], 0, w=8, unit="reqps")
ts("Import rows/s", [('sum by (result) (rate(import_rows_total[$__rate_interval]))', "{{result}}")], 8, w=8, unit="reqps")
ts("Campaign chunk p99", [('histogram_quantile(0.99, sum by (le) (rate(campaign_chunk_duration_seconds_bucket[$__rate_interval])))', "p99")], 16, w=8, unit="s")
adv(8)

row("API & Infra")
ts("HTTP requests by status", [('sum by (status) (rate(http_requests_total[$__rate_interval]))', "{{status}}")], 0, w=8, unit="reqps")
ts("HTTP p99 by route", [('histogram_quantile(0.99, sum by (le, route) (rate(http_request_duration_seconds_bucket[$__rate_interval])))', "{{route}}")], 8, w=8, unit="s")
ts("Infra errors / targets up", [('sum by (op) (rate(worker_infra_errors_total[$__rate_interval]))', "err {{op}}"),
                                 ('sum by (job) (up)', "up {{job}}")], 16, w=8)
adv(8)

json.dump({"uid": "notify-main", "title": "Notification System", "tags": ["notifications"],
    "timezone": "browser", "schemaVersion": 39, "refresh": "10s", "time": {"from": "now-1h", "to": "now"},
    "panels": panels, "templating": {"list": []}}, open(sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(__file__), "dashboards", "notifications.json"), "w"), indent=1)
print(len(panels), "panels")
