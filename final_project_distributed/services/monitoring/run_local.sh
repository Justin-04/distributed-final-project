#!/usr/bin/env bash
# Start the monitoring stack locally (run ../gateway/run_local.sh first).
#
#   dji-gateway-log-exporter    :4040  gateway access log → per-replica metrics
#   dji-gateway-nginx-exporter  :9113  nginx stub_status → connections
#   dji-prometheus              :9090  scrapes the exporters and the Agent A replicas
#   dji-grafana                 :3000  dashboards (anonymous read-only; admin / admin)
set -euo pipefail
cd "$(dirname "$0")"
M="$(pwd -W 2>/dev/null || pwd)"

# Same Docker network as the gateway and the Agent A replicas (created by
# ../gateway/run_local.sh), so Prometheus reaches them by container name.
docker network inspect dji-net >/dev/null 2>&1 || docker network create dji-net >/dev/null
docker volume inspect dji-prometheus-data >/dev/null 2>&1 || docker volume create dji-prometheus-data >/dev/null

for c in dji-gateway-log-exporter dji-gateway-nginx-exporter dji-prometheus dji-grafana; do
  docker rm -f "$c" >/dev/null 2>&1 || true
done

MSYS_NO_PATHCONV=1 docker run -d --name dji-gateway-log-exporter --network dji-net \
  -v dji-gateway-logs:/var/log/gateway:ro \
  -v "$M/nginxlog-exporter.yml:/etc/nginxlog-exporter.yml:ro" \
  --restart unless-stopped \
  quay.io/martinhelmich/prometheus-nginxlog-exporter:v1.11.0 -config-file /etc/nginxlog-exporter.yml

MSYS_NO_PATHCONV=1 docker run -d --name dji-gateway-nginx-exporter --network dji-net \
  --restart unless-stopped \
  nginx/nginx-prometheus-exporter:1.3.0 --nginx.scrape-uri=http://dji-gateway:8081/stub_status

MSYS_NO_PATHCONV=1 docker run -d --name dji-prometheus --network dji-net -p 9090:9090 \
  -v "$M/prometheus.local.yml:/etc/prometheus/prometheus.yml:ro" \
  -v dji-prometheus-data:/prometheus \
  --restart unless-stopped \
  prom/prometheus:v2.54.1 --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.retention.time=7d --web.enable-lifecycle

MSYS_NO_PATHCONV=1 docker run -d --name dji-grafana --network dji-net -p 3000:3000 \
  -e PROMETHEUS_URL=http://dji-prometheus:9090 \
  -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
  -e GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/gateway-agent-a.json \
  -v "$M/grafana/provisioning:/etc/grafana/provisioning:ro" \
  -v "$M/grafana/dashboards:/var/lib/grafana/dashboards:ro" \
  --restart unless-stopped \
  grafana/grafana:11.2.2

echo "Prometheus: http://localhost:9090/targets"
echo "Grafana:    http://localhost:3000  (dashboard 'Gateway & Agent A')"
