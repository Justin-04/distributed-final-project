#!/usr/bin/env bash
# Start three Agent A replicas (Docker containers) behind the nginx API gateway.
#
#   bash services/gateway/run_local.sh            # (re)start replicas + gateway
#   docker ps --filter name=agent-a               # the three replicas
#
# Replicas: agent-a-1/2/3 on the private network `dji-net`; also published on
# 127.0.0.1:8000/8003/8004 for direct checks. Gateway: dji-gateway on :8080.
# Each replica's hostname is its container name (returned as X-Instance / in /health).
# The replicas reach the services still running on the Windows host (MCP server :8002,
# Agent B :8011, MongoDB :27017, Redis :6380) through `host-svc`, mapped to the host's
# IPv4 address (Docker's host.docker.internal also resolves to IPv6, which the host
# services do not listen on).
#
# Requires the image: docker build -f services/agent-system-a/Dockerfile -t dji-agent-a:step3 .
set -euo pipefail
cd "$(dirname "$0")"
G="$(pwd -W 2>/dev/null || pwd)"
ENV_FILE="$(cd ../agent-system-a && (pwd -W 2>/dev/null || pwd))/.env"
IMAGE="${AGENT_A_IMAGE:-dji-agent-a:step3}"

HOST4=$(docker run --rm alpine:3.20 getent ahostsv4 host.docker.internal | awk 'NR==1{print $1}')
echo "Windows host IPv4 seen from Docker: $HOST4"
docker network inspect dji-net >/dev/null 2>&1 || docker network create dji-net >/dev/null
# the gateway writes its metrics log here; the Prometheus log exporter reads it
docker volume inspect dji-gateway-logs >/dev/null 2>&1 || docker volume create dji-gateway-logs >/dev/null

i=1
for port in 8000 8003 8004; do
  name="agent-a-$i"
  docker rm -f "$name" >/dev/null 2>&1 || true
  MSYS_NO_PATHCONV=1 docker run -d --name "$name" --hostname "$name" --network dji-net \
    -p "127.0.0.1:$port:8000" \
    --add-host "host-svc:$HOST4" \
    --env-file "$ENV_FILE" \
    -e PYTHONUNBUFFERED=1 \
    -e MCP_SERVER_URL=http://host-svc:8002 \
    -e AGENT_B_URL=http://host-svc:8011 \
    -e MONGODB_URI=mongodb://host-svc:27017/dji_rag \
    -e REDIS_URL=redis://host-svc:6380 \
    -e BERT_MODEL_PATH=/app/models/bert_intent_classifier/model \
    -e LANGFUSE_BASE_URL=http://host-svc:5001 \
    --restart unless-stopped "$IMAGE" >/dev/null
  echo "started $name (127.0.0.1:$port)"
  i=$((i + 1))
done

docker rm -f dji-gateway >/dev/null 2>&1 || true
MSYS_NO_PATHCONV=1 docker run -d --name dji-gateway --network dji-net -p 8080:8080 \
  -v "$G/nginx.local.conf:/etc/nginx/conf.d/default.conf:ro" \
  -v "$G/gateway.common.conf:/etc/nginx/gateway/gateway.common.conf:ro" \
  -v "$G/gateway.server.conf:/etc/nginx/gateway/gateway.server.conf:ro" \
  -v "$G/gateway.status.conf:/etc/nginx/gateway/gateway.status.conf:ro" \
  -v dji-gateway-logs:/var/log/gateway \
  --restart unless-stopped nginx:1.27-alpine >/dev/null
echo "started dji-gateway (:8080)"

sleep 2
curl -s 127.0.0.1:8080/gateway/health && echo
