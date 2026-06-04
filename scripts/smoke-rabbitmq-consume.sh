#!/usr/bin/env bash
set -euo pipefail

: "${VIDEOGEN_RABBITMQ_AMQPS_URLS:?VIDEOGEN_RABBITMQ_AMQPS_URLS required}"

# Extract first URL host for TLS check (never print the full URL with credentials)
FIRST_URL="${VIDEOGEN_RABBITMQ_AMQPS_URLS%%,*}"
HOST_PORT=$(echo "$FIRST_URL" | sed 's|amqps://[^@]*@||' | sed 's|/.*||')
HOST="${HOST_PORT%%:*}"
PORT="${HOST_PORT##*:}"
PORT="${PORT:-5671}"

echo "Checking TLS to broker host: ${HOST}:${PORT}"
openssl s_client -connect "${HOST}:${PORT}" -servername "${HOST}" </dev/null 2>/dev/null \
  | openssl x509 -noout -subject -ext subjectAltName 2>/dev/null \
  || echo "WARNING: TLS check failed — verify broker certificate SANs"

echo "Smoke check complete. To test actual consumption, run worker with VIDEOGEN_RABBITMQ_ENABLED=true"
