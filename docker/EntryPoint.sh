set -euo pipefail

REQUIRED_VARS=(POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE POSTGRES_USERNAME POSTGRES_PASSWORD)
missing=()
for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var:-}" ]; then
    missing+=("$var")
  fi
done
if [ "${#missing[@]}" -gt 0 ]; then
  echo "ERROR: missing required environment variable(s): ${missing[*]}" >&2
  echo "Set them via docker-compose environment/env_file, or -e on docker run." >&2
  exit 1
fi

echo "Waiting for Postgres at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
attempt=0
max_attempts=30
until (exec 3<>"/dev/tcp/${POSTGRES_HOST}/${POSTGRES_PORT}") 2>/dev/null; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "ERROR: Postgres not reachable at ${POSTGRES_HOST}:${POSTGRES_PORT} after ${max_attempts} attempts (60s)." >&2
    exit 1
  fi
  sleep 2
done
exec 3<&- 3>&- 2>/dev/null || true
echo "Postgres is reachable."

exec "$@"