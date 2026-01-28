#!/bin/bash
# Run all ClickHouse migrations in order

set -e

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-gtp}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-gtp_local}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="$SCRIPT_DIR/../migrations/clickhouse"

echo "Running ClickHouse migrations..."

for f in "$MIGRATIONS_DIR"/*.sql; do
    echo "  Running $(basename "$f")..."
    docker exec -i clickhouse clickhouse-client \
        --user "$CLICKHOUSE_USER" \
        --password "$CLICKHOUSE_PASSWORD" \
        < "$f"
done

echo "Done! All migrations applied."
