#!/bin/bash
set -e

# Log startup
echo "🚀 Starting custom PostgreSQL entrypoint with PostGIS..."

# Start PostgreSQL in the background using the official entrypoint
docker-entrypoint.sh postgres &

# Wait until PostgreSQL is ready to accept connections
until pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" > /dev/null 2>&1; do
  echo "⏳ Waiting for PostgreSQL to become ready..."
  sleep 2
done

echo "✅ PostgreSQL is ready for connections."

# Wait for the background PostgreSQL process to stay in the foreground
wait -n
