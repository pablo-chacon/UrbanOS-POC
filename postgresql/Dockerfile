# Use PostgreSQL 15 as the base image
FROM postgres:15

# Install PostGIS and dependencies
RUN apt-get update && \
    apt-get install -y postgis postgresql-15-postgis-3 && \
    rm -rf /var/lib/apt/lists/*

# Copy the initialization SQL script to create tables and extensions
COPY lbrp_db.sql /docker-entrypoint-initdb.d/init.sql

# Copy the custom entrypoint script
COPY custom_entrypoint.sh /usr/local/bin/custom_entrypoint.sh
RUN chmod +x /usr/local/bin/custom_entrypoint.sh

# Expose the default PostgreSQL port
EXPOSE 5432

# Add a health check to ensure PostgreSQL is ready
HEALTHCHECK --interval=10s --timeout=5s --retries=5 CMD pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"

# Use the custom entrypoint script
ENTRYPOINT ["/usr/local/bin/custom_entrypoint.sh"]
