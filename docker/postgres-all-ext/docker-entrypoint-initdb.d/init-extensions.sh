#!/bin/bash
# docker/postgres-all-ext/docker-entrypoint-initdb.d/init-extensions.sh
#
# Initialize all extensions during database creation.
# This script is executed automatically by docker-entrypoint.sh
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Built-in extensions (contrib)
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";
    CREATE EXTENSION IF NOT EXISTS "ltree";
    CREATE EXTENSION IF NOT EXISTS "intarray";
    CREATE EXTENSION IF NOT EXISTS "tablefunc";
    CREATE EXTENSION IF NOT EXISTS "pg_trgm";
    CREATE EXTENSION IF NOT EXISTS "hstore";
    CREATE EXTENSION IF NOT EXISTS "cube";
    CREATE EXTENSION IF NOT EXISTS "earthdistance";
    CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

    -- Extensions that require packages to be installed first
    -- PostGIS
    DO \$\$
    BEGIN
        IF EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = 'postgis') THEN
            CREATE EXTENSION IF NOT EXISTS postgis;
            CREATE EXTENSION IF NOT EXISTS postgis_topology;
            CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
        END IF;
    END
    \$\$;

    -- pgvector
    DO \$\$
    BEGIN
        IF EXISTS(SELECT 1 FROM pg_available_extensions WHERE name = 'vector') THEN
            CREATE EXTENSION IF NOT EXISTS vector;
        END IF;
    END
    \$\$;
EOSQL

echo "PostgreSQL extensions initialized successfully."