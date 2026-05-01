# docker/postgres-all-ext/Dockerfile
# Multi-arch Dockerfile for PostgreSQL with all extensions
#
# Supported architectures: linux/amd64, linux/arm64, linux/ppc64le, linux/s390x
# Supported PostgreSQL versions: 14, 15, 16, 17, 18
#
# Usage:
#   docker build -t postgres-all-ext:16 .
#   docker run -e POSTGRES_PASSWORD=secret postgres-all-ext:16
#
# Build arguments:
#   PG_VERSION: PostgreSQL version (default: 16)
#   POSTGIS_VERSION: PostGIS version (default: 3.4)
#   PGVECTOR_VERSION: pgvector version (default: 0.8.0)
#
# Build example:
#   docker build --build-arg PG_VERSION=16 -t postgres-all-ext:16 .
#

ARG PG_VERSION=16
FROM postgres:${PG_VERSION}-bookworm

LABEL maintainer="rhosocial"
LABEL org.opencontainers.image.description="PostgreSQL with all common extensions for testing"
LABEL org.opencontainers.image.source="https://github.com/rhosocial/python-activerecord-postgres"

# Build arguments
ARG PG_VERSION
ARG POSTGIS_VERSION=3.4
ARG PGVECTOR_VERSION=0.8.0

# Environment variables
ENV PG_MAJOR=${PG_VERSION} \
    POSTGIS_MAJOR=${POSTGIS_VERSION} \
    PGVECTOR_VERSION=${PGVECTOR_VERSION}

# Install all extensions
# Note: bookworm is Debian-based with multi-arch support
RUN apt-get update && apt-get install -y --no-install-recommends \
    # PostGIS (spatial database extension)
    postgresql-${PG_MAJOR}-postgis-${POSTGIS_MAJOR} \
    postgresql-${PG_MAJOR}-postgis-${POSTGIS_MAJOR}-scripts \
    \
    # pgvector (vector similarity search)
    postgresql-${PG_MAJOR}-pgvector \
    \
    # earthdistance (great-circle distance calculations)
    # Requires cube extension as dependency
    postgresql-${PG_MAJOR}-cube \
    postgresql-${PG_MAJOR}-earthdistance \
    \
    # Additional contrib extensions for comprehensive testing
    postgresql-${PG_MAJOR}-uuid-ossp \
    postgresql-${PG_MAJOR}-pg-trgm \
    postgresql-${PG_MAJOR}-hstore \
    \
    # Utilities
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Copy initialization scripts
COPY docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/
RUN chmod 755 /docker-entrypoint-initdb.d/*.sh /docker-entrypoint-initdb.d/*.sql 2>/dev/null || true

# Expose default port
EXPOSE 5432

# Default environment variables
ENV POSTGRES_DB=test_db \
    POSTGRES_USER=root \
    POSTGRES_PASSWORD=password

# Health check
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB} || exit 1

# Use default postgres entrypoint
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["postgres"]