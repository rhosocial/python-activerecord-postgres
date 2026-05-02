#!/bin/bash
# named_procedure.sh - PostgreSQL CLI named-procedure command example

set -e

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DATABASE="${POSTGRES_DATABASE:-test}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

export POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE POSTGRES_USER POSTGRES_PASSWORD

PYTHON_CMD="python -m rhosocial.activerecord.backend.impl.postgres"

echo "=========================================="
echo "PostgreSQL CLI - named-procedure command examples"
echo "=========================================="

$PYTHON_CMD named-procedure --list rhosocial.activerecord.backend.impl.postgres.examples.named_procedures 2>/dev/null || echo "(No named procedures found)"
$PYTHON_CMD named-connection --list rhosocial.activerecord.backend.impl.postgres.examples.named_connections