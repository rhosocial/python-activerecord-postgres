#!/bin/bash
# named_expression.sh - PostgreSQL CLI named-expression command example

set -e

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DATABASE="${POSTGRES_DATABASE:-test}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

export POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE POSTGRES_USER POSTGRES_PASSWORD

PYTHON_CMD="python -m rhosocial.activerecord.backend.impl.postgres"

echo "=========================================="
echo "PostgreSQL CLI - named-expression command examples"
echo "=========================================="

$PYTHON_CMD named-expression --list rhosocial.activerecord.backend.impl.postgres.examples.named_expressions.order_expressions 2>/dev/null || echo "(No named expressions found)"
$PYTHON_CMD named-connection --list rhosocial.activerecord.backend.impl.postgres.examples.named_connections