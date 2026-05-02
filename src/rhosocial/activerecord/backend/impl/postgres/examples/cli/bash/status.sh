#!/bin/bash
# status.sh - PostgreSQL CLI status command example

set -e

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DATABASE="${POSTGRES_DATABASE:-test}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

export POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE POSTGRES_USER POSTGRES_PASSWORD

PYTHON_CMD="python -m rhosocial.activerecord.backend.impl.postgres"

echo "=========================================="
echo "PostgreSQL CLI - status command examples"
echo "=========================================="

$PYTHON_CMD status all
$PYTHON_CMD status config
$PYTHON_CMD status performance
$PYTHON_CMD status storage
$PYTHON_CMD status databases
$PYTHON_CMD status all -o json