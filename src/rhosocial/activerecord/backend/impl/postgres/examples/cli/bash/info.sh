#!/bin/bash
# info.sh - PostgreSQL CLI info command example

set -e

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DATABASE="${POSTGRES_DATABASE:-test}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

export POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE POSTGRES_USER POSTGRES_PASSWORD

PYTHON_CMD="python -m rhosocial.activerecord.backend.impl.postgres"

echo "=========================================="
echo "PostgreSQL CLI - info command examples"
echo "=========================================="

echo ""
echo "--- Basic info (table output) ---"
$PYTHON_CMD info

echo ""
echo "--- Verbose info (protocol families) ---"
$PYTHON_CMD info -v

echo ""
echo "--- Detailed verbose (all details) ---"
$PYTHON_CMD info -vv

echo ""
echo "--- JSON output ---"
$PYTHON_CMD info -o json

echo ""
echo "--- Info with specific PostgreSQL version ---"
$PYTHON_CMD info --version 15.0.0

echo ""
echo "--- Rich ASCII output ---"
$PYTHON_CMD info --rich-ascii