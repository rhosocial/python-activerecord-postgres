#!/bin/bash
# introspect.sh - PostgreSQL CLI introspect command example

set -e

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_DATABASE="${POSTGRES_DATABASE:-test}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"

export POSTGRES_HOST POSTGRES_PORT POSTGRES_DATABASE POSTGRES_USER POSTGRES_PASSWORD

PYTHON_CMD="python -m rhosocial.activerecord.backend.impl.postgres"

TEST_TABLE_USERS="cli_test_users"
TEST_TABLE_ORDERS="cli_test_orders"

cleanup() {
    $PYTHON_CMD query "DROP TABLE IF EXISTS $TEST_TABLE_ORDERS CASCADE" 2>/dev/null || true
    $PYTHON_CMD query "DROP TABLE IF EXISTS $TEST_TABLE_USERS CASCADE" 2>/dev/null || true
}

setup() {
    $PYTHON_CMD query "CREATE TABLE IF NOT EXISTS $TEST_TABLE_USERS (id SERIAL PRIMARY KEY, name VARCHAR(100))"
    $PYTHON_CMD query "INSERT INTO $TEST_TABLE_USERS (name) VALUES ('Alice'), ('Bob')"
    $PYTHON_CMD query "CREATE TABLE IF NOT EXISTS $TEST_TABLE_ORDERS (id SERIAL PRIMARY KEY, user_id INTEGER, amount DECIMAL(10,2))"
    $PYTHON_CMD query "INSERT INTO $TEST_TABLE_ORDERS (user_id, amount) VALUES (1, 100.00)"
}

trap cleanup EXIT

echo "=========================================="
echo "PostgreSQL CLI - introspect command examples"
echo "=========================================="

setup

$PYTHON_CMD introspect tables
$PYTHON_CMD introspect views
$PYTHON_CMD introspect table $TEST_TABLE_USERS
$PYTHON_CMD introspect columns $TEST_TABLE_USERS
$PYTHON_CMD introspect indexes $TEST_TABLE_USERS
$PYTHON_CMD introspect foreign-keys $TEST_TABLE_ORDERS
$PYTHON_CMD introspect database
$PYTHON_CMD introspect tables -o json