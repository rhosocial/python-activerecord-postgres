#!/bin/bash
# query.sh - PostgreSQL CLI query command example

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
    $PYTHON_CMD query "CREATE TABLE IF NOT EXISTS $TEST_TABLE_USERS (id SERIAL PRIMARY KEY, name VARCHAR(100), status VARCHAR(20) DEFAULT 'active')"
    $PYTHON_CMD query "INSERT INTO $TEST_TABLE_USERS (name, status) VALUES ('Alice', 'active'), ('Bob', 'inactive'), ('Charlie', 'active')"
    $PYTHON_CMD query "CREATE TABLE IF NOT EXISTS $TEST_TABLE_ORDERS (id SERIAL PRIMARY KEY, user_id INTEGER, amount DECIMAL(10,2))"
    $PYTHON_CMD query "INSERT INTO $TEST_TABLE_ORDERS (user_id, amount) VALUES (1, 100.00), (1, 200.00), (2, 150.00)"
}

trap cleanup EXIT

echo "=========================================="
echo "PostgreSQL CLI - query command examples"
echo "=========================================="

setup

$PYTHON_CMD query "SELECT * FROM $TEST_TABLE_USERS"
$PYTHON_CMD query "SELECT * FROM $TEST_TABLE_USERS WHERE status = 'active'"
$PYTHON_CMD query "SELECT u.name, o.amount FROM $TEST_TABLE_USERS u JOIN $TEST_TABLE_ORDERS o ON u.id = o.user_id"
$PYTHON_CMD query "SELECT user_id, SUM(amount) as total FROM $TEST_TABLE_ORDERS GROUP BY user_id"
$PYTHON_CMD query "SELECT * FROM $TEST_TABLE_USERS" -o json
$PYTHON_CMD query "SELECT * FROM $TEST_TABLE_USERS LIMIT 2" -o csv
echo "SELECT * FROM $TEST_TABLE_USERS WHERE id > 0" > /tmp/test_query.sql
$PYTHON_CMD query -f /tmp/test_query.sql
rm -f /tmp/test_query.sql