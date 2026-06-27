#!/usr/bin/env bash
# ===========================================================================
# demo_chain.sh — dependency chain migration (PostgreSQL)
#
# Scenarios:
#   - multiple migrations with dependencies executed in order
#   - dependency not satisfied — rejected
#   - rollback in reverse order
#
# Usage:
#   cd python-activerecord-postgres
#   DEMO_VENV_PYTHON=.venv3.14-ubuntu26.04/bin/python \
#     PYTHONPATH=src \
#     bash src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/demo_chain.sh
# ===========================================================================
set -euo pipefail

if [ -d "./src" ]; then
    export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
fi

MODULE="rhosocial.activerecord.backend.impl.postgres.examples.named_migrations"
V001="${MODULE}.migrations.V001CreateUsers"
V002="${MODULE}.migrations.V002CreatePosts"
STORE="./demo_pg_chain_mig.json"
VENV_PYTHON="${DEMO_VENV_PYTHON:-python3}"
PYTHON="$VENV_PYTHON -m rhosocial.activerecord.backend.impl.postgres"

rm -f "$STORE"
echo "=== Dependency Chain Migration (PostgreSQL) ==="
echo

echo "[1] List all migrations (dependencies column):"
$PYTHON named-migration "${MODULE}.migrations" --list -o table
echo

echo "[2] V002CreatePosts dependencies (should declare v001_create_users):"
$PYTHON named-migration "$V002" --describe
echo

echo "[3] Run V002 before V001 (should fail):"
$PYTHON named-migration "$V002" --host localhost --database test --direction up --record-store "$STORE" 2>&1 || true
echo

echo "[4] Apply V001 (create users table):"
$PYTHON named-migration "$V001" --host localhost --database test --direction up --record-store "$STORE"
echo

echo "[5] Apply V002 (create posts table):"
$PYTHON named-migration "$V002" --host localhost --database test --direction up --record-store "$STORE"
echo

echo "[6] Rollback V002 (downstream first):"
$PYTHON named-migration "$V002" --host localhost --database test --direction down --record-store "$STORE"
echo

echo "[7] Rollback V001:"
$PYTHON named-migration "$V001" --host localhost --database test --direction down --record-store "$STORE"
echo

echo "[8] Record store final state (all rolled back):"
cat "$STORE"
echo

rm -f "$STORE"
echo "=== Dependency Chain Migration Complete (PostgreSQL) ==="