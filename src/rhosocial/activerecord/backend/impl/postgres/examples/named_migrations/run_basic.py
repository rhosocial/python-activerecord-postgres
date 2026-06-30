# src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/run_basic.py
"""
Basic migration example — single migration UP then DOWN (PostgreSQL).

This script demonstrates:
  1. Creating a PostgreSQL backend
  2. Running a single ``NamedMigration`` UP (creates ``users`` table)
  3. Verifying the table was created
  4. Running the same migration DOWN (drops ``users`` table)
  5. Showing JSON record store persistence
  6. Dry-run mode (no actual changes)
  7. Duplicate execution protection

Usage:
    python -m rhosocial.activerecord.backend.impl.postgres.examples.named_migrations.run_basic
"""

from pathlib import Path
import tempfile
import os

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.migration import (
    MigrationRunner,
    MigrationDirection,
    JSONFileMigrationRecordStore,
    MigrationAlreadyAppliedError,
)


def main():
    print("=" * 60)
    print("Named Migration Demo — Basic (PostgreSQL)")
    print("=" * 60)

    config = PostgresConnectionConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DATABASE", "test"),
        username=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    print("\n[1] PostgreSQL backend connected.")

    store_path = Path(tempfile.gettempdir()) / "mig_pg_basic.json"
    if store_path.exists():
        store_path.unlink()
    store = JSONFileMigrationRecordStore(store_path)
    print(f"[2] Record store: {store_path}")

    fqn = (
        "rhosocial.activerecord.backend.impl.postgres.examples"
        ".named_migrations.migrations.V001CreateUsers"
    )
    runner = MigrationRunner(fqn)

    print("\n[3] Dry-run (UP) — no actual changes …")
    result = runner.run(backend, MigrationDirection.UP, dry_run=True)
    print(f"    Result: version={result.version}, success={result.success}")
    print("    ✓ Dry-run completed (table not created).")

    print("\n[4] Applying v001_create_users (UP) …")
    result = runner.run(backend, MigrationDirection.UP, record_store=store)
    print(f"    Result: version={result.version}, success={result.success}")
    print("    ✓ Table 'users' created.")

    print("\n[5] Duplicate UP (should be rejected) …")
    try:
        runner.run(backend, MigrationDirection.UP, record_store=store)
        print("    ✗ ERROR: should have raised!")
    except MigrationAlreadyAppliedError as e:
        print(f"    ✓ {e}")

    print("\n[6] Rolling back v001_create_users (DOWN) …")
    result = runner.run(backend, MigrationDirection.DOWN, record_store=store)
    print(f"    Result: version={result.version}, success={result.success}")
    print("    ✓ Table 'users' dropped.")

    applied = store.get_applied()
    print(f"\n[7] Applied migrations: {len(applied)} (should be 0)")

    backend.disconnect()
    if store_path.exists():
        store_path.unlink()
    print("\n=== PostgreSQL basic migration demo completed ===")


if __name__ == "__main__":
    main()