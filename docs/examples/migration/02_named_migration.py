# docs/examples/migration/02_named_migration.py
"""Class-style ``NamedMigration`` on PostgreSQL, plus the CLI wiring.

Runs the repo's own example migration (V001CreateUsers) through MigrationRunner with
this backend's PostgresConnectionConfig / PostgresBackend, and prints the equivalent
``named-migration`` CLI commands. Requires a reachable PostgreSQL server; configure it
with POSTGRES_*. Run from the repository root with PYTHONPATH=src.

For the migration system itself see the core migration chapter; for the full set of
class-style demos see src/rhosocial/activerecord/backend/impl/postgres/examples/named_migrations/.
"""
import os
import tempfile

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.migration import (
    JSONFileMigrationRecordStore,
    MigrationDirection,
    MigrationRunner,
)

MIGRATION_FQN = (
    "rhosocial.activerecord.backend.impl.postgres.examples"
    ".named_migrations.migrations.V001CreateUsers"
)


def build_backend():
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
    return backend


def main():
    backend = build_backend()
    rec_path = tempfile.mktemp(suffix=".json")
    store = JSONFileMigrationRecordStore(rec_path)
    runner = MigrationRunner(MIGRATION_FQN)

    try:
        # The class-style dry run captures ctx.execute(...) named-expression SQL.
        preview = runner.run(backend, MigrationDirection.UP, dry_run=True)
        print("\n== dry run ==")
        print("  version:", preview.version)
        for fqn, sql, _params in preview.dry_run_sql or []:
            print(f"  [{fqn}] {sql}")

        up = runner.run(backend, MigrationDirection.UP, record_store=store)
        print("\n== UP ==")
        print("  version:", up.version, "success:", up.success)
        print("  applied:", [r.version for r in store.get_applied()])

        down = runner.run(backend, MigrationDirection.DOWN, record_store=store)
        print("\n== DOWN ==")
        print("  version:", down.version, "success:", down.success)
        print("  applied after rollback:", [r.version for r in store.get_applied()])
    finally:
        backend.disconnect()
        if os.path.exists(rec_path):
            os.remove(rec_path)

    print("\n== equivalent CLI (python -m ... named-migration) ==")
    print("  ", f"python -m rhosocial.activerecord.backend.impl.postgres named-migration "
          f"{MIGRATION_FQN} --host localhost --database mydb --direction up "
          f"--record-store ./migrations.json")
    print("  ", f"python -m rhosocial.activerecord.backend.impl.postgres named-migration "
          f"{MIGRATION_FQN} --database mydb --direction up --dry-run")


if __name__ == "__main__":
    main()
