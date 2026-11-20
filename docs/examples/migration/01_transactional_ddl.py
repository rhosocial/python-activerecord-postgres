# docs/examples/migration/01_transactional_ddl.py
"""Function-style PostgreSQL migrations: dry run, transactional DDL, version gating.

Requires a reachable PostgreSQL server; configure it with the POSTGRES_* environment
variables. Run from the repository root with PYTHONPATH=src.

Demonstrates (see docs/en_US/migration/README.md for the system itself):
  - configuring this repo's PostgresConnectionConfig / PostgresBackend
  - the dialect's transactional DDL support and the all-or-nothing failure path
  - @migration functions emitted through ctx.apply() and run via run_migration_group
  - gating a PostgreSQL 14+ type on the adapted server version
No raw SQL strings are built here: every DDL statement comes from a model helper.
"""
import os
import tempfile
from typing import Annotated, Optional

from rhosocial.activerecord.base import UseSqlType
from rhosocial.activerecord.backend.expression.types import IntegerType, VarCharType
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresInt4MultirangeType,
)
from rhosocial.activerecord.backend.migration import (
    JSONFileMigrationRecordStore,
    MigrationDirection,
    MigrationFunctionExecutionError,
    migration,
    run_migration_group,
)
from rhosocial.activerecord.model import ActiveRecord


class User(ActiveRecord):
    @classmethod
    def table_name(cls) -> str:
        return "users"

    id: Annotated[Optional[int], UseSqlType(IntegerType())] = None
    name: Annotated[str, UseSqlType(VarCharType(length=64))]


class Archive(ActiveRecord):
    """Uses INT4MULTIRANGE, which exists only on PostgreSQL 14+."""

    @classmethod
    def table_name(cls) -> str:
        return "archives"

    id: Annotated[Optional[int], UseSqlType(IntegerType())] = None
    spans: Annotated[Optional[tuple], UseSqlType(PostgresInt4MultirangeType())] = None


@migration("v001_create_users", description="create the initial users table")
def create_users(ctx):
    if ctx.direction == MigrationDirection.UP:
        ctx.apply(User.create_table_statement(backend=ctx.backend))
    else:
        ctx.apply(User.drop_table_statement(if_exists=True, backend=ctx.backend))


@migration("v002_create_archive", description="create a PostgreSQL 14+ multirange table")
def create_archive(ctx):
    # The dialect carries the version detected by introspect_and_adapt().
    if not ctx.backend.dialect.version_at_least((14, 0, 0)):
        print("  [gate] server < 14: skipping INT4MULTIRANGE archive table")
        return
    if ctx.direction == MigrationDirection.UP:
        ctx.apply(Archive.create_table_statement(backend=ctx.backend))
    else:
        ctx.apply(Archive.drop_table_statement(if_exists=True, backend=ctx.backend))


@migration("v003_failing_step", description="intentional failure to show DDL rollback")
def failing_step(ctx):
    raise RuntimeError("intentional failure for the transactional DDL demo")


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
    users = f"{__name__}:create_users"
    archive = f"{__name__}:create_archive"
    failing = f"{__name__}:failing_step"

    try:
        print("dialect supports transactional DDL:",
              backend.dialect.supports_transactional_ddl())

        # 1) dry run — renders SQL through the backend, writes nothing.
        preview = run_migration_group(backend, [users, archive], dry_run=True)
        print("\n== dry run ==")
        print("  statements:", preview.dry_run_total)
        for label, sql, _params in preview.dry_run_sql:
            print(f"  [{label}] {sql}")
        print("  records written:", preview.records_written)

        # 2) real UP — one transaction, DDL committed only if every step succeeds.
        up = run_migration_group(backend, [users, archive], record_store=store)
        print("\n== UP ==")
        print("  applied:", up.applied)
        print("  records written:", up.records_written)

        # 3) a failing step — on PostgreSQL the group's DDL is rolled back cleanly.
        print("\n== failing step (expect rollback) ==")
        try:
            run_migration_group(backend, [failing], record_store=store)
        except MigrationFunctionExecutionError as exc:
            print("  version:", exc.version)
            print("  transactional_ddl:", exc.transactional_ddl)
            print("  cause:", exc.cause)

        # 4) DOWN — reverse order; a clean rollback removes the applied versions.
        down = run_migration_group(
            backend, [users, archive], direction=MigrationDirection.DOWN, record_store=store
        )
        print("\n== DOWN ==")
        print("  rolled back:", down.rolled_back)
        print("  applied versions after rollback:",
              [r.version for r in store.get_applied()])
    finally:
        backend.disconnect()
        if os.path.exists(rec_path):
            os.remove(rec_path)


if __name__ == "__main__":
    main()
