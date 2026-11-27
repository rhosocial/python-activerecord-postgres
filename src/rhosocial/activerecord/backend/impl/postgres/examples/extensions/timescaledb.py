# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/timescaledb.py
"""
TimescaleDB hypertable creation. Requires an amd64 image with TimescaleDB and
``shared_preload_libraries = timescaledb``.
"""

import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5432")),
    database=os.getenv("PG_DATABASE", "test"),
    username=os.getenv("PG_USERNAME", "postgres"),
    password=os.getenv("PG_PASSWORD", ""),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
backend.introspect_and_adapt()
dialect = backend.dialect

# ============================================================
# SECTION: Business Logic
# ============================================================
from rhosocial.activerecord.backend.expression import (  # noqa: E402
    ColumnConstraint,
    ColumnDefinition,
    ColumnNotNullConstraint,
    CreateTableStatement,
    DropTableStatement,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table  # noqa: E402
from rhosocial.activerecord.backend.expression.types import TimestampTzType  # noqa: E402
from rhosocial.activerecord.backend.impl.postgres.expression import (  # noqa: E402
    PostgresCreateExtensionStatement,
)
from rhosocial.activerecord.backend.impl.postgres.functions.timescaledb import (  # noqa: E402
    create_hypertable,
)
from rhosocial.activerecord.backend.options import ExecutionOptions  # noqa: E402
from rhosocial.activerecord.backend.schema import StatementType  # noqa: E402

DDL = ExecutionOptions(stmt_type=StatementType.DDL)

if dialect.is_extension_available("timescaledb") and not dialect.is_extension_installed(
    "timescaledb"
):
    backend.execute(
        *PostgresCreateExtensionStatement(dialect, "timescaledb").to_sql(), options=DDL
    )
    backend.introspect_and_adapt()

print("timescaledb:", dialect.supports_timescaledb())

TABLE = "test_timescaledb_example"
backend.execute(
    *DropTableStatement(dialect=dialect, table=Table(dialect, TABLE), if_exists=True).to_sql(),
    options=DDL,
)
backend.execute(
    *CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, TABLE),
        columns=[
            ColumnDefinition(
                dialect,
                name="ts",
                data_type=TimestampTzType(dialect=dialect),
                constraints=[ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect))],
            )
        ],
    ).to_sql(),
    options=DDL,
)
if dialect.supports_timescaledb():
    call = create_hypertable(dialect, TABLE, "ts")
    query = QueryStatement(dialect=dialect, select=[call])
    print("SQL:", query.to_sql()[0])
    print("result:", backend.execute(*query.to_sql(), options=DDL).data)

# ============================================================
# SECTION: Teardown
# ============================================================
backend.execute(
    *DropTableStatement(dialect=dialect, table=Table(dialect, TABLE), if_exists=True).to_sql(),
    options=DDL,
)
backend.disconnect()
