# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/diagnostics.py
"""
Diagnostics extensions: amcheck, pageinspect and pgstattuple.

All SQL is built with expression objects; no SQL strings are assembled here.
"""

import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

if __name__ == "__main__":
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
        ColumnPrimaryKeyConstraint,
        CreateTableStatement,
        DropTableStatement,
        InsertStatement,
        QueryStatement,
    )
    from rhosocial.activerecord.backend.expression.core import Parameter  # noqa: E402
    from rhosocial.activerecord.backend.expression.objects import Table  # noqa: E402
    from rhosocial.activerecord.backend.expression.statements import ValuesSource  # noqa: E402
    from rhosocial.activerecord.backend.impl.postgres.expression import (  # noqa: E402
        PostgresCreateExtensionStatement,
    )
    from rhosocial.activerecord.backend.impl.postgres.expression.types import (  # noqa: E402
        PostgresSerialType,
    )
    from rhosocial.activerecord.backend.impl.postgres.functions.amcheck import (  # noqa: E402
        bt_index_check,
    )
    from rhosocial.activerecord.backend.impl.postgres.functions.pageinspect import (  # noqa: E402
        get_raw_page,
    )
    from rhosocial.activerecord.backend.impl.postgres.functions.pgstattuple import (  # noqa: E402
        pgstattuple,
    )
    from rhosocial.activerecord.backend.options import ExecutionOptions  # noqa: E402
    from rhosocial.activerecord.backend.schema import StatementType  # noqa: E402

    DDL = ExecutionOptions(stmt_type=StatementType.DDL)
    DQL = ExecutionOptions(stmt_type=StatementType.DQL)

    for extension in ("amcheck", "pageinspect", "pgstattuple"):
        if dialect.is_extension_available(extension) and not dialect.is_extension_installed(extension):
            backend.execute(
                *PostgresCreateExtensionStatement(dialect, extension).to_sql(), options=DDL
            )
    backend.introspect_and_adapt()

    print("amcheck:", dialect.supports_amcheck())
    print("pageinspect:", dialect.supports_pageinspect())
    print("pgstattuple:", dialect.supports_pgstattuple())

    TABLE = "test_diagnostics_example"
    backend.execute(*DropTableStatement(dialect=dialect, table=Table(dialect, TABLE), if_exists=True).to_sql(), options=DDL)
    backend.execute(
        *CreateTableStatement(
            dialect=dialect,
            table=Table(dialect, TABLE),
            columns=[
                ColumnDefinition(
                    dialect,
                    name="id",
                    data_type=PostgresSerialType(dialect=dialect),
                    constraints=[
                        ColumnConstraint(
                            dialect, branch=ColumnPrimaryKeyConstraint(dialect)
                        )
                    ],
                )
            ],
        ).to_sql(),
        options=DDL,
    )
    backend.execute(
        *InsertStatement(
            dialect=dialect,
            into=Table(dialect, TABLE),
            columns=["id"],
            source=ValuesSource(dialect, [[Parameter(dialect, 1)]]),
        ).to_sql(),
        options=DDL,
    )

    checks = (
        (bt_index_check(dialect, f"{TABLE}_pkey"), True),
        (get_raw_page(dialect, TABLE, "main", 0), False),
        (pgstattuple(dialect, TABLE), True),
    )
    for check, show_result in checks:
        query = QueryStatement(dialect=dialect, select=[check])
        print("SQL:", query.to_sql()[0])
        if show_result:
            print("result:", backend.execute(*query.to_sql(), options=DQL).data)
        else:
            backend.execute(*query.to_sql(), options=DQL)

    # ============================================================
    # SECTION: Teardown
    # ============================================================
    backend.execute(*DropTableStatement(dialect=dialect, table=Table(dialect, TABLE), if_exists=True).to_sql(), options=DDL)
    backend.disconnect()
