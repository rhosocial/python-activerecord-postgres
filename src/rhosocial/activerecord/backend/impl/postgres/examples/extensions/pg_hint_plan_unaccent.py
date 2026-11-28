# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_hint_plan_unaccent.py
"""
pg_hint_plan + unaccent: optimizer hints and accent-insensitive text.

Demonstrates:
1. Detect both extensions and enable them with expression objects
2. LOAD pg_hint_plan in the session (needed without shared_preload_libraries)
3. Build a query carrying a hint payload on the statement itself
4. Normalize text with unaccent() in a query
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
    # SECTION: Business Logic (expression objects only)
    # ============================================================
    from rhosocial.activerecord.backend.expression import (  # noqa: E402
        Column,
        ColumnConstraint,
        ColumnDefinition,
        ColumnPrimaryKeyConstraint,
        CreateTableStatement,
        DropTableStatement,
        InsertStatement,
        QueryStatement,
    )
    from rhosocial.activerecord.backend.expression.value_types.parameter import Parameter  # noqa: E402
    from rhosocial.activerecord.backend.expression.objects import Table  # noqa: E402
    from rhosocial.activerecord.backend.expression.statements import ValuesSource  # noqa: E402
    from rhosocial.activerecord.backend.expression.types import TextType  # noqa: E402
    from rhosocial.activerecord.backend.impl.postgres.expression import (  # noqa: E402
        PostgresCreateExtensionStatement,
    )
    from rhosocial.activerecord.backend.impl.postgres.expression.types import (  # noqa: E402
        PostgresSerialType,
    )
    from rhosocial.activerecord.backend.impl.postgres.functions.pg_hint_plan import (  # noqa: E402
        hint_seq_scan,
        load_pg_hint_plan,
    )
    from rhosocial.activerecord.backend.impl.postgres.functions.unaccent import unaccent  # noqa: E402
    from rhosocial.activerecord.backend.options import ExecutionOptions  # noqa: E402
    from rhosocial.activerecord.backend.schema import StatementType  # noqa: E402

    DDL = ExecutionOptions(stmt_type=StatementType.DDL)

    TABLE = "test_hint_unaccent_example"

    for name in ("pg_hint_plan", "unaccent"):
        if dialect.is_extension_available(name) and not dialect.is_extension_installed(name):
            backend.execute(*PostgresCreateExtensionStatement(dialect, name).to_sql(), options=DDL)
    backend.introspect_and_adapt()

    print("pg_hint_plan:", dialect.supports_pg_hint_plan())
    print("unaccent:", dialect.supports_unaccent())

    if dialect.supports_pg_hint_plan():
        backend.execute(*load_pg_hint_plan(dialect).to_sql(), options=DDL)

    backend.execute(f"DROP TABLE IF EXISTS {TABLE}", (), options=DDL)
    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        ),
        ColumnDefinition(dialect, name="name", data_type=TextType(dialect=dialect)),
    ]
    backend.execute(
        *CreateTableStatement(dialect=dialect, table=Table(dialect, TABLE), columns=columns).to_sql(),
        options=DDL,
    )
    backend.execute(
        *InsertStatement(
            dialect=dialect,
            into=Table(dialect, TABLE),
            columns=["name"],
            source=ValuesSource(
                dialect, [[Parameter(dialect, value)] for value in ("Café", "Résumé", "plain")]
            ),
        ).to_sql()
    )

    query_args = dict(
        select=[Column(dialect, "id"), unaccent(dialect, Column(dialect, "name")).as_("normalized")],
        from_=Table(dialect, TABLE),
    )
    if dialect.supports_pg_hint_plan():
        query = QueryStatement(
            dialect, hints=(hint_seq_scan(dialect, Table(dialect, TABLE)),), **query_args
        )
    else:
        query = QueryStatement(dialect, **query_args)

    sql, params = query.to_sql()
    print("SQL:", sql)
    print("rows:", backend.execute(sql, params, options=ExecutionOptions(stmt_type=StatementType.DQL)).data)

    # ============================================================
    # SECTION: Teardown
    # ============================================================
    backend.execute(*DropTableStatement(dialect=dialect, table=Table(dialect, TABLE)).to_sql(), options=DDL)
    backend.disconnect()
