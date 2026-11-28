# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pgroonga.py
"""
PGroonga extension - multilingual full text search (including Chinese).

This example demonstrates:
1. Detect the extension and its version-aware capabilities
2. CREATE EXTENSION pgroonga via an expression object
3. Build a ``USING pgroonga`` index with the typed index expression
4. Search with the v2 operators (``&@``, ``&@~``) and rank with ``pgroonga_score``
5. Highlight matched keywords in HTML
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
    # SECTION: Business Logic (the pattern to learn)
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
    from rhosocial.activerecord.backend.expression.core import Parameter  # noqa: E402
    from rhosocial.activerecord.backend.expression.objects import Table  # noqa: E402
    from rhosocial.activerecord.backend.expression.statements import ValuesSource  # noqa: E402
    from rhosocial.activerecord.backend.expression.types import TextType  # noqa: E402
    from rhosocial.activerecord.backend.impl.postgres.expression import (  # noqa: E402
        PostgresCreateExtensionStatement,
    )
    from rhosocial.activerecord.backend.impl.postgres.expression.types import (  # noqa: E402
        PostgresSerialType,
    )
    from rhosocial.activerecord.backend.impl.postgres.functions.pgroonga import (  # noqa: E402
        pgroonga_highlight_html,
        pgroonga_match,
        pgroonga_query,
        pgroonga_score,
    )
    from rhosocial.activerecord.backend.options import ExecutionOptions  # noqa: E402
    from rhosocial.activerecord.backend.schema import StatementType  # noqa: E402

    DDL = ExecutionOptions(stmt_type=StatementType.DDL)

    if not dialect.is_extension_available("pgroonga"):
        raise SystemExit("PGroonga is not installed on this server; skipping example")

    # Enable the extension (idempotent), then refresh what the dialect knows.
    if not dialect.is_extension_installed("pgroonga"):
        sql, params = PostgresCreateExtensionStatement(dialect, "pgroonga").to_sql()
        backend.execute(sql, params)
        backend.introspect_and_adapt()

    print("PGroonga version:", dialect.get_extension_version("pgroonga"))
    print("Keyword match supported:", dialect.supports_pgroonga_match())

    TABLE = "test_pgroonga_example"
    INDEX = "idx_test_pgroonga_example"

    backend.execute(f"DROP INDEX IF EXISTS {INDEX}", (), options=DDL)
    backend.execute(f"DROP TABLE IF EXISTS {TABLE}", (), options=DDL)

    columns = [
        ColumnDefinition(
            dialect,
            name="id",
            data_type=PostgresSerialType(dialect=dialect),
            constraints=[ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect))],
        ),
        ColumnDefinition(dialect, name="body", data_type=TextType(dialect=dialect)),
    ]
    backend.execute(
        *CreateTableStatement(
            dialect=dialect, table=Table(dialect, TABLE), columns=columns
        ).to_sql(),
        options=DDL,
    )
    backend.execute(
        *dialect.build_pgroonga_index_expression(INDEX, TABLE, ["body"]).to_sql(),
        options=DDL,
    )

    rows = [
        [Parameter(dialect, "Groonga is a fast full text search engine.")],
        [Parameter(dialect, "中文全文检索：PostgreSQL 与 Groonga 的组合。")],
    ]
    backend.execute(
        *InsertStatement(
            dialect=dialect,
            into=Table(dialect, TABLE),
            columns=["body"],
            source=ValuesSource(dialect, rows),
        ).to_sql()
    )

    # Default TokenBigram tokenizer handles CJK without any text search config.
    query = QueryStatement(
        dialect=dialect,
        select=[Column(dialect, "id"), Column(dialect, "body")],
        from_=Table(dialect, TABLE),
        where=pgroonga_match(dialect, "body", "全文检索"),
    )
    print("CJK match SQL:", query.to_sql()[0])
    print("CJK match rows:", backend.execute(*query.to_sql()).data)

    # Query language with OR; ranking with pgroonga_score.
    ranked = QueryStatement(
        dialect=dialect,
        select=[Column(dialect, "id"), pgroonga_score(dialect).as_("score")],
        from_=Table(dialect, TABLE),
        where=pgroonga_query(dialect, "body", "Groonga OR 中文"),
    )
    print("Ranked SQL:", ranked.to_sql()[0])

    highlighted = QueryStatement(
        dialect=dialect,
        select=[pgroonga_highlight_html(dialect, "body", ["Groonga"]).as_("snippet")],
        from_=Table(dialect, TABLE),
        where=pgroonga_match(dialect, "body", "Groonga"),
    )
    print("Highlight SQL:", highlighted.to_sql()[0])

    # ============================================================
    # SECTION: Teardown
    # ============================================================
    backend.execute(f"DROP INDEX IF EXISTS {INDEX}", (), options=DDL)
    backend.execute(*DropTableStatement(dialect=dialect, table=Table(dialect, TABLE)).to_sql(), options=DDL)
    backend.disconnect()
