# src/rhosocial/activerecord/backend/impl/postgres/examples/indexes/json_key_indexes.py
"""Index keys *inside* a JSONB document — a product-events scenario.

Two real access patterns:
  * containment on a sub-document (``$.sku``) -> key-level GIN + jsonb_path_ops
  * exact lookup / ordering on a scalar key (``$.user.id``) -> text B-tree
"""

from rhosocial.activerecord.backend.expression.types import JsonBType
from rhosocial.activerecord.backend.impl.postgres.expression.types import PostgresSerialType


# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.functions import create_jsonb_index
from rhosocial.activerecord.backend.expression import (
    ColumnConstraint,
    ColumnDefinition,
    ColumnPrimaryKeyConstraint,
    CreateTableStatement,
    DropTableStatement,
)

if __name__ == "__main__":
    config = PostgresConnectionConfig(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("PG_DATABASE", "test"),
        username=os.getenv("PG_USERNAME", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
        sslmode=os.getenv("PG_SSLMODE"),
        sslrootcert=os.getenv("PG_SSLROOTCERT"),
    )
    backend = PostgresBackend(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    dialect = backend.dialect

    # ============================================================
    # SECTION: Business Logic (the pattern to learn)
    # ============================================================
    # The helper inlines the JSONPath as an escaped literal: PostgreSQL rejects a
    # bind parameter inside an index expression. Both shapes come from one API.
    sku_gin = create_jsonb_index(
        dialect,
        "events",
        "$.sku",
        column_name="payload",
        index_name="idx_events_sku_gin",
    )
    user_text = create_jsonb_index(
        dialect,
        "events",
        "$.user.id",
        column_name="payload",
        index_name="idx_events_user_id",
        as_text=True,
    )

    # A whole-column GIN index is still the right choice for "@>" on the *entire*
    # document. The same expression_opclasses mechanism binds the opclass.
    from rhosocial.activerecord.backend.expression.objects import Index, Table  # noqa: E402
    from rhosocial.activerecord.backend.impl.postgres.expression import (  # noqa: E402
        PostgresCreateIndexExpression,
    )
    from rhosocial.activerecord.backend.expression.functions.json import (  # noqa: E402
        json_extract,
    )

    whole_doc = json_extract(dialect, "payload", "$", inline_path=True)
    whole_gin = PostgresCreateIndexExpression(
        dialect,
        index=Index(dialect, "idx_events_payload_gin"),
        table=Table(dialect, "events"),
        columns=[whole_doc],
        index_type="GIN",
        expression_opclasses=[(whole_doc, "jsonb_ops")],
    )

    # ============================================================
    # SECTION: Execution (run the expressions)
    # ============================================================
    backend.execute(*DropTableStatement(
        dialect=dialect, table=Table(dialect, "events"), if_exists=True
    ).to_sql())
    backend.execute(*CreateTableStatement(
        dialect=dialect,
        table=Table(dialect, "events"),
        columns=[
            ColumnDefinition(dialect, "id", PostgresSerialType(), constraints=[
                ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
            ]),
            ColumnDefinition(dialect, "payload", JsonBType()),
        ],
    ).to_sql())

    for statement in (sku_gin, user_text, whole_gin):
        sql, params = statement.to_sql()
        print(f"SQL: {sql}")
        backend.execute(sql, params)

    # ============================================================
    # SECTION: Teardown (necessary for execution, reference only)
    # ============================================================
    backend.disconnect()
