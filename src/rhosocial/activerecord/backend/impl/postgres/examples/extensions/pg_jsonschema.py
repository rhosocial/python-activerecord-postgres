# src/rhosocial/activerecord/backend/impl/postgres/examples/extensions/pg_jsonschema.py
"""
pg_jsonschema: validate jsonb payloads against JSON Schema in the database.

All SQL is built with expression objects.
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
from rhosocial.activerecord.backend.expression import QueryStatement  # noqa: E402
from rhosocial.activerecord.backend.impl.postgres.expression import (  # noqa: E402
    PostgresCreateExtensionStatement,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_jsonschema import (  # noqa: E402
    jsonb_matches_schema,
    jsonschema_is_valid,
)
from rhosocial.activerecord.backend.options import ExecutionOptions  # noqa: E402
from rhosocial.activerecord.backend.schema import StatementType  # noqa: E402

DDL = ExecutionOptions(stmt_type=StatementType.DDL)

if dialect.is_extension_available("pg_jsonschema") and not dialect.is_extension_installed(
    "pg_jsonschema"
):
    backend.execute(
        *PostgresCreateExtensionStatement(dialect, "pg_jsonschema").to_sql(), options=DDL
    )
    backend.introspect_and_adapt()

print("pg_jsonschema:", dialect.supports_pg_jsonschema())

SCHEMA = '{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}}}'
for value in ('{"tags":["vip"]}', '{"tags":[1]}'):
    query = QueryStatement(
        dialect=dialect,
        select=[jsonb_matches_schema(dialect, SCHEMA, value).as_("valid")],
    )
    print("SQL:", query.to_sql()[0])
    print("result:", backend.execute(*query.to_sql()).data)

query = QueryStatement(
    dialect=dialect,
    select=[jsonschema_is_valid(dialect, SCHEMA).as_("schema_ok")],
)
print("schema valid:", backend.execute(*query.to_sql()).data)

# ============================================================
# SECTION: Teardown
# ============================================================
backend.disconnect()
