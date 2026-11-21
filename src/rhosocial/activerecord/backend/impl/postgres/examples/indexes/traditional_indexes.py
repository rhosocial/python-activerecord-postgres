# src/rhosocial/activerecord/backend/impl/postgres/examples/indexes/traditional_indexes.py
"""Real-world traditional indexes on an orders table.

Shows composite / partial / unique / operator-class / BRIN indexes built with
expression objects, and the ``PostgresIndexType`` enum that constrains the
``USING`` method (including extension methods).
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os

from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.expression import PostgresIndexType
from rhosocial.activerecord.backend.expression import (
    Column,
    ColumnConstraint,
    ColumnDefinition,
    ColumnNotNullConstraint,
    ColumnPrimaryKeyConstraint,
    CreateIndexStatement,
    CreateTableStatement,
    DropTableStatement,
    Literal,
)
from rhosocial.activerecord.backend.expression.objects import Index, Table
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate

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
dialect = backend.dialect

backend.execute(*DropTableStatement(
    dialect=dialect, table=Table(dialect, "orders"), if_exists=True
).to_sql())
backend.execute(*CreateTableStatement(
    dialect=dialect,
    table=Table(dialect, "orders"),
    columns=[
        ColumnDefinition(dialect, "id", dialect.parse_type("SERIAL"), constraints=[
            ColumnConstraint(dialect, branch=ColumnPrimaryKeyConstraint(dialect)),
        ]),
        ColumnDefinition(dialect, "customer_id", dialect.parse_type("INTEGER"), constraints=[
            ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
        ]),
        ColumnDefinition(dialect, "email", dialect.parse_type("TEXT")),
        ColumnDefinition(dialect, "status", dialect.parse_type("TEXT")),
        ColumnDefinition(dialect, "created_at", dialect.parse_type("TIMESTAMP")),
        ColumnDefinition(dialect, "tags", dialect.parse_type("TEXT[]")),
    ],
).to_sql())

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
# 1. Composite B-tree for "orders of one customer, newest first".
composite = CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_customer_created"),
    table=Table(dialect, "orders"),
    columns=["customer_id", "created_at"],
    index_type=PostgresIndexType.BTREE,
)

# 2. Partial index: only unpaid orders matter to the collections queue.
#    The WHERE value is inlined because a DDL statement cannot bind parameters.
partial = CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_unpaid"),
    table=Table(dialect, "orders"),
    columns=["customer_id"],
    where=ComparisonPredicate(
        dialect, "=", Column(dialect, "status"), Literal(dialect, "unpaid", inline=True)
    ),
)

# 3. Unique index enforcing one order per email.
unique = CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_email_unique"),
    table=Table(dialect, "orders"),
    columns=["email"],
    unique=True,
)

# 4. GIN on an array column for tag containment queries.
tags_gin = CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_tags"),
    table=Table(dialect, "orders"),
    columns=["tags"],
    index_type=PostgresIndexType.GIN,
)

# 5. BRIN on a naturally ordered timestamp, cheap on a huge append-only table.
created_brin = CreateIndexStatement(
    dialect,
    index=Index(dialect, "idx_orders_created_brin"),
    table=Table(dialect, "orders"),
    columns=["created_at"],
    index_type=PostgresIndexType.BRIN,
)

print("Enum groups:")
print("  native:", dialect.get_supported_index_types())
print("  extension:", dialect.get_supported_extension_index_types())

# ============================================================
# SECTION: Execution (run the expressions)
# ============================================================
for statement in (composite, partial, unique, tags_gin, created_brin):
    sql, params = statement.to_sql()
    print(f"SQL: {sql}")
    backend.execute(sql, params)

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()
