"""
Table partitioning - PostgreSQL 10+.

This example demonstrates:
1. CREATE TABLE ... PARTITION BY RANGE with expression nodes
2. Creating individual partitions (CREATE TABLE ... PARTITION OF)
3. Inserting data into a partitioned table
4. Querying partitioned tables
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.impl.postgres.expression import (
    PostgresCreatePartitionStatement,
    PostgresSerialType,
)
from rhosocial.activerecord.backend.expression import (
    ColumnDefinition,
    ColumnConstraint,
    ColumnNotNullConstraint,
    CreateTableStatement,
    DropTableStatement,
    InsertStatement,
    PartitionClause,
    PartitionStrategy,
    ValuesSource,
    QueryStatement,
)
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.core import Literal, AllColumnsWildcard, Column
from rhosocial.activerecord.backend.expression.types import DateType, DecimalType, VarCharType
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', '5432')),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

# Drop partitions first, then parent table
for partition in ['orders_2024_q1', 'orders_2024_q2', 'orders_2024_q3', 'orders_2024_q4']:
    drop = DropTableStatement(dialect=dialect, table=Table(dialect, partition), if_exists=True, cascade=True)
    sql, params = drop.to_sql()
    backend.execute(sql, params)

drop_parent = DropTableStatement(dialect=dialect, table=Table(dialect, 'orders'), if_exists=True, cascade=True)
sql, params = drop_parent.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. Create the partitioned parent table with the expression API.
#    PARTITION BY is a clause node on CreateTableStatement; the SQL is the
#    dialect's, not the caller's.
parent_expr = CreateTableStatement(
    dialect=dialect,
    table=Table(dialect, 'orders'),
    columns=[
        ColumnDefinition(
            dialect, 'id', PostgresSerialType(dialect=dialect),
        ),
        ColumnDefinition(
            dialect, 'order_date', DateType(dialect=dialect),
            constraints=[
                ColumnConstraint(dialect, branch=ColumnNotNullConstraint(dialect)),
            ],
        ),
        ColumnDefinition(
            dialect, 'customer_name', VarCharType(length=100, dialect=dialect),
        ),
        ColumnDefinition(
            dialect, 'amount', DecimalType(precision=10, scale=2, dialect=dialect),
        ),
    ],
    partition=PartitionClause(
        dialect=dialect,
        method=PartitionStrategy.RANGE,
        keys=[Column(dialect, 'order_date')],
    ),
)
sql, params = parent_expr.to_sql()
backend.execute(sql, params)
print("Created partitioned table: orders")

# 2. Create individual partitions using raw SQL
partitions = [
    ("orders_2024_q1", "2024-01-01", "2024-04-01"),
    ("orders_2024_q2", "2024-04-01", "2024-07-01"),
    ("orders_2024_q3", "2024-07-01", "2024-10-01"),
    ("orders_2024_q4", "2024-10-01", "2025-01-01"),
]

for part_name, start_val, end_val in partitions:
    # The partition statement is a node like everything else: the values are
    # its parameters and the spelling is the formatter's, so nothing about the
    # partition is decided at the call site.
    part_sql, _ = PostgresCreatePartitionStatement(
        dialect=dialect,
        partition_name=part_name,
        parent_table="orders",
        partition_type="RANGE",
        partition_values={"from": start_val, "to": end_val},
    ).to_sql()
    backend.execute(part_sql)
    print(f"Created partition: {part_name} [{start_val}, {end_val})")

# ============================================================
# SECTION: Execution (insert and query)
# ============================================================

# 3. Insert data (PostgreSQL routes to correct partition automatically)
insert_expr = InsertStatement(
    dialect=dialect,
    into=Table(dialect, 'orders'),
    columns=['order_date', 'customer_name', 'amount'],
    source=ValuesSource(dialect, [
        [Literal(dialect, '2024-02-15'), Literal(dialect, 'Alice'), Literal(dialect, 150.00)],
        [Literal(dialect, '2024-05-20'), Literal(dialect, 'Bob'), Literal(dialect, 250.00)],
        [Literal(dialect, '2024-08-10'), Literal(dialect, 'Charlie'), Literal(dialect, 350.00)],
        [Literal(dialect, '2024-11-05'), Literal(dialect, 'Diana'), Literal(dialect, 450.00)],
    ]),
)
sql, params = insert_expr.to_sql()
backend.execute(sql, params)
print("Inserted 4 orders across all partitions")

# 4. Query all data (transparent across partitions)
query_all = QueryStatement(
    dialect=dialect,
    select=[AllColumnsWildcard(dialect)],
    from_=Table(dialect, 'orders'),
)
sql, params = query_all.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"All orders: {result.data}")

# 5. Query specific partition directly
query_q1 = QueryStatement(
    dialect=dialect,
    select=[Column(dialect, 'customer_name'), Column(dialect, 'amount')],
    from_=Table(dialect, 'orders_2024_q1'),
)
sql, params = query_q1.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"Q1 orders: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
for partition in ['orders_2024_q1', 'orders_2024_q2', 'orders_2024_q3', 'orders_2024_q4']:
    drop = DropTableStatement(dialect=dialect, table=Table(dialect, partition), if_exists=True, cascade=True)
    sql, params = drop.to_sql()
    backend.execute(sql, params)

drop_parent = DropTableStatement(dialect=dialect, table=Table(dialect, 'orders'), if_exists=True, cascade=True)
sql, params = drop_parent.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Requires PostgreSQL 10+ for declarative partitioning
# 2. PARTITION BY RANGE is the most common strategy
# 3. Other strategies: PARTITION BY LIST, PARTITION BY HASH (PG 11+)
# 4. PostgreSQL automatically routes INSERT to correct partition
# 5. Queries on parent table are transparent across all partitions
# 6. Individual partitions can be queried directly for performance
# 7. Nested partitioning is supported: a partition created with
#    PostgresCreatePartitionStatement(partition_clause=...) is itself
#    PARTITION BY <strategy>; PostgreSQL lets each HASH branch choose its own
#    child strategy. See PostgresPartitionPlan for the tree-maintenance API.
# 8. A partitioned table's primary key must include every level's partition
#    key, otherwise the server rejects the child that is itself partitioned.
