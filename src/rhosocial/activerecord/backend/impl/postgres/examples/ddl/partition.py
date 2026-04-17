"""
Table partitioning - PostgreSQL 10+.

This example demonstrates:
1. CREATE TABLE ... PARTITION BY RANGE
2. Creating individual partitions
3. Inserting data into partitioned table
4. Querying partitioned tables
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.expression import (
    DropTableExpression,
    InsertExpression,
    ValuesSource,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.core import Literal, WildcardExpression, Column
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

config = PostgresConnectionConfig(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', '5432')),
    database=os.getenv('PG_DATABASE', 'test'),
    username=os.getenv('PG_USERNAME', 'postgres'),
    password=os.getenv('PG_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

dql_options = ExecutionOptions(stmt_type=StatementType.DQL)

# Drop partitions first, then parent table
for partition in ['orders_2024_q1', 'orders_2024_q2', 'orders_2024_q3', 'orders_2024_q4']:
    drop = DropTableExpression(dialect=dialect, table_name=partition, if_exists=True, cascade=True)
    sql, params = drop.to_sql()
    backend.execute(sql, params)

drop_parent = DropTableExpression(dialect=dialect, table_name='orders', if_exists=True, cascade=True)
sql, params = drop_parent.to_sql()
backend.execute(sql, params)

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================

# 1. Create partitioned parent table using raw SQL
# (Expression API does not yet support PARTITION BY syntax)
parent_sql = """
CREATE TABLE orders (
    id SERIAL,
    order_date DATE NOT NULL,
    customer_name VARCHAR(100),
    amount NUMERIC(10,2)
) PARTITION BY RANGE (order_date)
"""
backend.execute(parent_sql)
print("Created partitioned table: orders")

# 2. Create individual partitions using raw SQL
partitions = [
    ("orders_2024_q1", "2024-01-01", "2024-04-01"),
    ("orders_2024_q2", "2024-04-01", "2024-07-01"),
    ("orders_2024_q3", "2024-07-01", "2024-10-01"),
    ("orders_2024_q4", "2024-10-01", "2025-01-01"),
]

for part_name, start_val, end_val in partitions:
    part_sql = f"""
    CREATE TABLE {part_name} PARTITION OF orders
    FOR VALUES FROM ('{start_val}') TO ('{end_val}')
    """
    backend.execute(part_sql)
    print(f"Created partition: {part_name} [{start_val}, {end_val})")

# ============================================================
# SECTION: Execution (insert and query)
# ============================================================

# 3. Insert data (PostgreSQL routes to correct partition automatically)
insert_expr = InsertExpression(
    dialect=dialect,
    into='orders',
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
query_all = QueryExpression(
    dialect=dialect,
    select=[WildcardExpression(dialect)],
    from_=TableExpression(dialect, 'orders'),
)
sql, params = query_all.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"All orders: {result.data}")

# 5. Query specific partition directly
query_q1 = QueryExpression(
    dialect=dialect,
    select=[Column(dialect, 'customer_name'), Column(dialect, 'amount')],
    from_=TableExpression(dialect, 'orders_2024_q1'),
)
sql, params = query_q1.to_sql()
result = backend.execute(sql, params, options=dql_options)
print(f"Q1 orders: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
for partition in ['orders_2024_q1', 'orders_2024_q2', 'orders_2024_q3', 'orders_2024_q4']:
    drop = DropTableExpression(dialect=dialect, table_name=partition, if_exists=True, cascade=True)
    sql, params = drop.to_sql()
    backend.execute(sql, params)

drop_parent = DropTableExpression(dialect=dialect, table_name='orders', if_exists=True, cascade=True)
sql, params = drop_parent.to_sql()
backend.execute(sql, params)
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Requires PostgreSQL 10+ for declarative partitioning
# 2. PARTITION BY RANGE is the most common strategy
# 3. Other strategies: PARTITION BY LIST, PARTITION BY HASH
# 4. PostgreSQL automatically routes INSERT to correct partition
# 5. Queries on parent table are transparent across all partitions
# 6. Individual partitions can be queried directly for performance
# 7. Expression API does not yet support PARTITION BY - use raw SQL
