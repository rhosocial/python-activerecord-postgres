"""
Window function support example - ROW_NUMBER(), RANK(), DENSE_RANK().
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    port=int(os.getenv('POSTGRES_PORT', 5432)),
    database=os.getenv('POSTGRES_DATABASE', 'test'),
    username=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', ''),
)
backend = PostgresBackend(connection_config=config)
backend.connect()
dialect = backend.dialect

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.expression.advanced_functions import (
    WindowFunctionCall,
    WindowSpecification,
    WindowFrameSpecification,
)
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause

# ROW_NUMBER() - sequential rank without gaps
row_number = WindowFunctionCall(
    dialect=dialect,
    function_name='ROW_NUMBER',
    window_spec=WindowSpecification(
        dialect=dialect,
        partition_by=[Column(dialect, 'department')],
        order_by=OrderByClause(dialect, [Column(dialect, 'salary')]),
        frame=WindowFrameSpecification(
            dialect=dialect,
            frame_type='ROWS',
            start_frame='UNBOUNDED PRECEDING',
            end_frame='CURRENT ROW',
        ),
    ),
    alias='row_number',
)

# RANK() - rank with gaps (same values get same rank, skip ranks)
rank = WindowFunctionCall(
    dialect=dialect,
    function_name='RANK',
    window_spec=WindowSpecification(
        dialect=dialect,
        partition_by=[Column(dialect, 'department')],
        order_by=OrderByClause(dialect, [Column(dialect, 'salary')]),
    ),
    alias='rank',
)

# DENSE_RANK() - rank without gaps
dense_rank = WindowFunctionCall(
    dialect=dialect,
    function_name='DENSE_RANK',
    window_spec=WindowSpecification(
        dialect=dialect,
        partition_by=[Column(dialect, 'department')],
        order_by=OrderByClause(dialect, [Column(dialect, 'salary')]),
    ),
    alias='dense_rank',
)

# Generate SQL
sql, params = row_number.to_sql()
print(f"ROW_NUMBER: {sql}")
print(f"Params: {params}")

sql, params = rank.to_sql()
print(f"RANK: {sql}")

sql, params = dense_rank.to_sql()
print(f"DENSE_RANK: {sql}")

# ============================================================
# SECTION: Full Query Example
# ============================================================
# Using window functions in a SELECT query
query = QueryExpression(
    dialect=dialect,
    select=[
        Column(dialect, 'id'),
        Column(dialect, 'name'),
        Column(dialect, 'department'),
        Column(dialect, 'salary'),
        row_number,
    ],
    from_=TableExpression(dialect, 'employees'),
)

sql, params = query.to_sql()
print(f"Full query SQL: {sql}")
print(f"Params: {params}")

# ============================================================
# SECTION: Output (reference)
# ============================================================
# Expected output:
# ROW_NUMBER: ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary
#    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS row_number
# RANK: RANK() OVER (PARTITION BY department ORDER BY salary) AS rank
# DENSE_RANK: DENSE_RANK() OVER (PARTITION BY department ORDER BY salary) AS dense_rank
