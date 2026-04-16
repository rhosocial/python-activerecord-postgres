"""
Window function support example - ROW_NUMBER(), RANK(), DENSE_RANK().
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

config = PostgresConnectionConfig(
    host='localhost',
    port=5432,
    database='test',
    user='test',
    password='test',
)
backend = PostgresBackend(config)
dialect = backend.dialect

# ============================================================
# SECTION: Business Logic (the pattern to learn)
# ============================================================
from rhosocial.activerecord.backend.expression import (
    WindowFunctionExpression,
    WindowFrameBounded,
    WindowFrameRows,
)
from rhosocial.activerecord.backend.expression.core import TableExpression, ColumnExpression
from rhosocial.activerecord.backend.expression import aggregate

# Example: Rank employees by salary within department
employees = TableExpression('employees')

# ROW_NUMBER() - sequential rank without gaps
row_number = WindowFunctionExpression(
    func='ROW_NUMBER',
    partition_by=[
        ColumnExpression('department', table=employees),
    ],
    order_by=[('salary', 'DESC')],
    window_frame=WindowFrameBounded(
        kind=WindowFrameRows,
        start='UNBOUNDED PRECEDING',
        end='CURRENT ROW',
    ),
)

# RANK() - rank with gaps (same values get same rank, skip ranks)
rank = WindowFunctionExpression(
    func='RANK',
    partition_by=[
        ColumnExpression('department', table=employees),
    ],
    order_by=[('salary', 'DESC')],
)

# DENSE_RANK() - rank without gaps
dense_rank = WindowFunctionExpression(
    func='DENSE_RANK',
    partition_by=[
        ColumnExpression('department', table=employees),
    ],
    order_by=[('salary', 'DESC')],
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
# SECTION: Output (reference)
# ============================================================
# Expected output:
# SELECT ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC 
#    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS row_number
# SELECT RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
# SELECT DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank