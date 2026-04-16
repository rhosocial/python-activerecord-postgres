"""
CTE (Common Table Expression) example - WITH clause.
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
    CTEExpression,
    CTEClause,
    CTEType,
)
from rhosocial.activerecord.backend.expression.core import TableExpression

# Example: Recursive CTE for hierarchy
employees = TableExpression('employees')

# Simple CTE - calculate statistics first
cte_stats = CTEExpression(
    name='dept_stats',
    cte_type=CTEType.RECURSIVE,
    columns=['department', 'avg_salary', 'count'],
    query=None,  # Would need SelectQueryExpression here
)

# CTE with multiple common uses
cte_recent = CTEExpression(
    name='recent_orders',
    query=None,  # SelectQueryExpression
)

# ============================================================
# SECTION: Output (reference)
# ============================================================
# Expected CTE SQL:
# WITH dept_stats AS (SELECT department, AVG(salary) AS avg_salary, COUNT(*) AS count FROM employees GROUP BY department)
# SELECT * FROM dept_stats WHERE avg_salary > 50000

# WITH RECURSIVE employee_hierarchy AS (...)
# SELECT * FROM employee_hierarchy