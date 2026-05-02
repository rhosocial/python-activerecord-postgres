"""
Query runtime functions and statement-level constants without a data source - PostgreSQL.

This example demonstrates:
1. SELECT CURRENT_TIMESTAMP, NOW(), etc. without FROM clause
2. How to use factory functions for SQL niladic functions in SELECT list
3. Various PostgreSQL date/time functions as standalone queries
4. PostgreSQL-specific info functions (CURRENT_USER, SESSION_USER, etc.)
"""

# ============================================================
# SECTION: Setup (necessary for execution, reference only)
# ============================================================
import os
from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.backend.expression import QueryExpression
from rhosocial.activerecord.backend.expression.core import FunctionCall, Literal
from rhosocial.activerecord.backend.expression.functions.datetime import (
    current_timestamp,
    now,
    current_date,
    current_time,
    localtimestamp,
)
from rhosocial.activerecord.backend.expression.functions.system import (
    current_user,
    session_user,
)

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

# ============================================================
# SECTION: SELECT CURRENT_TIMESTAMP
# ============================================================
# Use factory functions for SQL statement-level constants.
# current_timestamp(dialect) generates CURRENT_TIMESTAMP (no parentheses)
# which is the SQL:2003 standard niladic form, valid in both DDL DEFAULT
# and SELECT contexts.

query_ts = QueryExpression(
    dialect=dialect,
    select=[current_timestamp(dialect)],
)
sql, params = query_ts.to_sql()
print(f"CURRENT_TIMESTAMP SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Result: {result.data}")

# ============================================================
# SECTION: SELECT NOW()
# ============================================================
query_now = QueryExpression(
    dialect=dialect,
    select=[now(dialect)],
)
sql, params = query_now.to_sql()
print(f"NOW() SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Result: {result.data}")

# ============================================================
# SECTION: SELECT CURRENT_DATE, CURRENT_TIME
# ============================================================
query_date = QueryExpression(
    dialect=dialect,
    select=[
        current_date(dialect),
        current_time(dialect),
    ],
)
sql, params = query_date.to_sql()
print(f"CURRENT_DATE/CURRENT_TIME SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Result: {result.data}")

# ============================================================
# SECTION: SELECT with multiple functions
# ============================================================
query_multi = QueryExpression(
    dialect=dialect,
    select=[
        now(dialect).as_('current_time'),
        current_date(dialect).as_('current_date'),
        current_user(dialect).as_('current_user'),
        session_user(dialect).as_('session_user'),
    ],
)
sql, params = query_multi.to_sql()
print(f"Multi-function SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Result: {result.data}")

# ============================================================
# SECTION: SELECT with function arguments
# ============================================================
# FunctionCall supports arguments, e.g., date_part, to_char

query_format = QueryExpression(
    dialect=dialect,
    select=[
        FunctionCall(dialect, 'TO_CHAR', now(dialect), Literal(dialect, 'YYYY-MM-DD')).as_('formatted_date'),
    ],
)
sql, params = query_format.to_sql()
print(f"TO_CHAR SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Result: {result.data}")

# ============================================================
# SECTION: SELECT LOCALTIMESTAMP
# ============================================================
query_localts = QueryExpression(
    dialect=dialect,
    select=[localtimestamp(dialect)],
)
sql, params = query_localts.to_sql()
print(f"LOCALTIMESTAMP SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Result: {result.data}")

# ============================================================
# SECTION: SELECT version()
# ============================================================
query_version = QueryExpression(
    dialect=dialect,
    select=[FunctionCall(dialect, 'VERSION').as_('db_version')],
)
sql, params = query_version.to_sql()
print(f"VERSION() SQL: {sql}")
result = backend.execute(sql, params, options=dql_options)
print(f"Result: {result.data}")

# ============================================================
# SECTION: Teardown (necessary for execution, reference only)
# ============================================================
backend.disconnect()

# ============================================================
# SECTION: Summary
# ============================================================
# Key points:
# 1. Use factory functions (current_timestamp, now, current_date, etc.) for SQL niladic functions
# 2. QueryExpression without from_ clause generates "SELECT ..." without FROM
# 3. Use FunctionCall(dialect, 'FUNC', arg1, arg2) for regular functions with arguments
# 4. Use .as_('alias') for column aliases in SELECT
# 5. SQL:2003 niladic functions (CURRENT_TIMESTAMP, CURRENT_DATE, etc.) omit parentheses
# 6. PostgreSQL supports CURRENT_USER, SESSION_USER as niladic functions
# 7. Common PostgreSQL info functions: VERSION, CURRENT_QUERY, PG_BACKEND_PID
