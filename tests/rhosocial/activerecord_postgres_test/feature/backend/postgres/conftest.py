# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/conftest.py
"""Fixtures for PostgreSQL materialized view integration tests."""

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import (
    Column,
    FunctionCall,
    GroupByHavingClause,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresCreateMaterializedViewExpression,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DDL = ExecutionOptions(stmt_type=StatementType.DDL)
DQL = ExecutionOptions(stmt_type=StatementType.DQL)

_SETUP_SQL = [
    "DROP MATERIALIZED VIEW IF EXISTS mv_sales_summary CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_sales_daily CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_reporting.sales_summary CASCADE",
    "DROP SCHEMA IF EXISTS mv_reporting CASCADE",
    "DROP TABLE IF EXISTS mv_sales CASCADE",
    """
    CREATE TABLE mv_sales (
        id SERIAL PRIMARY KEY,
        product_id INTEGER NOT NULL,
        amount DECIMAL(10,2) NOT NULL,
        sale_date DATE NOT NULL
    )
    """,
    """
    INSERT INTO mv_sales (product_id, amount, sale_date) VALUES
        (1, 100.00, '2024-01-01'),
        (1, 150.00, '2024-01-02'),
        (2, 200.00, '2024-01-01'),
        (2, 250.00, '2024-01-03')
    """,
]

_TEARDOWN_SQL = [
    "DROP MATERIALIZED VIEW IF EXISTS mv_sales_summary CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_sales_daily CASCADE",
    "DROP MATERIALIZED VIEW IF EXISTS mv_reporting.sales_summary CASCADE",
    "DROP SCHEMA IF EXISTS mv_reporting CASCADE",
    "DROP TABLE IF EXISTS mv_sales CASCADE",
]


@pytest.fixture
def summary_query(mv_backend):
    """Aggregation over the mv_sales fixture table."""
    def _build():
        dialect = mv_backend.dialect
        return QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "product_id"),
                FunctionCall(dialect, "COUNT", Column(dialect, "id")),
                FunctionCall(dialect, "SUM", Column(dialect, "amount")),
            ],
            from_=TableExpression(dialect, "mv_sales"),
            group_by_having=GroupByHavingClause(
                dialect=dialect, group_by=[Column(dialect, "product_id")]
            ),
        )

    return _build


@pytest.fixture
def daily_query(mv_backend):
    """Per-day aggregation over the mv_sales fixture table."""
    def _build():
        dialect = mv_backend.dialect
        return QueryExpression(
            dialect=dialect,
            select=[
                Column(dialect, "sale_date"),
                FunctionCall(dialect, "SUM", Column(dialect, "amount")),
            ],
            from_=TableExpression(dialect, "mv_sales"),
            group_by_having=GroupByHavingClause(
                dialect=dialect, group_by=[Column(dialect, "sale_date")]
            ),
        )

    return _build


@pytest.fixture
def mv_backend(postgres_backend_single):
    """Backend with the mv_sales base table populated."""
    backend = postgres_backend_single
    for statement in _SETUP_SQL:
        backend.execute(statement)
    backend.introspector.clear_cache()
    yield backend
    try:
        backend.introspector.clear_cache()
        for statement in _TEARDOWN_SQL:
            backend.execute(statement)
    except Exception:
        pass


@pytest_asyncio.fixture
async def async_mv_backend(async_postgres_backend_single):
    """Async backend with the mv_sales base table populated."""
    backend = async_postgres_backend_single
    for statement in _SETUP_SQL:
        await backend.execute(statement)
    backend.introspector.clear_cache()
    yield backend
    try:
        backend.introspector.clear_cache()
        for statement in _TEARDOWN_SQL:
            await backend.execute(statement)
    except Exception:
        pass


@pytest.fixture
def create_summary_mv(mv_backend, summary_query):
    """Factory that creates a summary materialized view on the live server."""

    def _create(view_name="mv_sales_summary", query=None, **kwargs):
        expression = PostgresCreateMaterializedViewExpression(
            dialect=mv_backend.dialect,
            view_name=view_name,
            query=query if query is not None else summary_query(),
            **kwargs,
        )
        sql, params = expression.to_sql()
        mv_backend.execute(sql, params, options=DDL)
        mv_backend.introspector.clear_cache()
        return expression

    return _create

