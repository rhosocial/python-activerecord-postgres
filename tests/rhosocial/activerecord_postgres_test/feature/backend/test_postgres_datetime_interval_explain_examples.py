# tests/rhosocial/activerecord_postgres_test/feature/backend/
# test_postgres_datetime_interval_explain_examples.py
"""PostgreSQL EXPLAIN examples for datetime interval expressions and indexes."""

from decimal import Decimal

import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import (
    Column,
    ComparisonPredicate,
    Literal,
    LogicalPredicate,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.functions import (
    date_add,
    date_diff,
    date_sub,
    date_trunc,
    extract,
)
from rhosocial.activerecord.backend.expression.query_parts import OrderByClause
from rhosocial.activerecord.backend.impl.postgres import PostgresExplainResult
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


_SETUP_STMTS = [
    "DROP TABLE IF EXISTS explain_temporal_events CASCADE",
    """CREATE TABLE explain_temporal_events (
        id SERIAL PRIMARY KEY,
        category VARCHAR(32) NOT NULL,
        created_at TIMESTAMP NOT NULL,
        started_at TIMESTAMP NOT NULL,
        ended_at TIMESTAMP NOT NULL
    )""",
    "CREATE INDEX idx_temporal_created_at ON explain_temporal_events(created_at)",
    """CREATE INDEX idx_temporal_started_ended
        ON explain_temporal_events(started_at, ended_at)""",
    """CREATE INDEX idx_temporal_category_created
        ON explain_temporal_events(category, created_at)""",
    """INSERT INTO explain_temporal_events
        (category, created_at, started_at, ended_at)
    SELECT
        CASE
            WHEN n IN (55, 58, 62) THEN 'deploy'
            WHEN n % 3 = 0 THEN 'billing'
            WHEN n % 3 = 1 THEN 'report'
            ELSE 'maintenance'
        END,
        TIMESTAMP '2026-01-01 00:00:00' + n * INTERVAL '1 day',
        TIMESTAMP '2026-01-01 00:00:00' + n * INTERVAL '1 day'
            + INTERVAL '10 minutes',
        TIMESTAMP '2026-01-01 00:00:00' + n * INTERVAL '1 day'
            + INTERVAL '40 minutes'
    FROM generate_series(0, 199) AS seq(n)""",
    "ANALYZE explain_temporal_events",
]

_CLEANUP_STMTS = ["DROP TABLE IF EXISTS explain_temporal_events CASCADE"]
_DQL_OPTIONS = ExecutionOptions(stmt_type=StatementType.DQL)
_INDEX_SCAN_TERMS = ("INDEX SCAN", "BITMAP INDEX SCAN", "INDEX ONLY SCAN")
_DATETIME_INDEX_NAMES = (
    "IDX_TEMPORAL_CATEGORY_CREATED",
    "IDX_TEMPORAL_CREATED_AT",
)


@pytest.fixture(scope="function")
def temporal_indexed_backend(postgres_backend):
    for stmt in _SETUP_STMTS:
        postgres_backend.execute(stmt)
    yield postgres_backend
    try:
        for stmt in _CLEANUP_STMTS:
            postgres_backend.execute(stmt)
    except Exception:
        pass


@pytest_asyncio.fixture(scope="function")
async def async_temporal_indexed_backend(async_postgres_backend):
    for stmt in _SETUP_STMTS:
        await async_postgres_backend.execute(stmt)
    yield async_postgres_backend
    try:
        for stmt in _CLEANUP_STMTS:
            await async_postgres_backend.execute(stmt)
    except Exception:
        pass


def _combined_plan(result: PostgresExplainResult) -> str:
    return " ".join(row.line.upper() for row in result.rows)


def _assert_datetime_index_used(plan: str):
    assert any(term in plan for term in _INDEX_SCAN_TERMS)
    assert any(index_name in plan for index_name in _DATETIME_INDEX_NAMES)


def _range_filter(dialect, column_name: str, start: str, end: str):
    return LogicalPredicate(
        dialect,
        "AND",
        ComparisonPredicate(
            dialect, ">=", Column(dialect, column_name), Literal(dialect, start)
        ),
        ComparisonPredicate(dialect, "<", Column(dialect, column_name), Literal(dialect, end)),
    )


def _category_created_filter(dialect):
    return LogicalPredicate(
        dialect,
        "AND",
        ComparisonPredicate(
            dialect, "=", Column(dialect, "category"), Literal(dialect, "deploy")
        ),
        _range_filter(
            dialect,
            "created_at",
            "2026-02-20 00:00:00",
            "2026-03-10 00:00:00",
        ),
    )


def _datetime_expression_query(dialect):
    return QueryExpression(
        dialect,
        select=[
            Column(dialect, "id"),
            extract(dialect, "year", Column(dialect, "created_at")).as_(
                "created_year"
            ),
            date_trunc(dialect, "month", Column(dialect, "created_at")).as_(
                "created_month"
            ),
            date_add(dialect, Column(dialect, "started_at"), 30, "minute").as_(
                "starts_plus_30m"
            ),
            date_sub(dialect, Column(dialect, "ended_at"), 1, "hour").as_(
                "ended_minus_1h"
            ),
            date_diff(
                dialect, "minute", Column(dialect, "started_at"), Column(dialect, "ended_at")
            ).as_("duration_minutes"),
        ],
        from_=TableExpression(dialect, "explain_temporal_events"),
        where=_category_created_filter(dialect),
        order_by=OrderByClause(
            dialect,
            [(Column(dialect, "category"), "ASC"), (Column(dialect, "created_at"), "ASC")],
        ),
    )


def _assert_temporal_rows(rows):
    assert rows is not None
    assert len(rows) == 3
    assert rows[0]["created_year"] in (2026, Decimal("2026"))
    assert rows[0]["created_month"] is not None
    assert rows[0]["starts_plus_30m"] is not None
    assert rows[0]["ended_minus_1h"] is not None
    assert round(float(rows[0]["duration_minutes"])) == 30


class TestSyncPostgresDateTimeIntervalExplainExamples:
    def test_created_at_range_explain_returns_temporal_plan(
        self, temporal_indexed_backend
    ):
        dialect = temporal_indexed_backend.dialect
        result = temporal_indexed_backend.explain(
            QueryExpression(
                dialect,
                select=[Column(dialect, "id"), Column(dialect, "created_at")],
                from_=TableExpression(dialect, "explain_temporal_events"),
                where=_range_filter(
                    dialect,
                    "created_at",
                    "2026-02-20 00:00:00",
                    "2026-03-10 00:00:00",
                ),
            )
        )

        assert isinstance(result, PostgresExplainResult)
        assert len(result.rows) > 0
        plan = _combined_plan(result)
        assert "EXPLAIN_TEMPORAL_EVENTS" in plan
        assert "SCAN" in plan

    def test_datetime_range_index_is_available_when_seqscan_disabled(
        self, temporal_indexed_backend
    ):
        dialect = temporal_indexed_backend.dialect
        temporal_indexed_backend.execute("SET enable_seqscan = off")
        try:
            result = temporal_indexed_backend.explain(
                QueryExpression(
                    dialect,
                    select=[
                        Column(dialect, "id"),
                        Column(dialect, "category"),
                        Column(dialect, "created_at"),
                    ],
                    from_=TableExpression(dialect, "explain_temporal_events"),
                    where=_category_created_filter(dialect),
                )
            )
        finally:
            temporal_indexed_backend.execute("RESET enable_seqscan")

        plan = _combined_plan(result)
        _assert_datetime_index_used(plan)
        assert result.is_index_used is True
        assert result.is_full_scan is False

    def test_datetime_interval_expressions_work_with_indexed_filter(
        self, temporal_indexed_backend
    ):
        dialect = temporal_indexed_backend.dialect
        query = _datetime_expression_query(dialect)
        temporal_indexed_backend.execute("SET enable_seqscan = off")
        try:
            explain_result = temporal_indexed_backend.explain(
                query
            )
        finally:
            temporal_indexed_backend.execute("RESET enable_seqscan")

        plan = _combined_plan(explain_result)
        _assert_datetime_index_used(plan)
        assert explain_result.is_index_used is True

        query_result = temporal_indexed_backend.execute(
            *query.to_sql(),
            options=_DQL_OPTIONS,
        )
        _assert_temporal_rows(query_result.data)


class TestAsyncPostgresDateTimeIntervalExplainExamples:
    @pytest.mark.asyncio
    async def test_datetime_range_index_is_available_when_seqscan_disabled(
        self, async_temporal_indexed_backend
    ):
        dialect = async_temporal_indexed_backend.dialect
        await async_temporal_indexed_backend.execute("SET enable_seqscan = off")
        try:
            result = await async_temporal_indexed_backend.explain(
                QueryExpression(
                    dialect,
                    select=[
                        Column(dialect, "id"),
                        Column(dialect, "category"),
                        Column(dialect, "created_at"),
                    ],
                    from_=TableExpression(dialect, "explain_temporal_events"),
                    where=_category_created_filter(dialect),
                )
            )
        finally:
            await async_temporal_indexed_backend.execute("RESET enable_seqscan")

        plan = _combined_plan(result)
        _assert_datetime_index_used(plan)
        assert result.is_index_used is True

    @pytest.mark.asyncio
    async def test_datetime_interval_expressions_work_with_indexed_filter(
        self, async_temporal_indexed_backend
    ):
        dialect = async_temporal_indexed_backend.dialect
        query = _datetime_expression_query(dialect)
        await async_temporal_indexed_backend.execute("SET enable_seqscan = off")
        try:
            explain_result = await async_temporal_indexed_backend.explain(
                query
            )
        finally:
            await async_temporal_indexed_backend.execute("RESET enable_seqscan")

        plan = _combined_plan(explain_result)
        _assert_datetime_index_used(plan)
        assert explain_result.is_index_used is True

        query_result = await async_temporal_indexed_backend.execute(
            *query.to_sql(),
            options=_DQL_OPTIONS,
        )
        _assert_temporal_rows(query_result.data)
