# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/functions/test_seconds_between.py
"""Signed, fractional seconds between two instants on PostgreSQL.

What matters here is what a calendar-component sum gets wrong: the sign,
the zero, the fraction, and the boundaries where a month or a year changes.
``EXTRACT(EPOCH FROM (end - start))`` is a ``numeric``, so the value keeps the
microseconds PostgreSQL stores and turns negative when the end is earlier.
"""

import pytest

from rhosocial.activerecord.backend.expression import (
    Column,
    Literal,
    QueryStatement,
    seconds_between,
)
from rhosocial.activerecord.backend.expression.datetime import SecondsBetweenExpression
from rhosocial.activerecord.backend.expression.value_types import NumericValue
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

_SPANS = [
    ("2026-06-04 10:00:00", "2026-06-04 10:00:00", 0.0),
    ("2026-06-04 10:00:00", "2026-06-04 11:00:00", 3600.0),
    ("2026-06-04 11:00:00", "2026-06-04 10:00:00", -3600.0),
    ("2026-01-31 23:59:59", "2026-02-01 00:00:00", 1.0),
    ("2025-12-31 12:00:00", "2026-01-01 12:00:00", 86400.0),
    ("2026-06-04 10:00:00.000", "2026-06-04 10:00:00.500", 0.5),
    ("2026-06-04 10:00:00.250", "2026-06-04 10:00:00.000", -0.25),
]

_TABLE = "postgres_seconds_between_spans"


def _setup_statements() -> list:
    statements = [f"DROP TABLE IF EXISTS {_TABLE} CASCADE"]
    statements.append(
        f"""
        CREATE TABLE {_TABLE} (
            id INTEGER PRIMARY KEY,
            started_at TIMESTAMP NOT NULL,
            ended_at TIMESTAMP NOT NULL
        )
        """
    )
    for index, (start, end, _) in enumerate(_SPANS, start=1):
        statements.append(
            f"INSERT INTO {_TABLE} (id, started_at, ended_at) VALUES "
            f"({index}, TIMESTAMP '{start}', TIMESTAMP '{end}')"
        )
    return statements


_CLEANUP = f"DROP TABLE IF EXISTS {_TABLE} CASCADE"
_DQL_OPTIONS = ExecutionOptions(stmt_type=StatementType.DQL)


def _seconds_query(dialect) -> QueryStatement:
    return QueryStatement(
        dialect,
        select=[
            seconds_between(
                dialect,
                Column(dialect, "started_at"),
                Column(dialect, "ended_at"),
            ).as_("seconds"),
            Column(dialect, "id"),
        ],
        from_=_TABLE,
    )


class TestSecondsBetweenExpression:
    """The SQL PostgreSQL renders for the node."""

    def test_columns(self, postgres_dialect):
        expr = seconds_between(
            postgres_dialect,
            Column(postgres_dialect, "started_at"),
            Column(postgres_dialect, "ended_at"),
        )

        sql, params = expr.to_sql()

        assert sql == 'EXTRACT(EPOCH FROM ("ended_at" - "started_at"))'
        assert params == ()

    def test_literal_params_follow_sql_order(self, postgres_dialect):
        expr = seconds_between(
            postgres_dialect,
            Literal(postgres_dialect, "2026-01-01 00:00:00"),
            Literal(postgres_dialect, "2026-01-02 00:00:00"),
        )

        sql, params = expr.to_sql()

        assert sql == "EXTRACT(EPOCH FROM (%s - %s))"
        # ``end`` is written first, so its parameter binds first.
        assert params == ("2026-01-02 00:00:00", "2026-01-01 00:00:00")

    def test_factory_coerces_string_to_column(self, postgres_dialect):
        expr = seconds_between(postgres_dialect, "started_at", "ended_at")

        sql, params = expr.to_sql()

        assert sql == 'EXTRACT(EPOCH FROM ("ended_at" - "started_at"))'
        assert params == ()

    def test_alias(self, postgres_dialect):
        expr = seconds_between(
            postgres_dialect,
            Column(postgres_dialect, "started_at"),
            Column(postgres_dialect, "ended_at"),
        ).as_("elapsed")

        sql, params = expr.to_sql()

        assert sql == (
            'EXTRACT(EPOCH FROM ("ended_at" - "started_at")) AS "elapsed"'
        )
        assert params == ()

    def test_is_a_signed_fractional_number(self, postgres_dialect):
        expr = seconds_between(
            postgres_dialect,
            Column(postgres_dialect, "started_at"),
            Column(postgres_dialect, "ended_at"),
        )

        assert isinstance(expr, SecondsBetweenExpression)
        assert isinstance(expr, NumericValue)
        assert expr.format_method == "format_seconds_between_expression"

    def test_query_integration(self, postgres_dialect):
        query = QueryStatement(
            postgres_dialect,
            select=[
                seconds_between(
                    postgres_dialect,
                    Column(postgres_dialect, "started_at"),
                    Column(postgres_dialect, "ended_at"),
                ).as_("seconds")
            ],
            from_="spans",
        )

        sql, params = query.to_sql()

        assert 'EXTRACT(EPOCH FROM ("ended_at" - "started_at")) AS "seconds"' in sql
        assert params == ()


class TestSyncSecondsBetweenExecution:
    """The value the PostgreSQL spelling actually produces."""

    def test_signed_fractional_semantics(self, postgres_backend_single):
        backend = postgres_backend_single
        try:
            for statement in _setup_statements():
                backend.execute(statement)

            query = _seconds_query(backend.dialect)
            sql, params = query.to_sql()
            result = backend.execute(sql, params, options=_DQL_OPTIONS)

            rows = {row["id"]: row["seconds"] for row in result.data}
            for index, (_, _, expected) in enumerate(_SPANS, start=1):
                assert float(rows[index]) == pytest.approx(expected, abs=1e-6)
        finally:
            backend.execute(_CLEANUP)


class TestAsyncSecondsBetweenExecution:
    """The same value asked through the async backend."""

    @pytest.mark.asyncio
    async def test_signed_fractional_semantics(
        self, async_postgres_backend_single
    ):
        backend = async_postgres_backend_single
        try:
            for statement in _setup_statements():
                await backend.execute(statement)

            query = _seconds_query(backend.dialect)
            sql, params = query.to_sql()
            result = await backend.execute(sql, params, options=_DQL_OPTIONS)

            rows = {row["id"]: row["seconds"] for row in result.data}
            for index, (_, _, expected) in enumerate(_SPANS, start=1):
                assert float(rows[index]) == pytest.approx(expected, abs=1e-6)
        finally:
            await backend.execute(_CLEANUP)
