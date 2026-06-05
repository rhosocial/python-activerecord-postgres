# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/
# test_postgres_datetime_interval_expressions.py
"""Tests for PostgreSQL datetime interval expressions."""

import pytest

from rhosocial.activerecord.backend.expression import Column, Literal, QueryExpression
from rhosocial.activerecord.backend.expression.functions import (
    date_add,
    date_diff,
    date_part,
    date_sub,
    date_trunc,
    extract,
    interval,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgresDateTimeIntervalExpressions:
    @pytest.mark.parametrize(
        "field",
        ["year", "month", "day", "hour", "minute", "second"],
    )
    def test_extract_datetime_fields(
        self, postgres_dialect: PostgresDialect, field: str
    ):
        expr = extract(postgres_dialect, field, Column(postgres_dialect, "created_at"))

        sql, params = expr.to_sql()

        assert sql == f'EXTRACT({field.upper()} FROM "created_at")'
        assert params == ()

    def test_date_part_uses_extract_mapping(self, postgres_dialect: PostgresDialect):
        expr = date_part(
            postgres_dialect, "day", Column(postgres_dialect, "created_at")
        )

        sql, params = expr.to_sql()

        assert sql == 'EXTRACT(DAY FROM "created_at")'
        assert params == ()

    def test_date_trunc_expression(self, postgres_dialect: PostgresDialect):
        expr = date_trunc(
            postgres_dialect, "month", Column(postgres_dialect, "created_at")
        )

        sql, params = expr.to_sql()

        assert sql == "DATE_TRUNC('month', \"created_at\")"
        assert params == ()

    def test_interval_expression(self, postgres_dialect: PostgresDialect):
        expr = interval(postgres_dialect, 2, "hour")

        sql, params = expr.to_sql()

        assert sql == "INTERVAL '2' HOUR"
        assert params == ()

    def test_date_add_column_source(self, postgres_dialect: PostgresDialect):
        expr = date_add(
            postgres_dialect, Column(postgres_dialect, "created_at"), 1, "day"
        )

        sql, params = expr.to_sql()

        assert sql == '"created_at" + INTERVAL \'1\' DAY'
        assert params == ()

    def test_date_sub_interval_expression(self, postgres_dialect: PostgresDialect):
        expr = date_sub(
            postgres_dialect,
            Column(postgres_dialect, "created_at"),
            interval(postgres_dialect, 2, "hour"),
        )

        sql, params = expr.to_sql()

        assert sql == '"created_at" - INTERVAL \'2\' HOUR'
        assert params == ()

    def test_date_add_literal_source_params_order(
        self, postgres_dialect: PostgresDialect
    ):
        expr = date_add(
            postgres_dialect,
            Literal(postgres_dialect, "2026-06-04 10:00:00"),
            30,
            "minute",
        )

        sql, params = expr.to_sql()

        assert sql == "%s + INTERVAL '30' MINUTE"
        assert params == ("2026-06-04 10:00:00",)

    @pytest.mark.parametrize(
        "unit,factor",
        [
            ("second", "1"),
            ("minute", "60"),
            ("hour", "3600"),
            ("day", "86400"),
            ("week", "604800"),
        ],
    )
    def test_date_diff_epoch_units(
        self, postgres_dialect: PostgresDialect, unit: str, factor: str
    ):
        expr = date_diff(
            postgres_dialect,
            unit,
            Column(postgres_dialect, "started_at"),
            Column(postgres_dialect, "ended_at"),
        )

        sql, params = expr.to_sql()

        assert sql == (
            '(EXTRACT(EPOCH FROM ("ended_at" - "started_at"))'
            f" / {factor})"
        )
        assert params == ()

    def test_date_diff_month(self, postgres_dialect: PostgresDialect):
        expr = date_diff(
            postgres_dialect,
            "month",
            Column(postgres_dialect, "started_at"),
            Column(postgres_dialect, "ended_at"),
        )

        sql, params = expr.to_sql()

        assert sql == (
            '((EXTRACT(YEAR FROM "ended_at") - EXTRACT(YEAR FROM "started_at")) '
            '* 12 + (EXTRACT(MONTH FROM "ended_at") - '
            'EXTRACT(MONTH FROM "started_at")))'
        )
        assert params == ()

    def test_date_diff_month_literal_params(self, postgres_dialect: PostgresDialect):
        expr = date_diff(
            postgres_dialect,
            "month",
            Literal(postgres_dialect, "2026-01-01"),
            Literal(postgres_dialect, "2026-06-01"),
        )

        sql, params = expr.to_sql()

        assert sql == (
            "((EXTRACT(YEAR FROM %s) - EXTRACT(YEAR FROM %s)) "
            "* 12 + (EXTRACT(MONTH FROM %s) - EXTRACT(MONTH FROM %s)))"
        )
        assert params == (
            "2026-06-01",
            "2026-01-01",
            "2026-06-01",
            "2026-01-01",
        )

    def test_date_diff_year(self, postgres_dialect: PostgresDialect):
        expr = date_diff(
            postgres_dialect,
            "year",
            Column(postgres_dialect, "started_at"),
            Column(postgres_dialect, "ended_at"),
        )

        sql, params = expr.to_sql()

        assert sql == (
            '(EXTRACT(YEAR FROM "ended_at") - EXTRACT(YEAR FROM "started_at"))'
        )
        assert params == ()

    def test_alias_and_cast(self, postgres_dialect: PostgresDialect):
        expr = date_diff(
            postgres_dialect,
            "day",
            Column(postgres_dialect, "started_at"),
            Column(postgres_dialect, "ended_at"),
        ).cast("INTEGER").as_("elapsed_days")

        sql, params = expr.to_sql()

        assert sql == (
            '(EXTRACT(EPOCH FROM ("ended_at" - "started_at")) / 86400)'
            '::INTEGER AS "elapsed_days"'
        )
        assert params == ()

    def test_query_expression_integration(self, postgres_dialect: PostgresDialect):
        shifted = date_add(
            postgres_dialect, Column(postgres_dialect, "created_at"), 1, "day"
        )
        query = QueryExpression(
            postgres_dialect,
            select=[
                extract(postgres_dialect, "year", Column(postgres_dialect, "created_at"))
            ],
            from_="events",
            where=shifted > Literal(postgres_dialect, "2026-01-01"),
        )

        sql, params = query.to_sql()

        assert 'EXTRACT(YEAR FROM "created_at")' in sql
        assert '"created_at" + INTERVAL \'1\' DAY > %s' in sql
        assert params == ("2026-01-01",)
