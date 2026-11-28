# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_json_index.py
"""Construction tests for JSON-key indexes and ARROW path rendering."""
import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression import JSONColumn
from rhosocial.activerecord.backend.expression.functions.advanced_functions import JSONPathMode
from rhosocial.activerecord.backend.expression.functions.json import json_extract_text
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.json import create_jsonb_index


class TestJsonbIndexHelper:
    def test_gin_index_on_key(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = create_jsonb_index(d, "events", "$.payload").to_sql()
        assert 'CREATE INDEX "idx_events_data_gin" ON "t"' not in sql
        assert 'CREATE INDEX "idx_events_data_gin" ON "events"' in sql
        assert "USING GIN" in sql
        assert "jsonb_path_ops" in sql
        assert "'$.payload'::jsonpath" in sql
        assert params == ()

    def test_text_index_on_key(self):
        d = PostgresDialect((15, 0, 0))
        sql, params = create_jsonb_index(
            d, "events", "$.name", as_text=True
        ).to_sql()
        assert "USING GIN" not in sql
        assert "#>> '{}'" in sql
        assert params == ()

    def test_custom_opclass(self):
        d = PostgresDialect((15, 0, 0))
        sql, _ = create_jsonb_index(
            d, "events", "$.payload", opclass="jsonb_ops"
        ).to_sql()
        assert "jsonb_ops" in sql
        assert "jsonb_path_ops" not in sql

    def test_no_opclass(self):
        d = PostgresDialect((15, 0, 0))
        sql, _ = create_jsonb_index(
            d, "events", "$.payload", opclass=None
        ).to_sql()
        assert "jsonb_ops" not in sql


class TestArrowPathRendering:
    def test_single_key(self):
        d = PostgresDialect((15, 0, 0))
        expr = JSONColumn(d, "data").json_path("name", mode=JSONPathMode.ARROW)
        assert expr.to_sql() == ('"data"->>\'name\'', ())

    def test_nested_chain(self):
        d = PostgresDialect((15, 0, 0))
        expr = (
            JSONColumn(d, "data")
            .json_value("profile", mode=JSONPathMode.ARROW)
            .json_path("name", mode=JSONPathMode.ARROW)
        )
        assert expr.to_sql() == ('"data"->\'profile\'->>\'name\'', ())

    def test_array_index(self):
        d = PostgresDialect((15, 0, 0))
        expr = JSONColumn(d, "data").json_path("tags", 0, mode=JSONPathMode.ARROW)
        assert expr.to_sql() == ('"data"->\'tags\'->>0', ())

    def test_unsupported_path_is_refused(self):
        d = PostgresDialect((15, 0, 0))
        expr = JSONColumn(d, "data").json_path(
            "$.tags[*]", mode=JSONPathMode.ARROW
        )
        with pytest.raises(UnsupportedFeatureError, match="ARROW"):
            expr.to_sql()


class TestInlinePath:
    def test_inline_path_has_no_parameters(self):
        d = PostgresDialect((15, 0, 0))
        expr = json_extract_text(d, "data", "$.name", inline_path=True)
        sql, params = expr.to_sql()
        assert "%s" not in sql
        assert params == ()
        assert "'$.name'::jsonpath" in sql

    def test_default_path_stays_parameterized(self):
        d = PostgresDialect((15, 0, 0))
        expr = json_extract_text(d, "data", "$.name")
        sql, params = expr.to_sql()
        assert "%s" in sql
        assert params == ("$.name",)
