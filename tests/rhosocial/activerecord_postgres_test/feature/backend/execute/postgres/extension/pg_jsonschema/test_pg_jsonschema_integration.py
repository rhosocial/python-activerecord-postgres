# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/postgres/extension/
#   pg_jsonschema/test_pg_jsonschema_integration.py
"""Integration tests for pg_jsonschema JSON Schema validation."""

import pytest
import pytest_asyncio

from rhosocial.activerecord_postgres_test.feature.backend.utils import (
    ensure_extension_installed,
    async_ensure_extension_installed,
)
from rhosocial.activerecord.backend.expression import QueryStatement
from rhosocial.activerecord.backend.impl.postgres.functions.pg_jsonschema import (
    json_matches_schema,
    jsonb_matches_schema,
    jsonschema_is_valid,
    jsonschema_validation_errors,
)

OBJECT_SCHEMA = '{"type":"object","properties":{"tags":{"type":"array","items":{"type":"string"}}}}'
VALID_INSTANCE = '{"tags":["vip"]}'
INVALID_INSTANCE = '{"tags":[1]}'


@pytest.fixture
def jsonschema_env(postgres_backend_single):
    backend = postgres_backend_single
    ensure_extension_installed(backend, "pg_jsonschema")
    return backend, backend.dialect


@pytest_asyncio.fixture
async def async_jsonschema_env(async_postgres_backend_single):
    backend = async_postgres_backend_single
    await async_ensure_extension_installed(backend, "pg_jsonschema")
    return backend, backend.dialect


def _fetch(backend, expression):
    sql, params = QueryStatement(dialect=backend.dialect, select=[expression]).to_sql()
    return backend.fetch_one(sql, params)


class TestPgJsonSchemaIntegration:
    def test_capability(self, jsonschema_env):
        _, dialect = jsonschema_env
        assert dialect.supports_pg_jsonschema()
        assert dialect.get_extension_version("pg_jsonschema") is not None

    def test_jsonb_validation(self, jsonschema_env):
        backend, _ = jsonschema_env
        column = "value"
        valid = jsonb_matches_schema(backend.dialect, OBJECT_SCHEMA, VALID_INSTANCE).as_(column)
        invalid = jsonb_matches_schema(backend.dialect, OBJECT_SCHEMA, INVALID_INSTANCE).as_(column)
        assert _fetch(backend, valid)[column] is True
        assert _fetch(backend, invalid)[column] is False

    def test_json_validation(self, jsonschema_env):
        backend, dialect = jsonschema_env
        assert _fetch(backend, json_matches_schema(dialect, OBJECT_SCHEMA, VALID_INSTANCE).as_("ok"))["ok"] is True

    def test_schema_validity_and_errors(self, jsonschema_env):
        backend, dialect = jsonschema_env
        assert _fetch(backend, jsonschema_is_valid(dialect, OBJECT_SCHEMA).as_("ok"))["ok"] is True
        assert _fetch(backend, jsonschema_is_valid(dialect, '{"type":"bogus"}').as_("ok"))["ok"] is False
        sql, params = jsonschema_validation_errors(dialect, OBJECT_SCHEMA, VALID_INSTANCE).to_sql()
        result = backend.fetch_one(f"SELECT cardinality({sql}) AS count", params)
        assert result["count"] == 0


class TestAsyncPgJsonSchemaIntegration:
    @pytest.mark.asyncio
    async def test_async_validation(self, async_jsonschema_env):
        backend, dialect = async_jsonschema_env
        assert dialect.supports_pg_jsonschema()
        sql, params = QueryStatement(
            dialect=dialect,
            select=[jsonb_matches_schema(dialect, OBJECT_SCHEMA, VALID_INSTANCE).as_("ok")],
        ).to_sql()
        result = await backend.fetch_one(sql, params)
        assert result["ok"] is True
