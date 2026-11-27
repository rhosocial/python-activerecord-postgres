# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_pg_jsonschema_functions.py
"""SQL construction tests for the pg_jsonschema factories."""

import pytest

from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.pg_jsonschema import (
    json_matches_schema,
    jsonb_matches_schema,
    jsonschema_is_valid,
    jsonschema_validation_errors,
)
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


@pytest.fixture
def dialect():
    return PostgresDialect()


class TestPgJsonSchemaFactories:
    def test_json_matches_schema(self, dialect):
        assert json_matches_schema(dialect, '{"type":"object"}', "{}").to_sql() == (
            "JSON_MATCHES_SCHEMA(%s::JSON, %s::JSON)",
            ('{"type":"object"}', "{}"),
        )

    def test_jsonb_matches_schema(self, dialect):
        assert jsonb_matches_schema(dialect, '{"type":"object"}', "{}").to_sql() == (
            "JSONB_MATCHES_SCHEMA(%s::JSON, %s::JSONB)",
            ('{"type":"object"}', "{}"),
        )

    def test_column_instance_is_not_recast(self, dialect):
        sql, params = jsonb_matches_schema(dialect, '{"type":"object"}', Column(dialect, "payload")).to_sql()
        assert sql == 'JSONB_MATCHES_SCHEMA(%s::JSON, "payload")'
        assert params == ('{"type":"object"}',)

    def test_schema_is_valid(self, dialect):
        assert jsonschema_is_valid(dialect, '{"type":"object"}').to_sql() == (
            "JSONSCHEMA_IS_VALID(%s::JSON)",
            ('{"type":"object"}',),
        )

    def test_validation_errors(self, dialect):
        assert jsonschema_validation_errors(dialect, '{"maxLength":4}', '"abcdef"').to_sql() == (
            "JSONSCHEMA_VALIDATION_ERRORS(%s::JSON, %s::JSON)",
            ('{"maxLength":4}', '"abcdef"'),
        )


class TestCapabilityGate:
    @pytest.mark.parametrize(
        ("version", "expected"),
        [("0.3.4", True), ("0.2.0", False)],
    )
    def test_version_gate(self, version, expected):
        dialect = PostgresDialect()
        dialect._extensions = {
            "pg_jsonschema": PostgresExtensionInfo(
                name="pg_jsonschema", installed=True, available=True, version=version
            )
        }
        assert dialect.supports_pg_jsonschema() is expected

    def test_not_installed(self):
        assert PostgresDialect().supports_pg_jsonschema() is False
