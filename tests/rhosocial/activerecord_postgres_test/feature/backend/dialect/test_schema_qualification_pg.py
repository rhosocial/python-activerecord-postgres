# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_schema_qualification_pg.py
"""
PostgreSQL-specific schema qualification assertions (L2).

The core suite (``test_schema_qualification.py``) runs against ``DummyDialect``.
This file pins what is *PostgreSQL semantics* and must not regress.

**Where the alias rule actually lives.** PostgreSQL rejects a schema-qualified
reference to an aliased range (``invalid reference to FROM-clause entry``).
That suppression is performed by ``FieldProxy`` -- which sets
``schema_name=None`` whenever a table alias is in effect
(``base/field_proxy.py:193-197``) -- and **not** by ``format_column``. So a
``Column`` built directly with both an aliased ``table`` and a ``schema_name``
renders a three-part reference that PostgreSQL would reject. That is the
documented contract boundary: model column access goes through ``FieldProxy``;
hand-built ``Column`` objects are the caller's responsibility.

The historical bug that motivated the PG override is recorded in
``test_qualified_reference_context.py``.
"""

from rhosocial.activerecord.backend.expression.core import Column, TableExpression
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

SCHEMA = "ar_crm"
TABLE = "users"


class TestPostgresColumnQualifierForm:
    """Three-part vs two-part qualification, and the alias boundary."""

    def test_t27_no_alias_keeps_three_parts(self):
        d = PostgresDialect()
        col = Column(d, "id", table=TABLE, schema_name=SCHEMA)
        sql, params = col.to_sql()

        assert sql == f'"{SCHEMA}"."{TABLE}"."id"'
        assert params == ()

    def test_t26_alias_without_schema_is_two_parts(self):
        """The shape ``FieldProxy`` produces for an aliased range."""
        d = PostgresDialect()
        col = Column(d, "id", table="u", schema_name=None)
        sql, _ = col.to_sql()

        assert sql == '"u"."id"'

    def test_t26b_column_alias_suppresses_schema_in_pg_formatter(self):
        """A column alias makes ``format_column`` emit two parts.

        This is the branch ``PostgresColumnMixin.format_column`` actually
        guards (``if schema_name and not alias``).
        """
        d = PostgresDialect()
        col = Column(d, "id", table=TABLE, schema_name=SCHEMA, alias="x")
        sql, _ = col.to_sql()

        assert sql == '"users"."id" AS "x"'

    def test_t26c_aliased_table_plus_schema_is_three_parts(self):
        """Documents the contract boundary -- ``FieldProxy`` prevents this case.

        Asserted so that a future change to either side is visible: if PG's
        ``format_column`` starts dropping the schema on its own, or
        ``FieldProxy`` stops nulling it, one of these two tests must be
        revisited deliberately.
        """
        d = PostgresDialect()
        col = Column(d, "id", table="u", schema_name=SCHEMA)
        assert col.to_sql()[0] == f'"{SCHEMA}"."u"."id"'

    def test_t19_column_schema_without_table_drops_schema(self, dialect_note=None):
        """C6 -- a schema with no table is silently discarded.

        Currently *not* an error: the core ladder is
        ``if schema_name and expr.table:`` so the schema evaporates. Phase 5
        will turn this into an explicit failure; this test documents today's
        behaviour so the change is a deliberate, visible one.
        """
        d = PostgresDialect()
        assert Column(d, "id", schema_name=SCHEMA).to_sql()[0] == '"id"'


class TestPostgresTableRangeForm:
    def test_from_range_is_schema_qualified(self):
        d = PostgresDialect()
        assert (
            TableExpression(d, TABLE, schema_name=SCHEMA).to_sql()[0]
            == f'"{SCHEMA}"."{TABLE}"'
        )

    def test_from_range_without_schema_is_bare(self):
        d = PostgresDialect()
        assert TableExpression(d, TABLE).to_sql()[0] == f'"{TABLE}"'

    def test_alias_keeps_schema_qualification_on_range(self):
        """The range stays qualified; only *column refs* need the alias."""
        d = PostgresDialect()
        sql = TableExpression(d, TABLE, schema_name=SCHEMA, alias="u").to_sql()[0]
        assert sql == f'"{SCHEMA}"."{TABLE}" AS "u"'


class TestPostgresCreateExtensionSchemaQuoting:
    """C11 -- ``SCHEMA {schema}`` must go through ``format_identifier``."""

    def test_t24_extension_schema_is_quoted(self):
        from rhosocial.activerecord.backend.impl.postgres.expression.ddl.extension import (
            PostgresCreateExtensionExpression,
        )

        d = PostgresDialect()
        sql, _ = PostgresCreateExtensionExpression(
            d, "postgis", schema="My Schema"
        ).to_sql()

        assert 'SCHEMA "My Schema"' in sql, f"schema 未加引号: {sql}"
        assert "SCHEMA My Schema" not in sql
