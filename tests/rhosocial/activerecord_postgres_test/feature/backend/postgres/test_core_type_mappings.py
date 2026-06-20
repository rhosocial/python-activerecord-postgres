"""Tests for PostgreSQL core type → SQL string mappings."""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


@pytest.fixture
def dialect():
    return PostgresDialect(version=(16, 0, 0))


class TestCoreTypeMappings:
    """Verify core DataType → SQL rendering via PostgreSQL dialect."""

    # The five critical mappings
    def test_boolean_to_boolean(self, dialect):
        from rhosocial.activerecord.backend.expression.types import BooleanType
        assert BooleanType().to_sql(dialect) == ("BOOLEAN", ())

    def test_tinyint_to_smallint(self, dialect):
        from rhosocial.activerecord.backend.expression.types import TinyIntType
        assert TinyIntType().to_sql(dialect) == ("SMALLINT", ())

    def test_blob_to_bytea(self, dialect):
        from rhosocial.activerecord.backend.expression.types import BlobType
        assert BlobType().to_sql(dialect) == ("BYTEA", ())

    def test_datetime_to_timestamp(self, dialect):
        from rhosocial.activerecord.backend.expression.types import DateTimeType
        assert DateTimeType().to_sql(dialect) == ("TIMESTAMP", ())

    def test_int_to_integer(self, dialect):
        from rhosocial.activerecord.backend.expression.types import IntType
        assert IntType().to_sql(dialect) == ("INTEGER", ())

    # Standard integer family
    def test_smallint(self, dialect):
        from rhosocial.activerecord.backend.expression.types import SmallIntType
        assert SmallIntType().to_sql(dialect) == ("SMALLINT", ())

    def test_integer(self, dialect):
        from rhosocial.activerecord.backend.expression.types import IntegerType
        assert IntegerType().to_sql(dialect) == ("INTEGER", ())

    def test_bigint(self, dialect):
        from rhosocial.activerecord.backend.expression.types import BigIntType
        assert BigIntType().to_sql(dialect) == ("BIGINT", ())

    # Numeric family
    def test_real(self, dialect):
        from rhosocial.activerecord.backend.expression.types import RealType
        assert RealType().to_sql(dialect) == ("REAL", ())

    def test_double(self, dialect):
        from rhosocial.activerecord.backend.expression.types import DoubleType
        assert DoubleType().to_sql(dialect) == ("DOUBLE PRECISION", ())

    def test_numeric(self, dialect):
        from rhosocial.activerecord.backend.expression.types import DecimalType
        assert DecimalType(10, 2).to_sql(dialect) == ("DECIMAL(10,2)", ())
        assert DecimalType().to_sql(dialect) == ("DECIMAL", ())

    # String family
    def test_varchar(self, dialect):
        from rhosocial.activerecord.backend.expression.types import VarCharType
        assert VarCharType(255).to_sql(dialect) == ("VARCHAR(255)", ())
        assert VarCharType().to_sql(dialect) == ("VARCHAR", ())

    def test_text(self, dialect):
        from rhosocial.activerecord.backend.expression.types import TextType
        assert TextType().to_sql(dialect) == ("TEXT", ())

    # JSON family
    def test_json(self, dialect):
        from rhosocial.activerecord.backend.expression.types import JsonType
        assert JsonType().to_sql(dialect) == ("JSON", ())

    def test_jsonb(self, dialect):
        from rhosocial.activerecord.backend.expression.types import JsonBType
        assert JsonBType().to_sql(dialect) == ("JSONB", ())
