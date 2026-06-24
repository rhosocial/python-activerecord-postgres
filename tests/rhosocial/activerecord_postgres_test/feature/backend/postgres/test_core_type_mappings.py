"""Tests for PostgreSQL core type → SQL string mappings."""

import pytest

from rhosocial.activerecord.backend.dialect.mixins.ddl_type import DDLTypeMixin


@pytest.fixture
def dialect():
    # Import directly to avoid triggering backend init which requires psycopg
    from rhosocial.activerecord.backend.impl.postgres.mixins.types import (
        PostgresTypeFormatSupportMixin,
    )
    from rhosocial.activerecord.backend.dialect.base import SQLDialectBase

    # Build a minimal dialect class that only has the type formatting
    class TestPGDialect(PostgresTypeFormatSupportMixin, SQLDialectBase):
        pass

    d = TestPGDialect()
    d.version = (16, 0, 0)
    return d


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


class TestArrayType:
    """PostgreSQL array type (T[]) rendering and parsing."""

    def test_array_1d_rendering(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType
        arr = PostgresArrayType(IntegerType())
        assert arr.to_sql(dialect) == ("INTEGER[]", ())

    def test_array_2d_rendering(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType
        arr = PostgresArrayType(IntegerType(), dimensions=2)
        assert arr.to_sql(dialect) == ("INTEGER[][]", ())

    def test_array_varchar_rendering(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import VarCharType
        arr = PostgresArrayType(VarCharType(255))
        assert arr.to_sql(dialect) == ("VARCHAR(255)[]", ())

    def test_array_boolean_rendering(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import BooleanType
        arr = PostgresArrayType(BooleanType())
        assert arr.to_sql(dialect) == ("BOOLEAN[]", ())

    def test_array_equality(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import (
            ArrayType, IntegerType, VarCharType,
        )
        a1 = PostgresArrayType(IntegerType(), 2)
        a2 = PostgresArrayType(IntegerType(), 2)
        a3 = PostgresArrayType(VarCharType(255), 2)
        assert a1 == a2
        assert a1 != a3
        assert hash(a1) == hash(a2)
        # PostgresArrayType.is_equivalent ignores dimensions
        a4 = PostgresArrayType(IntegerType(), 1)
        assert a1.is_equivalent(a4)

    def test_parse_array_bracket_suffix(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType
        result = dialect.parse_type("INTEGER[]")
        assert isinstance(result, PostgresArrayType)
        assert result.dimensions == 1
        assert isinstance(result.element_type, IntegerType)

    def test_parse_array_2d_bracket_suffix(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType
        result = dialect.parse_type("INTEGER[][]")
        assert isinstance(result, PostgresArrayType)
        # PG normalises all array declarations to 1-D internally
        assert result.dimensions == 1
        assert isinstance(result.element_type, IntegerType)

    def test_parse_array_keyword(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import IntegerType
        result = dialect.parse_type("INTEGER ARRAY")
        assert isinstance(result, PostgresArrayType)
        assert result.dimensions == 1
        assert isinstance(result.element_type, IntegerType)

    def test_parse_array_varchar(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import VarCharType
        result = dialect.parse_type("VARCHAR(255)[]")
        assert isinstance(result, PostgresArrayType)
        assert result.dimensions == 1
        assert isinstance(result.element_type, VarCharType)
        assert result.element_type.length == 255

    def test_parse_non_array_passthrough(self, dialect):
        from rhosocial.activerecord.backend.expression.types import IntegerType
        result = dialect.parse_type("INTEGER")
        assert isinstance(result, IntegerType)

    def test_is_element_type_equivalent(self, dialect):
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
            PostgresArrayType,
        )
        from rhosocial.activerecord.backend.expression.types import (
            IntegerType, SmallIntType,
        )
        a1 = PostgresArrayType(IntegerType(), 2)
        a2 = PostgresArrayType(IntegerType(), 1)   # different dimension
        a3 = PostgresArrayType(SmallIntType())      # different element type
        # Same element, different dimension → equivalent
        assert a1.is_element_type_equivalent(a2)
        # Different element → not equivalent
        assert not a1.is_element_type_equivalent(a3)
        # Plain IntegerType (not array) as other → matches element
        assert a1.is_element_type_equivalent(IntegerType())
        # Plain SmallIntType as other → doesn't match Integer element
        assert not a1.is_element_type_equivalent(SmallIntType())
