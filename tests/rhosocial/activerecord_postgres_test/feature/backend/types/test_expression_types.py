# tests/rhosocial/activerecord_postgres_test/feature/backend/types/test_expression_types.py
"""Tests for PostgreSQL-specific DataType subclasses (pure, no DB).

Covers the ``synonyms()`` registrations, equality/hash semantics and
array-type equivalence used by introspection and schema comparison.
"""

from rhosocial.activerecord.backend.expression.types import (
    IntegerType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresArrayType,
    PostgresBigSerialType,
    PostgresBitType,
    PostgresByteaType,
    PostgresCharacterVaryingType,
    PostgresSerialType,
    PostgresSmallSerialType,
    PostgresVarBitType,
    PostgresVectorType,
)


class TestPostgresTypeSynonyms:
    def test_character_varying_synonyms(self):
        assert PostgresCharacterVaryingType.synonyms() == {"VarCharType"}

    def test_bytea_synonyms(self):
        assert PostgresByteaType.synonyms() == {"BlobType"}

    def test_small_serial_synonyms(self):
        assert PostgresSmallSerialType.synonyms() == {"SmallSerialType", "SmallIntType"}

    def test_serial_synonyms(self):
        assert PostgresSerialType.synonyms() == {"SerialType", "IntegerType"}

    def test_big_serial_synonyms(self):
        assert PostgresBigSerialType.synonyms() == {"BigSerialType", "BigIntType"}


class TestPostgresBitTypeEquality:
    def test_equal(self):
        assert PostgresBitType(8) == PostgresBitType(8)

    def test_not_equal_values(self):
        assert PostgresBitType(8) != PostgresBitType(16)

    def test_not_equal_types(self):
        assert PostgresBitType(8) != PostgresVarBitType(8)
        assert PostgresBitType(8) != "not a type"

    def test_none_vs_value(self):
        assert PostgresBitType(None) != PostgresBitType(8)

    def test_hash(self):
        assert hash(PostgresBitType(8)) == hash((type(PostgresBitType(8)), 8))


class TestPostgresVarBitTypeEquality:
    def test_equal(self):
        assert PostgresVarBitType(16) == PostgresVarBitType(16)

    def test_not_equal_values(self):
        assert PostgresVarBitType(16) != PostgresVarBitType(32)

    def test_not_equal_types(self):
        assert PostgresVarBitType(16) != PostgresBitType(16)
        assert PostgresVarBitType(16) != object()

    def test_hash(self):
        assert hash(PostgresVarBitType(16)) == hash((type(PostgresVarBitType(16)), 16))


class TestPostgresVectorTypeEquality:
    def test_equal(self):
        assert PostgresVectorType(384) == PostgresVectorType(384)

    def test_not_equal_values(self):
        assert PostgresVectorType(384) != PostgresVectorType(768)

    def test_not_equal_types(self):
        assert PostgresVectorType(384) != PostgresBitType(8)
        assert PostgresVectorType(384) != "vector"

    def test_hash(self):
        assert hash(PostgresVectorType(384)) == hash((type(PostgresVectorType(384)), 384))


class TestPostgresArrayType:
    def test_is_equivalent_matching_element(self):
        arr1 = PostgresArrayType(IntegerType(), dimensions=1)
        arr2 = PostgresArrayType(IntegerType(), dimensions=3)
        assert arr1.is_equivalent(arr2)

    def test_is_equivalent_non_array(self):
        arr = PostgresArrayType(IntegerType(), dimensions=1)
        assert arr.is_equivalent(IntegerType()) is False
        assert arr.is_equivalent(None) is False

    def test_is_equivalent_different_element(self):
        arr1 = PostgresArrayType(IntegerType(), dimensions=1)
        arr2 = PostgresArrayType(VarCharType(), dimensions=1)
        assert arr1.is_equivalent(arr2) is False
