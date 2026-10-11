# tests/rhosocial/activerecord_postgres_test/feature/backend/types/test_expression_types.py
"""Tests for PostgreSQL-specific DataType subclasses (pure, no DB).

Covers the base classes each PostgreSQL type is anchored to, equality and
hashing, and the array-type comparison that introspection and schema
comparison rely on.
"""

import pytest

from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BigIntType,
    BlobType,
    DecimalType,
    IntegerType,
    SmallIntType,
    TextType,
    UUIDType,
    VarCharType,
    XmlType,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresArrayType,
    PostgresBigSerialType,
    PostgresBitType,
    PostgresByteaType,
    PostgresCitextType,
    PostgresHalfvecType,
    PostgresMoneyType,
    PostgresSerialType,
    PostgresSmallSerialType,
    PostgresSparsevecType,
    PostgresUUIDType,
    PostgresVarBitType,
    PostgresVectorType,
    PostgresXMLType,
)


class TestPostgresTypesAreAnchoredToCoreConcepts:
    """A backend type that says "I am this concept" must be that concept.

    Without this, ``isinstance(col.data_type, IntegerType)`` is false for a
    SERIAL column and every caller has to know the backend's private type list.
    """

    @pytest.mark.parametrize("cls,base", [
        (PostgresSmallSerialType, SmallIntType),
        (PostgresSerialType, IntegerType),
        (PostgresBigSerialType, BigIntType),
        (PostgresByteaType, BlobType),
        (PostgresUUIDType, UUIDType),
        (PostgresXMLType, XmlType),
        (PostgresCitextType, TextType),
        (PostgresArrayType, ArrayType),
    ])
    def test_derives_from(self, cls, base):
        assert issubclass(cls, base)

    def test_money_is_not_a_decimal(self):
        """``money`` changes the *result type* of division, stores a scale the
        schema does not record, and prints locale-dependently. Any one of those
        makes it a different concept from ``NUMERIC``."""
        assert not issubclass(PostgresMoneyType, DecimalType)


class TestPostgresSerialWidths:
    """SERIAL is an integer of a given width plus a sequence default."""

    @pytest.mark.parametrize("cls,base", [
        (PostgresSmallSerialType, SmallIntType),
        (PostgresSerialType, IntegerType),
        (PostgresBigSerialType, BigIntType),
    ])
    def test_is_a_width_of_integer(self, cls, base):
        assert isinstance(cls(), base)

    def test_widths_still_distinguish_them(self):
        assert PostgresSmallSerialType() != PostgresSerialType()
        assert PostgresSerialType() != PostgresBigSerialType()


class TestDialectFirstConstructor:
    """``dialect`` is the first positional parameter of every DataType.

    A signature that puts ``n``/``dim`` first does not fail loudly when a
    caller follows the documented convention — the dialect object is silently
    stored as the bit count or dimension count, and the error only appears when
    the column is rendered.
    """

    def test_bit_type_takes_dialect_first(self):
        t = PostgresBitType(PostgresDialect(), 8)
        assert t.n == 8
        assert t.dialect is not None

    def test_varbit_type_takes_dialect_first(self):
        t = PostgresVarBitType(PostgresDialect(), 16)
        assert t.n == 16
        assert t.dialect is not None

    @pytest.mark.parametrize("cls", [
        PostgresVectorType, PostgresHalfvecType, PostgresSparsevecType,
    ])
    def test_vector_types_take_dialect_first(self, cls):
        t = cls(PostgresDialect(), 384)
        assert t.dim == 384
        assert t.dialect is not None

    @pytest.mark.parametrize("cls", [
        PostgresVectorType, PostgresHalfvecType, PostgresSparsevecType,
        PostgresBitType, PostgresVarBitType,
    ])
    def test_dialect_first_renders(self, cls):
        """The end of the silent-misplacement chain: what it renders must
        contain the number, not the dialect's repr."""
        sql, _ = cls(PostgresDialect(), 8).to_sql()
        assert "8" in sql


class TestPostgresBitTypeEquality:
    def test_equal(self):
        assert PostgresBitType(None, 8) == PostgresBitType(None, 8)

    def test_not_equal_values(self):
        assert PostgresBitType(None, 8) != PostgresBitType(None, 16)

    def test_not_equal_types(self):
        assert PostgresBitType(None, 8) != PostgresVarBitType(None, 8)
        assert PostgresBitType(None, 8) != "not a type"

    def test_none_vs_value(self):
        assert PostgresBitType(None) != PostgresBitType(None, 8)

    def test_hash(self):
        assert hash(PostgresBitType(None, 8)) == hash(PostgresBitType(None, 8))


class TestPostgresVarBitTypeEquality:
    def test_equal(self):
        assert PostgresVarBitType(None, 16) == PostgresVarBitType(None, 16)

    def test_not_equal_values(self):
        assert PostgresVarBitType(None, 16) != PostgresVarBitType(None, 32)

    def test_not_equal_types(self):
        assert PostgresVarBitType(None, 16) != PostgresBitType(None, 16)
        assert PostgresVarBitType(None, 16) != object()

    def test_hash(self):
        assert hash(PostgresVarBitType(None, 16)) == hash(PostgresVarBitType(None, 16))


class TestPostgresVectorTypeEquality:
    def test_equal(self):
        assert PostgresVectorType(None, 384) == PostgresVectorType(None, 384)

    def test_not_equal_values(self):
        assert PostgresVectorType(None, 384) != PostgresVectorType(None, 768)

    def test_not_equal_types(self):
        assert PostgresVectorType(None, 384) != PostgresBitType(None, 8)
        assert PostgresVectorType(None, 384) != "vector"

    def test_hash(self):
        assert hash(PostgresVectorType(None, 384)) == hash(PostgresVectorType(None, 384))


class TestPostgresArrayType:
    """Dimensions describe the declaration; PostgreSQL stores one dimension."""

    def test_element_type_comparison_ignores_dimensions(self):
        arr1 = PostgresArrayType(element_type=IntegerType(), dimensions=1)
        arr2 = PostgresArrayType(element_type=IntegerType(), dimensions=3)
        assert arr1.is_element_type_equivalent(arr2)

    def test_element_type_comparison_accepts_a_bare_element_type(self):
        """``*other*`` need not be an ArrayType — the question "does this array
        store the same kind of thing" is answerable against a bare
        ``IntegerType()``, which is what makes the call useful at a call site
        that has just an element type in hand."""
        arr = PostgresArrayType(element_type=IntegerType(), dimensions=1)
        assert arr.is_element_type_equivalent(IntegerType())
        assert not arr.is_element_type_equivalent(VarCharType())
        assert not arr.is_element_type_equivalent(None)

    def test_element_type_comparison_rejects_a_different_element(self):
        arr1 = PostgresArrayType(element_type=IntegerType(), dimensions=1)
        arr2 = PostgresArrayType(element_type=VarCharType(), dimensions=1)
        assert not arr1.is_element_type_equivalent(arr2)

    def test_dimensions_are_still_part_of_identity(self):
        """``==`` asks "same declaration"; the differ uses it, so a dimension
        difference is reported rather than quietly accepted."""
        arr1 = PostgresArrayType(element_type=IntegerType(), dimensions=1)
        arr3 = PostgresArrayType(element_type=IntegerType(), dimensions=3)
        assert arr1 != arr3
