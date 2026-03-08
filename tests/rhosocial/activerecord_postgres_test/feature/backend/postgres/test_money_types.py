# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_money_types.py
"""Unit tests for PostgreSQL MONEY type.

Tests for:
- PostgresMoney data class
- PostgresMoneyAdapter conversion
"""
import pytest
from decimal import Decimal

from rhosocial.activerecord.backend.impl.postgres.types.monetary import PostgresMoney
from rhosocial.activerecord.backend.impl.postgres.adapters.monetary import PostgresMoneyAdapter


class TestPostgresMoney:
    """Tests for PostgresMoney data class."""

    def test_create_from_decimal(self):
        """Test creating from Decimal."""
        m = PostgresMoney(Decimal('1234.56'))
        assert m.amount == Decimal('1234.56')

    def test_create_from_int(self):
        """Test creating from int."""
        m = PostgresMoney(100)
        assert m.amount == Decimal('100')

    def test_create_from_float(self):
        """Test creating from float."""
        m = PostgresMoney(99.99)
        assert m.amount == Decimal('99.99')

    def test_create_from_string(self):
        """Test creating from string."""
        m = PostgresMoney('1234.56')
        assert m.amount == Decimal('1234.56')

    def test_string_representation(self):
        """Test string representation."""
        m = PostgresMoney(Decimal('1234.56'))
        assert str(m) == '1234.56'

    def test_repr(self):
        """Test repr."""
        m = PostgresMoney(Decimal('1234.56'))
        assert '1234.56' in repr(m)

    def test_equality_money(self):
        """Test equality with PostgresMoney."""
        m1 = PostgresMoney(Decimal('100'))
        m2 = PostgresMoney(Decimal('100'))
        m3 = PostgresMoney(Decimal('200'))
        assert m1 == m2
        assert m1 != m3

    def test_equality_decimal(self):
        """Test equality with Decimal."""
        m = PostgresMoney(Decimal('100'))
        assert m == Decimal('100')

    def test_equality_numeric(self):
        """Test equality with int/float."""
        m = PostgresMoney(Decimal('100'))
        assert m == 100
        assert m == 100.0

    def test_less_than(self):
        """Test less than comparison."""
        m1 = PostgresMoney(Decimal('100'))
        m2 = PostgresMoney(Decimal('200'))
        assert m1 < m2
        assert m1 < 150

    def test_less_than_or_equal(self):
        """Test less than or equal comparison."""
        m1 = PostgresMoney(Decimal('100'))
        m2 = PostgresMoney(Decimal('100'))
        assert m1 <= m2
        assert m1 <= 100

    def test_greater_than(self):
        """Test greater than comparison."""
        m1 = PostgresMoney(Decimal('200'))
        m2 = PostgresMoney(Decimal('100'))
        assert m1 > m2
        assert m1 > 150

    def test_greater_than_or_equal(self):
        """Test greater than or equal comparison."""
        m1 = PostgresMoney(Decimal('100'))
        m2 = PostgresMoney(Decimal('100'))
        assert m1 >= m2
        assert m1 >= 100

    def test_addition(self):
        """Test addition."""
        m1 = PostgresMoney(Decimal('100'))
        m2 = PostgresMoney(Decimal('50'))
        result = m1 + m2
        assert result.amount == Decimal('150')
        assert isinstance(result, PostgresMoney)

    def test_addition_numeric(self):
        """Test addition with numeric."""
        m = PostgresMoney(Decimal('100'))
        result = m + 50
        assert result.amount == Decimal('150')

    def test_subtraction(self):
        """Test subtraction."""
        m1 = PostgresMoney(Decimal('100'))
        m2 = PostgresMoney(Decimal('30'))
        result = m1 - m2
        assert result.amount == Decimal('70')

    def test_multiplication(self):
        """Test multiplication with numeric."""
        m = PostgresMoney(Decimal('100'))
        result = m * 2
        assert result.amount == Decimal('200')
        assert isinstance(result, PostgresMoney)

    def test_division_by_numeric(self):
        """Test division by numeric."""
        m = PostgresMoney(Decimal('100'))
        result = m / 2
        assert result == Decimal('50')

    def test_division_by_money(self):
        """Test division by money (ratio)."""
        m1 = PostgresMoney(Decimal('200'))
        m2 = PostgresMoney(Decimal('100'))
        result = m1 / m2
        assert result == Decimal('2')

    def test_negation(self):
        """Test negation."""
        m = PostgresMoney(Decimal('100'))
        result = -m
        assert result.amount == Decimal('-100')

    def test_absolute_value(self):
        """Test absolute value."""
        m = PostgresMoney(Decimal('-100'))
        result = abs(m)
        assert result.amount == Decimal('100')

    def test_hash(self):
        """Test hashability."""
        m1 = PostgresMoney(Decimal('100'))
        m2 = PostgresMoney(Decimal('100'))
        assert hash(m1) == hash(m2)
        assert len({m1, m2}) == 1


class TestPostgresMoneyAdapter:
    """Tests for PostgresMoneyAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresMoneyAdapter()
        supported = adapter.supported_types
        assert PostgresMoney in supported
        # Note: Decimal is NOT registered to avoid conflict with base adapter

    def test_to_database_money(self):
        """Test converting PostgresMoney to database."""
        adapter = PostgresMoneyAdapter()
        m = PostgresMoney(Decimal('1234.56'))
        result = adapter.to_database(m, str)
        assert result == '1234.56'

    def test_to_database_decimal(self):
        """Test converting Decimal to database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.to_database(Decimal('1234.56'), str)
        assert result == '1234.56'

    def test_to_database_int(self):
        """Test converting int to database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.to_database(100, str)
        assert result == '100'

    def test_to_database_float(self):
        """Test converting float to database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.to_database(99.99, str)
        assert '99.99' in result

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.to_database('1234.56', str)
        assert result == '1234.56'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_string(self):
        """Test converting invalid string raises error."""
        adapter = PostgresMoneyAdapter()
        with pytest.raises(ValueError, match="Invalid money value"):
            adapter.to_database('not_a_number', str)

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresMoneyAdapter()
        with pytest.raises(TypeError):
            adapter.to_database([], str)

    def test_from_database_string_with_dollar(self):
        """Test converting dollar-formatted string from database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database('$1,234.56', PostgresMoney)
        assert result.amount == Decimal('1234.56')

    def test_from_database_string_with_euro(self):
        """Test converting euro-formatted string from database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database('€1.234,56', PostgresMoney)
        assert result.amount == Decimal('1234.56')

    def test_from_database_negative_string(self):
        """Test converting negative money string."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database('-$100.00', PostgresMoney)
        assert result.amount == Decimal('-100.00')

    def test_from_database_parentheses_negative(self):
        """Test converting parentheses-formatted negative."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database('($100.00)', PostgresMoney)
        assert result.amount == Decimal('-100.00')

    def test_from_database_decimal(self):
        """Test converting Decimal from database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database(Decimal('1234.56'), PostgresMoney)
        assert result.amount == Decimal('1234.56')

    def test_from_database_int(self):
        """Test converting int from database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database(100, PostgresMoney)
        assert result.amount == Decimal('100')

    def test_from_database_float(self):
        """Test converting float from database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database(99.99, PostgresMoney)
        assert result.amount == Decimal('99.99')

    def test_from_database_money(self):
        """Test converting PostgresMoney from database."""
        adapter = PostgresMoneyAdapter()
        m = PostgresMoney(Decimal('100'))
        result = adapter.from_database(m, PostgresMoney)
        assert result is m

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database(None, PostgresMoney)
        assert result is None

    def test_from_database_empty_string(self):
        """Test converting empty string from database."""
        adapter = PostgresMoneyAdapter()
        result = adapter.from_database('', PostgresMoney)
        assert result.amount == Decimal('0')

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database."""
        adapter = PostgresMoneyAdapter()
        with pytest.raises(TypeError):
            adapter.from_database([], PostgresMoney)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresMoneyAdapter()
        values = [
            PostgresMoney(Decimal('100')),
            Decimal('200'),
            None
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == '100'
        assert results[1] == '200'
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresMoneyAdapter()
        values = ['$100.00', '200', None]
        results = adapter.from_database_batch(values, PostgresMoney)
        assert results[0].amount == Decimal('100')
        assert results[1].amount == Decimal('200')
        assert results[2] is None

    def test_parse_plain_number(self):
        """Test parsing plain number string."""
        adapter = PostgresMoneyAdapter()
        result = adapter._parse_money_string('1234.56')
        assert result == Decimal('1234.56')

    def test_parse_with_commas(self):
        """Test parsing number with commas."""
        adapter = PostgresMoneyAdapter()
        result = adapter._parse_money_string('1,234.56')
        assert result == Decimal('1234.56')

    def test_parse_european_format(self):
        """Test parsing European format."""
        adapter = PostgresMoneyAdapter()
        result = adapter._parse_money_string('1.234,56')
        assert result == Decimal('1234.56')
