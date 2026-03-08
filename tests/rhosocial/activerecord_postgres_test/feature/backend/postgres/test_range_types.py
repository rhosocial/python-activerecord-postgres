# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_range_types.py
"""
Unit tests for PostgreSQL range types.

Tests for:
- PostgresRange data class
- PostgresRangeAdapter conversion
- PostgresMultirange data class (PG14+)
- PostgresMultirangeAdapter conversion
"""
import pytest
from datetime import date, datetime
from decimal import Decimal

from rhosocial.activerecord.backend.impl.postgres.types.range import (
    PostgresRange,
    PostgresMultirange,
    _find_bound_separator,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.range import (
    PostgresRangeAdapter,
    PostgresMultirangeAdapter,
)


class TestPostgresRange:
    """Tests for PostgresRange data class."""

    def test_create_range_with_bounds(self):
        """Test creating a range with both bounds."""
        r = PostgresRange(1, 10)
        assert r.lower == 1
        assert r.upper == 10
        assert r.lower_inc is True
        assert r.upper_inc is False

    def test_create_range_custom_bounds(self):
        """Test creating a range with custom inclusive/exclusive bounds."""
        r = PostgresRange(1, 10, lower_inc=False, upper_inc=True)
        assert r.lower == 1
        assert r.upper == 10
        assert r.lower_inc is False
        assert r.upper_inc is True

    def test_create_unbounded_below(self):
        """Test creating a range with no lower bound."""
        r = PostgresRange(None, 10)
        assert r.lower is None
        assert r.upper == 10
        assert r.is_unbounded_below is True

    def test_create_unbounded_above(self):
        """Test creating a range with no upper bound."""
        r = PostgresRange(1, None)
        assert r.lower == 1
        assert r.upper is None
        assert r.is_unbounded_above is True

    def test_create_empty_range(self):
        """Test creating an empty range."""
        r = PostgresRange.empty()
        assert r.is_empty is True
        assert r.lower is None
        assert r.upper is None

    def test_range_equality(self):
        """Test range equality comparison."""
        r1 = PostgresRange(1, 10)
        r2 = PostgresRange(1, 10)
        r3 = PostgresRange(1, 10, lower_inc=False)

        assert r1 == r2
        assert r1 != r3

    def test_range_hash(self):
        """Test that ranges are hashable."""
        r1 = PostgresRange(1, 10)
        r2 = PostgresRange(1, 10)
        
        assert hash(r1) == hash(r2)
        assert len({r1, r2}) == 1

    def test_contains_integer(self):
        """Test containment check for integer values."""
        r = PostgresRange(1, 10)  # [1, 10)
        
        assert 1 in r
        assert 5 in r
        assert 9 in r
        assert 10 not in r
        assert 0 not in r

    def test_contains_inclusive_upper(self):
        """Test containment with inclusive upper bound."""
        r = PostgresRange(1, 10, lower_inc=True, upper_inc=True)  # [1, 10]
        
        assert 10 in r
        assert 11 not in r

    def test_contains_exclusive_lower(self):
        """Test containment with exclusive lower bound."""
        r = PostgresRange(1, 10, lower_inc=False)  # (1, 10)
        
        assert 1 not in r
        assert 2 in r

    def test_empty_range_contains_nothing(self):
        """Test that empty range contains no values."""
        r = PostgresRange.empty()
        assert 5 not in r
        assert None not in r


class TestPostgresRangeToString:
    """Tests for PostgresRange string conversion."""

    def test_to_postgres_string_basic(self):
        """Test basic range to string conversion."""
        r = PostgresRange(1, 10)
        assert r.to_postgres_string() == '[1,10)'

    def test_to_postgres_string_inclusive(self):
        """Test inclusive bounds to string."""
        r = PostgresRange(1, 10, lower_inc=True, upper_inc=True)
        assert r.to_postgres_string() == '[1,10]'

    def test_to_postgres_string_exclusive(self):
        """Test exclusive bounds to string."""
        r = PostgresRange(1, 10, lower_inc=False, upper_inc=False)
        assert r.to_postgres_string() == '(1,10)'

    def test_to_postgres_string_unbounded_below(self):
        """Test unbounded below range to string."""
        r = PostgresRange(None, 10)
        assert r.to_postgres_string() == '(,10)'

    def test_to_postgres_string_unbounded_above(self):
        """Test unbounded above range to string."""
        r = PostgresRange(1, None)
        assert r.to_postgres_string() == '[1,)'

    def test_to_postgres_string_empty(self):
        """Test empty range to string."""
        r = PostgresRange.empty()
        assert r.to_postgres_string() == 'empty'

    def test_to_postgres_string_date_range(self):
        """Test date range to string."""
        r = PostgresRange(date(2024, 1, 1), date(2024, 12, 31))
        assert '2024-01-01' in r.to_postgres_string()


class TestPostgresRangeFromString:
    """Tests for parsing PostgreSQL range strings."""

    def test_from_postgres_string_basic(self):
        """Test parsing basic range string."""
        r = PostgresRange.from_postgres_string('[1,10)')
        assert r.lower == '1'
        assert r.upper == '10'
        assert r.lower_inc is True
        assert r.upper_inc is False

    def test_from_postgres_string_inclusive(self):
        """Test parsing inclusive bounds."""
        r = PostgresRange.from_postgres_string('[1,10]')
        assert r.lower_inc is True
        assert r.upper_inc is True

    def test_from_postgres_string_exclusive(self):
        """Test parsing exclusive bounds."""
        r = PostgresRange.from_postgres_string('(1,10)')
        assert r.lower_inc is False
        assert r.upper_inc is False

    def test_from_postgres_string_unbounded(self):
        """Test parsing unbounded range."""
        r = PostgresRange.from_postgres_string('(,10)')
        assert r.lower is None
        assert r.upper == '10'

    def test_from_postgres_string_empty(self):
        """Test parsing empty range."""
        r = PostgresRange.from_postgres_string('empty')
        assert r.is_empty is True

    def test_from_postgres_string_empty_uppercase(self):
        """Test parsing EMPTY (uppercase)."""
        r = PostgresRange.from_postgres_string('EMPTY')
        assert r.is_empty is True

    def test_from_postgres_string_invalid(self):
        """Test that invalid strings raise ValueError."""
        with pytest.raises(ValueError):
            PostgresRange.from_postgres_string('invalid')

    def test_from_postgres_string_too_short(self):
        """Test that too-short strings raise ValueError."""
        with pytest.raises(ValueError):
            PostgresRange.from_postgres_string('[]')


class TestFindBoundSeparator:
    """Tests for the bound separator finder."""

    def test_simple_separator(self):
        """Test finding separator in simple range."""
        assert _find_bound_separator('1,10') == 1

    def test_no_separator(self):
        """Test when no separator exists."""
        assert _find_bound_separator('110') == -1

    def test_quoted_string(self):
        """Test with quoted strings."""
        assert _find_bound_separator("'a,b',10") == 5

    def test_nested_structure(self):
        """Test with nested structures."""
        assert _find_bound_separator('(1,2),10') == 5


class TestPostgresRangeAdapter:
    """Tests for PostgresRangeAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresRangeAdapter()
        supported = adapter.supported_types
        assert PostgresRange in supported

    def test_to_database_range(self):
        """Test converting PostgresRange to database."""
        adapter = PostgresRangeAdapter()
        r = PostgresRange(1, 10)
        result = adapter.to_database(r, str)
        assert result == '[1,10)'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresRangeAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_tuple_two_elements(self):
        """Test converting tuple with two elements."""
        adapter = PostgresRangeAdapter()
        result = adapter.to_database((1, 10), str)
        assert result == '[1,10)'

    def test_to_database_tuple_three_elements(self):
        """Test converting tuple with three elements."""
        adapter = PostgresRangeAdapter()
        
        result = adapter.to_database((1, 10, '[]'), str)
        assert result == '[1,10]'
        
        result = adapter.to_database((1, 10, '(]'), str)
        assert result == '(1,10]'

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresRangeAdapter()
        result = adapter.to_database('[1,10)', str)
        assert result == '[1,10)'

    def test_to_database_empty_string(self):
        """Test converting empty string."""
        adapter = PostgresRangeAdapter()
        result = adapter.to_database('empty', str)
        assert result == 'empty'

    def test_to_database_invalid_type(self):
        """Test that invalid types raise TypeError."""
        adapter = PostgresRangeAdapter()
        with pytest.raises(TypeError):
            adapter.to_database(123, str)

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresRangeAdapter()
        result = adapter.from_database(None, PostgresRange)
        assert result is None

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresRangeAdapter()
        result = adapter.from_database('[1,10)', PostgresRange)
        assert result.lower == '1'
        assert result.upper == '10'

    def test_from_database_range_object(self):
        """Test converting PostgresRange object."""
        adapter = PostgresRangeAdapter()
        r = PostgresRange(1, 10)
        result = adapter.from_database(r, PostgresRange)
        assert result == r

    def test_from_database_tuple(self):
        """Test converting tuple from database."""
        adapter = PostgresRangeAdapter()
        result = adapter.from_database((1, 10), PostgresRange)
        assert result.lower == 1
        assert result.upper == 10

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresRangeAdapter()
        values = [
            PostgresRange(1, 10),
            PostgresRange(5, 15),
            None,
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == '[1,10)'
        assert results[1] == '[5,15)'
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresRangeAdapter()
        values = ['[1,10)', '[5,15)', None]
        results = adapter.from_database_batch(values, PostgresRange)
        assert results[0].lower == '1'
        assert results[1].lower == '5'
        assert results[2] is None


class TestPostgresMultirange:
    """Tests for PostgresMultirange data class."""

    def test_create_multirange(self):
        """Test creating a multirange."""
        ranges = [
            PostgresRange(1, 5),
            PostgresRange(10, 15),
        ]
        mr = PostgresMultirange(ranges)
        assert len(mr.ranges) == 2

    def test_empty_multirange(self):
        """Test creating an empty multirange."""
        mr = PostgresMultirange.empty()
        assert mr.is_empty is True
        assert len(mr.ranges) == 0

    def test_multirange_equality(self):
        """Test multirange equality."""
        mr1 = PostgresMultirange([PostgresRange(1, 5)])
        mr2 = PostgresMultirange([PostgresRange(1, 5)])
        assert mr1 == mr2

    def test_to_postgres_string(self):
        """Test multirange to string conversion."""
        mr = PostgresMultirange([PostgresRange(1, 5), PostgresRange(10, 15)])
        result = mr.to_postgres_string()
        assert result == '{[1,5), [10,15)}'

    def test_empty_to_postgres_string(self):
        """Test empty multirange to string."""
        mr = PostgresMultirange.empty()
        assert mr.to_postgres_string() == '{}'

    def test_from_postgres_string(self):
        """Test parsing multirange string."""
        mr = PostgresMultirange.from_postgres_string('{[1,5), [10,15)}')
        assert len(mr.ranges) == 2
        assert mr.ranges[0].lower == '1'
        assert mr.ranges[1].lower == '10'

    def test_from_postgres_string_empty(self):
        """Test parsing empty multirange."""
        mr = PostgresMultirange.from_postgres_string('{}')
        assert mr.is_empty


class TestPostgresMultirangeAdapter:
    """Tests for PostgresMultirangeAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresMultirangeAdapter()
        supported = adapter.supported_types
        assert PostgresMultirange in supported

    def test_to_database_multirange(self):
        """Test converting PostgresMultirange to database."""
        adapter = PostgresMultirangeAdapter()
        mr = PostgresMultirange([PostgresRange(1, 5), PostgresRange(10, 15)])
        result = adapter.to_database(mr, str)
        assert result == '{[1,5), [10,15)}'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresMultirangeAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_list(self):
        """Test converting list of ranges."""
        adapter = PostgresMultirangeAdapter()
        ranges = [PostgresRange(1, 5), PostgresRange(10, 15)]
        result = adapter.to_database(ranges, str)
        assert result == '{[1,5), [10,15)}'

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresMultirangeAdapter()
        result = adapter.from_database(None, PostgresMultirange)
        assert result is None

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresMultirangeAdapter()
        result = adapter.from_database('{[1,5), [10,15)}', PostgresMultirange)
        assert len(result.ranges) == 2

    def test_from_database_multirange_object(self):
        """Test converting PostgresMultirange object."""
        adapter = PostgresMultirangeAdapter()
        mr = PostgresMultirange([PostgresRange(1, 5)])
        result = adapter.from_database(mr, PostgresMultirange)
        assert result == mr


class TestRangeTypeMappings:
    """Tests for range type mappings."""

    def test_range_types_defined(self):
        """Test that all range types are defined."""
        adapter = PostgresRangeAdapter()
        assert 'int4range' in adapter.RANGE_TYPES
        assert 'int8range' in adapter.RANGE_TYPES
        assert 'numrange' in adapter.RANGE_TYPES
        assert 'tsrange' in adapter.RANGE_TYPES
        assert 'tstzrange' in adapter.RANGE_TYPES
        assert 'daterange' in adapter.RANGE_TYPES

    def test_element_types_defined(self):
        """Test that element types are defined."""
        adapter = PostgresRangeAdapter()
        assert adapter.ELEMENT_TYPES['int4range'] == 'integer'
        assert adapter.ELEMENT_TYPES['daterange'] == 'date'

    def test_multirange_types_defined(self):
        """Test that all multirange types are defined."""
        adapter = PostgresMultirangeAdapter()
        assert 'int4multirange' in adapter.MULTIRANGE_TYPES
        assert 'int8multirange' in adapter.MULTIRANGE_TYPES
        assert 'nummultirange' in adapter.MULTIRANGE_TYPES
