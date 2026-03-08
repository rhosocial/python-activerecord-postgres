# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_range_types_integration.py
"""
Integration tests for PostgreSQL range types with real database.

These tests require a live PostgreSQL connection and test:
- Range type storage and retrieval
- Range operators and functions
- Version-specific features (MULTIRANGE for PG14+)
"""
import pytest
import pytest_asyncio
from datetime import date, datetime
from decimal import Decimal

from rhosocial.activerecord.backend.impl.postgres.types.range import (
    PostgresRange,
    PostgresMultirange,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.range import PostgresRangeAdapter


class TestSyncRangeTypesIntegration:
    """Synchronous integration tests for range types."""

    @pytest.fixture
    def range_test_table(self, postgres_backend):
        """Create test table with range type columns."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_range_types")
        postgres_backend.execute("""
            CREATE TABLE test_range_types (
                id SERIAL PRIMARY KEY,
                int4_range INT4RANGE,
                int8_range INT8RANGE,
                num_range NUMRANGE,
                ts_range TSRANGE,
                tstz_range TSTZRANGE,
                date_range DATERANGE
            )
        """)
        yield "test_range_types"
        postgres_backend.execute("DROP TABLE IF EXISTS test_range_types")

    def test_int4range_insert_and_select(self, postgres_backend, range_test_table):
        """Test INT4RANGE insert and select."""
        postgres_backend.execute(
            f"INSERT INTO {range_test_table} (int4_range) VALUES (int4range(1, 100))"
        )
        result = postgres_backend.fetch_one(
            f"SELECT int4_range FROM {range_test_table} WHERE id = 1"
        )
        assert result is not None
        # Result should be a range string like '[1,100)'
        assert 'int4_range' in result

    def test_int4range_with_bounds(self, postgres_backend, range_test_table):
        """Test INT4RANGE with different bound types."""
        # Inclusive lower, exclusive upper (default)
        postgres_backend.execute(
            f"INSERT INTO {range_test_table} (int4_range) VALUES (int4range(1, 10, '[]'))"
        )
        result = postgres_backend.fetch_one(
            f"SELECT int4_range FROM {range_test_table}"
        )
        assert result is not None

    def test_range_null_handling(self, postgres_backend, range_test_table):
        """Test NULL range handling."""
        postgres_backend.execute(
            f"INSERT INTO {range_test_table} (int4_range) VALUES (NULL)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT int4_range FROM {range_test_table}"
        )
        assert result['int4_range'] is None

    def test_empty_range(self, postgres_backend, range_test_table):
        """Test empty range handling."""
        postgres_backend.execute(
            f"INSERT INTO {range_test_table} (int4_range) VALUES ('empty'::int4range)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT int4_range FROM {range_test_table}"
        )
        assert result is not None

    def test_daterange_insert_and_select(self, postgres_backend, range_test_table):
        """Test DATERANGE insert and select."""
        postgres_backend.execute(
            f"INSERT INTO {range_test_table} (date_range) VALUES (daterange('{date(2024, 1, 1)}', '{date(2024, 12, 31)}'))"
        )
        result = postgres_backend.fetch_one(
            f"SELECT date_range FROM {range_test_table}"
        )
        assert result is not None

    def test_numrange_with_decimal(self, postgres_backend, range_test_table):
        """Test NUMRANGE with decimal values."""
        postgres_backend.execute(
            f"INSERT INTO {range_test_table} (num_range) VALUES (numrange(1.5, 10.5))"
        )
        result = postgres_backend.fetch_one(
            f"SELECT num_range FROM {range_test_table}"
        )
        assert result is not None


class TestSyncRangeOperators:
    """Synchronous tests for range operators."""

    def test_contains_operator(self, postgres_backend):
        """Test @> contains operator."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) @> 5 AS contains
        """)
        assert result['contains'] is True

    def test_contains_false(self, postgres_backend):
        """Test @> operator returns false correctly."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) @> 15 AS contains
        """)
        assert result['contains'] is False

    def test_contained_by_operator(self, postgres_backend):
        """Test <@ contained by operator."""
        result = postgres_backend.fetch_one("""
            SELECT 5 <@ int4range(1, 10) AS contained
        """)
        assert result['contained'] is True

    def test_overlap_operator(self, postgres_backend):
        """Test && overlap operator."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) && int4range(5, 15) AS overlaps
        """)
        assert result['overlaps'] is True

    def test_overlap_false(self, postgres_backend):
        """Test && operator returns false correctly."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 5) && int4range(10, 15) AS overlaps
        """)
        assert result['overlaps'] is False

    def test_adjacent_operator(self, postgres_backend):
        """Test -|- adjacent operator."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) -|- int4range(10, 20) AS adjacent
        """)
        assert result['adjacent'] is True

    def test_union_operator(self, postgres_backend):
        """Test + union operator."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 5) + int4range(3, 10) AS union_range
        """)
        assert result is not None

    def test_intersection_operator(self, postgres_backend):
        """Test * intersection operator."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) * int4range(5, 15) AS intersection
        """)
        assert result is not None

    def test_difference_operator(self, postgres_backend):
        """Test - difference operator."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) - int4range(5, 15) AS difference
        """)
        assert result is not None


class TestSyncRangeFunctions:
    """Synchronous tests for range functions."""

    def test_lower_function(self, postgres_backend):
        """Test lower() function."""
        result = postgres_backend.fetch_one("""
            SELECT lower(int4range(1, 10)) AS lower_val
        """)
        assert result['lower_val'] == 1

    def test_upper_function(self, postgres_backend):
        """Test upper() function."""
        result = postgres_backend.fetch_one("""
            SELECT upper(int4range(1, 10)) AS upper_val
        """)
        assert result['upper_val'] == 10

    def test_isempty_function_true(self, postgres_backend):
        """Test isempty() function for empty range."""
        result = postgres_backend.fetch_one("""
            SELECT isempty('empty'::int4range) AS is_empty
        """)
        assert result['is_empty'] is True

    def test_isempty_function_false(self, postgres_backend):
        """Test isempty() function for non-empty range."""
        result = postgres_backend.fetch_one("""
            SELECT isempty(int4range(1, 10)) AS is_empty
        """)
        assert result['is_empty'] is False

    def test_lower_inc_function(self, postgres_backend):
        """Test lower_inc() function."""
        result = postgres_backend.fetch_one("""
            SELECT lower_inc(int4range(1, 10)) AS lower_inclusive
        """)
        assert result['lower_inclusive'] is True

    def test_upper_inc_function(self, postgres_backend):
        """Test upper_inc() function."""
        result = postgres_backend.fetch_one("""
            SELECT upper_inc(int4range(1, 10)) AS upper_inclusive
        """)
        assert result['upper_inclusive'] is False

    def test_range_merge_function(self, postgres_backend):
        """Test range_merge() function."""
        result = postgres_backend.fetch_one("""
            SELECT range_merge(int4range(1, 5), int4range(10, 15)) AS merged
        """)
        assert result is not None


class TestAsyncRangeTypesIntegration:
    """Asynchronous integration tests for range types."""

    @pytest_asyncio.fixture
    async def async_range_test_table(self, async_postgres_backend):
        """Create test table with range type columns."""
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_range_types_async")
        await async_postgres_backend.execute("""
            CREATE TABLE test_range_types_async (
                id SERIAL PRIMARY KEY,
                int4_range INT4RANGE,
                date_range DATERANGE
            )
        """)
        yield "test_range_types_async"
        await async_postgres_backend.execute("DROP TABLE IF EXISTS test_range_types_async")

    @pytest.mark.asyncio
    async def test_async_int4range_insert(
        self, async_postgres_backend, async_range_test_table
    ):
        """Test INT4RANGE insert and select (async)."""
        await async_postgres_backend.execute(
            f"INSERT INTO {async_range_test_table} (int4_range) VALUES (int4range(1, 100))"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT int4_range FROM {async_range_test_table}"
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_async_range_null(
        self, async_postgres_backend, async_range_test_table
    ):
        """Test NULL range handling (async)."""
        await async_postgres_backend.execute(
            f"INSERT INTO {async_range_test_table} (int4_range) VALUES (NULL)"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT int4_range FROM {async_range_test_table}"
        )
        assert result['int4_range'] is None

    @pytest.mark.asyncio
    async def test_async_contains_operator(self, async_postgres_backend):
        """Test @> contains operator (async)."""
        result = await async_postgres_backend.fetch_one("""
            SELECT int4range(1, 10) @> 5 AS contains
        """)
        assert result['contains'] is True


class TestMultirangeIntegration:
    """Integration tests for multirange types (PostgreSQL 14+)."""

    @pytest.fixture
    def multirange_test_table(self, postgres_backend):
        """Create test table with multirange columns (PG14+ only)."""
        # Skip fixture setup if multirange not supported
        if not postgres_backend.dialect.supports_multirange():
            pytest.skip("MULTIRANGE requires PostgreSQL 14+")
        
        postgres_backend.execute("DROP TABLE IF EXISTS test_multirange")
        postgres_backend.execute("""
            CREATE TABLE test_multirange (
                id SERIAL PRIMARY KEY,
                int4_multirange INT4MULTIRANGE
            )
        """)
        yield "test_multirange"
        postgres_backend.execute("DROP TABLE IF EXISTS test_multirange")

    def test_multirange_insert(self, postgres_backend, multirange_test_table):
        """Test multirange insert."""
        postgres_backend.execute(
            f"INSERT INTO {multirange_test_table} (int4_multirange) "
            f"VALUES ('{{[1,5], [10,15]}}'::int4multirange)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT int4_multirange FROM {multirange_test_table}"
        )
        assert result is not None

    def test_multirange_contains(self, postgres_backend, multirange_test_table):
        """Test multirange contains operator."""
        postgres_backend.execute(
            f"INSERT INTO {multirange_test_table} (int4_multirange) "
            f"VALUES ('{{[1,5], [10,15]}}'::int4multirange)"
        )
        result = postgres_backend.fetch_one("""
            SELECT int4_multirange @> 3 AS contains
            FROM test_multirange
        """)
        assert result['contains'] is True

    def test_multirange_constructor(self, postgres_backend):
        """Test multirange constructor function."""
        # Skip test if multirange not supported
        if not postgres_backend.dialect.supports_multirange_constructor():
            pytest.skip("Multirange constructor requires PostgreSQL 14+")
        
        result = postgres_backend.fetch_one("""
            SELECT int4multirange(int4range(1, 5), int4range(10, 15)) AS mr
        """)
        assert result is not None


class TestRangeVersionCompatibility:
    """Version compatibility tests for range types."""

    def test_basic_range_all_versions(self, postgres_backend):
        """Test basic range types work on all supported versions."""
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) AS basic_range
        """)
        assert result is not None

    def test_multirange_version_check(self, postgres_backend):
        """Test multirange version detection via protocol."""
        dialect = postgres_backend.dialect
        
        # Check version-based support
        version = postgres_backend.get_server_version()
        if version >= (14, 0, 0):
            assert dialect.supports_multirange() is True
            assert dialect.supports_multirange_constructor() is True
        else:
            assert dialect.supports_multirange() is False
            assert dialect.supports_multirange_constructor() is False

    def test_range_types_available(self, postgres_backend):
        """Test that numeric range types are available."""
        # Test int4range (works with integers)
        result = postgres_backend.fetch_one("""
            SELECT int4range(1, 10) AS r
        """)
        assert result is not None

        # Test int8range (works with bigints)
        result = postgres_backend.fetch_one("""
            SELECT int8range(1, 10) AS r
        """)
        assert result is not None

        # Test numrange (works with numerics)
        result = postgres_backend.fetch_one("""
            SELECT numrange(1.5, 10.5) AS r
        """)
        assert result is not None

    def test_daterange_with_dates(self, postgres_backend):
        """Test daterange with proper date values."""
        result = postgres_backend.fetch_one("""
            SELECT daterange('2024-01-01'::date, '2024-12-31'::date) AS r
        """)
        assert result is not None
