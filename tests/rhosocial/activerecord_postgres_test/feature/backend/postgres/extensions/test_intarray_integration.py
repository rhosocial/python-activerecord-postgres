# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_intarray_integration.py
"""
Integration tests for PostgreSQL intarray extension with real database.

These tests require a live PostgreSQL connection with intarray extension installed
and test:
- @> (contains) operator
- && (overlap) operator
- <@ (contained by) operator
- idx() function for array element access
"""
import pytest


class TestIntarrayIntegration:
    """Integration tests for intarray extension."""

    def test_intarray_contains_operator(self, postgres_backend_single):
        """Test @> (contains) operator for integer arrays."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("intarray"):
            pytest.skip("Extension 'intarray' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_intarray")
            backend.execute("""
                CREATE TABLE test_intarray (
                    id SERIAL PRIMARY KEY,
                    tags INTEGER[]
                )
            """)
            backend.execute("""
                INSERT INTO test_intarray (tags) VALUES
                    (ARRAY[1, 2, 3, 4, 5]),
                    (ARRAY[10, 20, 30]),
                    (ARRAY[3, 4])
            """)

            # Test @> : does tags contain ARRAY[3, 4]?
            result = backend.fetch_all("""
                SELECT id FROM test_intarray WHERE tags @> ARRAY[3, 4]
            """)
            ids = [r['id'] for r in result]
            assert 1 in ids  # [1,2,3,4,5] contains [3,4]
            assert 3 in ids  # [3,4] contains [3,4]

            # ARRAY[3, 4] should NOT be contained by [10, 20, 30]
            assert 2 not in ids
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_intarray")
            except Exception:
                pass

    def test_intarray_overlap_operator(self, postgres_backend_single):
        """Test && (overlap) operator for integer arrays."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("intarray"):
            pytest.skip("Extension 'intarray' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_intarray_overlap")
            backend.execute("""
                CREATE TABLE test_intarray_overlap (
                    id SERIAL PRIMARY KEY,
                    tags INTEGER[]
                )
            """)
            backend.execute("""
                INSERT INTO test_intarray_overlap (tags) VALUES
                    (ARRAY[1, 2, 3]),
                    (ARRAY[4, 5, 6]),
                    (ARRAY[3, 4])
            """)

            # Test && : arrays that overlap with ARRAY[3, 4]
            result = backend.fetch_all("""
                SELECT id FROM test_intarray_overlap WHERE tags && ARRAY[3, 4]
            """)
            ids = [r['id'] for r in result]
            assert 1 in ids  # [1,2,3] overlaps with [3,4]
            assert 2 in ids  # [4,5,6] overlaps with [3,4]
            assert 3 in ids  # [3,4] overlaps with [3,4]
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_intarray_overlap")
            except Exception:
                pass

    def test_intarray_contained_by_operator(self, postgres_backend_single):
        """Test <@ (contained by) operator for integer arrays."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("intarray"):
            pytest.skip("Extension 'intarray' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_intarray_contained")
            backend.execute("""
                CREATE TABLE test_intarray_contained (
                    id SERIAL PRIMARY KEY,
                    tags INTEGER[]
                )
            """)
            backend.execute("""
                INSERT INTO test_intarray_contained (tags) VALUES
                    (ARRAY[1, 2]),
                    (ARRAY[1, 2, 3, 4, 5]),
                    (ARRAY[10, 20])
            """)

            # Test <@ : which arrays are contained by ARRAY[1, 2, 3, 4, 5]?
            result = backend.fetch_all("""
                SELECT id FROM test_intarray_contained
                WHERE tags <@ ARRAY[1, 2, 3, 4, 5]
            """)
            ids = [r['id'] for r in result]
            assert 1 in ids  # [1,2] is contained by [1,2,3,4,5]
            assert 2 in ids  # [1,2,3,4,5] is contained by itself
            assert 3 not in ids  # [10,20] is NOT contained by [1,2,3,4,5]
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_intarray_contained")
            except Exception:
                pass

    def test_intarray_idx_function(self, postgres_backend_single):
        """Test idx() function for finding element position in array."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("intarray"):
            pytest.skip("Extension 'intarray' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_intarray_idx")
            backend.execute("""
                CREATE TABLE test_intarray_idx (
                    id SERIAL PRIMARY KEY,
                    tags INTEGER[]
                )
            """)
            backend.execute("""
                INSERT INTO test_intarray_idx (tags) VALUES (ARRAY[10, 20, 30, 40])
            """)

            # idx() returns the position of an element (1-based)
            result = backend.fetch_one("""
                SELECT idx(tags, 20) AS pos FROM test_intarray_idx WHERE id = 1
            """)
            assert result['pos'] == 2

            # Element not found returns 0
            result = backend.fetch_one("""
                SELECT idx(tags, 99) AS pos FROM test_intarray_idx WHERE id = 1
            """)
            assert result['pos'] == 0
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_intarray_idx")
            except Exception:
                pass

    def test_intarray_gin_index(self, postgres_backend_single):
        """Test creating GIN index on integer array with intarray operator class."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("intarray"):
            pytest.skip("Extension 'intarray' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_intarray_gin")
            backend.execute("DROP INDEX IF EXISTS idx_intarray_tags")
            backend.execute("""
                CREATE TABLE test_intarray_gin (
                    id SERIAL PRIMARY KEY,
                    tags INTEGER[]
                )
            """)

            # Create GIN index with intarray operator class
            backend.execute("""
                CREATE INDEX idx_intarray_tags ON test_intarray_gin USING gin (tags gin__int_ops)
            """)

            # Verify index exists
            result = backend.fetch_one("""
                SELECT COUNT(*) AS cnt
                FROM pg_indexes
                WHERE tablename = 'test_intarray_gin'
                  AND indexname = 'idx_intarray_tags'
            """)
            assert result['cnt'] >= 1

            # Insert data and query using the index
            backend.execute("""
                INSERT INTO test_intarray_gin (tags) VALUES (ARRAY[1, 5, 10])
            """)

            result = backend.fetch_all("""
                SELECT id FROM test_intarray_gin WHERE tags @> ARRAY[5]
            """)
            assert len(result) >= 1
        finally:
            try:
                backend.execute("DROP INDEX IF EXISTS idx_intarray_tags")
                backend.execute("DROP TABLE IF EXISTS test_intarray_gin")
            except Exception:
                pass
