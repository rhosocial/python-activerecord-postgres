# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_pg_trgm_integration.py
"""
Integration tests for PostgreSQL pg_trgm extension with real database.

These tests require a live PostgreSQL connection with pg_trgm extension installed
and test:
- similarity function and % operator
- GIN index creation for trigram matching
- show_trgm function
"""
import pytest


class TestPgTrgmIntegration:
    """Integration tests for pg_trgm extension."""

    def test_trgm_similarity(self, postgres_backend_single):
        """Test similarity function with text data."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pg_trgm"):
            pytest.skip("Extension 'pg_trgm' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_trgm")
            backend.execute("""
                CREATE TABLE test_trgm (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                )
            """)
            backend.execute("""
                INSERT INTO test_trgm (name) VALUES
                    ('hello world'),
                    ('hello earth'),
                    ('hi there'),
                    ('completely different')
            """)

            # Test similarity function
            result = backend.fetch_one("""
                SELECT similarity('hello', name) AS sim
                FROM test_trgm WHERE id = 1
            """)
            assert result['sim'] is not None
            assert result['sim'] > 0.0

            # "hello world" should be more similar to "hello" than "completely different"
            result_world = backend.fetch_one("""
                SELECT similarity('hello', name) AS sim
                FROM test_trgm WHERE id = 1
            """)
            result_different = backend.fetch_one("""
                SELECT similarity('hello', name) AS sim
                FROM test_trgm WHERE id = 4
            """)
            assert result_world['sim'] > result_different['sim']
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_trgm")
            except Exception:
                pass

    def test_trgm_similarity_operator(self, postgres_backend_single):
        """Test % similarity operator for text matching."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pg_trgm"):
            pytest.skip("Extension 'pg_trgm' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_trgm_ops")
            backend.execute("""
                CREATE TABLE test_trgm_ops (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                )
            """)
            backend.execute("""
                INSERT INTO test_trgm_ops (name) VALUES
                    ('PostgreSQL database'),
                    ('MySQL database'),
                    ('completely unrelated')
            """)

            # Test % operator: rows similar to 'PostgreSQL'
            result = backend.fetch_all("""
                SELECT name, similarity('PostgreSQL', name) AS sim
                FROM test_trgm_ops
                WHERE name % 'PostgreSQL'
                ORDER BY sim DESC
            """)
            assert result is not None
            assert len(result) >= 1
            # PostgreSQL database should be in the results
            names = [r['name'] for r in result]
            assert 'PostgreSQL database' in names
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_trgm_ops")
            except Exception:
                pass

    def test_trgm_index(self, postgres_backend_single):
        """Test creating GIN index with pg_trgm operator class."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pg_trgm"):
            pytest.skip("Extension 'pg_trgm' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_trgm_idx")
            backend.execute("DROP INDEX IF EXISTS idx_trgm_name")
            backend.execute("""
                CREATE TABLE test_trgm_idx (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                )
            """)

            # Create GIN index with trgm operator class
            backend.execute("""
                CREATE INDEX idx_trgm_name ON test_trgm_idx USING gin (name gin_trgm_ops)
            """)

            # Verify index exists
            result = backend.fetch_one("""
                SELECT COUNT(*) AS cnt
                FROM pg_indexes
                WHERE tablename = 'test_trgm_idx'
                  AND indexname = 'idx_trgm_name'
            """)
            assert result['cnt'] >= 1

            # Insert data and query using the index
            backend.execute("""
                INSERT INTO test_trgm_idx (name) VALUES ('application development')
            """)

            result = backend.fetch_all("""
                SELECT name FROM test_trgm_idx WHERE name % 'application'
            """)
            assert len(result) >= 1
        finally:
            try:
                backend.execute("DROP INDEX IF EXISTS idx_trgm_name")
                backend.execute("DROP TABLE IF EXISTS test_trgm_idx")
            except Exception:
                pass

    def test_trgm_show_trgm(self, postgres_backend_single):
        """Test show_trgm function to extract trigrams."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pg_trgm"):
            pytest.skip("Extension 'pg_trgm' not installed")

        result = backend.fetch_one("""
            SELECT show_trgm('hello') AS trigrams
        """)
        assert result is not None
        assert result['trigrams'] is not None
        # show_trgm returns an array of trigrams
        assert len(result['trigrams']) > 0

    def test_trgm_word_similarity(self, postgres_backend_single):
        """Test word_similarity function (PG 11+)."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("pg_trgm"):
            pytest.skip("Extension 'pg_trgm' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_trgm_word")
            backend.execute("""
                CREATE TABLE test_trgm_word (
                    id SERIAL PRIMARY KEY,
                    description TEXT
                )
            """)
            backend.execute("""
                INSERT INTO test_trgm_word (description) VALUES
                    ('The quick brown fox jumps over the lazy dog')
            """)

            # Test word_similarity function
            result = backend.fetch_one("""
                SELECT word_similarity('fox', description) AS ws
                FROM test_trgm_word WHERE id = 1
            """)
            assert result is not None
            assert result['ws'] is not None
            assert result['ws'] > 0.0
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_trgm_word")
            except Exception:
                pass
