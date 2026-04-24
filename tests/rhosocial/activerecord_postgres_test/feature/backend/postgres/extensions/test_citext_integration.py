# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_citext_integration.py
"""
Integration tests for PostgreSQL citext extension with real database.

These tests require a live PostgreSQL connection with citext extension installed
and test:
- Case-insensitive text comparison
- Unique constraints on citext columns
- citext column insertion and retrieval
"""
import pytest


class TestCitextIntegration:
    """Integration tests for citext extension."""

    def test_citext_comparison(self, postgres_backend_single):
        """Test that citext columns perform case-insensitive comparison."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("citext"):
            pytest.skip("Extension 'citext' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_citext")
            backend.execute("""
                CREATE TABLE test_citext (
                    id SERIAL PRIMARY KEY,
                    name CITEXT
                )
            """)

            backend.execute("""
                INSERT INTO test_citext (name) VALUES ('Hello World')
            """)

            # Case-insensitive WHERE clause
            result = backend.fetch_one("""
                SELECT name FROM test_citext WHERE name = 'hello world'
            """)
            assert result is not None
            assert result['name'] is not None

            # Also test with different casing
            result = backend.fetch_one("""
                SELECT name FROM test_citext WHERE name = 'HELLO WORLD'
            """)
            assert result is not None

            # Regular text column would not match
            result = backend.fetch_one("""
                SELECT name FROM test_citext WHERE name::text = 'hello world'
            """)
            # After casting to text, the comparison is case-sensitive
            assert result is None or result.get('name') is None
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_citext")
            except Exception:
                pass

    def test_citext_unique_constraint(self, postgres_backend_single):
        """Test that citext columns enforce unique constraints case-insensitively."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("citext"):
            pytest.skip("Extension 'citext' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_citext_unique")
            backend.execute("""
                CREATE TABLE test_citext_unique (
                    id SERIAL PRIMARY KEY,
                    email CITEXT UNIQUE
                )
            """)

            # Insert first record
            backend.execute("""
                INSERT INTO test_citext_unique (email) VALUES ('User@Example.COM')
            """)

            # Insert with different casing should violate unique constraint
            with pytest.raises(Exception) as exc_info:
                backend.execute("""
                    INSERT INTO test_citext_unique (email) VALUES ('user@example.com')
                """)
            # Should be a unique violation error
            assert exc_info.value is not None
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_citext_unique")
            except Exception:
                pass

    def test_citext_like_query(self, postgres_backend_single):
        """Test LIKE queries on citext columns are case-insensitive."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("citext"):
            pytest.skip("Extension 'citext' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_citext_like")
            backend.execute("""
                CREATE TABLE test_citext_like (
                    id SERIAL PRIMARY KEY,
                    name CITEXT
                )
            """)
            backend.execute("""
                INSERT INTO test_citext_like (name) VALUES
                    ('Alice Johnson'),
                    ('Bob Smith')
            """)

            # LIKE on citext is case-insensitive
            result = backend.fetch_all("""
                SELECT name FROM test_citext_like WHERE name LIKE 'alice%'
            """)
            assert len(result) >= 1
            assert result[0]['name'] is not None
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_citext_like")
            except Exception:
                pass

    def test_citext_join(self, postgres_backend_single):
        """Test that citext columns join case-insensitively."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("citext"):
            pytest.skip("Extension 'citext' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_citext_users")
            backend.execute("DROP TABLE IF EXISTS test_citext_roles")
            backend.execute("""
                CREATE TABLE test_citext_users (
                    id SERIAL PRIMARY KEY,
                    username CITEXT
                )
            """)
            backend.execute("""
                CREATE TABLE test_citext_roles (
                    id SERIAL PRIMARY KEY,
                    username CITEXT,
                    role TEXT
                )
            """)

            backend.execute("""
                INSERT INTO test_citext_users (username) VALUES ('Admin')
            """)
            backend.execute("""
                INSERT INTO test_citext_roles (username, role) VALUES ('admin', 'superuser')
            """)

            # Case-insensitive JOIN on citext columns
            result = backend.fetch_one("""
                SELECT u.username, r.role
                FROM test_citext_users u
                JOIN test_citext_roles r ON u.username = r.username
            """)
            assert result is not None
            assert result['role'] == 'superuser'
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_citext_users")
                backend.execute("DROP TABLE IF EXISTS test_citext_roles")
            except Exception:
                pass
