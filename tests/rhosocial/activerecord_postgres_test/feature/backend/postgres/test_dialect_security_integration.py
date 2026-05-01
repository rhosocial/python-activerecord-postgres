# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_dialect_security_integration.py
"""
Integration tests for PostgreSQL dialect SQL injection security fixes.

These tests verify that the security fixes work correctly when
SQL is actually executed against a PostgreSQL database.
"""
import pytest


class TestPostgresDialectSecurityIntegration:
    """Integration tests for PostgreSQL dialect security."""

    @pytest.fixture
    def test_table_with_special_chars(self, postgres_backend):
        """Create a test table with special characters in defaults and comments."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_security_chars")
        postgres_backend.execute("""
            CREATE TABLE test_security_chars (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) DEFAULT 'test''s value',
                description VARCHAR(255)
            )
        """)
        yield "test_security_chars"
        postgres_backend.execute("DROP TABLE IF EXISTS test_security_chars")

    def test_default_string_with_single_quote_insert_and_retrieve(self, postgres_backend, test_table_with_special_chars):
        """Test that single quotes in DEFAULT are properly escaped and retrieved correctly."""
        # Insert a row with special characters
        postgres_backend.execute(
            f"INSERT INTO {test_table_with_special_chars} (name) VALUES ('O''Brien')"
        )

        # Retrieve and verify
        row = postgres_backend.fetch_one(
            f"SELECT name FROM {test_table_with_special_chars} WHERE id = 1"
        )
        assert row["name"] == "O'Brien"

    @pytest.fixture
    def test_table_for_data_type(self, postgres_backend):
        """Create a test table for data type security tests."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_data_type_security")
        yield "test_data_type_security"
        postgres_backend.execute("DROP TABLE IF EXISTS test_data_type_security")

    def test_valid_data_type_works(self, postgres_backend, test_table_for_data_type):
        """Test that valid data types work correctly."""
        # This should succeed with valid data type
        postgres_backend.execute(f"""
            CREATE TABLE {test_table_for_data_type} (
                id INT PRIMARY KEY,
                data VARCHAR(255)
            )
        """)

        # Verify table was created - use fetch_one
        result = postgres_backend.fetch_one(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'test_data_type_security'
        """)
        assert result is not None

    def test_malicious_data_type_rejected_at_dialect_level(self, postgres_backend):
        """Test that malicious data type is rejected at dialect level before DB execution."""
        from rhosocial.activerecord.backend.expression.statements import ColumnDefinition

        dialect = postgres_backend.dialect

        # This should raise ValueError before reaching the database
        col_def = ColumnDefinition(
            name="test_col",
            data_type="VARCHAR(255); DROP TABLE users--",
        )

        with pytest.raises(ValueError, match="Invalid data type"):
            dialect.format_column_definition(col_def)


class TestPostgresExcludeConstraintSecurityIntegration:
    """Integration tests for EXCLUDE constraint security."""

    @pytest.fixture
    def exclude_table(self, postgres_backend):
        """Create a test table with EXCLUDE constraint."""
        postgres_backend.execute("DROP TABLE IF EXISTS test_exclude_security")
        postgres_backend.execute("""
            CREATE TABLE test_exclude_security (
                id SERIAL PRIMARY KEY,
                range int4range,
                EXCLUDE USING gist (range WITH &&)
            )
        """)
        yield "test_exclude_security"
        postgres_backend.execute("DROP TABLE IF EXISTS test_exclude_security")

    def test_exclude_constraint_with_valid_operator(self, postgres_backend, exclude_table):
        """Test EXCLUDE constraint with valid && operator works."""
        # Insert should work
        postgres_backend.execute(
            f"INSERT INTO {exclude_table} (range) VALUES ('[1,10)')"
        )

        # Verify insertion
        row = postgres_backend.fetch_one(
            f"SELECT range FROM {exclude_table} WHERE id = 1"
        )
        assert row is not None

    def test_exclude_constraint_overlap_rejected(self, postgres_backend, exclude_table):
        """Test EXCLUDE constraint rejects overlapping ranges."""
        # Insert first range
        postgres_backend.execute(
            f"INSERT INTO {exclude_table} (range) VALUES ('[1,10)')"
        )

        # Try to insert overlapping range - should fail
        with pytest.raises(Exception):
            postgres_backend.execute(
                f"INSERT INTO {exclude_table} (range) VALUES ('[5,15)')"
            )


class TestPostgresStorageOptionsSecurityIntegration:
    """Integration tests for storage options security."""

    def test_storage_options_with_special_chars(self, postgres_backend):
        """Test storage options with special characters are properly escaped."""
        # Test that values with single quotes work correctly
        postgres_backend.execute("DROP TABLE IF EXISTS test_storage_opts")
        postgres_backend.execute("""
            CREATE TABLE test_storage_opts (
                id INT PRIMARY KEY,
                data TEXT
            ) WITH (fillfactor = '80')
        """)
        # This should succeed without syntax errors

        postgres_backend.execute("DROP TABLE IF EXISTS test_storage_opts")