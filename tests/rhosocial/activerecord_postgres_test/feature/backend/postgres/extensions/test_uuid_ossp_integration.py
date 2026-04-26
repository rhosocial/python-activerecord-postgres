# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_uuid_ossp_integration.py
"""
Integration tests for PostgreSQL uuid-ossp extension with real database.

These tests require a live PostgreSQL connection with uuid-ossp extension installed
and test:
- uuid_generate_v4() function
- UUID column with uuid_generate_v4() default value
- uuid_generate_v1() function
"""
import pytest
import re


UUID_V4_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    re.IGNORECASE
)

UUID_V1_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE
)


class TestUuidOsspIntegration:
    """Integration tests for uuid-ossp extension."""

    def test_uuid_generate_v4(self, postgres_backend_single):
        """Test uuid_generate_v4() produces valid UUIDs."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("uuid-ossp"):
            pytest.skip("Extension 'uuid-ossp' not installed")

        result = backend.fetch_one("""
            SELECT uuid_generate_v4() AS uuid_val
        """)
        assert result is not None
        assert result['uuid_val'] is not None
        # Verify the UUID format (v4 has '4' in the version position)
        assert UUID_V4_PATTERN.match(str(result['uuid_val'])) is not None

    def test_uuid_generate_v4_unique(self, postgres_backend_single):
        """Test that uuid_generate_v4() produces unique values."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("uuid-ossp"):
            pytest.skip("Extension 'uuid-ossp' not installed")

        result1 = backend.fetch_one("SELECT uuid_generate_v4() AS uuid_val")
        result2 = backend.fetch_one("SELECT uuid_generate_v4() AS uuid_val")
        assert result1['uuid_val'] != result2['uuid_val']

    def test_uuid_column_default(self, postgres_backend_single):
        """Test creating a table with UUID column using uuid_generate_v4() as default."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("uuid-ossp"):
            pytest.skip("Extension 'uuid-ossp' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_uuid_ossp")
            backend.execute("""
                CREATE TABLE test_uuid_ossp (
                    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                    name TEXT
                )
            """)

            # Insert without specifying id (should auto-generate)
            backend.execute("""
                INSERT INTO test_uuid_ossp (name) VALUES ('test item')
            """)

            result = backend.fetch_one("""
                SELECT id, name FROM test_uuid_ossp WHERE name = 'test item'
            """)
            assert result is not None
            assert result['id'] is not None
            assert result['name'] == 'test item'
            # Verify the UUID is a v4
            assert UUID_V4_PATTERN.match(str(result['id'])) is not None
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_uuid_ossp")
            except Exception:
                pass

    def test_uuid_generate_v1(self, postgres_backend_single):
        """Test uuid_generate_v1() produces valid UUIDs."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("uuid-ossp"):
            pytest.skip("Extension 'uuid-ossp' not installed")

        result = backend.fetch_one("""
            SELECT uuid_generate_v1() AS uuid_val
        """)
        assert result is not None
        assert result['uuid_val'] is not None
        # Verify UUID format (v1)
        assert UUID_V1_PATTERN.match(str(result['uuid_val'])) is not None

    def test_uuid_column_explicit_insert(self, postgres_backend_single):
        """Test inserting explicit UUID values into a UUID column."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("uuid-ossp"):
            pytest.skip("Extension 'uuid-ossp' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_uuid_explicit")
            backend.execute("""
                CREATE TABLE test_uuid_explicit (
                    id UUID PRIMARY KEY,
                    label TEXT
                )
            """)

            # Generate a UUID and use it explicitly
            gen_result = backend.fetch_one("SELECT uuid_generate_v4() AS uuid_val")
            uuid_val = gen_result['uuid_val']

            backend.execute(f"""
                INSERT INTO test_uuid_explicit (id, label) VALUES ('{uuid_val}', 'explicit')
            """)

            result = backend.fetch_one("""
                SELECT id, label FROM test_uuid_explicit WHERE label = 'explicit'
            """)
            assert result is not None
            assert str(result['id']) == str(uuid_val)
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_uuid_explicit")
            except Exception:
                pass
