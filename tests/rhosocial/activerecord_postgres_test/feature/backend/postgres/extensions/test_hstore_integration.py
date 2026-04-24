# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_hstore_integration.py
"""
Integration tests for PostgreSQL hstore extension with real database.

These tests require a live PostgreSQL connection with hstore extension installed
and test:
- hstore column creation and data retrieval
- hstore operators (@>, ?, ||)
- hstore functions (each(), akeys(), avals())
"""
import pytest


class TestHstoreIntegration:
    """Integration tests for hstore extension."""

    def test_hstore_create_and_query(self, postgres_backend_single):
        """Test creating a table with hstore column, inserting and querying data."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore")
            backend.execute("""
                CREATE TABLE test_hstore (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)

            # Insert hstore data using literal syntax
            backend.execute("""
                INSERT INTO test_hstore (data) VALUES ('a=>1, b=>2')
            """)

            # Query back the data
            result = backend.fetch_one("SELECT data FROM test_hstore WHERE id = 1")
            assert result is not None
            assert result['data'] is not None

            # Access individual key
            result = backend.fetch_one("SELECT data->'a' AS val_a FROM test_hstore WHERE id = 1")
            assert result['val_a'] == '1'

            result = backend.fetch_one("SELECT data->'b' AS val_b FROM test_hstore WHERE id = 1")
            assert result['val_b'] == '2'
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore")
            except Exception:
                pass

    def test_hstore_operators_contains(self, postgres_backend_single):
        """Test hstore @> (contains) operator."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore_ops")
            backend.execute("""
                CREATE TABLE test_hstore_ops (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)
            backend.execute("""
                INSERT INTO test_hstore_ops (data) VALUES ('color=>red, size=>large')
            """)

            # Test @> operator: does data contain 'color=>red'?
            result = backend.fetch_one("""
                SELECT data @> 'color=>red' AS contains
                FROM test_hstore_ops WHERE id = 1
            """)
            assert result['contains'] is True

            # Test @> with non-existent pair
            result = backend.fetch_one("""
                SELECT data @> 'color=>blue' AS contains
                FROM test_hstore_ops WHERE id = 1
            """)
            assert result['contains'] is False
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore_ops")
            except Exception:
                pass

    def test_hstore_operators_key_exists(self, postgres_backend_single):
        """Test hstore ? (key exists) operator."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore_key")
            backend.execute("""
                CREATE TABLE test_hstore_key (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)
            backend.execute("""
                INSERT INTO test_hstore_key (data) VALUES ('name=>Alice, age=>30')
            """)

            # Test ? operator: does key 'name' exist?
            result = backend.fetch_one("""
                SELECT data ? 'name' AS has_name
                FROM test_hstore_key WHERE id = 1
            """)
            assert result['has_name'] is True

            # Test ? operator: does key 'email' exist?
            result = backend.fetch_one("""
                SELECT data ? 'email' AS has_email
                FROM test_hstore_key WHERE id = 1
            """)
            assert result['has_email'] is False
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore_key")
            except Exception:
                pass

    def test_hstore_operators_concat(self, postgres_backend_single):
        """Test hstore || (concat) operator."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore_concat")
            backend.execute("""
                CREATE TABLE test_hstore_concat (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)
            backend.execute("""
                INSERT INTO test_hstore_concat (data) VALUES ('a=>1')
            """)

            # Test || operator: merge hstores
            result = backend.fetch_one("""
                SELECT data || 'b=>2' AS merged
                FROM test_hstore_concat WHERE id = 1
            """)
            assert result['merged'] is not None
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore_concat")
            except Exception:
                pass

    def test_hstore_functions_each(self, postgres_backend_single):
        """Test hstore each() function to expand hstore to key-value rows."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore_each")
            backend.execute("""
                CREATE TABLE test_hstore_each (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)
            backend.execute("""
                INSERT INTO test_hstore_each (data) VALUES ('x=>10, y=>20, z=>30')
            """)

            # Test each() function
            result = backend.fetch_all("""
                SELECT each(data) AS kv_pair
                FROM test_hstore_each WHERE id = 1
            """)
            assert result is not None
            assert len(result) == 3
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore_each")
            except Exception:
                pass

    def test_hstore_functions_akeys_avals(self, postgres_backend_single):
        """Test hstore akeys() and avals() functions."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore_keys_vals")
            backend.execute("""
                CREATE TABLE test_hstore_keys_vals (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)
            backend.execute("""
                INSERT INTO test_hstore_keys_vals (data) VALUES ('first=>a, second=>b')
            """)

            # Test akeys(): get all keys as array
            result = backend.fetch_one("""
                SELECT akeys(data) AS keys
                FROM test_hstore_keys_vals WHERE id = 1
            """)
            assert result['keys'] is not None
            assert len(result['keys']) == 2

            # Test avals(): get all values as array
            result = backend.fetch_one("""
                SELECT avals(data) AS vals
                FROM test_hstore_keys_vals WHERE id = 1
            """)
            assert result['vals'] is not None
            assert len(result['vals']) == 2
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore_keys_vals")
            except Exception:
                pass

    def test_hstore_update_key_value(self, postgres_backend_single):
        """Test updating individual key in hstore column."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore_update")
            backend.execute("""
                CREATE TABLE test_hstore_update (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)
            backend.execute("""
                INSERT INTO test_hstore_update (data) VALUES ('status=>active')
            """)

            # Update a key value
            backend.execute("""
                UPDATE test_hstore_update SET data = data || 'status=>inactive'::hstore WHERE id = 1
            """)

            result = backend.fetch_one("""
                SELECT data->'status' AS status
                FROM test_hstore_update WHERE id = 1
            """)
            assert result['status'] == 'inactive'
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore_update")
            except Exception:
                pass

    def test_hstore_delete_key(self, postgres_backend_single):
        """Test deleting a key from hstore column."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("hstore"):
            pytest.skip("Extension 'hstore' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_hstore_delete")
            backend.execute("""
                CREATE TABLE test_hstore_delete (
                    id SERIAL PRIMARY KEY,
                    data HSTORE
                )
            """)
            backend.execute("""
                INSERT INTO test_hstore_delete (data) VALUES ('keep=>yes, remove=>me')
            """)

            # Delete a key using delete() function
            backend.execute("""
                UPDATE test_hstore_delete SET data = delete(data, 'remove') WHERE id = 1
            """)

            result = backend.fetch_one("""
                SELECT data ? 'remove' AS has_remove,
                       data ? 'keep' AS has_keep
                FROM test_hstore_delete WHERE id = 1
            """)
            assert result['has_remove'] is False
            assert result['has_keep'] is True
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_hstore_delete")
            except Exception:
                pass
