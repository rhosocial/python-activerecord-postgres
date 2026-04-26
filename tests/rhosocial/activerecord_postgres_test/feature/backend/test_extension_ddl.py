# tests/rhosocial/activerecord_postgres_test/feature/backend/test_extension_ddl.py
"""
PostgreSQL extension DDL expression tests.

This module tests CREATE EXTENSION and DROP EXTENSION expressions.
"""
import pytest


class TestPostgresCreateExtensionExpression:
    """Tests for PostgresCreateExtensionExpression."""

    def test_create_extension_basic(self, postgres_dialect):
        """Test basic CREATE EXTENSION with IF NOT EXISTS."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        expr = PostgresCreateExtensionExpression(
            dialect=postgres_dialect,
            name="uuid-ossp",
            if_not_exists=True,
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        assert params == ()

    def test_create_extension_with_schema(self, postgres_dialect):
        """Test CREATE EXTENSION with schema specification."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        expr = PostgresCreateExtensionExpression(
            dialect=postgres_dialect,
            name="hstore",
            schema="public",
            if_not_exists=True,
        )
        sql, params = expr.to_sql()

        assert sql == "CREATE EXTENSION IF NOT EXISTS hstore SCHEMA public"
        assert params == ()

    def test_create_extension_with_version(self, postgres_dialect):
        """Test CREATE EXTENSION with version."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        expr = PostgresCreateExtensionExpression(
            dialect=postgres_dialect,
            name="postgis",
            version="3.4.0",
            if_not_exists=True,
        )
        sql, params = expr.to_sql()

        assert sql == "CREATE EXTENSION IF NOT EXISTS postgis VERSION '3.4.0'"
        assert params == ()

    def test_create_extension_with_cascade(self, postgres_dialect):
        """Test CREATE EXTENSION with CASCADE."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        expr = PostgresCreateExtensionExpression(
            dialect=postgres_dialect,
            name="postgis",
            if_not_exists=False,
            cascade=True,
        )
        sql, params = expr.to_sql()

        assert sql == "CREATE EXTENSION postgis CASCADE"
        assert params == ()

    def test_create_extension_no_if_not_exists(self, postgres_dialect):
        """Test CREATE EXTENSION without IF NOT EXISTS."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        expr = PostgresCreateExtensionExpression(
            dialect=postgres_dialect,
            name="hstore",
            if_not_exists=False,
        )
        sql, params = expr.to_sql()

        assert sql == "CREATE EXTENSION hstore"
        assert params == ()

    def test_create_extension_full_options(self, postgres_dialect):
        """Test CREATE EXTENSION with all options."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        expr = PostgresCreateExtensionExpression(
            dialect=postgres_dialect,
            name="uuid-ossp",
            schema="extensions",
            version="1.1",
            if_not_exists=True,
            cascade=True,
        )
        sql, params = expr.to_sql()

        assert sql == 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp" SCHEMA extensions VERSION \'1.1\' CASCADE'
        assert params == ()


class TestPostgresDropExtensionExpression:
    """Tests for PostgresDropExtensionExpression."""

    def test_drop_extension_basic(self, postgres_dialect):
        """Test basic DROP EXTENSION with IF EXISTS."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresDropExtensionExpression,
        )

        expr = PostgresDropExtensionExpression(
            dialect=postgres_dialect,
            name="uuid-ossp",
            if_exists=True,
        )
        sql, params = expr.to_sql()

        assert sql == 'DROP EXTENSION IF EXISTS "uuid-ossp"'
        assert params == ()

    def test_drop_extension_with_cascade(self, postgres_dialect):
        """Test DROP EXTENSION with CASCADE."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresDropExtensionExpression,
        )

        expr = PostgresDropExtensionExpression(
            dialect=postgres_dialect,
            name="hstore",
            if_exists=False,
            cascade=True,
        )
        sql, params = expr.to_sql()

        assert sql == "DROP EXTENSION hstore CASCADE"
        assert params == ()

    def test_drop_extension_with_restrict(self, postgres_dialect):
        """Test DROP EXTENSION with RESTRICT."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresDropExtensionExpression,
        )

        expr = PostgresDropExtensionExpression(
            dialect=postgres_dialect,
            name="hstore",
            if_exists=False,
            restrict=True,
        )
        sql, params = expr.to_sql()

        assert sql == "DROP EXTENSION hstore RESTRICT"
        assert params == ()

    def test_drop_extension_no_if_exists(self, postgres_dialect):
        """Test DROP EXTENSION without IF EXISTS."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresDropExtensionExpression,
        )

        expr = PostgresDropExtensionExpression(
            dialect=postgres_dialect,
            name="hstore",
            if_exists=False,
        )
        sql, params = expr.to_sql()

        assert sql == "DROP EXTENSION hstore"
        assert params == ()

    def test_drop_extension_full_options(self, postgres_dialect):
        """Test DROP EXTENSION with all options."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresDropExtensionExpression,
        )

        expr = PostgresDropExtensionExpression(
            dialect=postgres_dialect,
            name="uuid-ossp",
            if_exists=True,
            cascade=True,
        )
        sql, params = expr.to_sql()

        assert sql == 'DROP EXTENSION IF EXISTS "uuid-ossp" CASCADE'
        assert params == ()


class TestExtensionExpressionExecution:
    """Tests for executing extension DDL expressions against real database."""

    def test_create_and_drop_extension(self, postgres_backend_single):
        """Test CREATE EXTENSION and DROP EXTENSION execution."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
            PostgresDropExtensionExpression,
        )

        dialect = postgres_backend_single.dialect
        backend = postgres_backend_single

        # Skip if extension already exists
        backend.introspect_and_adapt()
        already_exists = dialect.is_extension_installed("uuid-ossp")

        if not already_exists:
            # Create extension
            create_expr = PostgresCreateExtensionExpression(
                dialect=dialect,
                name="uuid-ossp",
                if_not_exists=True,
            )
            sql, params = create_expr.to_sql()
            backend.execute(sql, params)

            # Verify extension is installed
            backend.introspect_and_adapt()
            assert dialect.is_extension_installed("uuid-ossp")

            # Drop extension
            drop_expr = PostgresDropExtensionExpression(
                dialect=dialect,
                name="uuid-ossp",
                if_exists=True,
            )
            sql, params = drop_expr.to_sql()
            backend.execute(sql, params)

            # Verify extension is dropped
            backend.introspect_and_adapt()
            assert not dialect.is_extension_installed("uuid-ossp")

    def test_create_extension_idempotent(self, postgres_backend_single):
        """Test that CREATE EXTENSION IF NOT EXISTS is idempotent."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        dialect = postgres_backend_single.dialect
        backend = postgres_backend_single

        # Execute twice - should not fail
        expr = PostgresCreateExtensionExpression(
            dialect=dialect,
            name="pg_trgm",
            if_not_exists=True,
        )
        sql, params = expr.to_sql()

        # First creation
        backend.execute(sql, params)

        # Second creation (idempotent)
        backend.execute(sql, params)

    def test_create_extension_without_if_not_exists_fails_on_duplicate(self, postgres_backend_single):
        """Test that CREATE EXTENSION without IF NOT EXISTS fails if extension exists."""
        from rhosocial.activerecord.backend.impl.postgres.expression import (
            PostgresCreateExtensionExpression,
        )

        dialect = postgres_backend_single.dialect
        backend = postgres_backend_single

        # Skip if extension doesn't exist
        backend.introspect_and_adapt()
        if not dialect.is_extension_installed("pg_trgm"):
            pytest.skip("pg_trgm not installed, skipping duplicate test")

        # Try to create again without IF NOT EXISTS - should fail
        expr = PostgresCreateExtensionExpression(
            dialect=dialect,
            name="pg_trgm",
            if_not_exists=False,
        )
        sql, params = expr.to_sql()

        with pytest.raises(Exception):
            backend.execute(sql, params)