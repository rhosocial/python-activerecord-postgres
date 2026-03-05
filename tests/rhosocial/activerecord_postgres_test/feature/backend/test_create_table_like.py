# tests/rhosocial/activerecord_postgres_test/feature/backend/test_create_table_like.py
"""
PostgreSQL CREATE TABLE ... LIKE syntax tests.

This module tests the PostgreSQL-specific LIKE syntax for CREATE TABLE statements,
including INCLUDING/EXCLUDING options.
"""
import pytest
from rhosocial.activerecord.backend.expression import CreateTableExpression, ColumnDefinition
from rhosocial.activerecord.backend.expression.statements import ColumnConstraint, ColumnConstraintType
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class TestPostgreSQLCreateTableLike:
    """Tests for PostgreSQL CREATE TABLE ... LIKE syntax."""

    def test_basic_like_syntax(self):
        """Test basic CREATE TABLE ... LIKE syntax."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={'like_table': 'users'}
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users")'
        assert params == ()

    def test_like_with_including_defaults(self):
        """Test LIKE with INCLUDING DEFAULTS option."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': {'including': ['DEFAULTS']}
            }
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS)'
        assert params == ()

    def test_like_with_including_constraints(self):
        """Test LIKE with INCLUDING CONSTRAINTS option."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': {'including': ['CONSTRAINTS']}
            }
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING CONSTRAINTS)'
        assert params == ()

    def test_like_with_including_indexes(self):
        """Test LIKE with INCLUDING INDEXES option."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': {'including': ['INDEXES']}
            }
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING INDEXES)'
        assert params == ()

    def test_like_with_multiple_including_options(self):
        """Test LIKE with multiple INCLUDING options (dictionary format)."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': {
                    'including': ['DEFAULTS', 'CONSTRAINTS', 'INDEXES']
                }
            }
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS, INCLUDING CONSTRAINTS, INCLUDING INDEXES)'
        assert params == ()

    def test_like_with_including_and_excluding_options(self):
        """Test LIKE with both INCLUDING and EXCLUDING options."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': {
                    'including': ['DEFAULTS', 'CONSTRAINTS'],
                    'excluding': ['INDEXES', 'COMMENTS']
                }
            }
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS, INCLUDING CONSTRAINTS, EXCLUDING INDEXES, EXCLUDING COMMENTS)'
        assert params == ()

    def test_like_with_list_format_options(self):
        """Test LIKE with options in list format (backwards compatibility)."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': ['DEFAULTS', 'CONSTRAINTS']
            }
        )
        sql, params = create_expr.to_sql()

        # Should default to INCLUDING when just feature name is provided
        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS, INCLUDING CONSTRAINTS)'
        assert params == ()

    def test_like_with_tuple_format_options(self):
        """Test LIKE with options in tuple format (backwards compatibility)."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': [
                    ('INCLUDING', 'DEFAULTS'),
                    ('EXCLUDING', 'INDEXES')
                ]
            }
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING DEFAULTS, EXCLUDING INDEXES)'
        assert params == ()

    def test_like_with_if_not_exists(self):
        """Test CREATE TABLE ... LIKE with IF NOT EXISTS."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            if_not_exists=True,
            dialect_options={'like_table': 'users'}
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE IF NOT EXISTS "users_copy" (LIKE "users")'
        assert params == ()

    def test_like_with_temporary(self):
        """Test CREATE TEMPORARY TABLE ... LIKE."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="temp_users",
            columns=[],
            temporary=True,
            dialect_options={'like_table': 'users'}
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TEMPORARY TABLE "temp_users" (LIKE "users")'
        assert params == ()

    def test_like_with_schema_qualified_table(self):
        """Test CREATE TABLE ... LIKE with schema-qualified source table."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={'like_table': ('public', 'users')}
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "public"."users")'
        assert params == ()

    def test_like_ignores_columns(self):
        """Test that LIKE syntax ignores columns parameter."""
        dialect = PostgresDialect()
        columns = [
            ColumnDefinition("id", "INTEGER", constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)
            ]),
            ColumnDefinition("name", "VARCHAR(255)")
        ]
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=columns,
            dialect_options={'like_table': 'users'}
        )
        sql, params = create_expr.to_sql()

        # LIKE syntax should take precedence, columns should be ignored
        assert sql == 'CREATE TABLE "users_copy" (LIKE "users")'
        assert params == ()

    def test_like_with_temporary_and_if_not_exists(self):
        """Test CREATE TEMPORARY TABLE ... LIKE with IF NOT EXISTS."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="temp_users_copy",
            columns=[],
            temporary=True,
            if_not_exists=True,
            dialect_options={'like_table': ('public', 'users')}
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TEMPORARY TABLE IF NOT EXISTS "temp_users_copy" (LIKE "public"."users")'
        assert params == ()

    def test_like_with_all_option(self):
        """Test LIKE with INCLUDING ALL option."""
        dialect = PostgresDialect()
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users_copy",
            columns=[],
            dialect_options={
                'like_table': 'users',
                'like_options': {'including': ['ALL']}
            }
        )
        sql, params = create_expr.to_sql()

        assert sql == 'CREATE TABLE "users_copy" (LIKE "users", INCLUDING ALL)'
        assert params == ()

    def test_fallback_to_base_when_no_like(self):
        """Test that base implementation is used when LIKE is not specified."""
        dialect = PostgresDialect()
        columns = [
            ColumnDefinition("id", "INTEGER", constraints=[
                ColumnConstraint(ColumnConstraintType.PRIMARY_KEY)
            ]),
            ColumnDefinition("name", "VARCHAR(255)", constraints=[
                ColumnConstraint(ColumnConstraintType.NOT_NULL)
            ])
        ]
        create_expr = CreateTableExpression(
            dialect=dialect,
            table_name="users",
            columns=columns
        )
        sql, params = create_expr.to_sql()

        # Should use base implementation
        assert "CREATE TABLE" in sql
        assert '"users"' in sql
        assert '"id"' in sql
        assert '"name"' in sql
        assert "PRIMARY KEY" in sql
        assert "NOT NULL" in sql
