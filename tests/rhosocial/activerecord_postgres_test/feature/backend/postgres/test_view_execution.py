# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_view_execution.py
"""
Tests for PostgreSQL VIEW and MATERIALIZED VIEW functionality with actual database execution.

These tests verify that generated SQL statements execute correctly
against an actual PostgreSQL database.
"""
import pytest
from rhosocial.activerecord.backend.impl.postgres.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import (
    Column, Literal, FunctionCall, TableExpression, QueryExpression,
    CreateViewExpression, DropViewExpression,
    CreateMaterializedViewExpression, DropMaterializedViewExpression,
    RefreshMaterializedViewExpression, CreateTableExpression, DropTableExpression,
    InsertExpression, ColumnDefinition, ColumnConstraint, ColumnConstraintType,
    TableConstraint, TableConstraintType, ForeignKeyConstraint, ValuesSource
)
from rhosocial.activerecord.backend.expression.operators import RawSQLPredicate, RawSQLExpression
from rhosocial.activerecord.backend.expression.query_parts import GroupByHavingClause, WhereClause
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType


@pytest.fixture
def postgres_view_backend(postgres_backend):
    """Provides a PostgresBackend instance with test data for view tests."""
    backend = postgres_backend
    dialect = backend.dialect

    # Drop existing objects if they exist
    backend.execute("DROP MATERIALIZED VIEW IF EXISTS user_order_mv", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP MATERIALIZED VIEW IF EXISTS test_mv", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP VIEW IF EXISTS user_view", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP VIEW IF EXISTS active_users", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP TABLE IF EXISTS orders", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP TABLE IF EXISTS users", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))

    # Create users table
    backend.execute("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(255),
            status VARCHAR(20) DEFAULT 'active'
        )
    """, (), options=ExecutionOptions(stmt_type=StatementType.DDL))

    # Create orders table
    backend.execute("""
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            user_id INT,
            amount DECIMAL(10, 2),
            order_date DATE,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """, (), options=ExecutionOptions(stmt_type=StatementType.DDL))

    # Insert test data
    backend.execute(
        "INSERT INTO users (name, email, status) VALUES (%s, %s, %s)",
        ('Alice', 'alice@example.com', 'active'),
        options=ExecutionOptions(stmt_type=StatementType.DML)
    )
    backend.execute(
        "INSERT INTO users (name, email, status) VALUES (%s, %s, %s)",
        ('Bob', 'bob@example.com', 'inactive'),
        options=ExecutionOptions(stmt_type=StatementType.DML)
    )
    backend.execute(
        "INSERT INTO users (name, email, status) VALUES (%s, %s, %s)",
        ('Charlie', 'charlie@example.com', 'active'),
        options=ExecutionOptions(stmt_type=StatementType.DML)
    )

    backend.execute(
        "INSERT INTO orders (user_id, amount, order_date) VALUES (%s, %s, %s)",
        (1, 100.0, '2024-01-01'),
        options=ExecutionOptions(stmt_type=StatementType.DML)
    )
    backend.execute(
        "INSERT INTO orders (user_id, amount, order_date) VALUES (%s, %s, %s)",
        (1, 200.0, '2024-01-15'),
        options=ExecutionOptions(stmt_type=StatementType.DML)
    )
    backend.execute(
        "INSERT INTO orders (user_id, amount, order_date) VALUES (%s, %s, %s)",
        (2, 50.0, '2024-01-10'),
        options=ExecutionOptions(stmt_type=StatementType.DML)
    )

    yield backend

    # Cleanup
    backend.execute("DROP MATERIALIZED VIEW IF EXISTS user_order_mv", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP MATERIALIZED VIEW IF EXISTS test_mv", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP VIEW IF EXISTS user_view", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP VIEW IF EXISTS active_users", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP TABLE IF EXISTS orders", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))
    backend.execute("DROP TABLE IF EXISTS users", (),
                    options=ExecutionOptions(stmt_type=StatementType.DDL))


class TestPostgresViewExecution:
    """Tests for CREATE VIEW and DROP VIEW with actual execution."""

    def test_create_view_basic(self, postgres_view_backend):
        """Test basic CREATE VIEW executes successfully."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name"), Column(dialect, "email")],
            from_=TableExpression(dialect, "users")
        )

        create_view = CreateViewExpression(
            dialect,
            view_name="user_view",
            query=query
        )

        sql, params = create_view.to_sql()

        result = postgres_view_backend.execute(
            sql, params,
            options=ExecutionOptions(stmt_type=StatementType.DDL)
        )

        result = postgres_view_backend.execute(
            "SELECT * FROM user_view",
            (),
            options=ExecutionOptions(stmt_type=StatementType.SELECT, process_result_set=True)
        )

        assert result.data is not None
        assert len(result.data) == 3
        assert result.data[0]['name'] == 'Alice'

    def test_create_view_with_where(self, postgres_view_backend):
        """Test CREATE VIEW with WHERE clause."""
        # Note: PostgreSQL does not allow parameters in VIEW definitions
        # Using RawSQLPredicate to inline the condition value
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users"),
            where=WhereClause(dialect, condition=RawSQLPredicate(dialect, '"status" = \'active\''))
        )

        create_view = CreateViewExpression(
            dialect,
            view_name="active_users",
            query=query
        )

        sql, params = create_view.to_sql()

        result = postgres_view_backend.execute(
            sql, params,
            options=ExecutionOptions(stmt_type=StatementType.DDL)
        )

        result = postgres_view_backend.execute(
            "SELECT * FROM active_users",
            (),
            options=ExecutionOptions(stmt_type=StatementType.SELECT, process_result_set=True)
        )

        assert result.data is not None
        assert len(result.data) == 2

    def test_create_or_replace_view(self, postgres_view_backend):
        """Test CREATE OR REPLACE VIEW."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id")],
            from_=TableExpression(dialect, "users")
        )

        create_view = CreateViewExpression(
            dialect,
            view_name="user_view",
            query=query
        )

        sql, params = create_view.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        # Create again with OR REPLACE
        query2 = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_view2 = CreateViewExpression(
            dialect,
            view_name="user_view",
            query=query2,
            replace=True
        )

        sql, params = create_view2.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_drop_view(self, postgres_view_backend):
        """Test DROP VIEW."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id")],
            from_=TableExpression(dialect, "users")
        )

        create_view = CreateViewExpression(
            dialect,
            view_name="user_view",
            query=query
        )

        sql, params = create_view.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        drop_view = DropViewExpression(
            dialect,
            view_name="user_view"
        )

        sql, params = drop_view.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_drop_view_if_exists(self, postgres_view_backend):
        """Test DROP VIEW IF EXISTS."""
        dialect = postgres_view_backend.dialect

        drop_view = DropViewExpression(
            dialect,
            view_name="nonexistent_view",
            if_exists=True
        )

        sql, params = drop_view.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_drop_view_cascade(self, postgres_view_backend):
        """Test DROP VIEW CASCADE."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_view = CreateViewExpression(
            dialect,
            view_name="user_view",
            query=query
        )

        sql, params = create_view.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        drop_view = DropViewExpression(
            dialect,
            view_name="user_view",
            cascade=True
        )

        sql, params = drop_view.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None


class TestPostgresMaterializedViewExecution:
    """Tests for CREATE MATERIALIZED VIEW and related operations."""

    def test_create_materialized_view_basic(self, postgres_view_backend):
        """Test basic CREATE MATERIALIZED VIEW executes successfully."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name"), Column(dialect, "email")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query
        )

        sql, params = create_mv.to_sql()

        result = postgres_view_backend.execute(
            sql, params,
            options=ExecutionOptions(stmt_type=StatementType.DDL)
        )

        result = postgres_view_backend.execute(
            "SELECT * FROM test_mv",
            (),
            options=ExecutionOptions(stmt_type=StatementType.SELECT, process_result_set=True)
        )

        assert result.data is not None
        assert len(result.data) == 3
        assert result.data[0]['name'] == 'Alice'

    def test_create_materialized_view_with_data(self, postgres_view_backend):
        """Test CREATE MATERIALIZED VIEW WITH DATA."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query,
            with_data=True
        )

        sql, params = create_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

        result = postgres_view_backend.execute(
            "SELECT * FROM test_mv",
            (),
            options=ExecutionOptions(stmt_type=StatementType.SELECT, process_result_set=True)
        )
        assert result.data is not None
        assert len(result.data) == 3

    def test_create_materialized_view_with_no_data(self, postgres_view_backend):
        """Test CREATE MATERIALIZED VIEW WITH NO DATA."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query,
            with_data=False
        )

        sql, params = create_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_drop_materialized_view(self, postgres_view_backend):
        """Test DROP MATERIALIZED VIEW."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query
        )

        sql, params = create_mv.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        drop_mv = DropMaterializedViewExpression(
            dialect,
            view_name="test_mv"
        )

        sql, params = drop_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_drop_materialized_view_if_exists(self, postgres_view_backend):
        """Test DROP MATERIALIZED VIEW IF EXISTS."""
        dialect = postgres_view_backend.dialect

        drop_mv = DropMaterializedViewExpression(
            dialect,
            view_name="nonexistent_mv",
            if_exists=True
        )

        sql, params = drop_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_drop_materialized_view_cascade(self, postgres_view_backend):
        """Test DROP MATERIALIZED VIEW CASCADE."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query
        )

        sql, params = create_mv.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        drop_mv = DropMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            cascade=True
        )

        sql, params = drop_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_refresh_materialized_view(self, postgres_view_backend):
        """Test REFRESH MATERIALIZED VIEW."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query
        )

        sql, params = create_mv.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        refresh_mv = RefreshMaterializedViewExpression(
            dialect,
            view_name="test_mv"
        )

        sql, params = refresh_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_refresh_materialized_view_concurrently(self, postgres_view_backend):
        """Test REFRESH MATERIALIZED VIEW CONCURRENTLY."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query
        )

        sql, params = create_mv.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        # Create unique index required for CONCURRENTLY
        postgres_view_backend.execute(
            "CREATE UNIQUE INDEX test_mv_id_idx ON test_mv (id)",
            (),
            options=ExecutionOptions(stmt_type=StatementType.DDL)
        )

        refresh_mv = RefreshMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            concurrent=True
        )

        sql, params = refresh_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

    def test_refresh_materialized_view_with_data(self, postgres_view_backend):
        """Test REFRESH MATERIALIZED VIEW WITH DATA."""
        dialect = postgres_view_backend.dialect

        query = QueryExpression(
            dialect,
            select=[Column(dialect, "id"), Column(dialect, "name")],
            from_=TableExpression(dialect, "users")
        )

        create_mv = CreateMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            query=query,
            with_data=False
        )

        sql, params = create_mv.to_sql()
        postgres_view_backend.execute(sql, params,
                                      options=ExecutionOptions(stmt_type=StatementType.DDL))

        refresh_mv = RefreshMaterializedViewExpression(
            dialect,
            view_name="test_mv",
            with_data=True
        )

        sql, params = refresh_mv.to_sql()
        result = postgres_view_backend.execute(sql, params,
                                               options=ExecutionOptions(stmt_type=StatementType.DDL))
        assert result is not None

        result = postgres_view_backend.execute(
            "SELECT * FROM test_mv",
            (),
            options=ExecutionOptions(stmt_type=StatementType.SELECT, process_result_set=True)
        )
        assert result.data is not None
        assert len(result.data) == 3
