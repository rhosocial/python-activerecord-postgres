# tests/rhosocial/activerecord_test/feature/backend/postgres/test_expressions_transaction.py
"""Tests for PostgreSQL transaction expression classes."""
import pytest
from rhosocial.activerecord.backend.expression.transaction import (
    BeginTransactionExpression,
    CommitTransactionExpression,
    RollbackTransactionExpression,
    SavepointExpression,
    ReleaseSavepointExpression,
    SetTransactionExpression,
)
from rhosocial.activerecord.backend.transaction import IsolationLevel, TransactionMode


class TestPostgreSQLBeginTransactionExpression:
    """Tests for PostgreSQL BeginTransactionExpression."""

    def test_basic_begin(self, postgres_dialect):
        """Test basic BEGIN statement."""
        expr = BeginTransactionExpression(postgres_dialect)
        sql, params = expr.to_sql()
        assert sql == "BEGIN"
        assert params == ()

    def test_begin_with_isolation_level(self, postgres_dialect):
        """Test BEGIN with isolation level."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.isolation_level(IsolationLevel.SERIALIZABLE)
        sql, params = expr.to_sql()
        assert sql == "BEGIN ISOLATION LEVEL SERIALIZABLE"
        assert params == ()

    def test_begin_read_only(self, postgres_dialect):
        """Test BEGIN READ ONLY."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.read_only()
        sql, params = expr.to_sql()
        assert sql == "BEGIN READ ONLY"
        assert params == ()

    def test_begin_read_write(self, postgres_dialect):
        """Test BEGIN READ WRITE."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.read_write()
        sql, params = expr.to_sql()
        assert sql == "BEGIN READ WRITE"
        assert params == ()

    def test_begin_with_isolation_and_read_only(self, postgres_dialect):
        """Test BEGIN with isolation level and READ ONLY."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.isolation_level(IsolationLevel.REPEATABLE_READ).read_only()
        sql, params = expr.to_sql()
        assert "ISOLATION LEVEL REPEATABLE READ" in sql
        assert "READ ONLY" in sql
        assert params == ()

    def test_begin_deferrable(self, postgres_dialect):
        """Test BEGIN with DEFERRABLE (only for SERIALIZABLE)."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.isolation_level(IsolationLevel.SERIALIZABLE).deferrable()
        sql, params = expr.to_sql()
        assert "ISOLATION LEVEL SERIALIZABLE" in sql
        assert "DEFERRABLE" in sql
        assert params == ()

    def test_begin_not_deferrable(self, postgres_dialect):
        """Test BEGIN with NOT DEFERRABLE."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.isolation_level(IsolationLevel.SERIALIZABLE).deferrable(False)
        sql, params = expr.to_sql()
        assert "NOT DEFERRABLE" in sql
        assert params == ()

    def test_begin_deferrable_ignored_for_non_serializable(self, postgres_dialect):
        """Test DEFERRABLE is ignored for non-SERIALIZABLE isolation levels."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.isolation_level(IsolationLevel.READ_COMMITTED).deferrable()
        sql, params = expr.to_sql()
        assert "DEFERRABLE" not in sql
        assert "READ COMMITTED" in sql

    @pytest.mark.parametrize("level,expected_name", [
        (IsolationLevel.READ_UNCOMMITTED, "READ UNCOMMITTED"),
        (IsolationLevel.READ_COMMITTED, "READ COMMITTED"),
        (IsolationLevel.REPEATABLE_READ, "REPEATABLE READ"),
        (IsolationLevel.SERIALIZABLE, "SERIALIZABLE"),
    ])
    def test_all_isolation_levels(self, postgres_dialect, level, expected_name):
        """Test all isolation levels."""
        expr = BeginTransactionExpression(postgres_dialect)
        expr.isolation_level(level)
        sql, params = expr.to_sql()
        assert expected_name in sql
        assert params == ()


class TestPostgreSQLCommitRollback:
    """Tests for PostgreSQL COMMIT and ROLLBACK."""

    def test_commit(self, postgres_dialect):
        """Test COMMIT statement."""
        expr = CommitTransactionExpression(postgres_dialect)
        sql, params = expr.to_sql()
        assert sql == "COMMIT"
        assert params == ()

    def test_rollback(self, postgres_dialect):
        """Test ROLLBACK statement."""
        expr = RollbackTransactionExpression(postgres_dialect)
        sql, params = expr.to_sql()
        assert sql == "ROLLBACK"
        assert params == ()

    def test_rollback_to_savepoint(self, postgres_dialect):
        """Test ROLLBACK TO SAVEPOINT statement."""
        expr = RollbackTransactionExpression(postgres_dialect)
        expr.to_savepoint("my_savepoint")
        sql, params = expr.to_sql()
        assert "ROLLBACK" in sql
        assert "SAVEPOINT" in sql
        assert params == ()


class TestPostgreSQLSavepoint:
    """Tests for PostgreSQL SAVEPOINT operations."""

    def test_savepoint(self, postgres_dialect):
        """Test SAVEPOINT statement."""
        expr = SavepointExpression(postgres_dialect, "my_savepoint")
        sql, params = expr.to_sql()
        assert "SAVEPOINT" in sql
        assert "my_savepoint" in sql
        assert params == ()

    def test_release_savepoint(self, postgres_dialect):
        """Test RELEASE SAVEPOINT statement."""
        expr = ReleaseSavepointExpression(postgres_dialect, "my_savepoint")
        sql, params = expr.to_sql()
        assert "RELEASE SAVEPOINT" in sql
        assert "my_savepoint" in sql
        assert params == ()


class TestPostgreSQLSetTransaction:
    """Tests for PostgreSQL SET TRANSACTION."""

    def test_set_transaction_isolation(self, postgres_dialect):
        """Test SET TRANSACTION with isolation level."""
        expr = SetTransactionExpression(postgres_dialect)
        expr.isolation_level(IsolationLevel.SERIALIZABLE)
        sql, params = expr.to_sql()
        assert "SET TRANSACTION" in sql
        assert "ISOLATION LEVEL SERIALIZABLE" in sql
        assert params == ()

    def test_set_transaction_read_only(self, postgres_dialect):
        """Test SET TRANSACTION READ ONLY."""
        expr = SetTransactionExpression(postgres_dialect)
        expr.read_only()
        sql, params = expr.to_sql()
        assert "SET TRANSACTION" in sql
        assert "READ ONLY" in sql
        assert params == ()

    def test_set_session_characteristics(self, postgres_dialect):
        """Test SET SESSION CHARACTERISTICS AS TRANSACTION."""
        expr = SetTransactionExpression(postgres_dialect)
        expr.session(True).isolation_level(IsolationLevel.SERIALIZABLE).read_only()
        sql, params = expr.to_sql()
        assert "SET SESSION CHARACTERISTICS AS TRANSACTION" in sql
        assert "ISOLATION LEVEL SERIALIZABLE" in sql
        assert "READ ONLY" in sql
        assert params == ()

    def test_set_transaction_deferrable(self, postgres_dialect):
        """Test SET TRANSACTION DEFERRABLE."""
        expr = SetTransactionExpression(postgres_dialect)
        expr.isolation_level(IsolationLevel.SERIALIZABLE).deferrable()
        sql, params = expr.to_sql()
        assert "ISOLATION LEVEL SERIALIZABLE" in sql
        assert "DEFERRABLE" in sql
        assert params == ()


class TestPostgreSQLTransactionCapabilities:
    """Tests for PostgreSQL transaction capabilities."""

    def test_supports_transaction_mode(self, postgres_dialect):
        """Test PostgreSQL supports transaction mode."""
        assert postgres_dialect.supports_transaction_mode() == True

    def test_supports_isolation_level_in_begin(self, postgres_dialect):
        """Test PostgreSQL supports isolation level in BEGIN."""
        assert postgres_dialect.supports_isolation_level_in_begin() == True

    def test_supports_read_only_transaction(self, postgres_dialect):
        """Test PostgreSQL supports READ ONLY transactions."""
        assert postgres_dialect.supports_read_only_transaction() == True

    def test_supports_deferrable_transaction(self, postgres_dialect):
        """Test PostgreSQL supports DEFERRABLE transactions."""
        assert postgres_dialect.supports_deferrable_transaction() == True

    def test_supports_savepoint(self, postgres_dialect):
        """Test PostgreSQL supports savepoints."""
        assert postgres_dialect.supports_savepoint() == True
