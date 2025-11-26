# tests/rhosocial/activerecord_postgres_test/feature/backend/test_transaction_backend.py
import pytest


@pytest.fixture
def setup_test_table(postgres_backend):
    # For transactions, we need to manually handle commit/rollback, so turn autocommit off for the backend connection.
    original_autocommit_state = postgres_backend._connection.autocommit
    postgres_backend._connection.autocommit = False
    
    try:
        with postgres_backend.transaction():
            postgres_backend.execute("DROP TABLE IF EXISTS test_table")
            postgres_backend.execute("""
                CREATE TABLE test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    age INT
                )
            """)
        
        yield
        
        with postgres_backend.transaction():
            postgres_backend.execute("DROP TABLE IF EXISTS test_table")
    finally:
        # Restore autocommit state
        postgres_backend._connection.autocommit = original_autocommit_state


def test_transaction_commit(postgres_backend, setup_test_table):
    """Test transaction commit"""
    with postgres_backend.transaction():
        sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
        params = ("test", 20)
        postgres_backend.execute(sql, params)

    row = postgres_backend.fetch_one("SELECT * FROM test_table WHERE name = %s", ("test",))
    assert row is not None


def test_transaction_rollback(postgres_backend, setup_test_table):
    """Test transaction rollback"""
    try:
        with postgres_backend.transaction():
            sql = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
            params = ("test", 20)
            postgres_backend.execute(sql, params)
            raise Exception("Force rollback")
    except Exception:
        pass
    
    row = postgres_backend.fetch_one("SELECT * FROM test_table WHERE name = %s", ("test",))
    assert row is None


def test_nested_transaction(postgres_backend, setup_test_table):
    """Test nested transactions (savepoints)"""
    with postgres_backend.transaction():
        sql_outer = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
        params_outer = ("outer", 20)
        postgres_backend.execute(sql_outer, params_outer)

        with postgres_backend.transaction():
            sql_inner = "INSERT INTO test_table (name, age) VALUES (%s, %s)"
            params_inner = ("inner", 30)
            postgres_backend.execute(sql_inner, params_inner)

    rows = postgres_backend.fetch_all("SELECT * FROM test_table ORDER BY age")
    assert len(rows) == 2


def test_transaction_get_cursor(postgres_backend):
    """Test that _get_cursor can be called within a transaction context."""
    original_autocommit_state = postgres_backend._connection.autocommit
    postgres_backend._connection.autocommit = False
    try:
        with postgres_backend.transaction():
            cursor = postgres_backend._get_cursor()
            assert cursor is not None
    finally:
        postgres_backend._connection.autocommit = original_autocommit_state
