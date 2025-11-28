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

def test_nested_transaction_rollback_inner(postgres_backend, setup_test_table):
    """Test rolling back a nested transaction while committing the outer one."""
    with postgres_backend.transaction():
        # Outer transaction statement
        postgres_backend.execute("INSERT INTO test_table (name, age) VALUES (%s, %s)", ("outer", 40))

        try:
            with postgres_backend.transaction():
                # Inner transaction statement
                postgres_backend.execute("INSERT INTO test_table (name, age) VALUES (%s, %s)", ("inner", 50))
                raise ValueError("Rollback inner transaction")
        except ValueError:
            # Expected exception
            pass

    # The outer transaction should be committed, but the inner one rolled back.
    rows = postgres_backend.fetch_all("SELECT * FROM test_table ORDER BY name")
    assert len(rows) == 1
    assert rows[0]['name'] == "outer"


def test_nested_transaction_rollback_outer(postgres_backend, setup_test_table):
    """Test rolling back the outer transaction after a nested one completes."""
    try:
        with postgres_backend.transaction():
            # Outer transaction statement
            postgres_backend.execute("INSERT INTO test_table (name, age) VALUES (%s, %s)", ("outer", 60))

            with postgres_backend.transaction():
                # Inner transaction statement, this should be released to the outer savepoint
                postgres_backend.execute("INSERT INTO test_table (name, age) VALUES (%s, %s)", ("inner", 70))
            
            # This will cause the entire transaction, including the inner "committed" part, to be rolled back.
            raise ValueError("Rollback outer transaction")
    except ValueError:
        # Expected exception
        pass

    # Nothing should have been committed.
    count = postgres_backend.fetch_one("SELECT COUNT(*) FROM test_table")
    assert count['count'] == 0