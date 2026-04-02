# tests/rhosocial/activerecord_postgres_test/feature/backend/test_connection_resilience.py
"""
Connection Resilience Tests

Tests for PostgreSQL backend's ability to handle connection loss scenarios:
1. Connection terminated by pg_terminate_backend()
2. Automatic reconnection via Plan A (pre-query check) and Plan B (error retry)
3. Manual ping reconnection

These tests verify the implementation of:
- Plan A: Pre-query connection check in _get_cursor()
- Plan B: Error retry mechanism in execute()

Both synchronous (PostgresBackend) and asynchronous (AsyncPostgresBackend) are tested.

Note: Unlike MySQL's is_connected() which actively checks connection state,
psycopg v3's closed/broken attributes only update after an operation fails.
Therefore, tests focus on actual query execution and reconnection behavior.
"""
import pytest
import pytest_asyncio
import asyncio
import time
import logging

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend, AsyncPostgresBackend


logger = logging.getLogger(__name__)


def print_separator(title: str):
    """Print a visual separator for test sections."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


# ============================================================================
# Synchronous Backend Tests
# ============================================================================


class TestConnectionStateAttributes:
    """Tests for the closed/broken attributes in various states."""

    def test_connection_not_closed_when_connected(self, postgres_backend_single: PostgresBackend):
        """Verify closed and broken are False for active connection."""
        assert postgres_backend_single._connection is not None
        assert postgres_backend_single._connection.closed is False
        assert postgres_backend_single._connection.broken is False

    def test_connection_closed_after_disconnect(self, postgres_backend_single: PostgresBackend):
        """Verify closed is True after explicit disconnect."""
        postgres_backend_single.disconnect()
        # After disconnect, _connection should be None
        assert postgres_backend_single._connection is None


class TestTerminateConnectionRecovery:
    """Tests for pg_terminate_backend() recovery via Plan A + Plan B."""

    def test_terminate_triggers_reconnection(
        self,
        postgres_backend_single: PostgresBackend,
        postgres_control_backend: PostgresBackend
    ):
        """Test that pg_terminate_backend() triggers automatic reconnection."""
        print_separator("Test: pg_terminate_backend Recovery")

        # 1. Get current connection PID
        result = postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        original_pid = result.data[0]['pid']
        print(f"Original connection PID: {original_pid}")

        # 2. Terminate the connection
        print(f"Terminating connection {original_pid}...")
        postgres_control_backend.execute(f"SELECT pg_terminate_backend({original_pid})")
        time.sleep(1)

        # 3. Execute query - should trigger reconnection (Plan A or Plan B)
        print("Executing query after connection terminated...")
        result = postgres_backend_single.execute("SELECT 1 AS test")
        print(f"Query succeeded: {result}")
        assert result.data[0]['test'] == 1

        # 4. Verify new connection PID
        new_pid_result = postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        new_pid = new_pid_result.data[0]['pid']
        print(f"New connection PID: {new_pid}")
        assert new_pid != original_pid

    def test_multiple_queries_after_reconnection(
        self,
        postgres_backend_single: PostgresBackend,
        postgres_control_backend: PostgresBackend
    ):
        """Test that multiple queries work correctly after reconnection."""
        print_separator("Test: Multiple Queries After Reconnection")

        # Terminate connection
        result = postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        original_pid = result.data[0]['pid']
        postgres_control_backend.execute(f"SELECT pg_terminate_backend({original_pid})")
        time.sleep(1)

        # Execute multiple queries
        for i in range(5):
            result = postgres_backend_single.execute(f"SELECT {i} AS value")
            assert result.data[0]['value'] == i
            print(f"Query {i + 1}/5 succeeded")


class TestIdleTimeoutRecovery:
    """Tests for idle_in_transaction_session_timeout recovery."""

    def test_idle_timeout_triggers_reconnection(
        self,
        postgres_backend_single: PostgresBackend,
        postgres_control_backend: PostgresBackend
    ):
        """
        Test that idle_in_transaction_session_timeout triggers automatic reconnection.

        Note: We test idle_in_transaction_session_timeout instead of statement_timeout
        because it's more relevant to connection pooling scenarios.
        """
        print_separator("Test: idle_in_transaction_session_timeout Recovery")

        # 1. Get original timeout
        result = postgres_backend_single.execute(
            "SHOW idle_in_transaction_session_timeout"
        )
        original_timeout = result.data[0]['idle_in_transaction_session_timeout']
        print(f"Original idle_in_transaction_session_timeout: {original_timeout}")

        # 2. Set short timeout (5 seconds) in a transaction
        test_timeout = "5s"
        print(f"Setting idle_in_transaction_session_timeout to {test_timeout}...")
        postgres_backend_single.execute(
            f"SET SESSION idle_in_transaction_session_timeout = '{test_timeout}'"
        )

        # Verify setting
        result = postgres_backend_single.execute(
            "SHOW idle_in_transaction_session_timeout"
        )
        current_timeout = result.data[0]['idle_in_transaction_session_timeout']
        print(f"Current timeout: {current_timeout}")

        # 3. Wait for timeout
        print("Waiting 7 seconds for timeout...")
        time.sleep(7)

        # 4. Execute query - should trigger reconnection
        print("Executing query after timeout...")
        try:
            result = postgres_backend_single.execute("SELECT 1 AS test")
            print(f"Query succeeded: {result}")
            assert result.data[0]['test'] == 1
        finally:
            # Restore original timeout via control backend
            try:
                postgres_control_backend.execute(
                    f"SET GLOBAL idle_in_transaction_session_timeout = '{original_timeout}'"
                )
            except Exception:
                pass  # Ignore cleanup errors


class TestPingReconnect:
    """Tests for manual ping reconnection."""

    def test_ping_reconnect_after_terminate(
        self,
        postgres_backend_single: PostgresBackend,
        postgres_control_backend: PostgresBackend
    ):
        """Test that ping(reconnect=True) can restore a terminated connection."""
        print_separator("Test: Ping Reconnect")

        # 1. Get current connection PID
        result = postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        original_pid = result.data[0]['pid']
        print(f"Original connection PID: {original_pid}")

        # 2. Terminate the connection
        print(f"Terminating connection {original_pid}...")
        postgres_control_backend.execute(f"SELECT pg_terminate_backend({original_pid})")
        time.sleep(1)

        # 3. Ping with reconnect - should succeed
        print("Calling ping(reconnect=True)...")
        ping_result = postgres_backend_single.ping(reconnect=True)
        print(f"Ping result: {ping_result}")
        assert ping_result is True, "Ping should succeed with reconnect=True"

        # 4. Verify connection is restored
        assert postgres_backend_single._connection is not None
        assert postgres_backend_single._connection.closed is False

        # 5. Verify new connection PID
        new_pid_result = postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        new_pid = new_pid_result.data[0]['pid']
        print(f"New connection PID: {new_pid}")
        assert new_pid != original_pid

    def test_ping_alive_connection_returns_true(self, postgres_backend_single: PostgresBackend):
        """Test that ping returns True for alive connections."""
        print_separator("Test: Ping Alive Connection")

        ping_result = postgres_backend_single.ping(reconnect=False)
        print(f"Ping result for alive connection: {ping_result}")
        assert ping_result is True


# ============================================================================
# Asynchronous Backend Tests
# ============================================================================


class TestAsyncConnectionStateAttributes:
    """Async tests for the closed/broken attributes in various states."""

    @pytest.mark.asyncio
    async def test_connection_not_closed_when_connected(self, async_postgres_backend_single: AsyncPostgresBackend):
        """Verify closed and broken are False for active connection."""
        assert async_postgres_backend_single._connection is not None
        assert async_postgres_backend_single._connection.closed is False
        assert async_postgres_backend_single._connection.broken is False

    @pytest.mark.asyncio
    async def test_connection_closed_after_disconnect(self, async_postgres_backend_single: AsyncPostgresBackend):
        """Verify closed is True after explicit disconnect."""
        await async_postgres_backend_single.disconnect()
        # After disconnect, _connection should be None
        assert async_postgres_backend_single._connection is None


class TestAsyncTerminateConnectionRecovery:
    """Async tests for pg_terminate_backend() recovery."""

    @pytest.mark.asyncio
    async def test_terminate_triggers_reconnection(
        self,
        async_postgres_backend_single: AsyncPostgresBackend,
        async_postgres_control_backend: AsyncPostgresBackend
    ):
        """Test that pg_terminate_backend() triggers automatic reconnection."""
        print_separator("Test: Async pg_terminate_backend Recovery")

        # 1. Get current connection PID
        result = await async_postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        original_pid = result.data[0]['pid']
        print(f"Original connection PID: {original_pid}")

        # 2. Terminate the connection
        print(f"Terminating connection {original_pid}...")
        await async_postgres_control_backend.execute(f"SELECT pg_terminate_backend({original_pid})")
        await asyncio.sleep(1)

        # 3. Execute query - should trigger reconnection
        print("Executing query after connection terminated...")
        result = await async_postgres_backend_single.execute("SELECT 1 AS test")
        print(f"Query succeeded: {result}")
        assert result.data[0]['test'] == 1

        # 4. Verify new connection PID
        new_pid_result = await async_postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        new_pid = new_pid_result.data[0]['pid']
        print(f"New connection PID: {new_pid}")
        assert new_pid != original_pid

    @pytest.mark.asyncio
    async def test_multiple_queries_after_reconnection(
        self,
        async_postgres_backend_single: AsyncPostgresBackend,
        async_postgres_control_backend: AsyncPostgresBackend
    ):
        """Test that multiple queries work correctly after reconnection."""
        print_separator("Test: Async Multiple Queries After Reconnection")

        # Terminate connection
        result = await async_postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        original_pid = result.data[0]['pid']
        await async_postgres_control_backend.execute(f"SELECT pg_terminate_backend({original_pid})")
        await asyncio.sleep(1)

        # Execute multiple queries
        for i in range(5):
            result = await async_postgres_backend_single.execute(f"SELECT {i} AS value")
            assert result.data[0]['value'] == i
            print(f"Query {i + 1}/5 succeeded")


class TestAsyncPingReconnect:
    """Async tests for manual ping reconnection."""

    @pytest.mark.asyncio
    async def test_ping_reconnect_after_terminate(
        self,
        async_postgres_backend_single: AsyncPostgresBackend,
        async_postgres_control_backend: AsyncPostgresBackend
    ):
        """Test that ping(reconnect=True) can restore a terminated connection."""
        print_separator("Test: Async Ping Reconnect")

        # 1. Get current connection PID
        result = await async_postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        original_pid = result.data[0]['pid']
        print(f"Original connection PID: {original_pid}")

        # 2. Terminate the connection
        print(f"Terminating connection {original_pid}...")
        await async_postgres_control_backend.execute(f"SELECT pg_terminate_backend({original_pid})")
        await asyncio.sleep(1)

        # 3. Ping with reconnect - should succeed
        print("Calling ping(reconnect=True)...")
        ping_result = await async_postgres_backend_single.ping(reconnect=True)
        print(f"Ping result: {ping_result}")
        assert ping_result is True, "Ping should succeed with reconnect=True"

        # 4. Verify connection is restored
        assert async_postgres_backend_single._connection.closed is False

        # 5. Verify new connection PID
        new_pid_result = await async_postgres_backend_single.execute("SELECT pg_backend_pid() AS pid")
        new_pid = new_pid_result.data[0]['pid']
        print(f"New connection PID: {new_pid}")
        assert new_pid != original_pid

    @pytest.mark.asyncio
    async def test_ping_alive_connection_returns_true(self, async_postgres_backend_single: AsyncPostgresBackend):
        """Test that ping returns True for alive connections."""
        print_separator("Test: Async Ping Alive Connection")

        ping_result = await async_postgres_backend_single.ping(reconnect=False)
        print(f"Ping result for alive connection: {ping_result}")
        assert ping_result is True
