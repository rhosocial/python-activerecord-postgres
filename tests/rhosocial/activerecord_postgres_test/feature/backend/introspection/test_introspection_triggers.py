# tests/rhosocial/activerecord_postgres_test/feature/backend/introspection/test_introspection_triggers.py
"""
Tests for PostgreSQL trigger introspection functionality.

This module tests the list_triggers and get_trigger_info methods for retrieving trigger metadata.
"""

import pytest

from rhosocial.activerecord.backend.introspection.types import TriggerInfo

# SQL statements for trigger tests (individual statements, no executescript)
_TRIGGER_SQL = [
    "DROP TABLE IF EXISTS users CASCADE",
    """
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE OR REPLACE FUNCTION update_timestamp_func()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at := CURRENT_TIMESTAMP;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
    """,
    """
    CREATE TRIGGER update_user_timestamp
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE PROCEDURE update_timestamp_func()
    """,
]

_CLEANUP_TRIGGER_SQL = [
    "DROP TRIGGER IF EXISTS update_user_timestamp ON users",
    "DROP FUNCTION IF EXISTS update_timestamp_func()",
    "DROP TABLE IF EXISTS users CASCADE",
]


def _execute_sql_list(backend, sql_list):
    for sql in sql_list:
        backend.execute(sql)


class TestListTriggers:
    """Tests for list_triggers method."""

    def test_list_triggers_returns_list(self, postgres_backend_single):
        """Test that list_triggers returns a list."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("users")
            assert isinstance(triggers, list)
            assert len(triggers) > 0
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)

    def test_list_triggers_includes_expected_triggers(self, postgres_backend_single):
        """Test that list_triggers includes expected triggers."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("users")
            trigger_names = [t.name for t in triggers]
            assert "update_user_timestamp" in trigger_names
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)

    def test_list_triggers_nonexistent_table(self, postgres_backend_single):
        """Test list_triggers for non-existent table."""
        triggers = postgres_backend_single.introspector.list_triggers("nonexistent_table")

        assert isinstance(triggers, list)
        assert len(triggers) == 0

    def test_list_triggers_caching(self, postgres_backend_single):
        """Test that trigger list is cached."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers1 = postgres_backend_single.introspector.list_triggers("users")
            triggers2 = postgres_backend_single.introspector.list_triggers("users")
            assert triggers1 is triggers2
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)


class TestGetTriggerInfo:
    """Tests for get_trigger_info method."""

    def test_get_trigger_info_existing(self, postgres_backend_single):
        """Test get_trigger_info for existing trigger."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            trigger = postgres_backend_single.introspector.get_trigger_info(
                "update_user_timestamp"
            )
            assert trigger is not None
            assert isinstance(trigger, TriggerInfo)
            assert trigger.name == "update_user_timestamp"
            assert trigger.table_name == "users"
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)

    def test_get_trigger_info_nonexistent(self, postgres_backend_single):
        """Test get_trigger_info for non-existent trigger."""
        trigger = postgres_backend_single.introspector.get_trigger_info(
            "nonexistent_trigger"
        )
        assert trigger is None

    def test_get_trigger_info_nonexistent_table(self, postgres_backend_single):
        """Test get_trigger_info when no triggers exist for a schema."""
        trigger = postgres_backend_single.introspector.get_trigger_info(
            "some_trigger"
        )
        assert trigger is None


class TestTriggerInfoDetails:
    """Tests for TriggerInfo detail properties."""

    def test_trigger_timing(self, postgres_backend_single):
        """Test that trigger timing is correctly identified."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("users")
            for trigger in triggers:
                if trigger.name == "update_user_timestamp":
                    assert trigger.timing.upper() == "BEFORE"
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)

    def test_trigger_event(self, postgres_backend_single):
        """Test that trigger event is correctly identified."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("users")
            for trigger in triggers:
                if trigger.name == "update_user_timestamp":
                    assert "UPDATE" in [e.upper() for e in trigger.events]
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)

    def test_trigger_per_row(self, postgres_backend_single):
        """Test that trigger level is correctly identified as ROW."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("users")
            for trigger in triggers:
                if trigger.name == "update_user_timestamp":
                    assert trigger.level == "ROW"
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)

    def test_trigger_function(self, postgres_backend_single):
        """Test that trigger function name is stored in extra."""
        _execute_sql_list(postgres_backend_single, _TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("users")
            for trigger in triggers:
                if trigger.name == "update_user_timestamp":
                    assert trigger.extra.get("function_name") == "update_timestamp_func"
        finally:
            _execute_sql_list(postgres_backend_single, _CLEANUP_TRIGGER_SQL)


class TestAsyncTriggerIntrospection:
    """Async tests for trigger introspection."""

    @pytest.mark.asyncio
    async def test_async_list_triggers(self, async_postgres_backend_single):
        """Test async list_triggers returns trigger info."""
        for sql in _TRIGGER_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        try:
            triggers = await async_postgres_backend_single.introspector.list_triggers("users")
            assert isinstance(triggers, list)
            assert len(triggers) > 0
        finally:
            for sql in _CLEANUP_TRIGGER_SQL:
                await async_postgres_backend_single.execute(sql)

    @pytest.mark.asyncio
    async def test_async_get_trigger_info(self, async_postgres_backend_single):
        """Test async get_trigger_info for existing trigger."""
        for sql in _TRIGGER_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        try:
            trigger = await async_postgres_backend_single.introspector.get_trigger_info(
                "update_user_timestamp"
            )
            assert trigger is not None
            assert trigger.name == "update_user_timestamp"
        finally:
            for sql in _CLEANUP_TRIGGER_SQL:
                await async_postgres_backend_single.execute(sql)

    @pytest.mark.asyncio
    async def test_async_list_triggers_caching(self, async_postgres_backend_single):
        """Test that async trigger list is cached."""
        for sql in _TRIGGER_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        try:
            triggers1 = await async_postgres_backend_single.introspector.list_triggers("users")
            triggers2 = await async_postgres_backend_single.introspector.list_triggers("users")
            assert triggers1 is triggers2
        finally:
            for sql in _CLEANUP_TRIGGER_SQL:
                await async_postgres_backend_single.execute(sql)
