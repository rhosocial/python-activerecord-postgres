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


# SQL statements for testing multiple trigger timings (AFTER INSERT, AFTER UPDATE, AFTER DELETE)
_MULTI_TIMING_TRIGGER_SQL = [
    "DROP TABLE IF EXISTS audit_log CASCADE",
    "DROP TABLE IF EXISTS products CASCADE",
    """
    CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        price DECIMAL(10, 2) NOT NULL
    )
    """,
    """
    CREATE TABLE audit_log (
        id SERIAL PRIMARY KEY,
        table_name VARCHAR(50),
        action VARCHAR(20),
        record_id INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE OR REPLACE FUNCTION log_product_insert()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO audit_log (table_name, action, record_id)
        VALUES ('products', 'INSERT', NEW.id);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
    """,
    """
    CREATE OR REPLACE FUNCTION log_product_update()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO audit_log (table_name, action, record_id)
        VALUES ('products', 'UPDATE', NEW.id);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
    """,
    """
    CREATE OR REPLACE FUNCTION log_product_delete()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO audit_log (table_name, action, record_id)
        VALUES ('products', 'DELETE', OLD.id);
        RETURN OLD;
    END;
    $$ LANGUAGE plpgsql
    """,
    # AFTER INSERT trigger
    """
    CREATE TRIGGER trg_after_insert_product
    AFTER INSERT ON products
    FOR EACH ROW
    EXECUTE PROCEDURE log_product_insert()
    """,
    # AFTER UPDATE trigger
    """
    CREATE TRIGGER trg_after_update_product
    AFTER UPDATE ON products
    FOR EACH ROW
    EXECUTE PROCEDURE log_product_update()
    """,
    # AFTER DELETE trigger
    """
    CREATE TRIGGER trg_after_delete_product
    AFTER DELETE ON products
    FOR EACH ROW
    EXECUTE PROCEDURE log_product_delete()
    """,
]

_MULTI_TIMING_CLEANUP_SQL = [
    "DROP TRIGGER IF EXISTS trg_after_insert_product ON products",
    "DROP TRIGGER IF EXISTS trg_after_update_product ON products",
    "DROP TRIGGER IF EXISTS trg_after_delete_product ON products",
    "DROP FUNCTION IF EXISTS log_product_insert()",
    "DROP FUNCTION IF EXISTS log_product_update()",
    "DROP FUNCTION IF EXISTS log_product_delete()",
    "DROP TABLE IF EXISTS audit_log CASCADE",
    "DROP TABLE IF EXISTS products CASCADE",
]


# SQL statements for testing INSTEAD OF trigger on view
_INSTEAD_OF_TRIGGER_SQL = [
    "DROP VIEW IF EXISTS user_view CASCADE",
    "DROP TABLE IF EXISTS users_base CASCADE",
    """
    CREATE TABLE users_base (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100)
    )
    """,
    """
    CREATE VIEW user_view AS
    SELECT id, name, email FROM users_base
    """,
    """
    CREATE OR REPLACE FUNCTION instead_of_insert_user()
    RETURNS TRIGGER AS $$
    BEGIN
        INSERT INTO users_base (name, email)
        VALUES (NEW.name, NEW.email);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
    """,
    """
    CREATE TRIGGER trg_instead_of_insert
    INSTEAD OF INSERT ON user_view
    FOR EACH ROW
    EXECUTE PROCEDURE instead_of_insert_user()
    """,
]

_INSTEAD_OF_CLEANUP_SQL = [
    "DROP TRIGGER IF EXISTS trg_instead_of_insert ON user_view",
    "DROP FUNCTION IF EXISTS instead_of_insert_user()",
    "DROP VIEW IF EXISTS user_view CASCADE",
    "DROP TABLE IF EXISTS users_base CASCADE",
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


class TestTriggerTimingAfter:
    """Tests for AFTER trigger timing detection.

    This test class verifies that AFTER triggers are correctly identified
    for INSERT, UPDATE, and DELETE events. This is a regression test for
    a bug where the tgtype bitmask was incorrectly interpreted.

    The tgtype bitmask encoding:
    - Bit 0 (1): ROW level trigger
    - Bit 1 (2): BEFORE trigger
    - Bit 2 (4): INSERT event
    - Bit 3 (8): DELETE event
    - Bit 4 (16): UPDATE event
    - Bit 5 (32): TRUNCATE event
    - Bit 6 (64): INSTEAD OF trigger

    For timing: BEFORE has bit 1 set, INSTEAD OF has bit 6 set,
    and AFTER has neither (default case).
    """

    def test_after_insert_trigger_timing(self, postgres_backend_single):
        """Test that AFTER INSERT trigger timing is correctly identified."""
        _execute_sql_list(postgres_backend_single, _MULTI_TIMING_TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("products")
            trigger_map = {t.name: t for t in triggers}

            # AFTER INSERT trigger
            assert "trg_after_insert_product" in trigger_map
            trigger = trigger_map["trg_after_insert_product"]
            assert trigger.timing.upper() == "AFTER"
            assert "INSERT" in [e.upper() for e in trigger.events]
        finally:
            _execute_sql_list(postgres_backend_single, _MULTI_TIMING_CLEANUP_SQL)

    def test_after_update_trigger_timing(self, postgres_backend_single):
        """Test that AFTER UPDATE trigger timing is correctly identified."""
        _execute_sql_list(postgres_backend_single, _MULTI_TIMING_TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("products")
            trigger_map = {t.name: t for t in triggers}

            # AFTER UPDATE trigger
            assert "trg_after_update_product" in trigger_map
            trigger = trigger_map["trg_after_update_product"]
            assert trigger.timing.upper() == "AFTER"
            assert "UPDATE" in [e.upper() for e in trigger.events]
        finally:
            _execute_sql_list(postgres_backend_single, _MULTI_TIMING_CLEANUP_SQL)

    def test_after_delete_trigger_timing(self, postgres_backend_single):
        """Test that AFTER DELETE trigger timing is correctly identified."""
        _execute_sql_list(postgres_backend_single, _MULTI_TIMING_TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("products")
            trigger_map = {t.name: t for t in triggers}

            # AFTER DELETE trigger
            assert "trg_after_delete_product" in trigger_map
            trigger = trigger_map["trg_after_delete_product"]
            assert trigger.timing.upper() == "AFTER"
            assert "DELETE" in [e.upper() for e in trigger.events]
        finally:
            _execute_sql_list(postgres_backend_single, _MULTI_TIMING_CLEANUP_SQL)

    def test_all_after_triggers_listed(self, postgres_backend_single):
        """Test that all three AFTER triggers are correctly listed."""
        _execute_sql_list(postgres_backend_single, _MULTI_TIMING_TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("products")
            trigger_names = [t.name for t in triggers]

            # All three AFTER triggers should be present
            assert "trg_after_insert_product" in trigger_names
            assert "trg_after_update_product" in trigger_names
            assert "trg_after_delete_product" in trigger_names

            # All should have AFTER timing
            for trigger in triggers:
                assert trigger.timing.upper() == "AFTER"
        finally:
            _execute_sql_list(postgres_backend_single, _MULTI_TIMING_CLEANUP_SQL)


class TestTriggerTimingInsteadOf:
    """Tests for INSTEAD OF trigger timing detection.

    INSTEAD OF triggers are used on views and have bit 6 (64) set in tgtype.
    This tests that they are correctly identified.
    """

    def test_instead_of_trigger_timing(self, postgres_backend_single):
        """Test that INSTEAD OF trigger timing is correctly identified."""
        _execute_sql_list(postgres_backend_single, _INSTEAD_OF_TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            # Query triggers on the view (table_name = 'user_view')
            triggers = postgres_backend_single.introspector.list_triggers("user_view")

            assert len(triggers) > 0, "Should have at least one trigger on the view"

            trigger_map = {t.name: t for t in triggers}
            assert "trg_instead_of_insert" in trigger_map

            trigger = trigger_map["trg_instead_of_insert"]
            assert trigger.timing.upper() == "INSTEAD OF"
            assert "INSERT" in [e.upper() for e in trigger.events]
            assert trigger.level == "ROW"
        finally:
            _execute_sql_list(postgres_backend_single, _INSTEAD_OF_CLEANUP_SQL)

    def test_instead_of_trigger_on_view(self, postgres_backend_single):
        """Test that INSTEAD OF trigger is associated with the view."""
        _execute_sql_list(postgres_backend_single, _INSTEAD_OF_TRIGGER_SQL)
        postgres_backend_single.introspector.clear_cache()

        try:
            triggers = postgres_backend_single.introspector.list_triggers("user_view")

            for trigger in triggers:
                if trigger.name == "trg_instead_of_insert":
                    assert trigger.table_name == "user_view"
                    break
            else:
                pytest.fail("INSTEAD OF trigger not found")
        finally:
            _execute_sql_list(postgres_backend_single, _INSTEAD_OF_CLEANUP_SQL)


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


class TestAsyncTriggerTimingAfter:
    """Async tests for AFTER trigger timing detection."""

    @pytest.mark.asyncio
    async def test_async_after_insert_trigger_timing(self, async_postgres_backend_single):
        """Test that AFTER INSERT trigger timing is correctly identified."""
        for sql in _MULTI_TIMING_TRIGGER_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        try:
            triggers = await async_postgres_backend_single.introspector.list_triggers("products")
            trigger_map = {t.name: t for t in triggers}

            assert "trg_after_insert_product" in trigger_map
            trigger = trigger_map["trg_after_insert_product"]
            assert trigger.timing.upper() == "AFTER"
            assert "INSERT" in [e.upper() for e in trigger.events]
        finally:
            for sql in _MULTI_TIMING_CLEANUP_SQL:
                await async_postgres_backend_single.execute(sql)

    @pytest.mark.asyncio
    async def test_async_after_update_trigger_timing(self, async_postgres_backend_single):
        """Test that AFTER UPDATE trigger timing is correctly identified."""
        for sql in _MULTI_TIMING_TRIGGER_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        try:
            triggers = await async_postgres_backend_single.introspector.list_triggers("products")
            trigger_map = {t.name: t for t in triggers}

            assert "trg_after_update_product" in trigger_map
            trigger = trigger_map["trg_after_update_product"]
            assert trigger.timing.upper() == "AFTER"
            assert "UPDATE" in [e.upper() for e in trigger.events]
        finally:
            for sql in _MULTI_TIMING_CLEANUP_SQL:
                await async_postgres_backend_single.execute(sql)

    @pytest.mark.asyncio
    async def test_async_after_delete_trigger_timing(self, async_postgres_backend_single):
        """Test that AFTER DELETE trigger timing is correctly identified."""
        for sql in _MULTI_TIMING_TRIGGER_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        try:
            triggers = await async_postgres_backend_single.introspector.list_triggers("products")
            trigger_map = {t.name: t for t in triggers}

            assert "trg_after_delete_product" in trigger_map
            trigger = trigger_map["trg_after_delete_product"]
            assert trigger.timing.upper() == "AFTER"
            assert "DELETE" in [e.upper() for e in trigger.events]
        finally:
            for sql in _MULTI_TIMING_CLEANUP_SQL:
                await async_postgres_backend_single.execute(sql)


class TestAsyncTriggerTimingInsteadOf:
    """Async tests for INSTEAD OF trigger timing detection."""

    @pytest.mark.asyncio
    async def test_async_instead_of_trigger_timing(self, async_postgres_backend_single):
        """Test that INSTEAD OF trigger timing is correctly identified."""
        for sql in _INSTEAD_OF_TRIGGER_SQL:
            await async_postgres_backend_single.execute(sql)
        async_postgres_backend_single.introspector.clear_cache()

        try:
            triggers = await async_postgres_backend_single.introspector.list_triggers("user_view")

            assert len(triggers) > 0, "Should have at least one trigger on the view"

            trigger_map = {t.name: t for t in triggers}
            assert "trg_instead_of_insert" in trigger_map

            trigger = trigger_map["trg_instead_of_insert"]
            assert trigger.timing.upper() == "INSTEAD OF"
            assert "INSERT" in [e.upper() for e in trigger.events]
        finally:
            for sql in _INSTEAD_OF_CLEANUP_SQL:
                await async_postgres_backend_single.execute(sql)
