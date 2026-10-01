# tests/rhosocial/activerecord_postgres_test/feature/backend/test_search_path_param.py
"""Regression tests: ``search_path`` must reach the server, and compose with options.

``search_path`` is a PostgreSQL *server runtime parameter*, not a libpq
connection keyword. Handed to libpq as a keyword it fails the connect with
``invalid connection option "search_path"`` -- so before it was folded into
``options``, the documented way to resolve unqualified names could not be used
at all. That matters more than a merely ineffective setting: ``search_path``
is the only thing that decides where a model without ``__schema_name__``
resolves, since ``default_schema`` never affected generated SQL.

The unit tests pin the shape handed to the driver. The end-to-end test then
confirms the server really changed where an unqualified name lands, because a
correctly shaped ``options`` string that the server ignores would satisfy
everything above it.
"""

from typing import ClassVar, Optional
from unittest.mock import patch

import pytest

from rhosocial.activerecord.backend.impl.postgres.backend.async_backend import AsyncPostgresBackend
from rhosocial.activerecord.backend.impl.postgres.backend.backend import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


def _config(**kwargs):
    base = dict(
        host="db.example.com",
        port=5432,
        database="test_db",
        username="tester",
        password="secret",
    )
    base.update(kwargs)
    return PostgresConnectionConfig(**base)


def _driver_kwargs(config):
    """Run the real connect() with the driver stubbed; return what it built."""
    captured = {}

    class FakeConnection:
        autocommit = True

        def close(self):
            pass

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return FakeConnection()

    with patch("psycopg.connect", fake_connect):
        PostgresBackend(connection_config=config).connect()
    return captured


async def _async_driver_kwargs(config):
    """Same, for the async backend, whose connect() is a coroutine."""
    captured = {}

    class FakeAsyncConnection:
        async def set_autocommit(self, value):
            self.autocommit = value

        async def close(self):
            pass

    async def fake_connect(**kwargs):
        captured.update(kwargs)
        return FakeAsyncConnection()

    with patch(
        "rhosocial.activerecord.backend.impl.postgres.backend.async_backend.AsyncConnection.connect",
        fake_connect,
    ):
        backend = AsyncPostgresBackend(connection_config=config)
        await backend.connect()
    return captured


class TestSearchPathIsFoldedIntoOptions:
    def test_not_passed_as_a_connection_keyword(self):
        """The exact shape that used to make connecting impossible."""
        captured = _driver_kwargs(_config(search_path="tenant_a"))
        assert "search_path" not in captured, (
            "search_path has no libpq connection keyword; forwarded verbatim, "
            f"libpq rejects it: {captured}"
        )

    def test_arrives_inside_options(self):
        captured = _driver_kwargs(_config(search_path="tenant_a"))
        assert "-c search_path=tenant_a" in captured.get("options", "")

    def test_composes_with_dict_options(self):
        """A caller's own options must survive, not be overwritten."""
        captured = _driver_kwargs(
            _config(search_path="tenant_a", options={"statement_timeout": "30000"})
        )
        options = captured.get("options", "")
        assert "-c search_path=tenant_a" in options
        assert "-c statement_timeout=30000" in options

    def test_composes_with_string_options(self):
        captured = _driver_kwargs(
            _config(search_path="tenant_a", options="-c statement_timeout=30000")
        )
        options = captured.get("options", "")
        assert "-c search_path=tenant_a" in options
        assert "-c statement_timeout=30000" in options

    def test_accepts_a_search_path_list(self):
        captured = _driver_kwargs(_config(search_path="tenant_a, shared"))
        assert "-c search_path=tenant_a, shared" in captured.get("options", "")

    def test_absent_search_path_invents_nothing(self):
        assert not _driver_kwargs(_config()).get("options")

    @pytest.mark.asyncio
    async def test_async_backend_folds_it_the_same_way(self):
        """The async connect() had the same list and the same defect."""
        captured = await _async_driver_kwargs(_config(search_path="tenant_a"))
        assert "search_path" not in captured
        assert "-c search_path=tenant_a" in captured.get("options", "")


class TestGenuineConnectionKeywordsAreUntouched:
    def test_libpq_keywords_still_pass_through_verbatim(self):
        """Splitting the list must not swallow real connection keywords."""
        captured = _driver_kwargs(
            _config(search_path="tenant_a", application_name="probe", client_encoding="UTF8")
        )
        assert captured.get("application_name") == "probe"
        assert captured.get("client_encoding") == "UTF8"

    def test_default_schema_stays_inert(self):
        """It never reached the server and still must not."""
        captured = _driver_kwargs(_config(default_schema="tenant_a"))
        assert "default_schema" not in captured
        assert not captured.get("options")


@pytest.mark.parametrize("backend_cls", [PostgresBackend])
class TestAgainstServer:
    """The point of the field: where does an unqualified name actually land?"""

    def test_unqualified_model_follows_search_path(self, backend_cls):
        from rhosocial.activerecord.base.field_proxy import FieldProxy
        from rhosocial.activerecord.model import ActiveRecord
        from providers.scenarios import get_enabled_scenarios, get_scenario_raw

        scenarios = get_enabled_scenarios()
        if not scenarios:
            pytest.skip("no postgres scenario registered for this job")
        _, base = get_scenario_raw(next(iter(scenarios)))
        admin = backend_cls(connection_config=base)
        admin.connect()
        cur = admin._connection.cursor()
        try:
            cur.execute("DROP SCHEMA IF EXISTS ar_sp_probe CASCADE")
            cur.execute("CREATE SCHEMA ar_sp_probe")
            cur.execute("CREATE TABLE ar_sp_probe.sp_orders(id int)")
            cur.execute("INSERT INTO ar_sp_probe.sp_orders VALUES (1)")
            cur.execute("DROP TABLE IF EXISTS sp_orders")
            cur.execute("CREATE TABLE sp_orders(id int)")
            cur.execute("INSERT INTO sp_orders VALUES (2)")
        finally:
            cur.close()
            admin.disconnect()

        class Probe(ActiveRecord):
            __table_name__ = "sp_orders"
            c: ClassVar[FieldProxy] = FieldProxy()
            id: Optional[int] = None

        try:
            from dataclasses import replace

            scoped = backend_cls(
                connection_config=replace(base, search_path="ar_sp_probe")
            )
            scoped.connect()
            scoped.introspect_and_adapt()
            Probe.__backend__ = scoped
            Probe.__backend_class__ = backend_cls
            Probe.__connection_config__ = scoped.config

            cur = scoped._connection.cursor()
            cur.execute("SHOW search_path")
            assert cur.fetchone()[0] == "ar_sp_probe"
            cur.close()

            # No __schema_name__, so the range is unqualified and the server
            # decides -- which is the whole mechanism under test.
            sql, _ = Probe.query().select(Probe.c.id).to_sql()
            assert sql == 'SELECT "sp_orders"."id" FROM "sp_orders"'
            assert [r.id for r in Probe.query().all()] == [1]

            unscoped = backend_cls(connection_config=base)
            unscoped.connect()
            unscoped.introspect_and_adapt()
            Probe.__backend__ = unscoped
            Probe.__connection_config__ = unscoped.config
            assert [r.id for r in Probe.query().all()] == [2]
            unscoped.disconnect()
            scoped.disconnect()
        finally:
            admin2 = backend_cls(connection_config=base)
            admin2.connect()
            cur = admin2._connection.cursor()
            cur.execute("DROP SCHEMA IF EXISTS ar_sp_probe CASCADE")
            cur.execute("DROP TABLE IF EXISTS sp_orders")
            cur.close()
            admin2.disconnect()
