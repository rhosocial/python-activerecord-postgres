# tests/providers/pooling.py
"""Database pooling helpers for the PostgreSQL test providers.

Under parallel (pytest-xdist) runs with a positive pool size the providers
reuse a per-worker pooled database ``{database}_{index}`` on the scenario's
PostgreSQL server instead of the shared scenario ``database`` schema, so
scenario variants of the same test can run concurrently on different workers
without conflicting. The pool name prefix is derived from the scenario's
configured ``database`` (the YAML ``database`` field), so e.g.
``database: test_db`` produces pooled databases ``test_db_0``, ``test_db_1``,
... Serial runs (no ``-n``) keep the previous behaviour: the provider connects
to the scenario's configured ``database``.

The scenario name selects the server (host/port); the pool index selects the
database name. The two are deliberately unrelated.
"""

import psycopg

from rhosocial.activerecord.testsuite.core.pool import (
    pooled_database_name,
    register_base_database,
    register_pool_reset_handler,
)

from .scenarios import SCENARIO_MAP, get_scenario_raw

# Derive each scenario's pooled-database base name from its configured
# ``database`` (YAML ``database`` field). Registered at import time so any
# caller of pooled_database_name() / resolve_database_name() resolves names
# consistent with the scenario configuration.
for _scenario_name, _scenario_config in SCENARIO_MAP.items():
    register_base_database(_scenario_name, _scenario_config["database"])


def resolve_database_name(scenario_name: str):
    """
    Return the pooled database name (e.g. ``test_db_3``) used by a test for
    the given scenario, or ``None`` when pooling is inactive (callers then fall
    back to the scenario's configured database).
    """
    return pooled_database_name(scenario_name)


def _escape_identifier(name: str) -> str:
    """Escape a PostgreSQL identifier for use inside double quotes."""
    return name.replace('"', '""')


def _connect_maintenance(config) -> psycopg.Connection:
    """Connect to a maintenance database (``postgres`` or the configured one)."""
    for dbname in ("postgres", config.database):
        try:
            return psycopg.connect(
                host=config.host,
                port=config.port,
                dbname=dbname,
                user=config.username,
                password=config.password,
                connect_timeout=10,
                autocommit=True,
            )
        except psycopg.Error:
            continue
    return None


def _reset_postgres_database(scenario_name: str, db_name: str) -> None:
    """Ensure the pooled database exists and is empty on the scenario's server.

    Connects to the server selected by ``scenario_name``, creates the pooled
    ``db_name`` database if missing, and drops all leftover non-extension
    tables so the test starts from a clean state. Extension-owned tables are
    preserved (e.g. PostGIS metadata). Errors are swallowed: a failed reset
    must not hide the underlying test failure.
    """
    if scenario_name not in SCENARIO_MAP:
        return
    _, config = get_scenario_raw(scenario_name)
    try:
        admin_conn = _connect_maintenance(config)
        if admin_conn is None:
            return
        try:
            with admin_conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                if cursor.fetchone() is None:
                    cursor.execute(f'CREATE DATABASE "{_escape_identifier(db_name)}"')
        finally:
            admin_conn.close()
    except Exception:
        return

    try:
        conn = psycopg.connect(
            host=config.host,
            port=config.port,
            dbname=db_name,
            user=config.username,
            password=config.password,
            connect_timeout=10,
            autocommit=True,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT n.nspname, c.relname
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r', 'p', 'm')
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                      AND NOT EXISTS (
                          SELECT 1 FROM pg_depend d
                          JOIN pg_extension e ON d.refobjid = e.oid
                          WHERE d.classid = 'pg_class'::regclass
                            AND d.objid = c.oid
                            AND d.deptype = 'e'
                      )
                    """
                )
                for schema, table in cursor.fetchall():
                    cursor.execute(
                        f'DROP TABLE IF EXISTS "{_escape_identifier(schema)}"."{_escape_identifier(table)}" CASCADE'
                    )
        finally:
            conn.close()
    except Exception:
        pass


register_pool_reset_handler(_reset_postgres_database)
