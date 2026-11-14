# tests/rhosocial/activerecord_postgres_test/feature/query/collation/conftest.py
"""L2 backend fixtures for the PostgreSQL-only collation test set.

These tests are backend-specific, so the fixture group lives here rather than
in the shared suite; the table definition still comes from the query topic.

- ``database``: one database per directory (session scope, emptied at start);
- ``schema``: the ``searchable_items`` table, dropped and recreated for every
  test (function scope), so each test starts from an empty table.
"""

from pathlib import Path

import pytest

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.testsuite.feature.query.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.feature.query.fixtures.definitions import (
    SearchableItemsDefinition,
)

DDL = ExecutionOptions(stmt_type=StatementType.DDL)

DEFINITIONS = (SearchableItemsDefinition,)


def _database_name() -> str:
    """Database name from this directory's namespace (below ``feature``)."""
    parts = Path(__file__).parent.parts
    start = parts.index("feature") + 1
    return "test_" + "_".join(parts[start:])


@pytest.fixture(scope="session")
def database_config(backend_scenario):
    """The scenario, its connection configuration, and the database name."""
    name = _database_name()
    return backend_scenario, backend_scenario.make_config(name), name


@pytest.fixture(scope="session")
def database(database_config):
    """The directory's database, prepared once and left for later files."""
    scenario, config, name = database_config
    scenario.reset_database(name)
    backend = scenario.backend_class(connection_config=config)
    backend.connect()
    backend.introspect_and_adapt()
    yield backend
    backend.disconnect()


@pytest.fixture(scope="function", autouse=True)
def schema(database):
    """Drop and recreate the table this directory uses, before every test."""
    fixtures = [definition(database.dialect) for definition in DEFINITIONS]
    for fixture in fixtures:
        database.execute(*fixture.create_table.to_sql(), options=DDL)
    yield
    for fixture in reversed(fixtures):
        database.execute(*fixture.drop_table.to_sql(), options=DDL)
