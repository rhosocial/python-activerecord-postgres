# tests/rhosocial/activerecord_postgres_test/feature/basic/core/conftest.py
"""L2 backend fixtures for the mapped-model basic tests.

The directory runs the shared ``basic`` mapped-model tests; the models they
request (``MappedUser``, ``MappedPost``, ``MappedComment``) read and write the
topic's ``users``, ``posts`` and ``comments`` tables, so the schema group is
exactly those three definitions.

- ``database``: one database per directory (session scope, emptied at start);
- ``schema``: those three tables, dropped and recreated for every test
  (function scope), so each test starts from empty tables.
"""

from pathlib import Path

import pytest

from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.testsuite.feature.basic.fixtures.models import (
    CommentDefinition,
    PostDefinition,
    UserDefinition,
)

DDL = ExecutionOptions(stmt_type=StatementType.DDL)

DEFINITIONS = (UserDefinition, PostDefinition, CommentDefinition)


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
    """Drop and recreate the tables this directory uses, before every test."""
    fixtures = [definition(database.dialect) for definition in DEFINITIONS]
    for fixture in fixtures:
        database.execute(*fixture.create_table.to_sql(), options=DDL)
    yield
    for fixture in reversed(fixtures):
        database.execute(*fixture.drop_table.to_sql(), options=DDL)
