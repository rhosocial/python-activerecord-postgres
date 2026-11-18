# tests/rhosocial/activerecord_postgres_test/feature/basic/types_complex/conftest.py
"""Use the shared ``basic/types_complex`` test set, gated on PostGIS.

The shared fixture declares its geometry column through ``CustomType``, which
PostgreSQL renders verbatim: DDL *builds* even when PostGIS is absent, so the
shared capability gate (which only skips when DDL building fails) never fires
and ``CREATE TABLE`` raises ``type "geometry" does not exist`` on a plain
server. This backend bridge adds the missing capability gate locally:

* the shared ``schema`` fixture is autouse, so every test in the group builds
  the GEOMETRY table at setup. We override it as opt-in so declaration-only
  tests keep running on servers without PostGIS;
* the tests that actually build or use the table are skipped with a named
  reason when the scenario's server has no ``geometry`` type. The catalog is
  queried directly: a server can advertise PostGIS as installable while no
  database actually has the type, which the extension-name probe cannot tell.
"""

import re
from functools import lru_cache
from pathlib import Path

import pytest

import support.postgres
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType
from rhosocial.activerecord.testsuite.feature.basic.fixtures.operations import (
    create_schema,
    drop_schema,
)
from rhosocial.activerecord.testsuite.feature.basic.fixtures.types_complex_models import (
    COMPLEX_FIXTURES,
)
from rhosocial.activerecord.testsuite.feature.basic.types_complex.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.feature.basic.types_complex.conftest import (
    _unsupported_reason,
)

_SKIP_REASON = "geometry type not available on this server (PostGIS required)"

_GEOMETRY_TYPE_SQL = "SELECT 1 FROM pg_type WHERE typname = 'geometry' LIMIT 1"


@lru_cache(maxsize=None)
def _geometry_type_available(scenario_name: str) -> bool:
    """Whether the scenario's server actually has a ``geometry`` type.

    Ask the catalog, not the extension name: PostGIS can be advertised as
    installable while the type is missing from the database the tests run in.
    Probe ``template1`` because the per-directory test database is created
    from it with no explicit TEMPLATE, so the type's presence there decides
    whether the test tables can use it.
    """
    scenario = support.postgres.scenario(scenario_name)
    backend = scenario.backend_class(connection_config=scenario.make_config("template1"))
    try:
        backend.connect()
        try:
            result = backend.execute(
                _GEOMETRY_TYPE_SQL, (), options=ExecutionOptions(stmt_type=StatementType.DQL)
            )
            return bool(result.data)
        finally:
            backend.disconnect()
    except Exception:
        # A probe we could not perform is not evidence that the type is absent.
        # Leave the test to run so a real connection problem surfaces normally.
        return True


@pytest.fixture(scope="function")
def schema(database):
    """Opt-in override of the shared autouse fixture.

    Declaration-only tests must not build the GEOMETRY table at setup: on a
    server without PostGIS that would make them error before they can assert
    anything. Tests that need the tables request this fixture (directly or
    through the ``complex_*_class`` fixtures) as before.
    """
    why = _unsupported_reason(database.dialect)
    if why is not None:
        pytest.skip(why)
    create_schema(database, COMPLEX_FIXTURES)
    yield
    drop_schema(database, COMPLEX_FIXTURES)


def pytest_collection_modifyitems(config, items):
    """Skip the PostGIS-dependent tests on scenarios without PostGIS."""
    here = Path(__file__).resolve().parent
    for item in items:
        path = Path(str(item.fspath)).resolve()
        if here not in path.parents:
            continue
        # Only tests that build or use the tables request ``schema`` (directly
        # or through the complex_*_class fixtures). Declaration-only tests do
        # not, so they keep running on servers without PostGIS.
        if "schema" not in item.fixturenames:
            continue
        match = re.search(r"\[([^\]]+)\]\s*$", item.name)
        if match is None:
            # No scenario in the node id: let the test run rather than
            # guessing about a server we have not identified.
            continue
        if not _geometry_type_available(match.group(1)):
            item.add_marker(pytest.mark.skip(reason=_SKIP_REASON))
