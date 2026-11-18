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
  reason when the scenario's server has no PostGIS, using the dialect's own
  ``is_extension_available`` probe.
"""

import re
from functools import lru_cache
from pathlib import Path

import pytest

import support.postgres
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

_SKIP_REASON = "PostGIS not available on this server"


@lru_cache(maxsize=None)
def _postgis_available(scenario_name: str) -> bool:
    """Ask the scenario's dialect whether PostGIS can be used on its server."""
    scenario = support.postgres.scenario(scenario_name)
    backend = scenario.backend_class(connection_config=scenario.make_config("postgres"))
    try:
        backend.connect()
        try:
            backend.introspect_and_adapt()
            return bool(backend.dialect.is_extension_available("postgis"))
        finally:
            backend.disconnect()
    except Exception:
        # A probe we could not perform is not evidence that PostGIS is absent.
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
        if not _postgis_available(match.group(1)):
            item.add_marker(pytest.mark.skip(reason=_SKIP_REASON))
