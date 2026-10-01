# tests/rhosocial/activerecord_postgres_test/feature/query/cross_schema/conftest.py
"""Cross-schema fixtures for PostgreSQL.

These live in this repository rather than the shared testsuite because schema
qualification is dialect-specific: PostgreSQL permits three-part references for
an unaliased range and alias-only references for an aliased one, while MySQL
qualifies by database, BigQuery never qualifies a column, and Snowflake nests
schema inside a database. A shared fixture set would assert the lowest common
denominator and stop catching the dialect-specific mistakes.

Provisioning is delegated to this repository's query provider
(``tests/providers/query.py``), which creates ``ar_crm`` alongside the default
namespace and performs teardown. These fixtures only bind models to the shared
backend.
"""

import pytest

from rhosocial.activerecord.testsuite.core.registry import get_provider_registry
from rhosocial.activerecord.testsuite.feature.query.conftest import (
    PROVIDER_KEY_ASYNC,
    PROVIDER_KEY_SYNC,
    SCENARIO_PARAMS_ASYNC,
    SCENARIO_PARAMS_SYNC,
)

from rhosocial.activerecord_postgres_test.feature.query.cross_schema.models import (
    MixedSchemaOrder,
)


def _provider_for(key):
    registry = get_provider_registry()
    provider_class = registry.get_provider(key)
    return provider_class() if provider_class else None


@pytest.fixture(scope="function", params=SCENARIO_PARAMS_SYNC)
def pg_mixed_schema(request):
    """(User, Order, MixedSchemaOrder): default orders plus orders in ``ar_crm``."""
    provider = _provider_for(PROVIDER_KEY_SYNC)
    if provider is None:
        pytest.skip("No testsuite scenarios found")
    setup = getattr(provider, "setup_mixed_schema_fixtures", None)
    if setup is None:
        pytest.skip("Provider does not provide mixed-schema fixtures")

    scenario = request.param
    user_model, order_model, _ = setup(scenario)
    # Bind the local subclass to the shared backend the provider wired up.
    MixedSchemaOrder.__connection_config__ = user_model.__connection_config__
    MixedSchemaOrder.__backend_class__ = user_model.__backend_class__
    MixedSchemaOrder.__backend__ = user_model.__backend__
    try:
        yield user_model, order_model, MixedSchemaOrder
    finally:
        cleanup = getattr(provider, "cleanup_after_test", None)
        if cleanup:
            cleanup(scenario)


@pytest.fixture(scope="function", params=SCENARIO_PARAMS_ASYNC)
def pg_async_mixed_schema(request):
    """Async variant of :func:`pg_mixed_schema`.

    Yields the scenario name so the test body can await its own model setup;
    provisioning itself is synchronous in this provider.
    """
    provider = _provider_for(PROVIDER_KEY_ASYNC)
    if provider is None:
        pytest.skip("No async testsuite scenarios found")
    if getattr(provider, "setup_mixed_schema_fixtures", None) is None:
        pytest.skip("Provider does not provide mixed-schema fixtures")
    yield request.param
