# tests/rhosocial/activerecord_postgres_test/feature/basic/conftest.py
"""
Pytest configuration for basic feature tests.

This file imports fixtures from the corresponding testsuite, making them
available to the tests in this directory. It overrides ``type_adapter_fixtures``
so the scenario name is passed to the provider. Without this, the sync provider
always creates the ``type_adapter_tests`` table on the first registered scenario,
which collides with the async provider when both scenarios run in parallel.
"""

import pytest

from rhosocial.activerecord.testsuite.feature.basic.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.feature.basic.conftest import (  # noqa: F401
    SCENARIO_PARAMS_SYNC,
    PROVIDER_KEY_SYNC,
)
from rhosocial.activerecord.testsuite.core.registry import get_provider_registry


@pytest.fixture(scope="function", params=SCENARIO_PARAMS_SYNC)
def type_adapter_fixtures(request):
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider(PROVIDER_KEY_SYNC)
    provider = provider_class()
    model = provider.setup_type_adapter_model_and_schema(scenario)
    yield model
    provider.cleanup_after_test(scenario)
