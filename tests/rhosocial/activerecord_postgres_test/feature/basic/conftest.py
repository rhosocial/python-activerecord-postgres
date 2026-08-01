# tests/rhosocial/activerecord_postgres_test/feature/basic/conftest.py
"""
Pytest configuration for basic feature tests.

This file imports fixtures from the corresponding testsuite, making them
available to the tests in this directory.
"""

import pytest

from rhosocial.activerecord.testsuite.core.registry import get_provider_registry
from rhosocial.activerecord.testsuite.feature.basic.conftest import (  # noqa: F401
    SCENARIO_PARAMS_SYNC,
    type_adapter_fixtures as _original_type_adapter_fixtures,
    async_type_adapter_fixtures as _original_async_type_adapter_fixtures,
    PROVIDER_KEY_SYNC,
    PROVIDER_KEY_ASYNC,
)

# Override type_adapter_fixtures to pass scenario name to provider,
# ensuring each scenario uses its own database instance.
@pytest.fixture(scope="function", params=SCENARIO_PARAMS_SYNC)
def type_adapter_fixtures(request):
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider(PROVIDER_KEY_SYNC)
    provider = provider_class()
    model = provider.setup_type_adapter_model_and_schema(scenario)
    yield model
    provider.cleanup_after_test(scenario)

