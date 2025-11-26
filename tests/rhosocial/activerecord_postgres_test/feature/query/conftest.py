# tests/rhosocial/activerecord_postgres_test/feature/query/conftest.py
"""
This file defines the pytest fixtures for the "query" feature test group.

Fixtures are a core concept in pytest. They are functions that provide a fixed
baseline state or data for tests. In this case, they provide fully configured
ActiveRecord model classes that are ready to be used in tests.

The key mechanism here is `pytest.fixture` parameterization. The fixtures are
parameterized by the list of "scenarios" provided by the backend's provider.
This causes pytest to run each test that uses one of these fixtures multiple times,
once for each database configuration (scenario) the backend supports.
"""
import pytest
from rhosocial.activerecord.testsuite.core.registry import get_provider_registry


def get_scenarios():
    """
    A helper function that runs during pytest's collection phase to discover
    all available test scenarios from the backend's registered provider.
    """
    # Dynamically get the registry and the provider for this test group.
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider("feature.query.IQueryProvider")
    if not provider_class:
        return []
    # Instantiate the provider and get the list of scenario names.
    return provider_class().get_test_scenarios()


# Discover the scenarios at module import time.
scenarios = get_scenarios()

# If no scenarios are found, create a single dummy parameter that will cause
# the tests to be skipped with a helpful message.
SCENARIO_PARAMS = scenarios if scenarios else [pytest.param("default", marks=pytest.mark.skip(reason="No testsuite scenarios found"))]


@pytest.fixture(scope="function", params=SCENARIO_PARAMS)
def order_fixtures(request):
    """
    A pytest fixture that provides configured (User, Order, OrderItem) model classes
    for testing complex queries with related tables.
    It is parameterized to run for each available scenario.
    """
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider("feature.query.IQueryProvider")
    provider = provider_class()

    # Get all required models for the test via fixture group
    User, Order, OrderItem = provider.setup_order_fixtures(scenario)

    yield (User, Order, OrderItem)

    # Cleanup after test
    provider.cleanup_after_test(scenario)


@pytest.fixture(scope="function", params=SCENARIO_PARAMS)
def blog_fixtures(request):
    """
    A pytest fixture that provides configured (User, Post, Comment) model classes
    for testing complex queries with related tables.
    It is parameterized to run for each available scenario.
    """
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider("feature.query.IQueryProvider")
    provider = provider_class()

    # Get all required models for the test via fixture group
    User, Post, Comment = provider.setup_blog_fixtures(scenario)

    yield (User, Post, Comment)

    # Cleanup after test
    provider.cleanup_after_test(scenario)


@pytest.fixture(scope="function", params=SCENARIO_PARAMS)
def json_user_fixture(request):
    """
    A pytest fixture that provides configured JsonUser model class
    for testing JSON functionality.
    It is parameterized to run for each available scenario.
    """
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider("feature.query.IQueryProvider")
    provider = provider_class()

    # Get JsonUser model for the test via fixture group
    (JsonUser,) = provider.setup_json_user_fixtures(scenario)

    yield JsonUser

    # Cleanup after test
    provider.cleanup_after_test(scenario)


@pytest.fixture(scope="function", params=SCENARIO_PARAMS)
def tree_fixtures(request):
    """
    A pytest fixture that provides configured Node model class
    for testing tree structure and recursive CTE functionality.
    It is parameterized to run for each available scenario.
    """
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider("feature.query.IQueryProvider")
    provider = provider_class()

    # Get Node model for the test via fixture group
    (Node,) = provider.setup_tree_fixtures(scenario)

    yield (Node,)

    # Cleanup after test
    provider.cleanup_after_test(scenario)


@pytest.fixture(scope="function", params=SCENARIO_PARAMS)
def combined_fixtures(request):
    """
    A pytest fixture that provides configured (User, Order, OrderItem, Post, Comment) model classes
    for testing complex queries with multiple related tables.
    It is parameterized to run for each available scenario.
    """
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider("feature.query.IQueryProvider")
    provider = provider_class()

    # Get all required models for the test via fixture group
    User, Order, OrderItem, Post, Comment = provider.setup_combined_fixtures(scenario)

    yield (User, Order, OrderItem, Post, Comment)

    # Cleanup after test
    provider.cleanup_after_test(scenario)


@pytest.fixture(scope="function", params=SCENARIO_PARAMS)
def extended_order_fixtures(request):
    """
    A pytest fixture that provides configured extended (User, ExtendedOrder, ExtendedOrderItem) model classes
    for testing advanced grouping functionality.
    It is parameterized to run for each available scenario.
    """
    scenario = request.param
    provider_registry = get_provider_registry()
    provider_class = provider_registry.get_provider("feature.query.IQueryProvider")
    provider = provider_class()

    # Get all required models for the test via fixture group
    User, ExtendedOrder, ExtendedOrderItem = provider.setup_extended_order_fixtures(scenario)

    yield (User, ExtendedOrder, ExtendedOrderItem)

    # Cleanup after test
    provider.cleanup_after_test(scenario)
