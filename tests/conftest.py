# tests/conftest.py
"""
This is the root pytest configuration file for the rhosocial-activerecord-postgres package's test suite.

Its primary responsibility is to configure the environment so that the external
`rhosocial-activerecord-testsuite` can find and use the backend-specific
implementations (Providers) defined within this project.
"""
import asyncio
import os
import sys

import pytest


def ensure_compatible_event_loop():
    if sys.platform == "win32":
        policy = asyncio.get_event_loop_policy()
        if not isinstance(policy, asyncio.WindowsSelectorEventLoopPolicy):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


ensure_compatible_event_loop()


# Set the environment variable that the testsuite uses to locate the provider registry.
# The testsuite is a generic package and doesn't know the specific location of the
# provider implementations for this backend (postgres). This environment variable
# acts as a bridge, pointing the testsuite to the correct import path.
#
# `setdefault` is used to ensure that this value is set only if it hasn't been
# set already, allowing for overrides in different environments if needed.
os.environ.setdefault(
    'TESTSUITE_PROVIDER_REGISTRY',
    'providers.registry:provider_registry'
)

# =============================================================================
# Scenario Parallel Scheduling Plugin
#
# Usage:
#   pytest --scenario-parallel -n <scenario_count> --dist=loadgroup tests/
#
# Design: All tests for the same scenario are pinned to the same xdist worker
#         (via nodeid @suffix), while tests for different scenarios run in
#         parallel. Behavior is unchanged without --scenario-parallel.
# =============================================================================


def _get_scenario_names():
    """Lazy import to avoid side effects during conftest loading."""
    try:
        from providers.scenarios import SCENARIO_MAP
        return set(SCENARIO_MAP.keys()), list(SCENARIO_MAP.keys())
    except Exception:
        return set(), []


def _extract_scenario_from_item(item, scenario_name_set):
    """Extract scenario name from item's callspec.

    Returns:
        str:  scenario name when exactly one scenario param is found
        None: no scenario params (not a scenario-parametrized test)
        list: multiple distinct scenario names (cross-scenario test)
    """
    callspec = getattr(item, 'callspec', None)
    if callspec is None:
        return None
    scenario_values = [
        v for v in callspec.params.values()
        if isinstance(v, str) and v in scenario_name_set
    ]
    if len(scenario_values) == 1:
        return scenario_values[0]
    if len(scenario_values) >= 2:
        return scenario_values  # cross-scenario: caller checks isinstance(result, list)
    return None


def _add_xdist_group_marker(item, group_name):
    """Append @group_name suffix to item._nodeid for loadgroup scheduling."""
    suffix = f"@{group_name}"
    if suffix not in item.nodeid:
        item._nodeid = item.nodeid + suffix


def pytest_addoption(parser):
    parser.addoption(
        '--scenario-parallel',
        action='store_true',
        default=False,
        help='Scenario parallel mode: distribute scenarios across workers, keep each scenario on one worker.',
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "xdist_group(name): specify the xdist group for a test (provided by pytest-xdist).",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption('--scenario-parallel', default=False):
        return

    scenario_name_set, scenario_name_list = _get_scenario_names()
    if not scenario_name_set:
        return

    scenario_items = []
    cross_scenario_items = []
    normal_items = []

    for item in items:
        result = _extract_scenario_from_item(item, scenario_name_set)
        if isinstance(result, list):
            item.add_marker(
                pytest.mark.skip(
                    reason="Cross-scenario test skipped in --scenario-parallel mode. "
                           "Run without --scenario-parallel for serial execution."
                )
            )
            cross_scenario_items.append(item)
        elif isinstance(result, str):
            _add_xdist_group_marker(item, result)
            scenario_items.append(item)
        else:
            normal_items.append(item)

    def sort_key(item):
        result = _extract_scenario_from_item(item, scenario_name_set)
        if not isinstance(result, str):
            return ('~', 0)
        try:
            scenario_idx = scenario_name_list.index(result)
        except ValueError:
            scenario_idx = 0
        base = item.nodeid.split('[')[0] if '[' in item.nodeid else item.nodeid
        return (base, scenario_idx)

    first_scenario = scenario_name_list[0]
    for item in normal_items:
        _add_xdist_group_marker(item, first_scenario)
    scenario_items.extend(normal_items)
    normal_items = []

    scenario_items.sort(key=sort_key)
    items[:] = scenario_items + normal_items + cross_scenario_items

    groups: dict = {}
    for item in scenario_items:
        sn = _extract_scenario_from_item(item, scenario_name_set)
        if isinstance(sn, str):
            base = item.nodeid.split('[')[0] if '[' in item.nodeid else item.nodeid
            groups.setdefault(base, set()).add(sn)

    print(f"\n[ScenarioParallel] {len(items)} items: {len(scenario_items)} scenario-param + "
          f"{len(normal_items)} normal + {len(cross_scenario_items)} cross-scenario (skipped)")
    print(f"[ScenarioParallel] {len(groups)} test methods, {len(scenario_name_list)} scenarios in parallel")
    print(f"[ScenarioParallel] Suggested: --dist=loadgroup -n {len(scenario_name_list)}")
