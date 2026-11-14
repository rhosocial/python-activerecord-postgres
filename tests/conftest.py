# tests/conftest.py
"""Root configuration for the PostgreSQL backend's test tree.

The backend knows its own classes and how a scenario maps to a connection, so
no declaration table is needed here: the scenario is exposed as a parameterized
pytest fixture that the testsuite environment consumes.
"""

import asyncio
import selectors
import sys

import pytest

import support.postgres

_SCENARIOS = [support.postgres.scenario(name) for name in support.postgres.SCENARIOS]


if sys.platform == "win32":

    def pytest_asyncio_loop_factories(config, item):
        """Give async tests the event loop psycopg's async mode requires.

        On Windows the platform default is the ProactorEventLoop, which does
        not implement the ``add_reader``/``add_writer`` methods psycopg
        waits on (psycopg/waiting.py), so every async backend test would
        fail at connect. A selector loop is what the driver can use, and
        this hook is how pytest-asyncio (1.x) is told to build one -- the
        supported replacement for the deprecated
        ``WindowsSelectorEventLoopPolicy``, which also had the drawback of
        switching the loop for the whole process.

        The hook exists only on Windows, and pytest-asyncio requires a
        non-empty mapping from it -- so it cannot answer "nothing" on other
        platforms to keep them untouched; not defining it is how that is
        said. Elsewhere the default loop is already selector-based, and the
        async tests' node ids stay exactly as they were.
        """
        return {"selector": lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())}


@pytest.fixture(scope="session", params=_SCENARIOS, ids=lambda scenario: scenario.name)
def backend_scenario(request):
    """Return the scenario the tests run against; its id is the scenario name."""
    return request.param
