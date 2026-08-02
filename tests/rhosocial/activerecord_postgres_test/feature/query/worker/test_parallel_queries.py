# tests/rhosocial/activerecord_postgres_test/feature/query/worker/test_parallel_queries.py
"""
Bridge file for parallel queries worker tests.

Imports tests from testsuite and makes them discoverable by pytest.
"""
from rhosocial.activerecord.testsuite.feature.query.worker.conftest import (
    order_fixtures_for_worker,  # noqa: F401
    async_order_fixtures_for_worker,  # noqa: F401
)
from rhosocial.activerecord.testsuite.feature.query.worker.test_parallel_queries import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.query.worker.test_parallel_queries_async import *  # noqa: F403

