# tests/rhosocial/activerecord_postgres_test/feature/query/connection/test_cte_query_context.py
"""
CTEQuery Context Test Module for PostgreSQL backend.

This module imports and runs the shared tests from the testsuite package,
ensuring PostgreSQL backend compatibility for CTEQuery connection pool context awareness.
"""
from rhosocial.activerecord.testsuite.feature.query.connection.conftest import (
    sync_pool_and_model,  # noqa: F401
    async_pool_and_model,  # noqa: F401
)

# Import shared tests from testsuite package
from rhosocial.activerecord.testsuite.feature.query.connection.test_cte_query_context import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.query.connection.test_cte_query_context_async import *  # noqa: F403

