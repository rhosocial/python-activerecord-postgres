# tests/rhosocial/activerecord_postgres_test/feature/query/connection/test_active_query_context.py
"""
ActiveQuery Context Test Module for PostgreSQL backend.

This module imports and runs the shared tests from the testsuite package,
ensuring PostgreSQL backend compatibility for ActiveQuery connection pool context awareness.
"""
from rhosocial.activerecord.testsuite.feature.query.connection.conftest import (
    sync_pool_and_model,  # noqa: F401
    async_pool_and_model,  # noqa: F401
)

# Import shared tests from testsuite package
from rhosocial.activerecord.testsuite.feature.query.connection.test_active_query_context import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.query.connection.test_active_query_context_async import *  # noqa: F403

