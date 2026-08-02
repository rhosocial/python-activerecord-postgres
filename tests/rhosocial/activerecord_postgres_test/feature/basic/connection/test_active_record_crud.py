# tests/rhosocial/activerecord_postgres_test/feature/basic/connection/test_active_record_crud.py
"""
Basic ActiveRecord CRUD Test Module for PostgreSQL backend.

This module imports and runs the shared tests from the testsuite package,
ensuring PostgreSQL backend compatibility for connection pool CRUD operations.
"""
from rhosocial.activerecord.testsuite.feature.basic.connection.conftest import (
    sync_pool_for_crud,  # noqa: F401
    async_pool_for_crud,  # noqa: F401
)

# Import shared tests from testsuite package
from rhosocial.activerecord.testsuite.feature.basic.connection.test_active_record_crud import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.connection.test_active_record_crud_async import *  # noqa: F403

