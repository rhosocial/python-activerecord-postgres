# tests/rhosocial/activerecord_postgres_test/feature/basic/worker/test_parallel_crud.py
"""
Bridge file for parallel CRUD worker tests.

Imports tests from testsuite and makes them discoverable by pytest.
"""
from rhosocial.activerecord.testsuite.feature.basic.worker.conftest import (
    user_class_for_worker,  # noqa: F401
    async_user_class_for_worker,  # noqa: F401
)
from rhosocial.activerecord.testsuite.feature.basic.worker.test_parallel_crud import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.worker.test_parallel_crud_async import *  # noqa: F403

