# tests/rhosocial/activerecord_postgres_test/feature/basic/test_pydantic_native_validation.py
"""
Bridge file for Pydantic native validation tests from the shared testsuite.

This file imports the reusable testsuite tests so pytest discovers and runs them
against the PostgreSQL providers.
"""

# IMPORTANT: These imports are required for pytest discovery of testsuite
# fixtures and shared test classes. Do not remove them as unused imports.
from rhosocial.activerecord.testsuite.feature.basic.conftest import (
    async_pydantic_validated_model,  # noqa: F401
    pydantic_validated_model,  # noqa: F401
)

from rhosocial.activerecord.testsuite.feature.basic.validation.test_pydantic_native_validation import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.validation.test_pydantic_native_validation_async import *  # noqa: F403

