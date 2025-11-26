# tests/rhosocial/activerecord_postgres_test/feature/basic/test_validation.py
"""
Basic Validation Test Module for postgres backend.

This module imports and runs the shared tests from the testsuite package,
ensuring postgres backend compatibility.
"""
# IMPORTANT: These imports are essential for pytest to work correctly.
# Even though they may be flagged as "unused" by some IDEs or linters,
# they must not be removed. They are the mechanism by which pytest discovers
# the fixtures and the tests from the external testsuite package.

# Although the root conftest.py sets up the environment, explicitly importing
# the fixtures here makes the dependency clear and can help with test discovery
# in some IDEs. These fixtures are defined in the testsuite package and are
# parameterized to run against the scenarios defined in `providers/scenarios.py`.
from rhosocial.activerecord.testsuite.feature.basic.conftest import (
    user_class,
    type_case_class,
    type_test_model,
    validated_user_class,
    validated_user
)

# Import shared tests from testsuite package
from rhosocial.activerecord.testsuite.feature.basic.test_validation import (
    validated_user_data,
    test_field_validation,
    test_business_rule_validation,
    test_validation_on_update,
    test_null_field_validation,
    test_multiple_validation_errors
)
