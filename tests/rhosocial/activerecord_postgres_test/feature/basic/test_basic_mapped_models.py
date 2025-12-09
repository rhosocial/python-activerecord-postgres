# tests/rhosocial/activerecord_postgres_test/feature/basic/test_basic_mapped_models.py
"""
This module serves as a bridge to execute the generic basic mapped models tests
defined in the `rhosocial-activerecord-testsuite` package against the PostgreSQL backend.

The tests are dynamically parametrized with PostgreSQL-specific fixtures.
"""

# IMPORTANT:
# The following imports are essential for the test suite to discover and run the tests.
# The fixtures are defined in the `conftest.py` of the testsuite and are implemented
# by the provider in the current backend (`tests/providers/basic.py`).

# The `basic_model_fixtures` is a fixture that provides the necessary setup for the tests.
from rhosocial.activerecord.testsuite.feature.basic.conftest import mapped_models_fixtures

# The `TestBasicMappedModels` class contains the actual test cases.
from rhosocial.activerecord.testsuite.feature.basic.test_example_basic_fixtures import *
