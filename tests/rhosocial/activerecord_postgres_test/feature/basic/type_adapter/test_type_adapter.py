"""
This is a "bridge" file for the basic features test group, specifically for
type adapter tests.

Its purpose is to import the generic tests from the `rhosocial-activerecord-testsuite`
package and make them discoverable by `pytest` within this project's test run.
"""

# Note: fixtures are provided by the conftest override at
# feature/basic/conftest.py (which passes the scenario name to the provider).
# Do NOT re-import type_adapter_fixtures here -- pytest would register the
# testsuite's original (scenario-ignoring) fixture with this module as the
# most specific baseid, shadowing the conftest override.

# Import all tests from the generic testsuite file.
from rhosocial.activerecord.testsuite.feature.basic.type_adapter.test_type_adapter import *  # noqa: F403
from rhosocial.activerecord.testsuite.feature.basic.type_adapter.test_type_adapter_async import *  # noqa: F403

