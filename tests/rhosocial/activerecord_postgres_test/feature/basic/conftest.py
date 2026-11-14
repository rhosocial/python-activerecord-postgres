# tests/rhosocial/activerecord_postgres_test/feature/basic/conftest.py
"""
Pytest configuration for basic feature tests.

This file imports fixtures from the corresponding testsuite, making them
available to the tests in this directory. The testsuite fixture definitions
configure each model against the directory's logical database, so no
scenario-specific override is needed here.
"""

from rhosocial.activerecord.testsuite.feature.basic.conftest import *  # noqa: F403
