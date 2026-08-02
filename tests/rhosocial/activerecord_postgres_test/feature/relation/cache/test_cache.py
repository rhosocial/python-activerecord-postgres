"""
Test bridge for relation cache functionality.

This test file runs the relation cache tests from the testsuite using the PostgreSQL backend.
"""
import pytest  # noqa: F401


# Import the test classes from the testsuite
from rhosocial.activerecord.testsuite.feature.relation.cache.test_cache import TestRelationCache  # noqa: F401


# This will cause pytest to run all the tests in the imported class
# The tests will use the fixtures and providers configured in conftest.py
