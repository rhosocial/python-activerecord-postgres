# tests/rhosocial/activerecord_postgres_test/feature/rules/backend/__init__.py
"""Formal rule bridges for the postgres backend.

Each ``test_*`` module here re-exports one shared rule module from the
testsuite. Its assertions are driven by this package's ``conftest.py``
overrides, which point every fixture at the installed postgres implementation.
"""
