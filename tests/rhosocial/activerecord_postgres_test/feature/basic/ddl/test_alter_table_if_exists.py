# tests/rhosocial/activerecord_postgres_test/feature/basic/ddl/test_alter_table_if_exists.py
"""
ALTER TABLE IF [NOT] EXISTS tests (sync) for the PostgreSQL backend.

Thin bridge that runs the shared testsuite contract against the PostgreSQL
dialect, which supports all three modifiers (PG >= 9.6).
"""

from rhosocial.activerecord.testsuite.feature.basic.ddl.test_alter_table_if_exists import *  # noqa: F403